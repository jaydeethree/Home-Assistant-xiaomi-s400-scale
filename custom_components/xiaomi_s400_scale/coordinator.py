"""BLE coordinator for Xiaomi S400 Scale."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
import logging
import time

from bleak import BleakClient
from bleak.backends.device import BLEDevice
from bleak_retry_connector import BleakClientWithServiceCache, establish_connection

from homeassistant.components import bluetooth
from homeassistant.components.bluetooth.match import BluetoothCallbackMatcher
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import CALLBACK_TYPE, HomeAssistant, callback
from homeassistant.util import dt as dt_util

from .auth import AuthError, BadTokenError, NotifyHub, login
from .const import (
    RECONNECT_COOLDOWN_SECONDS,
    SETTLE_DELAY_SECONDS,
)
from .crypto import SessionKeys, decrypt_cmtp
from .protocol import (
    AVCTP,
    AVDTP,
    CMTP,
    RCV_OK,
    RCV_RDY,
    UPNP,
    VEND1A,
    VEND1C,
    parse_cmtp_payload,
)

logger = logging.getLogger(__name__)

type S400ConfigEntry = ConfigEntry[S400Coordinator]


@dataclass
class S400State:
    """Current state of the Xiaomi S400 Scale."""

    connected: bool = False
    live_weight_kg: float | None = None
    final_weight_kg: float | None = None
    stable: bool = False
    impedance_ohm: float | None = None
    impedance_low_ohm: float | None = None
    last_measurement: datetime | None = None


class S400Coordinator:
    """Event-driven BLE coordinator that connects when the scale advertises."""

    def __init__(
        self,
        hass: HomeAssistant,
        entry: S400ConfigEntry,
        address: str,
        token: bytes,
    ) -> None:
        self.hass = hass
        self.entry = entry
        self.address = address.upper()
        self.token = token
        self.state = S400State()

        self._listeners: list[Callable[[], None]] = []
        self._connect_lock = asyncio.Lock()
        self._active_task: asyncio.Task[None] | None = None
        self._unsub_bluetooth: CALLBACK_TYPE | None = None
        self._stopping = False
        self._auth_failed = False
        self._last_disconnect_mono: float = 0.0

    @callback
    def async_add_listener(self, update_callback: Callable[[], None]) -> Callable[[], None]:
        """Listen for state updates."""
        self._listeners.append(update_callback)

        @callback
        def _remove() -> None:
            if update_callback in self._listeners:
                self._listeners.remove(update_callback)

        return _remove

    @callback
    def _async_notify_listeners(self) -> None:
        for update_callback in list(self._listeners):
            update_callback()

    @callback
    def async_start(self) -> None:
        """Start listening for BLE advertisements from the scale."""
        self._stopping = False
        self._unsub_bluetooth = bluetooth.async_register_callback(
            self.hass,
            self._async_on_bluetooth_event,
            BluetoothCallbackMatcher(address=self.address, connectable=True),
            bluetooth.BluetoothScanningMode.ACTIVE,
        )
        # If the scale is already awake and advertising right now, connect immediately
        ble_device = bluetooth.async_ble_device_from_address(
            self.hass, self.address, connectable=True
        )
        if ble_device is not None:
            self._schedule_connect(ble_device)

    async def async_stop(self) -> None:
        """Stop BLE listener and disconnect if connected."""
        self._stopping = True
        if self._unsub_bluetooth is not None:
            self._unsub_bluetooth()
            self._unsub_bluetooth = None
        if self._active_task is not None and not self._active_task.done():
            self._active_task.cancel()
            try:
                await self._active_task
            except (asyncio.CancelledError, Exception):
                pass
        self._active_task = None

    @callback
    def _async_on_bluetooth_event(
        self,
        service_info: bluetooth.BluetoothServiceInfoBleak,
        change: bluetooth.BluetoothChange,
    ) -> None:
        """Handle an advertisement from the scale."""
        if self._stopping or self._auth_failed:
            return
        if self._connect_lock.locked():
            return
        if (time.monotonic() - self._last_disconnect_mono) < RECONNECT_COOLDOWN_SECONDS:
            return
        self._schedule_connect(service_info.device)

    @callback
    def _schedule_connect(self, ble_device: BLEDevice) -> None:
        if self._active_task is not None and not self._active_task.done():
            return
        self._active_task = self.entry.async_create_background_task(
            self.hass,
            self._async_connect_and_stream(ble_device),
            name=f"xiaomi_s400_scale_{self.address}",
        )

    async def _async_connect_and_stream(self, ble_device: BLEDevice) -> None:
        if self._connect_lock.locked():
            return
        async with self._connect_lock:
            disconnected_ev = asyncio.Event()

            def _on_disconnect(_client: BleakClient) -> None:
                disconnected_ev.set()

            client: BleakClientWithServiceCache | None = None
            try:
                logger.debug("Connecting to Xiaomi S400 Scale (%s)...", self.address)
                client = await establish_connection(
                    BleakClientWithServiceCache,
                    ble_device,
                    self.address,
                    disconnected_callback=_on_disconnect,
                    max_attempts=2,
                )
                hub = NotifyHub()
                for uuid in (UPNP, AVDTP, AVCTP, VEND1A, CMTP, VEND1C):
                    try:
                        await client.start_notify(uuid, hub.make_callback(uuid))
                    except Exception as exc:
                        logger.debug("Subscribe %s skipped: %s", uuid[:8], exc)

                await asyncio.sleep(SETTLE_DELAY_SECONDS)
                keys = await login(client, self.token, hub)
                logger.info("Authenticated with Xiaomi S400 Scale (%s)", self.address)

                self.state.connected = True
                self._async_notify_listeners()

                await self._cmtp_pump(client, keys, hub, disconnected_ev)

            except BadTokenError as exc:
                logger.error(
                    "BLE login token rejected by scale %s (%s). Triggering reauth.",
                    self.address,
                    exc,
                )
                self._auth_failed = True
                self.entry.async_start_reauth(self.hass)
            except AuthError as exc:
                logger.warning("BLE handshake failed on %s: %s", self.address, exc)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug("BLE connection ended for %s: %s", self.address, exc)
            finally:
                self._last_disconnect_mono = time.monotonic()
                if client is not None and client.is_connected:
                    try:
                        await client.disconnect()
                    except Exception as exc:
                        logger.debug("Disconnect cleanup error on %s: %s", self.address, exc)
                if (
                    self.state.connected
                    or self.state.stable
                    or self.state.live_weight_kg is not None
                ):
                    self.state.connected = False
                    self.state.stable = False
                    self.state.live_weight_kg = None
                    self._async_notify_listeners()

    async def _cmtp_pump(
        self,
        client: BleakClient,
        keys: SessionKeys,
        hub: NotifyHub,
        disconnected_ev: asyncio.Event,
    ) -> None:
        """Drain CMTP notifications, reassemble multi-frame messages, decrypt, and update state."""
        q = hub.queue(CMTP)
        expected = 0
        next_frame = 1
        buf = b""
        while not self._stopping and not disconnected_ev.is_set():
            try:
                data = await asyncio.wait_for(q.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            # Header: 00 00 00 SS LL_LO LL_HI
            if len(data) >= 6 and data[:3] == b"\x00\x00\x00":
                expected = data[4] | (data[5] << 8)
                next_frame = 1
                buf = b""
                await client.write_gatt_char(CMTP, RCV_RDY, response=False)
                continue

            # Data frame: NN_LO NN_HI <payload>
            if len(data) >= 2 and expected > 0:
                frame_num = data[0] | (data[1] << 8)
                if frame_num != next_frame:
                    logger.debug(
                        "CMTP frame out of sequence on %s: expected %d, got %d; resetting buffer",
                        self.address,
                        next_frame,
                        frame_num,
                    )
                    await client.write_gatt_char(CMTP, RCV_OK, response=False)
                    expected = 0
                    next_frame = 1
                    buf = b""
                    continue
                buf += data[2:]
                next_frame += 1
                if frame_num >= expected:
                    await self._handle_full_frame(keys, buf)
                    await client.write_gatt_char(CMTP, RCV_OK, response=False)
                    expected = 0
                    next_frame = 1
                    buf = b""

    async def _handle_full_frame(self, keys: SessionKeys, ciphertext: bytes) -> None:
        pt = decrypt_cmtp(keys, ciphertext)
        if pt is None:
            logger.debug("CMTP decrypt failed on %s: %s", self.address, ciphertext.hex())
            return
        event = parse_cmtp_payload(pt)
        if event is None:
            return

        if event.type == "live":
            if event.weight_kg is not None:
                self.state.live_weight_kg = event.weight_kg
            self.state.stable = event.stable
            self._async_notify_listeners()

        elif event.type == "final":
            if event.weight_kg is not None:
                self.state.live_weight_kg = event.weight_kg
                self.state.final_weight_kg = event.weight_kg
            self.state.stable = event.stable
            self.state.impedance_ohm = (
                event.impedance_ohm
                if (event.impedance_ohm and event.impedance_ohm > 0)
                else None
            )
            self.state.impedance_low_ohm = (
                event.impedance_low_ohm
                if (event.impedance_low_ohm and event.impedance_low_ohm > 0)
                else None
            )
            self.state.last_measurement = dt_util.utcnow()

            self._async_notify_listeners()
