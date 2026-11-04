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
    CMTP_IDLE_TIMEOUT_SECONDS,
    MAX_RECONNECT_COOLDOWN_SECONDS,
    RECONNECT_COOLDOWN_SECONDS,
    SETTLE_DELAY_SECONDS,
)
from .crypto import SessionKeys, decrypt_cmtp
from .protocol import (
    AVCTP,
    AVDTP,
    CMTP,
    RCV_OK,
    UPNP,
    VEND1A,
    VEND1C,
    CmtpReassembler,
    FrameSequenceError,
    parse_cmtp_payload,
)

logger = logging.getLogger(__name__)

type S400ConfigEntry = ConfigEntry[S400Coordinator]

MAX_CONSECUTIVE_BAD_TOKENS = 3
REQUIRED_NOTIFY_UUIDS = (UPNP, AVDTP, CMTP)
OPTIONAL_NOTIFY_UUIDS = (AVCTP, VEND1A, VEND1C)


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
        self._active_task: asyncio.Task[None] | None = None
        self._unsub_bluetooth: CALLBACK_TYPE | None = None
        self._stopping = False
        self._auth_failed = False
        self._consecutive_bad_tokens = 0
        self._consecutive_failures = 0
        self._last_error: str | None = None
        self._last_connect_time: datetime | None = None
        self._last_disconnect_mono: float = 0.0

    @property
    def auth_failed(self) -> bool:
        """Return True if authentication was permanently halted pending reauth."""
        return self._auth_failed

    @property
    def connecting(self) -> bool:
        """Return True if a BLE session task is currently active."""
        return self._active_task is not None and not self._active_task.done()

    @property
    def consecutive_failures(self) -> int:
        """Return the number of consecutive connection/auth failures."""
        return self._consecutive_failures

    @property
    def last_error(self) -> str | None:
        """Return the last connection/auth error message, if any."""
        return self._last_error

    @property
    def last_connect_time(self) -> datetime | None:
        """Return the UTC timestamp of the last successful authentication."""
        return self._last_connect_time

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
        if self._stopping or self._auth_failed or self.connecting:
            return
        cooldown = min(
            RECONNECT_COOLDOWN_SECONDS * (2**self._consecutive_failures),
            MAX_RECONNECT_COOLDOWN_SECONDS,
        )
        if (time.monotonic() - self._last_disconnect_mono) < cooldown:
            return
        self._schedule_connect(service_info.device)

    @callback
    def _schedule_connect(self, ble_device: BLEDevice) -> None:
        if self.connecting:
            return
        self._active_task = self.entry.async_create_background_task(
            self.hass,
            self._async_connect_and_stream(ble_device),
            name=f"xiaomi_s400_scale_{self.address}",
        )

    def _record_failure(self, message: str) -> None:
        """Record a connection or handshake failure and log with rate limiting."""
        self._consecutive_failures += 1
        self._last_error = message
        log_fn = logger.warning if self._consecutive_failures == 1 else logger.debug
        log_fn(
            "BLE session failed on %s (attempt %d): %s",
            self.address,
            self._consecutive_failures,
            message,
        )

    async def _async_connect_and_stream(self, ble_device: BLEDevice) -> None:
        hub = NotifyHub()

        def _on_disconnect(_client: BleakClient) -> None:
            hub.queue(CMTP).put_nowait(None)  # type: ignore[arg-type]

        client: BleakClientWithServiceCache | None = None
        authenticated = False
        try:
            logger.debug("Connecting to Xiaomi S400 Scale (%s)...", self.address)
            client = await establish_connection(
                BleakClientWithServiceCache,
                ble_device,
                self.address,
                disconnected_callback=_on_disconnect,
                max_attempts=2,
                ble_device_callback=lambda: bluetooth.async_ble_device_from_address(
                    self.hass, self.address, connectable=True
                ),
            )
            for uuid in REQUIRED_NOTIFY_UUIDS:
                await client.start_notify(uuid, hub.make_callback(uuid))
            for uuid in OPTIONAL_NOTIFY_UUIDS:
                try:
                    await client.start_notify(uuid, hub.make_callback(uuid))
                except Exception as exc:
                    logger.debug("Optional subscribe %s skipped: %s", uuid[:8], exc)

            await asyncio.sleep(SETTLE_DELAY_SECONDS)
            keys = await login(client, self.token, hub)
            authenticated = True
            logger.debug("Authenticated with Xiaomi S400 Scale (%s)", self.address)

            self._consecutive_bad_tokens = 0
            self._consecutive_failures = 0
            self._last_error = None
            self._last_connect_time = dt_util.utcnow()
            self.state.connected = True
            self._async_notify_listeners()

            await self._cmtp_pump(client, keys, hub)

        except BadTokenError as exc:
            self._consecutive_bad_tokens += 1
            self._consecutive_failures += 1
            self._last_error = str(exc)
            if self._consecutive_bad_tokens >= MAX_CONSECUTIVE_BAD_TOKENS:
                logger.error(
                    "BLE login token rejected %d times in a row by scale %s (%s). Triggering reauth.",
                    self._consecutive_bad_tokens,
                    self.address,
                    exc,
                )
                self._auth_failed = True
                self._async_notify_listeners()
                self.entry.async_start_reauth(self.hass)
            else:
                logger.warning(
                    "BLE login HMAC mismatch on %s (%d/%d before reauth): %s",
                    self.address,
                    self._consecutive_bad_tokens,
                    MAX_CONSECUTIVE_BAD_TOKENS,
                    exc,
                )
        except AuthError as exc:
            self._record_failure(str(exc))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if authenticated:
                logger.debug("BLE session ended on %s: %s", self.address, exc)
            else:
                self._record_failure(f"connection error: {exc}")
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
    ) -> None:
        """Drain CMTP notifications, reassemble multi-frame messages, decrypt, and update state."""
        q: asyncio.Queue[bytes | None] = hub.queue(CMTP)  # type: ignore[assignment]
        reassembler = CmtpReassembler()
        while True:
            try:
                async with asyncio.timeout(CMTP_IDLE_TIMEOUT_SECONDS):
                    data = await q.get()
            except TimeoutError:
                logger.debug(
                    "CMTP stream idle for %ss on %s; ending session",
                    CMTP_IDLE_TIMEOUT_SECONDS,
                    self.address,
                )
                break
            if data is None:
                break
            try:
                ack, msg = reassembler.feed(data)
            except FrameSequenceError as exc:
                logger.debug("CMTP %s on %s; resetting buffer", exc, self.address)
                await client.write_gatt_char(CMTP, RCV_OK, response=False)
                continue
            if msg is not None:
                await self._handle_full_frame(keys, msg)
            if ack is not None:
                await client.write_gatt_char(CMTP, ack, response=False)

    async def _handle_full_frame(self, keys: SessionKeys, ciphertext: bytes) -> None:
        pt = decrypt_cmtp(keys, ciphertext)
        if pt is None:
            logger.debug("CMTP decrypt failed on %s: %s", self.address, ciphertext.hex())
            return
        event = parse_cmtp_payload(pt)
        if event is None:
            logger.debug("Unparsed CMTP payload on %s: %s", self.address, pt.hex())
            return

        if event.type == "live":
            if event.weight_kg is not None:
                self.state.live_weight_kg = event.weight_kg
            self.state.stable = event.stable
            self._async_notify_listeners()

        elif event.type == "final":
            if (
                event.weight_kg is None
                or event.weight_kg <= 0
                or not event.stable
            ):
                logger.debug(
                    "Ignoring incomplete/unstable final frame on %s: %s",
                    self.address,
                    event,
                )
                return
            self.state.live_weight_kg = event.weight_kg
            self.state.final_weight_kg = event.weight_kg
            self.state.stable = True
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
