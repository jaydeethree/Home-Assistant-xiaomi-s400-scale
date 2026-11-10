"""BLE coordinator for Xiaomi S400 Scale."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
import contextlib
from dataclasses import dataclass
from datetime import datetime
import logging
import time

from bleak import BleakClient
from bleak.backends.device import BLEDevice
from bleak.exc import BleakError
from bleak_retry_connector import (
    BleakClientWithServiceCache,
    BleakOutOfConnectionSlotsError,
    establish_connection,
)

from homeassistant.components import bluetooth
from homeassistant.components.bluetooth.match import BluetoothCallbackMatcher
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import CALLBACK_TYPE, HomeAssistant, callback
from homeassistant.helpers.event import async_call_later
from homeassistant.helpers.start import async_at_started
from homeassistant.util import dt as dt_util

from .auth import AuthError, BadTokenError, NotifyHub, login
from .const import (
    ACTIVE_ADVERTISEMENT_SECONDS,
    CMTP_IDLE_TIMEOUT_SECONDS,
    FAILURE_RESET_SECONDS,
    MAX_RECONNECT_COOLDOWN_SECONDS,
    POST_FINAL_GRACE_SECONDS,
    RECENT_ADVERTISEMENT_SECONDS,
    RECONNECT_COOLDOWN_SECONDS,
    SETTLE_DELAY_SECONDS,
    STARTUP_SCANNER_CHECK_DELAY_SECONDS,
)
from .crypto import SessionKeys, decrypt_cmtp
from .protocol import (
    AVDTP,
    CMTP,
    RCV_OK,
    UPNP,
    FrameSequenceError,
    ParcelReassembler,
    parse_cmtp_payload,
)

_LOGGER = logging.getLogger(__name__)

type S400ConfigEntry = ConfigEntry[S400Coordinator]

MAX_CONSECUTIVE_BAD_TOKENS = 3
WARN_AFTER_CONSECUTIVE_FAILURES = 3
REQUIRED_NOTIFY_UUIDS = (UPNP, AVDTP, CMTP)


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
    """Event-driven BLE coordinator that maintains an authenticated session with the scale."""

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
        self._unsub_adv: CALLBACK_TYPE | None = None
        self._unsub_bluetooth: CALLBACK_TYPE | None = None
        self._cancel_cooldown_retry: CALLBACK_TYPE | None = None
        self._cancel_scanner_check: CALLBACK_TYPE | None = None
        self._stopping = False
        self._auth_failed = False
        self._consecutive_bad_tokens = 0
        self._consecutive_failures = 0
        self._warned_failure_streak = False
        self._last_failure_mono: float = 0.0
        self._last_service_info: bluetooth.BluetoothServiceInfoBleak | None = None
        self._weigh_in_in_progress = False
        self._awaiting_step_off = False
        self._last_error: str | None = None
        self._last_connect_time: datetime | None = None
        self._cooldown_until_mono: float = 0.0

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

    @property
    def cooldown_remaining(self) -> float:
        """Return seconds remaining in the current reconnect cooldown window."""
        return max(0.0, self._cooldown_until_mono - time.monotonic())

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
    def _async_get_ble_device(self, fallback: BLEDevice) -> BLEDevice:
        """Return the BLEDevice from the best available scanner, or fallback."""
        return (
            bluetooth.async_ble_device_from_address(
                self.hass, self.address, connectable=True
            )
            or fallback
        )

    @callback
    def async_start(self) -> None:
        """Start listening for BLE advertisements from the scale."""
        self._stopping = False

        @callback
        def _schedule_scanner_check(_hass: HomeAssistant) -> None:
            self._cancel_scanner_check = async_call_later(
                self.hass,
                STARTUP_SCANNER_CHECK_DELAY_SECONDS,
                self._async_check_connectable_scanners,
            )

        unsub_started = async_at_started(self.hass, _schedule_scanner_check)
        if self._cancel_scanner_check is None:
            self._cancel_scanner_check = unsub_started
        self._unsub_adv = bluetooth.async_register_advertisement_callback(
            self.hass,
            self._async_on_advertisement,
            self.address,
        )
        self._unsub_bluetooth = bluetooth.async_register_callback(
            self.hass,
            self._async_on_bluetooth_event,
            BluetoothCallbackMatcher(address=self.address, connectable=True),
            bluetooth.BluetoothScanningMode.PASSIVE,
        )

    @callback
    def _async_check_connectable_scanners(self, _now: datetime) -> None:
        """Warn once after startup if no connectable Bluetooth scanner is available."""
        self._cancel_scanner_check = None
        if self._stopping:
            return
        if bluetooth.async_scanner_count(self.hass, connectable=True) == 0:
            _LOGGER.warning(
                "No connectable Bluetooth adapters or proxies are available for %s (%s)",
                self.entry.title,
                self.address,
            )

    async def async_stop(self) -> None:
        """Stop BLE listener and disconnect if connected."""
        self._stopping = True
        if self._cancel_scanner_check is not None:
            self._cancel_scanner_check()
            self._cancel_scanner_check = None
        if self._cancel_cooldown_retry is not None:
            self._cancel_cooldown_retry()
            self._cancel_cooldown_retry = None
        if self._unsub_adv is not None:
            self._unsub_adv()
            self._unsub_adv = None
        if self._unsub_bluetooth is not None:
            self._unsub_bluetooth()
            self._unsub_bluetooth = None
        if self._active_task is not None and not self._active_task.done():
            self._active_task.cancel()
            try:
                await self._active_task
            except asyncio.CancelledError:
                if (curr := asyncio.current_task()) is not None and curr.cancelling():
                    raise
            except Exception:
                pass
        self._active_task = None

    @callback
    def _async_on_advertisement(
        self,
        service_info: bluetooth.BluetoothServiceInfoBleak,
    ) -> None:
        """Handle every raw advertisement from the scale (including unchanged payloads)."""
        self._async_handle_advertisement(service_info)

    @callback
    def _async_on_bluetooth_event(
        self,
        service_info: bluetooth.BluetoothServiceInfoBleak,
        change: bluetooth.BluetoothChange,
    ) -> None:
        """Handle a changed or replayed advertisement from habluetooth."""
        self._async_handle_advertisement(service_info)

    @callback
    def _async_handle_advertisement(
        self,
        service_info: bluetooth.BluetoothServiceInfoBleak,
    ) -> None:
        """Process an incoming advertisement and connect if not already connected."""
        if service_info is self._last_service_info:
            return
        self._last_service_info = service_info

        if self._stopping or self._auth_failed or self.connecting:
            return

        now_mono = time.monotonic()
        if (now_mono - service_info.time) > RECENT_ADVERTISEMENT_SECONDS:
            return

        if service_info.connectable:
            ble_device = self._async_get_ble_device(service_info.device)
        else:
            ble_device = bluetooth.async_ble_device_from_address(
                self.hass, self.address, connectable=True
            )
            if ble_device is None:
                return

        remaining = self.cooldown_remaining
        if remaining > 0:
            if self._cancel_cooldown_retry is None:
                self._cancel_cooldown_retry = async_call_later(
                    self.hass, remaining, self._async_cooldown_expired
                )
            return
        _LOGGER.debug(
            "Advertisement wake trigger on %s (source=%s, rssi=%s)",
            self.address,
            service_info.source,
            service_info.rssi,
        )
        self._schedule_connect(ble_device)

    @callback
    def _async_cooldown_expired(self, _now: datetime) -> None:
        """Connect after cooldown if the scale is actively advertising right now."""
        self._cancel_cooldown_retry = None
        if self._stopping or self._auth_failed or self.connecting:
            return
        service_info = bluetooth.async_last_service_info(
            self.hass, self.address, connectable=True
        )
        if (
            service_info is None
            or (time.monotonic() - service_info.time) > ACTIVE_ADVERTISEMENT_SECONDS
        ):
            return
        self._schedule_connect(self._async_get_ble_device(service_info.device))

    @callback
    def _schedule_connect(self, ble_device: BLEDevice) -> None:
        if self.connecting:
            return
        if self._cancel_cooldown_retry is not None:
            self._cancel_cooldown_retry()
            self._cancel_cooldown_retry = None
        self._active_task = self.entry.async_create_background_task(
            self.hass,
            self._async_connect_and_stream(ble_device),
            name=f"xiaomi_s400_scale_{self.address}",
        )

    def _decay_failures_if_stale(self) -> None:
        """Reset the failure streak if the last failure was more than FAILURE_RESET_SECONDS ago."""
        now_mono = time.monotonic()
        if (
            self._last_failure_mono > 0
            and (now_mono - self._last_failure_mono) > FAILURE_RESET_SECONDS
        ):
            self._consecutive_failures = 0
            self._warned_failure_streak = False
        self._last_failure_mono = now_mono

    def _record_failure(self, message: str) -> None:
        """Record a connection or handshake failure and log with rate limiting."""
        self._decay_failures_if_stale()
        self._consecutive_failures += 1
        self._last_error = message
        if (
            self._consecutive_failures >= WARN_AFTER_CONSECUTIVE_FAILURES
            and not self._warned_failure_streak
        ):
            self._warned_failure_streak = True
            log_fn = _LOGGER.warning
        else:
            log_fn = _LOGGER.debug
        log_fn(
            "BLE session failed on %s (attempt %d): %s",
            self.address,
            self._consecutive_failures,
            message,
        )

    async def _async_connect_and_stream(self, ble_device: BLEDevice) -> None:
        hub = NotifyHub()

        def _on_disconnect(_client: BleakClient) -> None:
            hub.mark_disconnected()

        client: BleakClientWithServiceCache | None = None
        authenticated = False
        self._weigh_in_in_progress = False
        self._awaiting_step_off = False
        try:
            _LOGGER.debug("Connecting to Xiaomi S400 Scale (%s)...", self.address)
            client = await establish_connection(
                BleakClientWithServiceCache,
                ble_device,
                self.entry.title,
                disconnected_callback=_on_disconnect,
                max_attempts=3,
                ble_device_callback=lambda: self._async_get_ble_device(ble_device),
            )
            try:
                for uuid in REQUIRED_NOTIFY_UUIDS:
                    await client.start_notify(uuid, hub.make_callback(uuid))
            except (BleakError, TimeoutError, EOFError, OSError):
                with contextlib.suppress(BleakError, TimeoutError, EOFError, OSError):
                    await client.clear_cache()
                raise

            await asyncio.sleep(SETTLE_DELAY_SECONDS)
            keys = await login(client, self.token, hub)
            authenticated = True
            if self._warned_failure_streak:
                _LOGGER.info(
                    "Connection to Xiaomi S400 Scale (%s) restored after %d failed attempts",
                    self.address,
                    self._consecutive_failures,
                )
            else:
                _LOGGER.debug("Authenticated with Xiaomi S400 Scale (%s)", self.address)

            self._consecutive_bad_tokens = 0
            self._consecutive_failures = 0
            self._warned_failure_streak = False
            self._last_failure_mono = 0.0
            self._last_error = None
            self._last_connect_time = dt_util.utcnow()
            self.state.connected = True
            self._async_notify_listeners()

            await self._cmtp_pump(client, keys, hub)

        except BadTokenError as exc:
            self._decay_failures_if_stale()
            self._consecutive_bad_tokens += 1
            self._consecutive_failures += 1
            self._last_error = str(exc)
            if self._consecutive_bad_tokens >= MAX_CONSECUTIVE_BAD_TOKENS:
                _LOGGER.error(
                    "BLE login token rejected %d times in a row by scale %s (%s). Triggering reauth.",
                    self._consecutive_bad_tokens,
                    self.address,
                    exc,
                )
                self._auth_failed = True
                self._async_notify_listeners()
                self.entry.async_start_reauth(self.hass)
            else:
                _LOGGER.warning(
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
        except BleakOutOfConnectionSlotsError as exc:
            if authenticated:
                _LOGGER.debug("BLE session ended on %s: %s", self.address, exc)
            else:
                self._record_failure(
                    f"all Bluetooth adapter/proxy connection slots are in use: {exc}"
                )
        except (BleakError, TimeoutError, EOFError, OSError) as exc:
            if authenticated:
                _LOGGER.debug("BLE session ended on %s: %s", self.address, exc)
            else:
                self._record_failure(f"connection error: {exc}")
        except Exception as exc:
            if not authenticated:
                self._record_failure(f"unexpected error: {exc}")
            _LOGGER.exception("Unexpected error in BLE session on %s", self.address)
        finally:
            if client is not None and client.is_connected:
                try:
                    await client.disconnect()
                except (BleakError, TimeoutError, EOFError, OSError) as exc:
                    _LOGGER.debug("Disconnect cleanup error on %s: %s", self.address, exc)
            if authenticated:
                cooldown = RECONNECT_COOLDOWN_SECONDS
            else:
                cooldown = min(
                    RECONNECT_COOLDOWN_SECONDS * (2**self._consecutive_failures),
                    MAX_RECONNECT_COOLDOWN_SECONDS,
                )
            self._cooldown_until_mono = time.monotonic() + cooldown
            if (
                not self._stopping
                and not self._auth_failed
                and self._cancel_cooldown_retry is None
            ):
                self._cancel_cooldown_retry = async_call_later(
                    self.hass, cooldown, self._async_cooldown_expired
                )
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
        """Drain CMTP notifications, reassemble multi-frame messages, decrypt, and update state.

        Keeps the authenticated GATT session open continuously while the scale is connected
        so live weight frames stream immediately when someone steps onto the scale.
        """
        q = hub.queue(CMTP)
        reassembler = ParcelReassembler()
        while True:
            if self._weigh_in_in_progress:
                timeout: float | None = CMTP_IDLE_TIMEOUT_SECONDS
            elif (
                self._awaiting_step_off
                or self.state.live_weight_kg is not None
                or self.state.stable
            ):
                timeout = POST_FINAL_GRACE_SECONDS
            else:
                timeout = None
            try:
                async with asyncio.timeout(timeout):
                    data = await q.get()
            except TimeoutError:
                _LOGGER.debug(
                    "Weigh-in idle for %ss on %s; clearing live weight state",
                    timeout,
                    self.address,
                )
                self._weigh_in_in_progress = False
                self._awaiting_step_off = False
                if self.state.live_weight_kg is not None or self.state.stable:
                    self.state.live_weight_kg = None
                    self.state.stable = False
                    self._async_notify_listeners()
                continue
            if data is None:
                _LOGGER.debug("BLE link disconnected on %s", self.address)
                return
            try:
                ack, msg = reassembler.feed(data)
            except FrameSequenceError as exc:
                _LOGGER.debug("CMTP %s on %s; resetting buffer", exc, self.address)
                # Mi Home v2 has no NACK/retransmit opcode; RCV_OK unblocks the sender.
                await client.write_gatt_char(CMTP, RCV_OK, response=False)
                continue
            if msg:
                await self._handle_full_frame(keys, msg)
            if ack is not None:
                await client.write_gatt_char(CMTP, ack, response=False)

    async def _handle_full_frame(self, keys: SessionKeys, ciphertext: bytes) -> None:
        """Decrypt and process a reassembled CMTP payload."""
        pt = decrypt_cmtp(keys, ciphertext)
        if pt is None:
            _LOGGER.debug("CMTP decrypt failed on %s: %s", self.address, ciphertext.hex())
            return
        event = parse_cmtp_payload(pt)
        if event is None:
            _LOGGER.debug("Unparsed CMTP payload on %s: %s", self.address, pt.hex())
            return

        if event.type == "live":
            was_in_progress = self._weigh_in_in_progress
            prev_stable = self.state.stable
            if event.weight_kg is None or event.weight_kg <= 0:
                self._awaiting_step_off = False
                self._weigh_in_in_progress = False
                self.state.live_weight_kg = event.weight_kg
                self.state.stable = False
            else:
                if not self._awaiting_step_off:
                    self._weigh_in_in_progress = True
                self.state.live_weight_kg = event.weight_kg
                self.state.stable = event.stable
            if (
                self._weigh_in_in_progress != was_in_progress
                or self.state.stable != prev_stable
            ):
                _LOGGER.debug(
                    "Live measurement on %s: weight=%s kg, stable=%s",
                    self.address,
                    self.state.live_weight_kg,
                    self.state.stable,
                )
            self._async_notify_listeners()
            return

        if event.type == "final":
            if (
                event.weight_kg is None
                or event.weight_kg <= 0
                or not event.stable
            ):
                _LOGGER.debug(
                    "Ignoring incomplete/unstable final frame on %s: %s",
                    self.address,
                    event,
                )
                return
            self._weigh_in_in_progress = False
            self._awaiting_step_off = True
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
            _LOGGER.debug(
                "Final measurement on %s: weight=%s kg, impedance=%s ohm, impedance_low=%s ohm",
                self.address,
                self.state.final_weight_kg,
                self.state.impedance_ohm,
                self.state.impedance_low_ohm,
            )

            self._async_notify_listeners()
