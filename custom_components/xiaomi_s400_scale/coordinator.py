"""BLE coordinator for Xiaomi S400 Scale."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
import contextlib
from dataclasses import dataclass
from datetime import datetime
import functools
import logging
import time
from typing import Protocol, cast

from bleak import BleakClient
from bleak.backends.device import BLEDevice
from bleak.exc import BleakCharacteristicNotFoundError, BleakError
from bleak_retry_connector import (
    BleakClientWithServiceCache,
    BleakOutOfConnectionSlotsError,
    establish_connection,
)

from homeassistant.components import bluetooth
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import CALLBACK_TYPE, HomeAssistant, callback
from homeassistant.helpers.event import async_call_later
from homeassistant.helpers.start import async_at_started
from homeassistant.util import dt as dt_util

from .auth import AuthError, BadTokenError, NotifyHub, login
from .const import (
    DISCONNECT_TIMEOUT_SECONDS,
    FAILURE_RESET_SECONDS,
    IDLE_WATCHDOG_SECONDS,
    LIVE_WEIGHT_RESET_SECONDS,
    MAX_OUT_OF_SLOTS_COOLDOWN_SECONDS,
    MAX_RECONNECT_COOLDOWN_SECONDS,
    ORPHAN_DISCONNECT_COOLDOWN_SECONDS,
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


class _ESPHomeAPIClient(Protocol):
    """Protocol for aioesphomeapi.APIClient Bluetooth disconnect support."""

    async def bluetooth_device_disconnect(self, address: int) -> None: ...


type _OrphanProxyTarget = tuple[str, str, _ESPHomeAPIClient, bool]


def _normalize_mac(address: str) -> str:
    """Normalize a MAC address string to uppercase hex digits without separators."""
    return address.replace(":", "").replace("-", "").upper()


MAX_CONSECUTIVE_BAD_TOKENS = 3
MAX_CONSECUTIVE_DECRYPT_FAILURES = 5
WARN_AFTER_CONSECUTIVE_FAILURES = 3
DISCONNECTED_ADV_POLL_SECONDS = 5.0
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
    profile_id: int | None = None
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

        self._address_norm = _normalize_mac(self.address)
        self._address_int = int(self._address_norm, 16)
        self._listeners: list[Callable[[], None]] = []
        self._measurement_listeners: list[Callable[[], None]] = []
        self._active_task: asyncio.Task[None] | None = None
        self._cleanup_task: asyncio.Task[None] | None = None
        self._last_orphan_disconnect_mono: dict[str, float] = {}
        self._probed_startup_proxies: set[str] = set()
        self._unsub_bluetooth: CALLBACK_TYPE | None = None
        self._unsub_started: CALLBACK_TYPE | None = None
        self._cancel_cooldown_retry: CALLBACK_TYPE | None = None
        self._cancel_scanner_check: CALLBACK_TYPE | None = None
        self._stopping = False
        self._auth_failed = False
        self._consecutive_bad_tokens = 0
        self._consecutive_decrypt_failures = 0
        self._consecutive_failures = 0
        self._warned_failure_streak = False
        self._last_failure_mono: float | None = None
        self._last_error: str | None = None
        self._last_connect_time: datetime | None = None
        self._cooldown_until_mono: float | None = None
        self._total_sessions = 0
        self._last_session_duration_s: float | None = None
        self._longest_session_duration_s: float | None = None
        self._last_disconnect_reason: str | None = None
        self._last_final_signature: (
            tuple[int, float, float | None, float | None, int | None] | None
        ) = None

    @property
    def auth_failed(self) -> bool:
        """Return True if authentication was permanently halted pending reauth."""
        return self._auth_failed

    @property
    def session_active(self) -> bool:
        """Return True if a BLE session task is currently active."""
        return self._active_task is not None and not self._active_task.done()

    @property
    def _cleanup_active(self) -> bool:
        """Return True if an orphaned proxy cleanup task is currently active."""
        return self._cleanup_task is not None and not self._cleanup_task.done()

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
        if self._cooldown_until_mono is None:
            return 0.0
        return max(0.0, self._cooldown_until_mono - time.monotonic())

    @property
    def total_sessions(self) -> int:
        """Return the total number of authenticated BLE sessions established."""
        return self._total_sessions

    @property
    def last_session_duration_s(self) -> float | None:
        """Return the duration in seconds of the most recent authenticated session."""
        return self._last_session_duration_s

    @property
    def longest_session_duration_s(self) -> float | None:
        """Return the longest authenticated session duration in seconds."""
        return self._longest_session_duration_s

    @property
    def last_disconnect_reason(self) -> str | None:
        """Return the reason the most recent authenticated session ended."""
        return self._last_disconnect_reason

    @callback
    def async_add_listener(
        self, update_callback: Callable[[], None]
    ) -> Callable[[], None]:
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
    def async_add_measurement_listener(
        self, measurement_callback: Callable[[], None]
    ) -> Callable[[], None]:
        """Listen for completed final measurements."""
        self._measurement_listeners.append(measurement_callback)

        @callback
        def _remove() -> None:
            if measurement_callback in self._measurement_listeners:
                self._measurement_listeners.remove(measurement_callback)

        return _remove

    @callback
    def _async_notify_measurement_listeners(self) -> None:
        for measurement_callback in list(self._measurement_listeners):
            measurement_callback()

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
        if self._unsub_bluetooth is not None:
            self._unsub_bluetooth()
            self._unsub_bluetooth = None
        if self._unsub_started is not None:
            self._unsub_started()
            self._unsub_started = None
        if self._cancel_scanner_check is not None:
            self._cancel_scanner_check()
            self._cancel_scanner_check = None

        @callback
        def _on_started(_hass: HomeAssistant) -> None:
            self._unsub_started = None
            if self._cancel_scanner_check is not None:
                self._cancel_scanner_check()
            self._cancel_scanner_check = async_call_later(
                self.hass,
                STARTUP_SCANNER_CHECK_DELAY_SECONDS,
                self._async_check_connectable_scanners,
            )
            if (
                not self._schedule_orphan_cleanup_if_needed()
                and (
                    service_info := bluetooth.async_last_service_info(
                        self.hass, self.address, connectable=True
                    )
                )
                is not None
            ):
                self._async_on_advertisement(service_info)
            self._async_ensure_reconnect_timer()

        @callback
        def _on_bluetooth_event(
            service_info: bluetooth.BluetoothServiceInfoBleak,
            _change: bluetooth.BluetoothChange,
        ) -> None:
            self._async_on_advertisement(service_info)

        self._unsub_bluetooth = bluetooth.async_register_callback(
            self.hass,
            _on_bluetooth_event,
            bluetooth.BluetoothCallbackMatcher(address=self.address, connectable=True),
            bluetooth.BluetoothScanningMode.PASSIVE,
        )
        self._unsub_started = async_at_started(self.hass, _on_started)

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
        if self._unsub_started is not None:
            self._unsub_started()
            self._unsub_started = None
        if self._cancel_scanner_check is not None:
            self._cancel_scanner_check()
            self._cancel_scanner_check = None
        if self._cancel_cooldown_retry is not None:
            self._cancel_cooldown_retry()
            self._cancel_cooldown_retry = None
        if self._unsub_bluetooth is not None:
            self._unsub_bluetooth()
            self._unsub_bluetooth = None
        for task in (self._cleanup_task, self._active_task):
            if task is not None and not task.done():
                if task.cancelling() == 0:
                    task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    if (
                        curr := asyncio.current_task()
                    ) is not None and curr.cancelling():
                        raise
                except Exception:
                    pass
        self._cleanup_task = None
        self._active_task = None

    def _get_esphome_api_client(
        self, scanner: bluetooth.BaseHaScanner
    ) -> _ESPHomeAPIClient | None:
        """Resolve the underlying ESPHome APIClient for a remote Bluetooth proxy scanner."""
        connector = getattr(scanner, "connector", None)
        client_factory = getattr(connector, "client", None)
        if isinstance(client_factory, functools.partial):
            for candidate in (
                client_factory.keywords.get("client_data"),
                *client_factory.args,
            ):
                api_client = getattr(candidate, "client", None)
                if callable(getattr(api_client, "bluetooth_device_disconnect", None)):
                    return cast(_ESPHomeAPIClient, api_client)

        for attr in ("_client_data", "client_data"):
            client_data = getattr(scanner, attr, None)
            api_client = getattr(client_data, "client", None)
            if callable(getattr(api_client, "bluetooth_device_disconnect", None)):
                return cast(_ESPHomeAPIClient, api_client)

        source_norm = _normalize_mac(scanner.source)
        for esphome_entry in self.hass.config_entries.async_loaded_entries("esphome"):
            runtime_data = getattr(esphome_entry, "runtime_data", None)
            if runtime_data is None or not getattr(runtime_data, "available", True):
                continue
            device_info = getattr(runtime_data, "device_info", None)
            mac_candidates = (
                esphome_entry.data.get("bluetooth_mac_address"),
                getattr(device_info, "bluetooth_mac_address", None),
                getattr(device_info, "mac_address", None),
                esphome_entry.unique_id,
            )
            if any(
                isinstance(mac, str) and _normalize_mac(mac) == source_norm
                for mac in mac_candidates
            ):
                api_client = getattr(runtime_data, "client", None)
                if callable(getattr(api_client, "bluetooth_device_disconnect", None)):
                    return cast(_ESPHomeAPIClient, api_client)

        return None

    def _find_orphaned_proxy_targets(self) -> list[_OrphanProxyTarget]:
        """Find ESPHome proxies holding an orphaned BLE connection to this scale."""
        try:
            scanners = bluetooth.async_current_scanners(self.hass)
        except RuntimeError:
            return []

        has_fresh_adv = False
        with contextlib.suppress(RuntimeError):
            if (
                last_info := bluetooth.async_last_service_info(
                    self.hass, self.address, connectable=True
                )
            ) is not None and (
                bluetooth.MONOTONIC_TIME() - last_info.time
            ) <= RECENT_ADVERTISEMENT_SECONDS:
                has_fresh_adv = True

        now_mono = time.monotonic()
        targets: list[_OrphanProxyTarget] = []
        for scanner in scanners:
            if not scanner.connectable:
                continue
            last_attempt = self._last_orphan_disconnect_mono.get(scanner.source)
            if (
                last_attempt is not None
                and (now_mono - last_attempt) < ORPHAN_DISCONNECT_COOLDOWN_SECONDS
            ):
                continue

            allocations = scanner.get_allocations()
            explicitly_allocated = allocations is not None and any(
                _normalize_mac(addr) == self._address_norm
                for addr in allocations.allocated
            )
            startup_unattributed_slot = (
                self._total_sessions == 0
                and not has_fresh_adv
                and scanner.source not in self._probed_startup_proxies
                and (
                    allocations is None
                    or (
                        allocations.free < allocations.slots
                        and not allocations.allocated
                    )
                )
            )
            if not explicitly_allocated and not startup_unattributed_slot:
                continue

            api_client = self._get_esphome_api_client(scanner)
            if api_client is None:
                continue

            targets.append(
                (
                    scanner.source,
                    scanner.name,
                    api_client,
                    startup_unattributed_slot and not explicitly_allocated,
                )
            )
        return targets

    async def _async_release_orphaned_proxy_sessions(
        self, targets: list[_OrphanProxyTarget] | None = None
    ) -> bool:
        """Disconnect any orphaned BLE connection to this scale held by an ESPHome proxy."""
        resolved = self._find_orphaned_proxy_targets() if targets is None else targets
        if not resolved:
            return False

        released = False
        for source, name, api_client, is_startup_probe in resolved:
            self._last_orphan_disconnect_mono[source] = time.monotonic()
            if is_startup_probe:
                _LOGGER.debug(
                    "Probing proxy %s (%s) to clear any orphaned BLE session for %s",
                    name,
                    source,
                    self.address,
                )
            else:
                _LOGGER.info(
                    "Releasing orphaned BLE connection to %s on proxy %s (%s)",
                    self.address,
                    name,
                    source,
                )
            try:
                async with asyncio.timeout(DISCONNECT_TIMEOUT_SECONDS):
                    await api_client.bluetooth_device_disconnect(self._address_int)
                if is_startup_probe:
                    self._probed_startup_proxies.add(source)
                released = True
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _LOGGER.debug(
                    "Failed to release orphaned BLE connection to %s on proxy %s (%s): %s",
                    self.address,
                    name,
                    source,
                    exc,
                )

        if released:
            with contextlib.suppress(RuntimeError):
                bluetooth.async_clear_advertisement_history(self.hass, self.address)
        return released

    @callback
    def _schedule_orphan_cleanup_if_needed(self) -> bool:
        """Schedule background cleanup if any proxy holds an orphaned connection while idle."""
        if (
            self._stopping
            or self.hass.is_stopping
            or self._auth_failed
            or self.session_active
            or self._cleanup_active
        ):
            return False
        targets = self._find_orphaned_proxy_targets()
        if not targets:
            return False
        self._cleanup_task = self.entry.async_create_background_task(
            self.hass,
            self._async_run_orphan_cleanup(targets),
            name=f"xiaomi_s400_scale_cleanup_{self.address}",
        )
        return True

    async def _async_run_orphan_cleanup(
        self, targets: list[_OrphanProxyTarget]
    ) -> None:
        """Run orphaned proxy cleanup and resume advertisement checks once settled."""
        try:
            if await self._async_release_orphaned_proxy_sessions(targets):
                await asyncio.sleep(SETTLE_DELAY_SECONDS)
        finally:
            if self._cleanup_task is asyncio.current_task():
                self._cleanup_task = None
        if (
            self._stopping
            or self.hass.is_stopping
            or self._auth_failed
            or self.session_active
        ):
            return
        with contextlib.suppress(RuntimeError):
            if (
                service_info := bluetooth.async_last_service_info(
                    self.hass, self.address, connectable=True
                )
            ) is not None:
                self._async_on_advertisement(service_info)
        self._async_ensure_reconnect_timer()

    @callback
    def _async_on_advertisement(
        self,
        service_info: bluetooth.BluetoothServiceInfoBleak,
    ) -> None:
        """Process an incoming advertisement and connect if not already connected."""
        if (
            self._stopping
            or self.hass.is_stopping
            or self._auth_failed
            or self.session_active
            or self._cleanup_active
        ):
            return

        if (
            bluetooth.MONOTONIC_TIME() - service_info.time
        ) > RECENT_ADVERTISEMENT_SECONDS:
            return

        if service_info.connectable:
            ble_device = self._async_get_ble_device(service_info.device)
        else:
            connectable_info = bluetooth.async_last_service_info(
                self.hass, self.address, connectable=True
            )
            if (
                connectable_info is None
                or (bluetooth.MONOTONIC_TIME() - connectable_info.time)
                > RECENT_ADVERTISEMENT_SECONDS
            ):
                return
            ble_device = self._async_get_ble_device(connectable_info.device)

        remaining = self.cooldown_remaining
        if remaining > 0:
            if self._cancel_cooldown_retry is None:
                self._cancel_cooldown_retry = async_call_later(
                    self.hass, remaining, self._async_cooldown_expired
                )
            return
        _LOGGER.debug(
            "Advertisement received on %s (source=%s, rssi=%s); connecting",
            self.address,
            service_info.source,
            service_info.rssi,
        )
        self._schedule_connect(ble_device)

    @callback
    def _async_ensure_reconnect_timer(self) -> None:
        """Keep re-checking cached advertisements while disconnected in case payloads do not change."""
        if (
            self._stopping
            or self.hass.is_stopping
            or self._auth_failed
            or self.session_active
            or self._cancel_cooldown_retry is not None
        ):
            return
        delay = max(self.cooldown_remaining, DISCONNECTED_ADV_POLL_SECONDS)
        self._cancel_cooldown_retry = async_call_later(
            self.hass, delay, self._async_cooldown_expired
        )

    @callback
    def _async_cooldown_expired(self, _now: datetime) -> None:
        """Reconnect after cooldown if the scale was seen recently, or re-arm while idle."""
        if self._cancel_cooldown_retry is not None:
            self._cancel_cooldown_retry()
            self._cancel_cooldown_retry = None
        if (
            not self._schedule_orphan_cleanup_if_needed()
            and (
                service_info := bluetooth.async_last_service_info(
                    self.hass, self.address, connectable=True
                )
            )
            is not None
        ):
            self._async_on_advertisement(service_info)
        self._async_ensure_reconnect_timer()

    @callback
    def _schedule_connect(self, ble_device: BLEDevice) -> None:
        if self.session_active:
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
            self._last_failure_mono is not None
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

    async def _async_disconnect_client(self, client: BleakClient) -> None:
        """Disconnect the Bleak client, shielded from task cancellation so proxies release the slot."""

        async def _disconnect() -> None:
            try:
                async with asyncio.timeout(DISCONNECT_TIMEOUT_SECONDS):
                    await client.disconnect()
            except TimeoutError:
                _LOGGER.debug("Disconnect timed out on %s", self.address)
            except (BleakError, EOFError, OSError) as exc:
                _LOGGER.debug("Disconnect cleanup error on %s: %s", self.address, exc)

        disconnect_task = self.hass.async_create_task(
            _disconnect(),
            name=f"xiaomi_s400_scale_disconnect_{self.address}",
        )
        try:
            await asyncio.shield(disconnect_task)
        except asyncio.CancelledError:
            disconnect_task.add_done_callback(
                lambda t: t.exception() if not t.cancelled() else None
            )
            raise

    async def _async_connect_and_stream(self, ble_device: BLEDevice) -> None:
        hub: NotifyHub | None = None

        def _on_disconnect(_client: BleakClient) -> None:
            if hub is not None:
                hub.mark_disconnected()

        client: BleakClientWithServiceCache | None = None
        authenticated = False
        out_of_slots = False
        session_start_mono: float | None = None
        disconnect_reason = "remote_disconnect"
        self._consecutive_decrypt_failures = 0
        try:
            if await self._async_release_orphaned_proxy_sessions():
                await asyncio.sleep(SETTLE_DELAY_SECONDS)
            _LOGGER.debug("Connecting to Xiaomi S400 Scale (%s)...", self.address)
            client = await establish_connection(
                BleakClientWithServiceCache,
                ble_device,
                self.entry.title,
                disconnected_callback=_on_disconnect,
                max_attempts=3,
                ble_device_callback=lambda: self._async_get_ble_device(ble_device),
            )
            hub = NotifyHub()
            if not client.is_connected:
                hub.mark_disconnected()
            try:
                for uuid in REQUIRED_NOTIFY_UUIDS:
                    await client.start_notify(uuid, hub.make_callback(uuid))
            except BleakError as exc:
                if isinstance(exc, BleakCharacteristicNotFoundError) or (
                    client.is_connected and "not found" in str(exc).lower()
                ):
                    with contextlib.suppress(
                        BleakError, TimeoutError, EOFError, OSError
                    ):
                        await client.clear_cache()
                raise

            await asyncio.sleep(SETTLE_DELAY_SECONDS)
            keys = await login(client, self.token, hub)
            authenticated = True
            session_start_mono = time.monotonic()
            self._total_sessions += 1
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
            self._last_failure_mono = None
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
            disconnect_reason = f"auth_error: {exc}"
            self._record_failure(str(exc))
        except asyncio.CancelledError:
            disconnect_reason = (
                "stopped" if (self._stopping or self.hass.is_stopping) else "cancelled"
            )
            raise
        except BleakOutOfConnectionSlotsError as exc:
            disconnect_reason = f"out_of_slots: {exc}"
            if authenticated:
                _LOGGER.debug("BLE session ended on %s: %s", self.address, exc)
            else:
                out_of_slots = True
                self._record_failure(
                    f"all Bluetooth adapter/proxy connection slots are in use: {exc}"
                )
        except (BleakError, TimeoutError, EOFError, OSError) as exc:
            disconnect_reason = f"ble_error: {exc}"
            if authenticated:
                _LOGGER.debug("BLE session ended on %s: %s", self.address, exc)
            else:
                self._record_failure(f"connection error: {exc}")
        except Exception as exc:
            disconnect_reason = f"unexpected_error: {exc}"
            self._record_failure(f"unexpected error: {exc}")
            _LOGGER.exception("Unexpected error in BLE session on %s", self.address)
        finally:
            if session_start_mono is not None:
                duration = round(max(0.0, time.monotonic() - session_start_mono), 1)
                self._last_session_duration_s = duration
                if (
                    self._longest_session_duration_s is None
                    or duration > self._longest_session_duration_s
                ):
                    self._longest_session_duration_s = duration
                self._last_disconnect_reason = disconnect_reason
            if client is not None and client.is_connected:
                await self._async_disconnect_client(client)
            if self._active_task is asyncio.current_task():
                self._active_task = None
            if authenticated and self._consecutive_failures == 0:
                cooldown = RECONNECT_COOLDOWN_SECONDS
            else:
                max_cooldown = (
                    MAX_OUT_OF_SLOTS_COOLDOWN_SECONDS
                    if out_of_slots
                    else MAX_RECONNECT_COOLDOWN_SECONDS
                )
                cooldown = min(
                    RECONNECT_COOLDOWN_SECONDS * (2**self._consecutive_failures),
                    max_cooldown,
                )
            self._cooldown_until_mono = time.monotonic() + cooldown
            if (
                not self._stopping
                and not self.hass.is_stopping
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
            has_live_state = self.state.live_weight_kg is not None or self.state.stable
            timeout = (
                LIVE_WEIGHT_RESET_SECONDS if has_live_state else IDLE_WATCHDOG_SECONDS
            )
            try:
                async with asyncio.timeout(timeout):
                    data = await q.get()
            except TimeoutError:
                # Local property check only (zero BLE traffic) in case a disconnect callback was missed.
                if not client.is_connected:
                    _LOGGER.debug(
                        "BLE link no longer connected on %s; ending session",
                        self.address,
                    )
                    return
                if has_live_state:
                    _LOGGER.debug(
                        "No CMTP frames for %ss on %s; clearing live weight state",
                        timeout,
                        self.address,
                    )
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
                try:
                    self._handle_full_frame(keys, msg)
                except AuthError:
                    raise
                except Exception:
                    _LOGGER.exception(
                        "Unexpected error handling CMTP frame on %s", self.address
                    )
            if ack is not None:
                await client.write_gatt_char(CMTP, ack, response=False)

    @callback
    def _handle_full_frame(self, keys: SessionKeys, ciphertext: bytes) -> None:
        """Decrypt and process a reassembled CMTP payload."""
        pt = decrypt_cmtp(keys, ciphertext)
        if pt is None:
            self._consecutive_decrypt_failures += 1
            _LOGGER.debug(
                "CMTP decrypt failed on %s (%d/%d): %s",
                self.address,
                self._consecutive_decrypt_failures,
                MAX_CONSECUTIVE_DECRYPT_FAILURES,
                ciphertext.hex(),
            )
            if self._consecutive_decrypt_failures >= MAX_CONSECUTIVE_DECRYPT_FAILURES:
                raise AuthError(
                    f"CMTP decryption failed {self._consecutive_decrypt_failures} times in a row; re-establishing session"
                )
            return
        self._consecutive_decrypt_failures = 0

        event = parse_cmtp_payload(pt)
        if event is None:
            _LOGGER.debug("Unparsed CMTP payload on %s: %s", self.address, pt.hex())
            return

        if event.type == "live":
            prev_weight = self.state.live_weight_kg
            prev_stable = self.state.stable
            if event.weight_kg is None or event.weight_kg <= 0:
                self.state.live_weight_kg = None
                self.state.stable = False
            else:
                self.state.live_weight_kg = event.weight_kg
                self.state.stable = event.stable
            if (
                self.state.live_weight_kg != prev_weight
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
            if event.weight_kg is None or event.weight_kg <= 0:
                _LOGGER.debug(
                    "Ignoring invalid final frame on %s: %s",
                    self.address,
                    event,
                )
                return
            impedance_ohm = (
                event.impedance_ohm
                if (event.impedance_ohm and event.impedance_ohm > 0)
                else None
            )
            impedance_low_ohm = (
                event.impedance_low_ohm
                if (event.impedance_low_ohm and event.impedance_low_ohm > 0)
                else None
            )
            if event.device_timestamp is not None and event.device_timestamp > 0:
                signature = (
                    event.device_timestamp,
                    event.weight_kg,
                    impedance_ohm,
                    impedance_low_ohm,
                    event.profile_id,
                )
                if signature == self._last_final_signature:
                    _LOGGER.debug(
                        "Ignoring duplicate final frame on %s (device_timestamp=%s)",
                        self.address,
                        event.device_timestamp,
                    )
                    return
                self._last_final_signature = signature
            self.state.live_weight_kg = event.weight_kg
            self.state.final_weight_kg = event.weight_kg
            self.state.stable = True
            self.state.impedance_ohm = impedance_ohm
            self.state.impedance_low_ohm = impedance_low_ohm
            self.state.profile_id = event.profile_id
            self.state.last_measurement = dt_util.utcnow()
            _LOGGER.debug(
                "Final measurement on %s: weight=%s kg, impedance=%s ohm, impedance_low=%s ohm, profile_id=%s, device_timestamp=%s",
                self.address,
                self.state.final_weight_kg,
                self.state.impedance_ohm,
                self.state.impedance_low_ohm,
                self.state.profile_id,
                event.device_timestamp,
            )

            self._async_notify_listeners()
            self._async_notify_measurement_listeners()
