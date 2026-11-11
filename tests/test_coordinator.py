"""Tests for S400Coordinator BLE session management, streaming, and error recovery."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
import functools
import logging
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from bleak.backends.characteristic import BleakGATTCharacteristic
from bleak.exc import BleakCharacteristicNotFoundError, BleakError
from bleak_retry_connector import BleakOutOfConnectionSlotsError
from habluetooth import Allocations
import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from homeassistant.components import bluetooth
from homeassistant.const import CONF_ADDRESS, CONF_TOKEN, EVENT_HOMEASSISTANT_STOP
from homeassistant.core import Event, HomeAssistant

from custom_components.xiaomi_s400_scale.auth import AuthError, BadTokenError
from custom_components.xiaomi_s400_scale.const import (
    DOMAIN,
    FAILURE_RESET_SECONDS,
    MAX_OUT_OF_SLOTS_COOLDOWN_SECONDS,
    MAX_RECONNECT_COOLDOWN_SECONDS,
    RECENT_ADVERTISEMENT_SECONDS,
)
from custom_components.xiaomi_s400_scale.coordinator import S400Coordinator
from custom_components.xiaomi_s400_scale.protocol import CMTP, RCV_OK, RCV_RDY

from .conftest import (
    REAL_FINAL_PLAINTEXT,
    REAL_LIVE_PLAINTEXT,
    TEST_ADDRESS,
    TEST_NAME,
    TEST_TOKEN_BYTES,
    TEST_TOKEN_HEX,
    encrypt_cmtp_message,
    make_ble_device,
    make_parcel_frames,
    make_service_info,
    make_session_keys,
)


async def _create_coordinator(hass: HomeAssistant) -> S400Coordinator:
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id=TEST_ADDRESS,
        title=TEST_NAME,
        data={CONF_ADDRESS: TEST_ADDRESS, CONF_TOKEN: TEST_TOKEN_HEX},
    )
    entry.add_to_hass(hass)
    coordinator = S400Coordinator(hass, entry, TEST_ADDRESS, TEST_TOKEN_BYTES)
    entry.runtime_data = coordinator

    async def _async_handle_stop(_event: Event) -> None:
        await coordinator.async_stop()

    hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, _async_handle_stop)
    return coordinator


async def test_advertisement_staleness_connectability_and_cooldown(
    hass: HomeAssistant,
) -> None:
    """Test stale/non-connectable advertisements are filtered and cooldown schedules retry."""
    coordinator = await _create_coordinator(hass)
    now_mono = bluetooth.MONOTONIC_TIME()

    with patch.object(coordinator, "_schedule_connect") as mock_sched:
        # 1. Stale advertisement is ignored
        stale = make_service_info(
            time_mono=now_mono - RECENT_ADVERTISEMENT_SECONDS - 5.0
        )
        coordinator._async_on_advertisement(stale)
        mock_sched.assert_not_called()

        # 2. Non-connectable advertisement without fresh connectable info is ignored
        non_conn = make_service_info(connectable=False, time_mono=now_mono)
        with patch(
            "custom_components.xiaomi_s400_scale.coordinator.bluetooth.async_last_service_info",
            return_value=None,
        ):
            coordinator._async_on_advertisement(non_conn)
        mock_sched.assert_not_called()

        # 3. Non-connectable advertisement WITH fresh connectable info connects
        fresh_conn = make_service_info(connectable=True, time_mono=now_mono)
        with patch(
            "custom_components.xiaomi_s400_scale.coordinator.bluetooth.async_last_service_info",
            return_value=fresh_conn,
        ):
            coordinator._async_on_advertisement(non_conn)
        mock_sched.assert_called_once()
        mock_sched.reset_mock()

        # 4. Active cooldown defers connect and arms retry timer
        coordinator._cooldown_until_mono = time.monotonic() + 10.0
        coordinator._async_on_advertisement(fresh_conn)
        mock_sched.assert_not_called()
        assert coordinator._cancel_cooldown_retry is not None

        # 5. Cooldown expiry checks async_last_service_info and connects
        coordinator._cancel_cooldown_retry()
        coordinator._cancel_cooldown_retry = None
        coordinator._cooldown_until_mono = None
        with patch(
            "custom_components.xiaomi_s400_scale.coordinator.bluetooth.async_last_service_info",
            return_value=fresh_conn,
        ):
            coordinator._async_cooldown_expired(datetime.now(UTC))
        mock_sched.assert_called_once()

    await coordinator.async_stop()


async def test_connectable_scanner_startup_warning(
    hass: HomeAssistant, caplog: pytest.LogCaptureFixture
) -> None:
    """Test warning is logged only when zero connectable scanners exist."""
    coordinator = await _create_coordinator(hass)

    with (
        caplog.at_level(logging.WARNING),
        patch(
            "custom_components.xiaomi_s400_scale.coordinator.bluetooth.async_scanner_count",
            return_value=0,
        ),
    ):
        coordinator._async_check_connectable_scanners(datetime.now(UTC))
    assert "No connectable Bluetooth adapters or proxies" in caplog.text

    caplog.clear()
    with (
        caplog.at_level(logging.WARNING),
        patch(
            "custom_components.xiaomi_s400_scale.coordinator.bluetooth.async_scanner_count",
            return_value=1,
        ),
    ):
        coordinator._async_check_connectable_scanners(datetime.now(UTC))
    assert caplog.text == ""

    await coordinator.async_stop()


async def test_connect_and_stream_live_final_duplicate_and_sequence_recovery(
    hass: HomeAssistant,
) -> None:
    """Test full encrypted CMTP streaming, duplicate suppression, and sequence error recovery."""
    coordinator = await _create_coordinator(hass)
    # Pre-seed warning streak so restoration log branch is exercised on login
    coordinator._consecutive_failures = 5
    coordinator._warned_failure_streak = True

    keys = make_session_keys()
    notify_callbacks: dict[str, object] = {}
    disconnected_cb_holder: list[object] = []

    client = MagicMock()
    client.is_connected = True
    client.disconnect = AsyncMock()
    client.write_gatt_char = AsyncMock()

    async def _start_notify(uuid: str, cb: object) -> None:
        notify_callbacks[uuid] = cb

    client.start_notify = AsyncMock(side_effect=_start_notify)

    async def _establish(*args: object, **kwargs: object) -> MagicMock:
        disconnected_cb_holder.append(kwargs["disconnected_callback"])
        return client

    connected_event = asyncio.Event()
    unsub_conn = coordinator.async_add_listener(
        lambda: connected_event.set() if coordinator.state.connected else None
    )
    measurement_events: list[float | None] = []
    unsub_meas = coordinator.async_add_measurement_listener(
        lambda: measurement_events.append(coordinator.state.final_weight_kg)
    )

    with (
        patch(
            "custom_components.xiaomi_s400_scale.coordinator.establish_connection",
            AsyncMock(side_effect=_establish),
        ),
        patch(
            "custom_components.xiaomi_s400_scale.coordinator.login",
            AsyncMock(return_value=keys),
        ),
    ):
        task = asyncio.create_task(
            coordinator._async_connect_and_stream(make_ble_device())
        )
        await connected_event.wait()

        cmtp_cb = notify_callbacks[CMTP]
        assert callable(cmtp_cb)
        char = MagicMock(spec=BleakGATTCharacteristic)

        # 1. Stream encrypted live weight (72.8 kg, stable=True)
        live_ct = encrypt_cmtp_message(keys, REAL_LIVE_PLAINTEXT, iteration=1)
        for frame in make_parcel_frames(live_ct):
            cmtp_cb(char, bytearray(frame))
        await asyncio.sleep(0.01)
        assert coordinator.state.live_weight_kg == 72.8
        assert coordinator.state.stable is True

        # 2. Stream zero live weight (0.0 kg) -> clears live_weight_kg and stable
        zero_live_pt = bytes(12) + b"\xa00,0"
        for frame in make_parcel_frames(
            encrypt_cmtp_message(keys, zero_live_pt, iteration=2)
        ):
            cmtp_cb(char, bytearray(frame))
        await asyncio.sleep(0.01)
        assert coordinator.state.live_weight_kg is None
        assert coordinator.state.stable is False

        # 3. Trigger FrameSequenceError on CMTP -> writes RCV_OK and stays connected
        cmtp_cb(char, bytearray(bytes.fromhex("000000000200")))
        cmtp_cb(char, bytearray(b"\x02\x00skipped"))
        await asyncio.sleep(0.01)
        assert coordinator.state.connected is True

        # 4. Stream real final measurement + duplicate retransmission
        final_ct_1 = encrypt_cmtp_message(keys, REAL_FINAL_PLAINTEXT, iteration=3)
        for frame in make_parcel_frames(final_ct_1):
            cmtp_cb(char, bytearray(frame))
        final_ct_dup = encrypt_cmtp_message(keys, REAL_FINAL_PLAINTEXT, iteration=4)
        for frame in make_parcel_frames(final_ct_dup):
            cmtp_cb(char, bytearray(frame))
        await asyncio.sleep(0.01)

        # Duplicate with same device_timestamp must be ignored (only 1 event)
        assert measurement_events == [72.8]
        assert coordinator.state.final_weight_kg == 72.8
        assert coordinator.state.impedance_ohm == 538.0
        assert coordinator.state.impedance_low_ohm == 494.0

        # Verify RCV_RDY and RCV_OK acks were written to CMTP
        written_payloads = [
            call.args[1] for call in client.write_gatt_char.await_args_list
        ]
        assert RCV_RDY in written_payloads
        assert RCV_OK in written_payloads

        # 5. Fire disconnect callback to end session cleanly
        client.is_connected = False
        disc_cb = disconnected_cb_holder[0]
        assert callable(disc_cb)
        disc_cb(client)
        await task

    unsub_conn()
    unsub_meas()
    assert coordinator.state.connected is False
    assert coordinator.total_sessions == 1
    assert coordinator.last_session_duration_s is not None
    assert coordinator.longest_session_duration_s is not None
    assert coordinator.last_disconnect_reason == "remote_disconnect"
    await coordinator.async_stop()


async def test_schedule_connect_stop_cancellation_and_error_branches(
    hass: HomeAssistant,
) -> None:
    """Test _schedule_connect cancellation on stop, AuthError, unexpected error, and unparsed payload."""
    coordinator = await _create_coordinator(hass)
    keys = make_session_keys()

    # Unparsed CMTP payload is safely ignored
    coordinator._handle_full_frame(keys, encrypt_cmtp_message(keys, b"short", 1))
    assert coordinator.state.final_weight_kg is None

    # AuthError and unexpected RuntimeError in _async_connect_and_stream record failure reasons
    with patch(
        "custom_components.xiaomi_s400_scale.coordinator.establish_connection",
        AsyncMock(side_effect=AuthError("handshake timeout")),
    ):
        await coordinator._async_connect_and_stream(make_ble_device())
    assert coordinator.last_error == "handshake timeout"

    with patch(
        "custom_components.xiaomi_s400_scale.coordinator.establish_connection",
        AsyncMock(side_effect=RuntimeError("unexpected crash")),
    ):
        await coordinator._async_connect_and_stream(make_ble_device())
    assert coordinator.last_error == "unexpected error: unexpected crash"

    # Active background connect task is cancelled cleanly by async_stop()
    started = asyncio.Event()

    async def _hang(*args: object, **kwargs: object) -> MagicMock:
        started.set()
        await asyncio.Future()
        return MagicMock()

    with patch(
        "custom_components.xiaomi_s400_scale.coordinator.establish_connection",
        AsyncMock(side_effect=_hang),
    ):
        coordinator._schedule_connect(make_ble_device())
        assert coordinator.session_active is True
        # Duplicate _schedule_connect while active is a no-op
        coordinator._schedule_connect(make_ble_device())
        await started.wait()
        await coordinator.async_stop()

    assert coordinator.session_active is False


async def test_cmtp_pump_live_reset_and_idle_watchdog_timeouts(
    hass: HomeAssistant,
) -> None:
    """Test _cmtp_pump clears stale live weight on timeout and exits if BLE link dropped."""
    coordinator = await _create_coordinator(hass)
    coordinator.state.live_weight_kg = 70.2
    coordinator.state.stable = True

    client = MagicMock()
    # First timeout: still connected -> clears live_weight_kg; second timeout: disconnected -> exits
    connected_checks = iter([True, False])
    type(client).is_connected = property(lambda _self: next(connected_checks))

    from custom_components.xiaomi_s400_scale.auth import NotifyHub

    hub = NotifyHub()
    keys = make_session_keys()

    with (
        patch(
            "custom_components.xiaomi_s400_scale.coordinator.LIVE_WEIGHT_RESET_SECONDS",
            0.01,
        ),
        patch(
            "custom_components.xiaomi_s400_scale.coordinator.IDLE_WATCHDOG_SECONDS",
            0.01,
        ),
    ):
        await coordinator._cmtp_pump(client, keys, hub)

    assert coordinator.state.live_weight_kg is None
    assert coordinator.state.stable is False
    await coordinator.async_stop()


async def test_consecutive_decrypt_failures_raise_auth_error(
    hass: HomeAssistant,
) -> None:
    """Test 5 consecutive CMTP decryption failures abort the session with AuthError."""
    coordinator = await _create_coordinator(hass)
    keys = make_session_keys()

    for _ in range(4):
        coordinator._handle_full_frame(keys, b"\x01\x00badtag")
    with pytest.raises(AuthError, match="CMTP decryption failed 5 times"):
        coordinator._handle_full_frame(keys, b"\x01\x00badtag")

    await coordinator.async_stop()


async def test_bad_token_streak_triggers_reauth(hass: HomeAssistant) -> None:
    """Test 3 consecutive BadTokenErrors halt coordinator and start reauth."""
    coordinator = await _create_coordinator(hass)
    client = MagicMock()
    client.is_connected = False
    client.start_notify = AsyncMock()

    with (
        patch(
            "custom_components.xiaomi_s400_scale.coordinator.establish_connection",
            AsyncMock(return_value=client),
        ),
        patch(
            "custom_components.xiaomi_s400_scale.coordinator.login",
            AsyncMock(side_effect=BadTokenError("HMAC mismatch")),
        ),
        patch.object(coordinator.entry, "async_start_reauth") as mock_reauth,
    ):
        await coordinator._async_connect_and_stream(make_ble_device())
        assert coordinator.auth_failed is False
        mock_reauth.assert_not_called()

        await coordinator._async_connect_and_stream(make_ble_device())
        assert coordinator.auth_failed is False
        mock_reauth.assert_not_called()

        await coordinator._async_connect_and_stream(make_ble_device())
        assert coordinator.auth_failed is True
        mock_reauth.assert_called_once_with(hass)

    await coordinator.async_stop()


async def test_gatt_cache_clear_and_out_of_slots_backoff(
    hass: HomeAssistant,
) -> None:
    """Test GATT cache clear on missing char and extended backoff on out-of-slots."""
    coordinator = await _create_coordinator(hass)

    # 1. Missing GATT characteristic triggers client.clear_cache()
    client = MagicMock()
    client.is_connected = True
    client.clear_cache = AsyncMock()
    client.disconnect = AsyncMock()
    client.start_notify = AsyncMock(
        side_effect=BleakCharacteristicNotFoundError("00000010")
    )

    with patch(
        "custom_components.xiaomi_s400_scale.coordinator.establish_connection",
        AsyncMock(return_value=client),
    ):
        await coordinator._async_connect_and_stream(make_ble_device())

    client.clear_cache.assert_awaited_once()
    assert coordinator.consecutive_failures == 1
    assert coordinator.cooldown_remaining <= MAX_RECONNECT_COOLDOWN_SECONDS

    # 2. Out-of-slots error uses MAX_OUT_OF_SLOTS_COOLDOWN_SECONDS cap
    coordinator._consecutive_failures = 7
    coordinator._last_failure_mono = time.monotonic()
    with patch(
        "custom_components.xiaomi_s400_scale.coordinator.establish_connection",
        AsyncMock(side_effect=BleakOutOfConnectionSlotsError("no free slots")),
    ):
        await coordinator._async_connect_and_stream(make_ble_device())

    assert coordinator.cooldown_remaining > MAX_RECONNECT_COOLDOWN_SECONDS
    assert coordinator.cooldown_remaining <= MAX_OUT_OF_SLOTS_COOLDOWN_SECONDS

    # 3. Failure streak decays if last failure was older than FAILURE_RESET_SECONDS,
    # even on freshly booted CI VMs where time.monotonic() < FAILURE_RESET_SECONDS.
    with patch(
        "custom_components.xiaomi_s400_scale.coordinator.time.monotonic",
        return_value=45.0,
    ):
        coordinator._last_failure_mono = 45.0 - FAILURE_RESET_SECONDS - 10.0
        coordinator._record_failure("transient timeout")
    assert coordinator.consecutive_failures == 1

    await coordinator.async_stop()


async def test_graceful_shutdown_completes_disconnect_without_double_cancel(
    hass: HomeAssistant,
) -> None:
    """Test HA shutdown double-cancel race does not abort client.disconnect() mid-flight."""
    coordinator = await _create_coordinator(hass)
    keys = make_session_keys()
    connected_event = asyncio.Event()
    disconnect_completed = False

    client = MagicMock()
    client.is_connected = True
    client.start_notify = AsyncMock()

    async def _slow_disconnect() -> None:
        nonlocal disconnect_completed
        await asyncio.sleep(0.05)
        client.is_connected = False
        disconnect_completed = True

    client.disconnect = AsyncMock(side_effect=_slow_disconnect)
    unsub = coordinator.async_add_listener(
        lambda: connected_event.set() if coordinator.state.connected else None
    )

    with (
        patch(
            "custom_components.xiaomi_s400_scale.coordinator.establish_connection",
            AsyncMock(return_value=client),
        ),
        patch(
            "custom_components.xiaomi_s400_scale.coordinator.login",
            AsyncMock(return_value=keys),
        ),
    ):
        coordinator._schedule_connect(make_ble_device())
        await connected_event.wait()
        assert coordinator._active_task is not None

        # Simulate HomeAssistant.async_stop(): background tasks are cancelled first,
        # and EVENT_HOMEASSISTANT_STOP calls coordinator.async_stop() on the same tick.
        coordinator._active_task.cancel("Home Assistant is stopping")
        await coordinator.async_stop()

    unsub()
    assert disconnect_completed is True
    client.disconnect.assert_awaited_once()
    assert coordinator.last_disconnect_reason == "stopped"
    assert coordinator.state.connected is False

    # Also verify _async_disconnect_client handles TimeoutError and BleakError cleanly
    hung_cancelled = False

    async def _hung_disconnect() -> None:
        nonlocal hung_cancelled
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            hung_cancelled = True
            raise

    hung_client = MagicMock()
    hung_client.disconnect = AsyncMock(side_effect=_hung_disconnect)
    with patch(
        "custom_components.xiaomi_s400_scale.coordinator.DISCONNECT_TIMEOUT_SECONDS",
        0.01,
    ):
        await coordinator._async_disconnect_client(hung_client)
    assert hung_cancelled is True

    err_client = MagicMock()
    err_client.disconnect = AsyncMock(side_effect=BleakError("already disconnected"))
    await coordinator._async_disconnect_client(err_client)


async def test_orphaned_proxy_cleanup_via_scanner_connector_and_cooldown(
    hass: HomeAssistant,
) -> None:
    """Test startup orphan cleanup via scanner.connector.client and cooldown rate-limiting."""
    coordinator = await _create_coordinator(hass)
    proxy_source = "AA:BB:CC:DD:EE:02"
    expected_addr_int = int(TEST_ADDRESS.replace(":", ""), 16)

    mock_api_client = MagicMock()
    mock_api_client.bluetooth_device_disconnect = AsyncMock()

    def _dummy_bleak_client(*args: object, **kwargs: object) -> None:
        return None

    scanner = MagicMock()
    scanner.source = proxy_source
    scanner.name = "esphome-proxy-living-room"
    scanner.connectable = True
    scanner.get_allocations.return_value = Allocations(
        adapter=proxy_source,
        slots=3,
        free=2,
        allocated=[TEST_ADDRESS],
    )
    scanner.connector = SimpleNamespace(
        client=functools.partial(
            _dummy_bleak_client,
            client_data=SimpleNamespace(client=mock_api_client),
        )
    )

    with (
        patch(
            "custom_components.xiaomi_s400_scale.coordinator.bluetooth.async_current_scanners",
            return_value=[scanner],
        ),
        patch(
            "custom_components.xiaomi_s400_scale.coordinator.bluetooth.async_clear_advertisement_history"
        ) as mock_clear_adv,
    ):
        coordinator.async_start()
        await hass.async_block_till_done()

        mock_api_client.bluetooth_device_disconnect.assert_awaited_once_with(
            expected_addr_int
        )
        mock_clear_adv.assert_called_once_with(hass, TEST_ADDRESS)

        # Immediate cooldown poll must respect ORPHAN_DISCONNECT_COOLDOWN_SECONDS
        coordinator._async_cooldown_expired(datetime.now(UTC))
        await hass.async_block_till_done()
        assert mock_api_client.bluetooth_device_disconnect.await_count == 1

    await coordinator.async_stop()


async def test_orphaned_proxy_esphome_entry_fallback_startup_probe_and_preconnect(
    hass: HomeAssistant,
) -> None:
    """Test esphome entry fallback, startup unattributed probe, clean skip, errors, and pre-connect cleanup."""
    coordinator = await _create_coordinator(hass)
    proxy_source = "AA:BB:CC:DD:EE:03"
    clean_source = "AA:BB:CC:DD:EE:04"
    expected_addr_int = int(TEST_ADDRESS.replace(":", ""), 16)

    mock_api_client = MagicMock()
    mock_api_client.bluetooth_device_disconnect = AsyncMock(
        side_effect=[OSError("socket busy"), None, None]
    )

    esphome_entry = MockConfigEntry(
        domain="esphome",
        unique_id="esphome_scale_proxy",
        title="Scale Proxy",
        data={},
    )
    esphome_entry.add_to_hass(hass)
    esphome_entry.runtime_data = SimpleNamespace(
        available=True,
        device_info=SimpleNamespace(
            bluetooth_mac_address=proxy_source,
            mac_address="AA:BB:CC:DD:EE:00",
        ),
        client=mock_api_client,
    )

    # 1. Startup probe on proxy with allocations=None; clean & non-connectable scanners ignored
    probe_scanner = SimpleNamespace(
        source=proxy_source,
        name="scale-proxy",
        connectable=True,
        connector=None,
        get_allocations=MagicMock(return_value=None),
    )

    clean_scanner = SimpleNamespace(
        source=clean_source,
        name="clean-proxy",
        connectable=True,
        connector=None,
        get_allocations=MagicMock(
            return_value=Allocations(
                adapter=clean_source,
                slots=3,
                free=3,
                allocated=[],
            )
        ),
    )

    passive_scanner = SimpleNamespace(
        source="AA:BB:CC:DD:EE:05",
        name="passive-proxy",
        connectable=False,
    )

    with (
        patch.object(
            hass.config_entries,
            "async_loaded_entries",
            side_effect=lambda domain: [esphome_entry] if domain == "esphome" else [],
        ),
        patch(
            "custom_components.xiaomi_s400_scale.coordinator.bluetooth.async_current_scanners",
            return_value=[probe_scanner, clean_scanner, passive_scanner],
        ),
        patch(
            "custom_components.xiaomi_s400_scale.coordinator.bluetooth.async_clear_advertisement_history"
        ) as mock_clear_adv,
    ):
        # First attempt fails with OSError -> does not mark probed or clear adv history
        released = await coordinator._async_release_orphaned_proxy_sessions()
        assert released is False
        assert proxy_source not in coordinator._probed_startup_proxies
        mock_clear_adv.assert_not_called()

        # Expire per-proxy cooldown and retry -> succeeds and marks proxy probed
        coordinator._last_orphan_disconnect_mono.clear()
        released = await coordinator._async_release_orphaned_proxy_sessions()
        assert released is True
        assert proxy_source in coordinator._probed_startup_proxies
        mock_clear_adv.assert_called_once_with(hass, TEST_ADDRESS)

        # Subsequent startup check skips already-probed proxy when not explicitly allocated
        coordinator._last_orphan_disconnect_mono.clear()
        assert coordinator._find_orphaned_proxy_targets() == []

        # 2. Explicitly allocated slot is released right before establish_connection
        probe_scanner.get_allocations.return_value = Allocations(
            adapter=proxy_source,
            slots=3,
            free=2,
            allocated=[TEST_ADDRESS.lower()],
        )
        with patch(
            "custom_components.xiaomi_s400_scale.coordinator.establish_connection",
            AsyncMock(side_effect=AuthError("stop after pre-connect check")),
        ):
            await coordinator._async_connect_and_stream(make_ble_device())

        assert mock_api_client.bluetooth_device_disconnect.await_count == 3
        mock_api_client.bluetooth_device_disconnect.assert_awaited_with(
            expected_addr_int
        )

    await coordinator.async_stop()
