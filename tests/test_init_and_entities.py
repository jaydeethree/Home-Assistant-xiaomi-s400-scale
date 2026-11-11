"""Tests for integration setup, entities, state restoration, and diagnostics."""

from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from pytest_homeassistant_custom_component.common import (
    MockConfigEntry,
    mock_restore_cache_with_extra_data,
)

from homeassistant.components.diagnostics import REDACTED
from homeassistant.components.sensor import SensorExtraStoredData
from homeassistant.config_entries import ConfigEntryState
from homeassistant.const import (
    CONF_ADDRESS,
    CONF_TOKEN,
    EVENT_HOMEASSISTANT_STOP,
    EVENT_STATE_CHANGED,
    STATE_OFF,
    STATE_ON,
    STATE_UNAVAILABLE,
    STATE_UNKNOWN,
)
from homeassistant.core import Event, EventStateChangedData, HomeAssistant, State
from homeassistant.helpers import device_registry as dr, entity_registry as er

from custom_components.xiaomi_s400_scale.const import (
    DOMAIN,
    MANUFACTURER,
    MODEL_ID,
    MODEL_NAME,
)
from custom_components.xiaomi_s400_scale.diagnostics import (
    async_get_config_entry_diagnostics,
)
from custom_components.xiaomi_s400_scale.event import EVENT_MEASUREMENT_COMPLETED
from custom_components.xiaomi_s400_scale.protocol import FINAL_CSV_FIELD_COUNT

from .conftest import (
    REAL_FINAL_PLAINTEXT,
    REAL_LIVE_PLAINTEXT,
    TEST_ADDRESS,
    TEST_NAME,
    TEST_SOURCE,
    TEST_TOKEN_HEX,
    encrypt_cmtp_message,
    make_service_info,
    make_session_keys,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
COMPONENT_DIR = REPO_ROOT / "custom_components" / "xiaomi_s400_scale"


def test_manifest_hacs_and_translations_consistency() -> None:
    """Test manifest.json, hacs.json, icons.json, and strings/en.json are valid and synced."""
    manifest = json.loads((COMPONENT_DIR / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["domain"] == DOMAIN
    assert manifest["config_flow"] is True
    # Hassfest requires domain, name, then alphabetical keys
    keys = list(manifest.keys())
    assert keys[:2] == ["domain", "name"]
    assert keys[2:] == sorted(keys[2:])

    hacs = json.loads((REPO_ROOT / "hacs.json").read_text(encoding="utf-8"))
    assert hacs["name"] == "Xiaomi S400 Scale"
    assert "homeassistant" in hacs

    strings = json.loads((COMPONENT_DIR / "strings.json").read_text(encoding="utf-8"))
    en_json = json.loads(
        (COMPONENT_DIR / "translations" / "en.json").read_text(encoding="utf-8")
    )
    assert strings == en_json

    icons = json.loads((COMPONENT_DIR / "icons.json").read_text(encoding="utf-8"))
    assert set(icons["entity"]["sensor"]) == set(strings["entity"]["sensor"])
    assert "measurement" in icons["entity"]["event"]


@pytest.mark.parametrize("bad_token", ["not_hex_at_all!", "00112233"])
async def test_setup_invalid_token_triggers_auth_failed(
    hass: HomeAssistant, bad_token: str
) -> None:
    """Test invalid or wrong-length token in entry.data raises ConfigEntryAuthFailed."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id=TEST_ADDRESS,
        title=TEST_NAME,
        data={CONF_ADDRESS: TEST_ADDRESS, CONF_TOKEN: bad_token},
    )
    entry.add_to_hass(hass)

    assert not await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    assert entry.state is ConfigEntryState.SETUP_ERROR


async def test_setup_stop_event_and_unload(hass: HomeAssistant) -> None:
    """Test setup registers entities/device, handles HA stop event, and unloads cleanly."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id=TEST_ADDRESS,
        title=TEST_NAME,
        data={CONF_ADDRESS: TEST_ADDRESS, CONF_TOKEN: TEST_TOKEN_HEX},
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    assert entry.state is ConfigEntryState.LOADED
    coordinator = entry.runtime_data

    dev_reg = dr.async_get(hass)
    devices = dr.async_entries_for_config_entry(dev_reg, entry.entry_id)
    assert len(devices) == 1
    device = devices[0]
    assert (DOMAIN, TEST_ADDRESS) in device.identifiers
    assert device.manufacturer == MANUFACTURER
    assert device.model == MODEL_NAME
    assert device.model_id == MODEL_ID
    assert (dr.CONNECTION_BLUETOOTH, TEST_ADDRESS) in device.connections

    ent_reg = er.async_get(hass)
    entries = er.async_entries_for_config_entry(ent_reg, entry.entry_id)
    # 6 sensors + 2 binary_sensors + 1 event = 9 entities
    assert len(entries) == 9

    with patch.object(
        coordinator, "async_stop", AsyncMock(wraps=coordinator.async_stop)
    ) as spy_stop:
        hass.bus.async_fire(EVENT_HOMEASSISTANT_STOP)
        await hass.async_block_till_done()
        spy_stop.assert_awaited_once()

    assert await hass.config_entries.async_unload(entry.entry_id)
    await hass.async_block_till_done()
    unloaded = hass.config_entries.async_get_entry(entry.entry_id)
    assert unloaded is not None
    assert unloaded.state is ConfigEntryState.NOT_LOADED


async def test_entities_live_final_event_ordering_and_availability(
    hass: HomeAssistant,
) -> None:
    """Test live/final sensor updates, sensor-before-event ordering, repeat readings, and availability."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id=TEST_ADDRESS,
        title="S400",
        data={CONF_ADDRESS: TEST_ADDRESS, CONF_TOKEN: TEST_TOKEN_HEX},
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    coordinator = entry.runtime_data
    keys = make_session_keys()

    assert hass.states.get("sensor.s400_weight").state == STATE_UNKNOWN  # type: ignore[union-attr]
    assert hass.states.get("binary_sensor.s400_connected").state == STATE_OFF  # type: ignore[union-attr]
    assert hass.states.get("binary_sensor.s400_weight_stable").state == STATE_OFF  # type: ignore[union-attr]
    assert hass.states.get("event.s400_measurement").state == STATE_UNKNOWN  # type: ignore[union-attr]

    # Simulate connected + live weight frame
    coordinator.state.connected = True
    coordinator._async_notify_listeners()
    coordinator._handle_full_frame(
        keys, encrypt_cmtp_message(keys, REAL_LIVE_PLAINTEXT, iteration=1)
    )
    await hass.async_block_till_done()

    assert hass.states.get("binary_sensor.s400_connected").state == STATE_ON  # type: ignore[union-attr]
    assert hass.states.get("sensor.s400_live_weight").state == "72.8"  # type: ignore[union-attr]
    assert hass.states.get("binary_sensor.s400_weight_stable").state == STATE_ON  # type: ignore[union-attr]

    # Record sensor state at the exact moment event.s400_measurement fires
    observed_weights_on_event: list[str] = []

    def _on_state_changed(event: Event[EventStateChangedData]) -> None:
        if event.data["entity_id"] == "event.s400_measurement":
            weight_state = hass.states.get("sensor.s400_weight")
            assert weight_state is not None
            observed_weights_on_event.append(weight_state.state)

    unsub = hass.bus.async_listen(EVENT_STATE_CHANGED, _on_state_changed)
    try:
        coordinator._handle_full_frame(
            keys, encrypt_cmtp_message(keys, REAL_FINAL_PLAINTEXT, iteration=2)
        )
        await hass.async_block_till_done()

        assert observed_weights_on_event == ["72.8"]
        assert hass.states.get("sensor.s400_weight").state == "72.8"  # type: ignore[union-attr]
        assert hass.states.get("sensor.s400_impedance").state == "538.0"  # type: ignore[union-attr]
        assert (
            hass.states.get("sensor.s400_low_frequency_impedance").state == "494.0"  # type: ignore[union-attr]
        )
        assert hass.states.get("sensor.s400_profile_id").state == "1"  # type: ignore[union-attr]
        event_state_1 = hass.states.get("event.s400_measurement")
        assert event_state_1 is not None
        assert event_state_1.attributes["event_type"] == EVENT_MEASUREMENT_COMPLETED
        assert event_state_1.attributes["weight_kg"] == 72.8
        assert event_state_1.attributes["impedance_ohm"] == 538.0
        assert event_state_1.attributes["impedance_low_ohm"] == 494.0
        assert event_state_1.attributes["profile_id"] == 1

        # Repeat identical weight reading with a new device_timestamp must fire event again
        fields = ["0"] * FINAL_CSV_FIELD_COUNT
        fields[2] = "1"
        fields[3] = "728"
        fields[4] = "0"
        fields[6] = "1794342300"
        fields[30] = "5380"
        fields[31] = "4940"
        repeat_pt = bytes(12) + b"\xa0" + ",".join(fields).encode("ascii")
        coordinator._handle_full_frame(
            keys, encrypt_cmtp_message(keys, repeat_pt, iteration=3)
        )
        await hass.async_block_till_done()

        assert len(observed_weights_on_event) == 2
    finally:
        unsub()

    # Halting auth marks all entities unavailable
    coordinator._auth_failed = True
    coordinator._async_notify_listeners()
    await hass.async_block_till_done()

    assert hass.states.get("sensor.s400_weight").state == STATE_UNAVAILABLE  # type: ignore[union-attr]
    assert hass.states.get("binary_sensor.s400_connected").state == STATE_UNAVAILABLE  # type: ignore[union-attr]
    assert hass.states.get("event.s400_measurement").state == STATE_UNAVAILABLE  # type: ignore[union-attr]


async def test_restore_sensor_restores_persisted_measurements(
    hass: HomeAssistant,
) -> None:
    """Test RestoreSensor restores final measurement sensors across HA restarts."""
    restored_ts = datetime(2026, 11, 10, 12, 0, 0, tzinfo=UTC)
    mock_restore_cache_with_extra_data(
        hass,
        [
            (
                State("sensor.s400_weight", "71.4"),
                SensorExtraStoredData(71.4, "kg").as_dict(),
            ),
            (
                State("sensor.s400_impedance", "520.5"),
                SensorExtraStoredData(520.5, "Ω").as_dict(),
            ),
            (
                State("sensor.s400_low_frequency_impedance", "480.2"),
                SensorExtraStoredData(480.2, "Ω").as_dict(),
            ),
            (
                State("sensor.s400_profile_id", "2"),
                SensorExtraStoredData(2, None).as_dict(),
            ),
            (
                State("sensor.s400_last_measurement", restored_ts.isoformat()),
                SensorExtraStoredData(restored_ts, None).as_dict(),
            ),
        ],
    )

    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id=TEST_ADDRESS,
        title="S400",
        data={CONF_ADDRESS: TEST_ADDRESS, CONF_TOKEN: TEST_TOKEN_HEX},
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    assert hass.states.get("sensor.s400_weight").state == "71.4"  # type: ignore[union-attr]
    assert hass.states.get("sensor.s400_impedance").state == "520.5"  # type: ignore[union-attr]
    assert hass.states.get("sensor.s400_low_frequency_impedance").state == "480.2"  # type: ignore[union-attr]
    assert hass.states.get("sensor.s400_profile_id").state == "2"  # type: ignore[union-attr]
    assert hass.states.get("sensor.s400_live_weight").state == STATE_UNKNOWN  # type: ignore[union-attr]
    assert entry.runtime_data.state.last_measurement == restored_ts


async def test_diagnostics_redacts_identifiers_and_mac_addresses(
    hass: HomeAssistant,
) -> None:
    """Test diagnostics output includes MiBeacon details and redacts all MACs and tokens."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id=TEST_ADDRESS,
        title=TEST_NAME,
        data={CONF_ADDRESS: TEST_ADDRESS, CONF_TOKEN: TEST_TOKEN_HEX},
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    coordinator = entry.runtime_data
    coordinator._last_error = f"Failed talking to {TEST_ADDRESS} via {TEST_SOURCE}"
    coordinator._last_disconnect_reason = f"ble_error on {TEST_ADDRESS.lower()}"

    info = make_service_info(source=TEST_SOURCE)
    with (
        patch(
            "custom_components.xiaomi_s400_scale.diagnostics.bluetooth.async_scanner_count",
            return_value=2,
        ),
        patch(
            "custom_components.xiaomi_s400_scale.diagnostics.bluetooth.async_last_service_info",
            return_value=info,
        ),
    ):
        diag = await async_get_config_entry_diagnostics(hass, entry)

    serialized = json.dumps(diag, default=str)
    assert TEST_ADDRESS not in serialized
    assert TEST_ADDRESS.lower() not in serialized
    assert TEST_SOURCE not in serialized
    assert TEST_TOKEN_HEX not in serialized

    assert diag["entry"]["data"][CONF_ADDRESS] == REDACTED
    assert diag["entry"]["data"][CONF_TOKEN] == REDACTED
    assert diag["connectable_scanners"] == 2
    assert diag["bluetooth"]["mibeacon_product_id"] == "0x30d9"
    assert diag["bluetooth"]["mibeacon_frame_counter"] == 22
    assert diag["bluetooth"]["source"] == REDACTED
