"""Tests for the Xiaomi S400 Scale config flow."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from bleak.exc import BleakError
import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from homeassistant import config_entries
from homeassistant.const import CONF_ADDRESS, CONF_TOKEN
from homeassistant.core import HomeAssistant
from homeassistant.data_entry_flow import FlowResultType

from custom_components.xiaomi_s400_scale.auth import AuthError, BadTokenError
from custom_components.xiaomi_s400_scale.const import DOMAIN, S400_PRODUCT_IDS
from custom_components.xiaomi_s400_scale.protocol import MIBEACON_SERVICE_UUID

from .conftest import (
    OTHER_MIBEACON_BYTES,
    TEST_ADDRESS,
    TEST_NAME,
    TEST_TOKEN_HEX,
    make_ble_device,
    make_service_info,
    make_session_keys,
)

NEW_TOKEN_HEX = "aabbccddeeff001122334455"


def _make_mock_client(*, connected: bool = True) -> MagicMock:
    client = MagicMock()
    client.is_connected = connected
    client.start_notify = AsyncMock()
    client.disconnect = AsyncMock()
    return client


@pytest.mark.parametrize("product_id", sorted(S400_PRODUCT_IDS))
async def test_bluetooth_discovery_by_product_id(
    hass: HomeAssistant, product_id: int
) -> None:
    """Test Bluetooth discovery recognizes all S400 MiBeacon product IDs."""
    mibeacon = b"\x10\x59" + product_id.to_bytes(2, "little") + b"\x01"
    info = make_service_info(
        name="",
        service_data={MIBEACON_SERVICE_UUID: mibeacon},
    )

    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": config_entries.SOURCE_BLUETOOTH},
        data=info,
    )
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "bluetooth_confirm"

    mock_client = _make_mock_client()
    with (
        patch(
            "custom_components.xiaomi_s400_scale.config_flow.async_ble_device_from_address",
            return_value=make_ble_device(),
        ),
        patch(
            "custom_components.xiaomi_s400_scale.config_flow.establish_connection",
            AsyncMock(return_value=mock_client),
        ),
        patch(
            "custom_components.xiaomi_s400_scale.config_flow.login",
            AsyncMock(return_value=make_session_keys()),
        ),
        patch(
            "custom_components.xiaomi_s400_scale.async_setup_entry",
            AsyncMock(return_value=True),
        ) as mock_setup,
    ):
        result2 = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {CONF_TOKEN: f"  {TEST_TOKEN_HEX.upper()}  "},
        )
        await hass.async_block_till_done()

    assert result2["type"] is FlowResultType.CREATE_ENTRY
    assert result2["title"] == f"Xiaomi S400 Scale ({TEST_ADDRESS[-5:]})"
    assert result2["data"] == {
        CONF_ADDRESS: TEST_ADDRESS,
        CONF_TOKEN: TEST_TOKEN_HEX,
    }
    mock_client.disconnect.assert_awaited_once()
    mock_setup.assert_awaited_once()


async def test_bluetooth_discovery_by_name_and_unsupported_abort(
    hass: HomeAssistant,
) -> None:
    """Test discovery by S400 name and abort on unsupported BLE devices."""
    unsupported = make_service_info(
        name="Xiaomi Thermometer",
        service_data={MIBEACON_SERVICE_UUID: OTHER_MIBEACON_BYTES},
    )
    res_abort = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": config_entries.SOURCE_BLUETOOTH},
        data=unsupported,
    )
    assert res_abort["type"] is FlowResultType.ABORT
    assert res_abort["reason"] == "not_supported"

    by_name = make_service_info(name=TEST_NAME, service_data={})
    res_ok = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": config_entries.SOURCE_BLUETOOTH},
        data=by_name,
    )
    assert res_ok["type"] is FlowResultType.FORM
    assert res_ok["step_id"] == "bluetooth_confirm"


async def test_bluetooth_discovery_already_configured(hass: HomeAssistant) -> None:
    """Test Bluetooth discovery aborts if the scale MAC is already configured."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id=TEST_ADDRESS,
        data={CONF_ADDRESS: TEST_ADDRESS, CONF_TOKEN: TEST_TOKEN_HEX},
    )
    entry.add_to_hass(hass)

    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": config_entries.SOURCE_BLUETOOTH},
        data=make_service_info(),
    )
    assert result["type"] is FlowResultType.ABORT
    assert result["reason"] == "already_configured"


async def test_user_flow_validation_errors_and_success(hass: HomeAssistant) -> None:
    """Test manual user flow input validation, connection errors, and entry creation."""
    configured_addr = "AA:BB:CC:DD:EE:99"
    existing = MockConfigEntry(
        domain=DOMAIN,
        unique_id=configured_addr,
        data={CONF_ADDRESS: configured_addr, CONF_TOKEN: TEST_TOKEN_HEX},
    )
    existing.add_to_hass(hass)

    discovered = [
        make_service_info(address=configured_addr),
        make_service_info(address=TEST_ADDRESS, name=TEST_NAME),
    ]

    with patch(
        "custom_components.xiaomi_s400_scale.config_flow.async_discovered_service_info",
        return_value=discovered,
    ):
        result = await hass.config_entries.flow.async_init(
            DOMAIN,
            context={"source": config_entries.SOURCE_USER},
        )
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "user"

    # 1. Invalid MAC + invalid token
    result = await hass.config_entries.flow.async_configure(
        result["flow_id"],
        {CONF_ADDRESS: "invalid-mac", CONF_TOKEN: "not-hex"},
    )
    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {
        CONF_ADDRESS: "invalid_mac",
        CONF_TOKEN: "invalid_token",
    }

    # 2. No connectable BLEDevice -> cannot_connect
    with patch(
        "custom_components.xiaomi_s400_scale.config_flow.async_ble_device_from_address",
        return_value=None,
    ):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {CONF_ADDRESS: TEST_ADDRESS.lower(), CONF_TOKEN: TEST_TOKEN_HEX},
        )
    assert result["errors"] == {"base": "cannot_connect"}

    # 3. BadTokenError -> invalid_auth on token field (also exercises disconnect error suppression)
    mock_client = _make_mock_client(connected=True)
    mock_client.disconnect.side_effect = BleakError("already closed")
    with (
        patch(
            "custom_components.xiaomi_s400_scale.config_flow.async_ble_device_from_address",
            return_value=make_ble_device(),
        ),
        patch(
            "custom_components.xiaomi_s400_scale.config_flow.establish_connection",
            AsyncMock(return_value=mock_client),
        ),
        patch(
            "custom_components.xiaomi_s400_scale.config_flow.login",
            AsyncMock(side_effect=BadTokenError("HMAC mismatch")),
        ),
    ):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {CONF_ADDRESS: TEST_ADDRESS, CONF_TOKEN: TEST_TOKEN_HEX},
        )
    assert result["errors"] == {CONF_TOKEN: "invalid_auth"}

    # 4. AuthError / TimeoutError -> cannot_connect
    mock_client_disconnected = _make_mock_client(connected=False)
    with (
        patch(
            "custom_components.xiaomi_s400_scale.config_flow.async_ble_device_from_address",
            return_value=make_ble_device(),
        ),
        patch(
            "custom_components.xiaomi_s400_scale.config_flow.establish_connection",
            AsyncMock(return_value=mock_client_disconnected),
        ),
        patch(
            "custom_components.xiaomi_s400_scale.config_flow.login",
            AsyncMock(side_effect=AuthError("timeout")),
        ),
    ):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {CONF_ADDRESS: TEST_ADDRESS, CONF_TOKEN: TEST_TOKEN_HEX},
        )
    assert result["errors"] == {"base": "cannot_connect"}

    # 5. Unexpected RuntimeError -> unknown
    with (
        patch(
            "custom_components.xiaomi_s400_scale.config_flow.async_ble_device_from_address",
            return_value=make_ble_device(),
        ),
        patch(
            "custom_components.xiaomi_s400_scale.config_flow.establish_connection",
            AsyncMock(side_effect=RuntimeError("boom")),
        ),
    ):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {CONF_ADDRESS: TEST_ADDRESS, CONF_TOKEN: TEST_TOKEN_HEX},
        )
    assert result["errors"] == {"base": "unknown"}

    # 6. Success using discovered device name and hyphenated MAC/token input
    ok_client = _make_mock_client()

    async def _establish_and_invoke_cb(*args: object, **kwargs: object) -> MagicMock:
        disc_cb = kwargs["disconnected_callback"]
        ble_cb = kwargs["ble_device_callback"]
        assert callable(ble_cb)
        ble_cb()
        assert callable(disc_cb)
        disc_cb(ok_client)
        return ok_client

    with (
        patch(
            "custom_components.xiaomi_s400_scale.config_flow.async_discovered_service_info",
            return_value=discovered,
        ),
        patch(
            "custom_components.xiaomi_s400_scale.config_flow.async_ble_device_from_address",
            return_value=make_ble_device(),
        ),
        patch(
            "custom_components.xiaomi_s400_scale.config_flow.establish_connection",
            AsyncMock(side_effect=_establish_and_invoke_cb),
        ),
        patch(
            "custom_components.xiaomi_s400_scale.config_flow.login",
            AsyncMock(return_value=make_session_keys()),
        ),
        patch(
            "custom_components.xiaomi_s400_scale.async_setup_entry",
            AsyncMock(return_value=True),
        ),
    ):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {
                CONF_ADDRESS: TEST_ADDRESS.replace(":", "-"),
                CONF_TOKEN: ":".join(
                    TEST_TOKEN_HEX[i : i + 2] for i in range(0, 24, 2)
                ),
            },
        )
        await hass.async_block_till_done()

    assert result["type"] is FlowResultType.CREATE_ENTRY
    assert result["title"] == TEST_NAME
    assert result["data"] == {
        CONF_ADDRESS: TEST_ADDRESS,
        CONF_TOKEN: TEST_TOKEN_HEX,
    }


async def test_reconfigure_stops_and_restarts_coordinator_on_failure_then_succeeds(
    hass: HomeAssistant,
) -> None:
    """Test reconfigure stops active coordinator before BLE validation and restarts on error."""
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

    result = await entry.start_reconfigure_flow(hass)
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "reconfigure"

    # Validation fails -> coordinator.async_stop() called before, async_start() called in finally
    with (
        patch.object(
            coordinator, "async_stop", wraps=coordinator.async_stop
        ) as spy_stop,
        patch.object(
            coordinator, "async_start", wraps=coordinator.async_start
        ) as spy_start,
        patch(
            "custom_components.xiaomi_s400_scale.config_flow.async_ble_device_from_address",
            return_value=None,
        ),
    ):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {CONF_TOKEN: NEW_TOKEN_HEX},
        )

    assert result["type"] is FlowResultType.FORM
    assert result["errors"] == {"base": "cannot_connect"}
    spy_stop.assert_awaited_once()
    spy_start.assert_called_once()

    # Now validation succeeds -> updates entry data and reloads cleanly
    with (
        patch(
            "custom_components.xiaomi_s400_scale.config_flow.async_ble_device_from_address",
            return_value=make_ble_device(),
        ),
        patch(
            "custom_components.xiaomi_s400_scale.config_flow.establish_connection",
            AsyncMock(return_value=_make_mock_client()),
        ),
        patch(
            "custom_components.xiaomi_s400_scale.config_flow.login",
            AsyncMock(return_value=make_session_keys(bytes.fromhex(NEW_TOKEN_HEX))),
        ),
    ):
        result2 = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {CONF_TOKEN: NEW_TOKEN_HEX},
        )
        await hass.async_block_till_done()

    assert result2["type"] is FlowResultType.ABORT
    assert result2["reason"] == "reconfigure_successful"
    assert entry.data[CONF_TOKEN] == NEW_TOKEN_HEX


async def test_reauth_flow_with_halted_coordinator(hass: HomeAssistant) -> None:
    """Test reauth flow does not restart halted coordinator on failure and updates on success."""
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
    coordinator._auth_failed = True

    result = await entry.start_reauth_flow(hass)
    assert result["type"] is FlowResultType.FORM
    assert result["step_id"] == "reauth_confirm"

    # Validation fails while auth_failed=True -> coordinator must NOT be restarted
    with (
        patch.object(coordinator, "async_start") as spy_start,
        patch(
            "custom_components.xiaomi_s400_scale.config_flow.async_ble_device_from_address",
            return_value=None,
        ),
    ):
        result = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {CONF_TOKEN: NEW_TOKEN_HEX},
        )
    assert result["errors"] == {"base": "cannot_connect"}
    spy_start.assert_not_called()

    # Valid new token -> updates entry and reloads with fresh coordinator
    with (
        patch(
            "custom_components.xiaomi_s400_scale.config_flow.async_ble_device_from_address",
            return_value=make_ble_device(),
        ),
        patch(
            "custom_components.xiaomi_s400_scale.config_flow.establish_connection",
            AsyncMock(return_value=_make_mock_client()),
        ),
        patch(
            "custom_components.xiaomi_s400_scale.config_flow.login",
            AsyncMock(return_value=make_session_keys(bytes.fromhex(NEW_TOKEN_HEX))),
        ),
    ):
        result2 = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {CONF_TOKEN: NEW_TOKEN_HEX},
        )
        await hass.async_block_till_done()

    assert result2["type"] is FlowResultType.ABORT
    assert result2["reason"] == "reauth_successful"
    assert entry.data[CONF_TOKEN] == NEW_TOKEN_HEX
    assert entry.runtime_data.auth_failed is False


async def test_validate_connection_disconnect_timeout(hass: HomeAssistant) -> None:
    """Test a hung client.disconnect() during config flow validation times out cleanly."""
    hung_cancelled = False

    async def _hung_disconnect() -> None:
        nonlocal hung_cancelled
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            hung_cancelled = True
            raise

    hung_client = _make_mock_client(connected=True)
    hung_client.disconnect = AsyncMock(side_effect=_hung_disconnect)

    result = await hass.config_entries.flow.async_init(
        DOMAIN,
        context={"source": config_entries.SOURCE_BLUETOOTH},
        data=make_service_info(),
    )
    assert result["type"] is FlowResultType.FORM

    with (
        patch(
            "custom_components.xiaomi_s400_scale.config_flow.DISCONNECT_TIMEOUT_SECONDS",
            0.01,
        ),
        patch(
            "custom_components.xiaomi_s400_scale.config_flow.async_ble_device_from_address",
            return_value=make_ble_device(),
        ),
        patch(
            "custom_components.xiaomi_s400_scale.config_flow.establish_connection",
            AsyncMock(return_value=hung_client),
        ),
        patch(
            "custom_components.xiaomi_s400_scale.config_flow.login",
            AsyncMock(return_value=make_session_keys()),
        ),
        patch(
            "custom_components.xiaomi_s400_scale.async_setup_entry",
            AsyncMock(return_value=True),
        ),
    ):
        result2 = await hass.config_entries.flow.async_configure(
            result["flow_id"],
            {CONF_TOKEN: TEST_TOKEN_HEX},
        )
        await hass.async_block_till_done()

    assert result2["type"] is FlowResultType.CREATE_ENTRY
    assert hung_cancelled is True
