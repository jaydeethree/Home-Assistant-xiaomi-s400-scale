"""Config flow for Xiaomi S400 Scale integration."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
import contextlib
import logging
import re
from typing import Any

from bleak import BleakClient
from bleak.exc import BleakError
from bleak_retry_connector import BleakClientWithServiceCache, establish_connection
import voluptuous as vol

from homeassistant.components.bluetooth import (
    BluetoothServiceInfoBleak,
    async_ble_device_from_address,
    async_discovered_service_info,
)
from homeassistant.config_entries import (
    ConfigEntry,
    ConfigEntryState,
    ConfigFlow,
    ConfigFlowResult,
)
from homeassistant.const import CONF_ADDRESS, CONF_TOKEN
from homeassistant.helpers.device_registry import format_mac
from homeassistant.helpers.selector import (
    SelectOptionDict,
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
)

from .auth import AuthError, BadTokenError, NotifyHub, login
from .const import DOMAIN, S400_PRODUCT_IDS, SETTLE_DELAY_SECONDS
from .protocol import AVDTP, MIBEACON_SERVICE_UUID, UPNP

_LOGGER = logging.getLogger(__name__)

MAC_REGEX = re.compile(r"^([0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}$")
HEX_24_REGEX = re.compile(r"^[0-9A-Fa-f]{24}$")

TEXT_SELECTOR = TextSelector(TextSelectorConfig(type=TextSelectorType.TEXT))
AUTH_NOTIFY_UUIDS = (UPNP, AVDTP)
VALIDATION_TIMEOUT_SECONDS = 35.0


def _clean_token(raw: str) -> str:
    return re.sub(r"[\s:-]", "", raw).lower()


def _clean_mac(raw: str) -> str:
    return format_mac(raw.strip()).upper()


def _validate_token(raw: str, errors: dict[str, str]) -> str | None:
    """Clean and validate a 24-hex-character BLE token."""
    token = _clean_token(raw)
    if not HEX_24_REGEX.match(token):
        errors[CONF_TOKEN] = "invalid_token"
        return None
    return token


def _is_s400_service_info(info: BluetoothServiceInfoBleak) -> bool:
    """Return True if the discovered BLE device matches a Xiaomi S400 Scale."""
    if "s400" in (info.name or "").lower():
        return True
    mibeacon = info.service_data.get(MIBEACON_SERVICE_UUID)
    if mibeacon is not None and len(mibeacon) >= 4:
        product_id = int.from_bytes(mibeacon[2:4], "little")
        if product_id in S400_PRODUCT_IDS:
            return True
    return False


class XiaomiS400ScaleConfigFlow(ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Xiaomi S400 Scale."""

    VERSION = 1

    def __init__(self) -> None:
        """Initialize the config flow."""
        self._discovery_info: BluetoothServiceInfoBleak | None = None

    async def _async_validate_connection(
        self, address: str, token_hex: str
    ) -> str | None:
        """Connect to the scale and verify the Mi Home v2 BLE login token.

        Returns None on success, or an error key ('cannot_connect', 'invalid_auth', or 'unknown').
        """
        ble_device = async_ble_device_from_address(self.hass, address, connectable=True)
        if ble_device is None:
            return "cannot_connect"

        hub: NotifyHub | None = None

        def _on_disconnect(_client: BleakClient) -> None:
            if hub is not None:
                hub.mark_disconnected()

        client: BleakClientWithServiceCache | None = None
        try:
            async with asyncio.timeout(VALIDATION_TIMEOUT_SECONDS):
                client = await establish_connection(
                    BleakClientWithServiceCache,
                    ble_device,
                    address,
                    disconnected_callback=_on_disconnect,
                    max_attempts=3,
                    ble_device_callback=lambda: (
                        async_ble_device_from_address(
                            self.hass, address, connectable=True
                        )
                        or ble_device
                    ),
                )
                hub = NotifyHub()
                if not client.is_connected:
                    hub.mark_disconnected()
                for uuid in AUTH_NOTIFY_UUIDS:
                    await client.start_notify(uuid, hub.make_callback(uuid))
                await asyncio.sleep(SETTLE_DELAY_SECONDS)
                await login(client, bytes.fromhex(token_hex), hub)
        except BadTokenError as exc:
            _LOGGER.debug("BLE token validation failed on %s: %s", address, exc)
            return "invalid_auth"
        except (AuthError, BleakError, TimeoutError, EOFError, OSError) as exc:
            _LOGGER.debug("BLE connection test failed on %s: %s", address, exc)
            return "cannot_connect"
        except Exception:
            _LOGGER.exception(
                "Unexpected error validating BLE connection on %s", address
            )
            return "unknown"
        finally:
            if client is not None and client.is_connected:
                with contextlib.suppress(BleakError, TimeoutError, EOFError, OSError):
                    await client.disconnect()
        return None

    async def async_step_bluetooth(
        self, discovery_info: BluetoothServiceInfoBleak
    ) -> ConfigFlowResult:
        """Handle the bluetooth discovery step."""
        if not _is_s400_service_info(discovery_info):
            return self.async_abort(reason="not_supported")
        await self.async_set_unique_id(discovery_info.address.upper())
        self._abort_if_unique_id_configured()
        self._discovery_info = discovery_info
        self.context["title_placeholders"] = {
            "name": discovery_info.name
            or f"Xiaomi S400 Scale ({discovery_info.address[-5:]})"
        }
        return await self.async_step_bluetooth_confirm()

    async def async_step_bluetooth_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Prompt for the 12-byte BLE login token after Bluetooth discovery."""
        assert self._discovery_info is not None
        errors: dict[str, str] = {}
        address = self._discovery_info.address.upper()

        default_name = (
            self._discovery_info.name or f"Xiaomi S400 Scale ({address[-5:]})"
        )

        if user_input is not None and (
            token := _validate_token(user_input[CONF_TOKEN], errors)
        ):
            if err := await self._async_validate_connection(address, token):
                errors[CONF_TOKEN if err == "invalid_auth" else "base"] = err
            else:
                return self.async_create_entry(
                    title=default_name,
                    data={
                        CONF_ADDRESS: address,
                        CONF_TOKEN: token,
                    },
                )

        schema = vol.Schema(
            {
                vol.Required(CONF_TOKEN): TEXT_SELECTOR,
            }
        )
        return self.async_show_form(
            step_id="bluetooth_confirm",
            data_schema=self.add_suggested_values_to_schema(schema, user_input),
            description_placeholders={
                "address": address,
                "name": default_name,
            },
            errors=errors,
        )

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle the manual user step."""
        errors: dict[str, str] = {}

        options: list[SelectOptionDict] = []
        discovered_names: dict[str, str] = {}
        current_ids = self._async_current_ids()
        for info in async_discovered_service_info(self.hass, connectable=True):
            addr = info.address.upper()
            if addr in current_ids:
                continue
            if _is_s400_service_info(info):
                if info.name:
                    discovered_names[addr] = info.name
                label = f"{info.name} ({addr})" if info.name else addr
                options.append(SelectOptionDict(value=addr, label=label))

        if user_input is not None:
            address = _clean_mac(user_input[CONF_ADDRESS])
            if not MAC_REGEX.match(address):
                errors[CONF_ADDRESS] = "invalid_mac"
            token = _validate_token(user_input[CONF_TOKEN], errors)

            if not errors and token is not None:
                await self.async_set_unique_id(address, raise_on_progress=False)
                self._abort_if_unique_id_configured()
                if err := await self._async_validate_connection(address, token):
                    errors[CONF_TOKEN if err == "invalid_auth" else "base"] = err
                else:
                    title = (
                        discovered_names.get(address)
                        or f"Xiaomi S400 Scale ({address[-5:]})"
                    )
                    return self.async_create_entry(
                        title=title,
                        data={
                            CONF_ADDRESS: address,
                            CONF_TOKEN: token,
                        },
                    )

        address_selector: Any = (
            SelectSelector(
                SelectSelectorConfig(
                    options=options,
                    custom_value=True,
                    mode=SelectSelectorMode.DROPDOWN,
                )
            )
            if options
            else TEXT_SELECTOR
        )

        schema = vol.Schema(
            {
                vol.Required(CONF_ADDRESS): address_selector,
                vol.Required(CONF_TOKEN): TEXT_SELECTOR,
            }
        )
        return self.async_show_form(
            step_id="user",
            data_schema=self.add_suggested_values_to_schema(schema, user_input),
            errors=errors,
        )

    async def _async_step_token_update(
        self,
        step_id: str,
        entry: ConfigEntry,
        user_input: dict[str, Any] | None,
        default_token: str = "",
    ) -> ConfigFlowResult:
        """Shared handler for updating the BLE login token (reauth and reconfigure)."""
        errors: dict[str, str] = {}
        address = entry.data[CONF_ADDRESS]

        if user_input is not None and (
            token := _validate_token(user_input[CONF_TOKEN], errors)
        ):
            coordinator = (
                entry.runtime_data
                if entry.state is ConfigEntryState.LOADED
                and hasattr(entry, "runtime_data")
                else None
            )
            restart_coordinator = coordinator is not None
            try:
                if coordinator is not None:
                    await coordinator.async_stop()
                err = await self._async_validate_connection(address, token)
                if err is None:
                    restart_coordinator = False
                    return self.async_update_reload_and_abort(
                        entry,
                        data_updates={CONF_TOKEN: token},
                    )
            finally:
                if (
                    restart_coordinator
                    and coordinator is not None
                    and entry.state is ConfigEntryState.LOADED
                    and not self.hass.is_stopping
                    and not coordinator.auth_failed
                ):
                    coordinator.async_start()
            errors[CONF_TOKEN if err == "invalid_auth" else "base"] = err

        schema = vol.Schema({vol.Required(CONF_TOKEN): TEXT_SELECTOR})
        suggested_values = (
            user_input
            if user_input is not None
            else ({CONF_TOKEN: default_token} if default_token else None)
        )
        return self.async_show_form(
            step_id=step_id,
            data_schema=self.add_suggested_values_to_schema(schema, suggested_values),
            description_placeholders={
                "address": address,
            },
            errors=errors,
        )

    async def async_step_reauth(
        self, entry_data: Mapping[str, Any]
    ) -> ConfigFlowResult:
        """Handle reauthorization when the scale's BLE token rotates."""
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Confirm new BLE login token."""
        return await self._async_step_token_update(
            "reauth_confirm", self._get_reauth_entry(), user_input
        )

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle reconfiguration to update the BLE login token."""
        entry = self._get_reconfigure_entry()
        return await self._async_step_token_update(
            "reconfigure",
            entry,
            user_input,
            default_token=entry.data.get(CONF_TOKEN, ""),
        )
