"""Config flow for Xiaomi S400 Scale integration."""

from __future__ import annotations

from collections.abc import Mapping
import re
from typing import Any

import voluptuous as vol

from homeassistant.components.bluetooth import (
    BluetoothServiceInfoBleak,
    async_discovered_service_info,
)
from homeassistant.config_entries import ConfigEntry, ConfigFlow, ConfigFlowResult
from homeassistant.const import CONF_ADDRESS
from homeassistant.helpers.device_registry import format_mac
from homeassistant.helpers.selector import (
    SelectOptionDict,
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
)

from .const import CONF_TOKEN, DOMAIN, S400_PRODUCT_IDS
from .protocol import MIBEACON_SERVICE_UUID

MAC_REGEX = re.compile(r"^([0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}$")
HEX_24_REGEX = re.compile(r"^[0-9A-Fa-f]{24}$")


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

        default_name = (
            self._discovery_info.name
            or f"Xiaomi S400 Scale ({self._discovery_info.address[-5:]})"
        )

        if user_input is not None:
            if token := _validate_token(user_input[CONF_TOKEN], errors):
                return self.async_create_entry(
                    title=default_name,
                    data={
                        CONF_ADDRESS: self._discovery_info.address.upper(),
                        CONF_TOKEN: token,
                    },
                )

        schema = vol.Schema(
            {
                vol.Required(CONF_TOKEN): str,
            }
        )
        return self.async_show_form(
            step_id="bluetooth_confirm",
            data_schema=self.add_suggested_values_to_schema(schema, user_input),
            description_placeholders={
                "address": self._discovery_info.address.upper(),
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
            else str
        )

        schema = vol.Schema(
            {
                vol.Required(CONF_ADDRESS): address_selector,
                vol.Required(CONF_TOKEN): str,
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

        if user_input is not None:
            if token := _validate_token(user_input[CONF_TOKEN], errors):
                return self.async_update_reload_and_abort(
                    entry,
                    data_updates={CONF_TOKEN: token},
                )

        schema = vol.Schema({vol.Required(CONF_TOKEN): str})
        suggested_values = (
            user_input
            if user_input is not None
            else ({CONF_TOKEN: default_token} if default_token else None)
        )
        return self.async_show_form(
            step_id=step_id,
            data_schema=self.add_suggested_values_to_schema(schema, suggested_values),
            description_placeholders={
                "address": entry.data[CONF_ADDRESS],
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
