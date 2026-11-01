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
from homeassistant.config_entries import ConfigFlow, ConfigFlowResult
from homeassistant.const import CONF_ADDRESS
from homeassistant.helpers.device_registry import format_mac
from homeassistant.helpers.selector import (
    SelectOptionDict,
    SelectSelector,
    SelectSelectorConfig,
    SelectSelectorMode,
)

from .const import CONF_TOKEN, DOMAIN
from .protocol import MIBEACON_SERVICE_UUID

MAC_REGEX = re.compile(r"^([0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}$")
HEX_24_REGEX = re.compile(r"^[0-9A-Fa-f]{24}$")


def _clean_token(raw: str) -> str:
    return raw.strip().replace(" ", "").lower()


def _clean_mac(raw: str) -> str:
    return format_mac(raw.strip()).upper()


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
            token = _clean_token(user_input[CONF_TOKEN])
            if not HEX_24_REGEX.match(token):
                errors[CONF_TOKEN] = "invalid_token"
            else:
                return self.async_create_entry(
                    title=default_name,
                    data={
                        CONF_ADDRESS: self._discovery_info.address.upper(),
                        CONF_TOKEN: token,
                    },
                )

        return self.async_show_form(
            step_id="bluetooth_confirm",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_TOKEN): str,
                }
            ),
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

        if user_input is not None:
            address = _clean_mac(user_input[CONF_ADDRESS])
            token = _clean_token(user_input[CONF_TOKEN])

            if not MAC_REGEX.match(address):
                errors[CONF_ADDRESS] = "invalid_mac"
            if not HEX_24_REGEX.match(token):
                errors[CONF_TOKEN] = "invalid_token"

            if not errors:
                await self.async_set_unique_id(address, raise_on_progress=False)
                self._abort_if_unique_id_configured()
                return self.async_create_entry(
                    title=f"Xiaomi S400 Scale ({address[-5:]})",
                    data={
                        CONF_ADDRESS: address,
                        CONF_TOKEN: token,
                    },
                )

        options: list[SelectOptionDict] = []
        current_ids = self._async_current_ids()
        for info in async_discovered_service_info(self.hass, connectable=True):
            addr = info.address.upper()
            if addr in current_ids:
                continue
            if (
                MIBEACON_SERVICE_UUID in info.service_uuids
                or "s400" in (info.name or "").lower()
                or "scale" in (info.name or "").lower()
            ):
                label = f"{info.name} ({addr})" if info.name else addr
                options.append(SelectOptionDict(value=addr, label=label))

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

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_ADDRESS): address_selector,
                    vol.Required(CONF_TOKEN): str,
                }
            ),
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
        reauth_entry = self._get_reauth_entry()
        errors: dict[str, str] = {}

        if user_input is not None:
            token = _clean_token(user_input[CONF_TOKEN])
            if not HEX_24_REGEX.match(token):
                errors[CONF_TOKEN] = "invalid_token"
            else:
                return self.async_update_reload_and_abort(
                    reauth_entry,
                    data_updates={CONF_TOKEN: token},
                )

        return self.async_show_form(
            step_id="reauth_confirm",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_TOKEN): str,
                }
            ),
            description_placeholders={
                "address": reauth_entry.data[CONF_ADDRESS],
            },
            errors=errors,
        )

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle reconfiguration to update the BLE login token."""
        reconfigure_entry = self._get_reconfigure_entry()
        errors: dict[str, str] = {}

        if user_input is not None:
            token = _clean_token(user_input[CONF_TOKEN])
            if not HEX_24_REGEX.match(token):
                errors[CONF_TOKEN] = "invalid_token"
            else:
                return self.async_update_reload_and_abort(
                    reconfigure_entry,
                    data_updates={CONF_TOKEN: token},
                )

        return self.async_show_form(
            step_id="reconfigure",
            data_schema=vol.Schema(
                {
                    vol.Required(
                        CONF_TOKEN,
                        default=reconfigure_entry.data.get(CONF_TOKEN, ""),
                    ): str,
                }
            ),
            description_placeholders={
                "address": reconfigure_entry.data[CONF_ADDRESS],
            },
            errors=errors,
        )
