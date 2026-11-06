"""Diagnostics support for Xiaomi S400 Scale."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from homeassistant.components import bluetooth
from homeassistant.components.diagnostics import async_redact_data
from homeassistant.const import CONF_ADDRESS
from homeassistant.core import HomeAssistant

from .const import CONF_TOKEN
from .coordinator import S400ConfigEntry

TO_REDACT = {CONF_ADDRESS, CONF_TOKEN, "title", "unique_id"}


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, entry: S400ConfigEntry
) -> dict[str, Any]:
    """Return diagnostics for a config entry."""
    coordinator = entry.runtime_data
    service_info = bluetooth.async_last_service_info(
        hass, coordinator.address, connectable=True
    )
    bt_info: dict[str, Any] | None = None
    if service_info is not None:
        bt_info = {
            "connectable": service_info.connectable,
            "rssi": service_info.rssi,
            "source": service_info.source,
            "time": service_info.time,
        }

    return {
        "entry": async_redact_data(entry.as_dict(), TO_REDACT),
        "state": asdict(coordinator.state),
        "auth_failed": coordinator.auth_failed,
        "connecting": coordinator.connecting,
        "consecutive_failures": coordinator.consecutive_failures,
        "cooldown_remaining": round(coordinator.cooldown_remaining, 1),
        "last_error": coordinator.last_error,
        "last_connect_time": coordinator.last_connect_time,
        "bluetooth": bt_info,
    }
