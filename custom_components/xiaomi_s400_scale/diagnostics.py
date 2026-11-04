"""Diagnostics support for Xiaomi S400 Scale."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

from homeassistant.components.diagnostics import async_redact_data
from homeassistant.core import HomeAssistant

from .const import CONF_TOKEN
from .coordinator import S400ConfigEntry

TO_REDACT = {CONF_TOKEN}


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, entry: S400ConfigEntry
) -> dict[str, Any]:
    """Return diagnostics for a config entry."""
    coordinator = entry.runtime_data
    return {
        "entry": async_redact_data(entry.as_dict(), TO_REDACT),
        "state": asdict(coordinator.state),
        "auth_failed": coordinator.auth_failed,
        "connecting": coordinator.connecting,
        "consecutive_failures": coordinator.consecutive_failures,
        "last_error": coordinator.last_error,
        "last_connect_time": coordinator.last_connect_time,
    }
