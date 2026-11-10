"""Diagnostics support for Xiaomi S400 Scale."""

from __future__ import annotations

from dataclasses import asdict
import re
import time
from typing import Any

from homeassistant.components import bluetooth
from homeassistant.components.diagnostics import REDACTED, async_redact_data
from homeassistant.const import CONF_ADDRESS, CONF_TOKEN
from homeassistant.core import HomeAssistant

from .coordinator import S400ConfigEntry
from .protocol import MIBEACON_SERVICE_UUID

TO_REDACT = {CONF_ADDRESS, CONF_TOKEN, "discovery_keys", "title", "unique_id"}


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
        mibeacon = service_info.service_data.get(MIBEACON_SERVICE_UUID)
        product_id: str | None = None
        frame_counter: int | None = None
        if mibeacon is not None and len(mibeacon) >= 4:
            product_id = f"0x{int.from_bytes(mibeacon[2:4], 'little'):04x}"
            if len(mibeacon) >= 5:
                frame_counter = mibeacon[4]
        bt_info = {
            "name": service_info.name,
            "connectable": service_info.connectable,
            "rssi": service_info.rssi,
            "source": service_info.source,
            "seconds_since_last_advertisement": round(
                max(0.0, time.monotonic() - service_info.time), 1
            ),
            "mibeacon_product_id": product_id,
            "mibeacon_frame_counter": frame_counter,
        }

    def _redact_address(text: str | None) -> str | None:
        if text is None:
            return None
        return re.sub(
            re.escape(coordinator.address),
            REDACTED,
            text,
            flags=re.IGNORECASE,
        )

    return {
        "entry": async_redact_data(entry.as_dict(), TO_REDACT),
        "state": asdict(coordinator.state),
        "auth_failed": coordinator.auth_failed,
        "session_active": coordinator.session_active,
        "consecutive_failures": coordinator.consecutive_failures,
        "cooldown_remaining": round(coordinator.cooldown_remaining, 1),
        "last_error": _redact_address(coordinator.last_error),
        "last_connect_time": coordinator.last_connect_time,
        "total_sessions": coordinator.total_sessions,
        "last_session_duration_s": coordinator.last_session_duration_s,
        "longest_session_duration_s": coordinator.longest_session_duration_s,
        "last_disconnect_reason": _redact_address(coordinator.last_disconnect_reason),
        "bluetooth": bt_info,
    }
