"""The Xiaomi S400 Scale integration."""

from __future__ import annotations

from homeassistant.const import CONF_ADDRESS, Platform
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed

from .const import CONF_TOKEN
from .coordinator import S400ConfigEntry, S400Coordinator

PLATFORMS: list[Platform] = [Platform.SENSOR, Platform.BINARY_SENSOR]


async def async_setup_entry(hass: HomeAssistant, entry: S400ConfigEntry) -> bool:
    """Set up Xiaomi S400 Scale from a config entry."""
    address: str = entry.data[CONF_ADDRESS]
    try:
        token = bytes.fromhex(entry.data[CONF_TOKEN])
    except (KeyError, TypeError, ValueError) as exc:
        raise ConfigEntryAuthFailed("Invalid hexadecimal BLE login token") from exc
    if len(token) != 12:
        raise ConfigEntryAuthFailed("BLE login token must be 12 bytes (24 hex chars)")

    coordinator = S400Coordinator(hass, entry, address, token)
    entry.runtime_data = coordinator

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    coordinator.async_start()

    return True


async def async_unload_entry(hass: HomeAssistant, entry: S400ConfigEntry) -> bool:
    """Unload a config entry."""
    if unload_ok := await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        await entry.runtime_data.async_stop()
    return unload_ok
