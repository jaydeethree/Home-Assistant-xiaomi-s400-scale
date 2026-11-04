"""The Xiaomi S400 Scale integration."""

from __future__ import annotations

from homeassistant.const import CONF_ADDRESS, Platform
from homeassistant.core import HomeAssistant

from .const import CONF_TOKEN
from .coordinator import S400ConfigEntry, S400Coordinator

PLATFORMS: list[Platform] = [Platform.SENSOR, Platform.BINARY_SENSOR]


async def async_setup_entry(hass: HomeAssistant, entry: S400ConfigEntry) -> bool:
    """Set up Xiaomi S400 Scale from a config entry."""
    address: str = entry.data[CONF_ADDRESS]
    token = bytes.fromhex(entry.data[CONF_TOKEN])

    coordinator = S400Coordinator(hass, entry, address, token)
    entry.runtime_data = coordinator

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    coordinator.async_start()
    entry.async_on_unload(coordinator.async_stop)

    return True


async def async_unload_entry(hass: HomeAssistant, entry: S400ConfigEntry) -> bool:
    """Unload a config entry."""
    return await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
