"""Base entity for Xiaomi S400 Scale."""

from __future__ import annotations

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_ADDRESS
from homeassistant.core import callback
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity import Entity, EntityDescription

from .const import DOMAIN
from .coordinator import S400Coordinator

MANUFACTURER = "Xiaomi"
MODEL_NAME = "Body Composition Scale S400 (MJTZC01YM)"


def get_coordinator(entry: ConfigEntry) -> S400Coordinator:
    """Retrieve the S400Coordinator from runtime_data or hass.data."""
    coordinator = getattr(entry, "runtime_data", None)
    if coordinator is not None:
        return coordinator
    return entry.hass_data[DOMAIN][entry.entry_id] if hasattr(entry, "hass_data") else None  # type: ignore[return-value]


class S400Entity(Entity):
    """Shared base entity for Xiaomi S400 Scale platforms."""

    _attr_has_entity_name = True
    _attr_should_poll = False

    def __init__(
        self,
        coordinator: S400Coordinator,
        entry: ConfigEntry,
        description: EntityDescription,
    ) -> None:
        """Initialize the entity."""
        self.coordinator = coordinator
        self.entity_description = description
        address = entry.data[CONF_ADDRESS].upper()

        self._attr_unique_id = f"{address}_{description.key}"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, address)},
            connections={(dr.CONNECTION_BLUETOOTH, address)},
            name=entry.title,
            manufacturer=MANUFACTURER,
            model=MODEL_NAME,
        )

    async def async_added_to_hass(self) -> None:
        """Subscribe to coordinator updates."""
        await super().async_added_to_hass()
        self.async_on_remove(
            self.coordinator.async_add_listener(self._handle_coordinator_update)
        )

    @callback
    def _handle_coordinator_update(self) -> None:
        """Handle updated data from the coordinator."""
        self.async_write_ha_state()
