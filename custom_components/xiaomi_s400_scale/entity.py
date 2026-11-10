"""Base entity for Xiaomi S400 Scale."""

from __future__ import annotations

from homeassistant.core import callback
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity import Entity, EntityDescription

from .const import DOMAIN, MANUFACTURER, MODEL_ID, MODEL_NAME
from .coordinator import S400Coordinator


class S400Entity(Entity):
    """Shared base entity for Xiaomi S400 Scale platforms."""

    _attr_has_entity_name = True
    _attr_should_poll = False

    def __init__(
        self,
        coordinator: S400Coordinator,
        description: EntityDescription,
    ) -> None:
        """Initialize the entity."""
        self.coordinator = coordinator
        self.entity_description = description
        self._last_state_key: tuple[bool, object] | None = None
        address = coordinator.address

        self._attr_unique_id = f"{address}_{description.key}"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, address)},
            connections={(dr.CONNECTION_BLUETOOTH, address)},
            name=coordinator.entry.title,
            manufacturer=MANUFACTURER,
            model=MODEL_NAME,
            model_id=MODEL_ID,
        )

    @property
    def available(self) -> bool:
        """Return False if authentication has halted pending reauth."""
        return not self.coordinator.auth_failed

    async def async_added_to_hass(self) -> None:
        """Subscribe to coordinator updates."""
        await super().async_added_to_hass()
        self._last_state_key = (
            self.available,
            self.state,
        )
        self.async_on_remove(
            self.coordinator.async_add_listener(self._handle_coordinator_update)
        )

    @callback
    def _handle_coordinator_update(self) -> None:
        """Handle updated data from the coordinator."""
        state_key = (
            self.available,
            self.state,
        )
        if state_key == self._last_state_key:
            return
        self._last_state_key = state_key
        self.async_write_ha_state()
