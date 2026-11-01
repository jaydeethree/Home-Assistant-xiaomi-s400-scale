"""Binary sensor platform for Xiaomi S400 Scale."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
    BinarySensorEntityDescription,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_ADDRESS, EntityCategory
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .coordinator import S400Coordinator, S400State


@dataclass(frozen=True, kw_only=True)
class S400BinarySensorEntityDescription(BinarySensorEntityDescription):
    """Describe a Xiaomi S400 Scale binary sensor entity."""

    is_on_fn: Callable[[S400State], bool]


BINARY_SENSORS: tuple[S400BinarySensorEntityDescription, ...] = (
    S400BinarySensorEntityDescription(
        key="stable",
        translation_key="stable",
        name="Weight Stable",
        icon="mdi:check-circle-outline",
        is_on_fn=lambda s: s.stable,
    ),
    S400BinarySensorEntityDescription(
        key="connected",
        translation_key="connected",
        name="Connected",
        device_class=BinarySensorDeviceClass.CONNECTIVITY,
        entity_category=EntityCategory.DIAGNOSTIC,
        is_on_fn=lambda s: s.connected,
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Xiaomi S400 Scale binary sensors from a config entry."""
    coordinator: S400Coordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        S400BinarySensorEntity(coordinator, entry, description)
        for description in BINARY_SENSORS
    )


class S400BinarySensorEntity(BinarySensorEntity):
    """Representation of a Xiaomi S400 Scale binary sensor."""

    entity_description: S400BinarySensorEntityDescription
    _attr_has_entity_name = True
    _attr_should_poll = False

    def __init__(
        self,
        coordinator: S400Coordinator,
        entry: ConfigEntry,
        description: S400BinarySensorEntityDescription,
    ) -> None:
        """Initialize the binary sensor."""
        self.coordinator = coordinator
        self.entity_description = description
        address = entry.data[CONF_ADDRESS].upper()

        self._attr_unique_id = f"{address}_{description.key}"
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, address)},
            connections={(dr.CONNECTION_BLUETOOTH, address)},
            name=entry.title,
            manufacturer="Xiaomi",
            model="Body Composition Scale S400 (MJTZC01YM)",
        )

    async def async_added_to_hass(self) -> None:
        """Subscribe to coordinator updates."""
        await super().async_added_to_hass()
        self.async_on_remove(
            self.coordinator.async_add_listener(self._handle_coordinator_update)
        )

    @callback
    def _handle_coordinator_update(self) -> None:
        self.async_write_ha_state()

    @property
    def is_on(self) -> bool:
        """Return true if the binary sensor is on."""
        return self.entity_description.is_on_fn(self.coordinator.state)
