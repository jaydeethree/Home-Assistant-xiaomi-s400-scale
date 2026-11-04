"""Binary sensor platform for Xiaomi S400 Scale."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
    BinarySensorEntityDescription,
)
from homeassistant.const import EntityCategory
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .coordinator import S400ConfigEntry, S400State
from .entity import S400Entity

PARALLEL_UPDATES = 0


@dataclass(frozen=True, kw_only=True)
class S400BinarySensorEntityDescription(BinarySensorEntityDescription):
    """Describe a Xiaomi Scale S400 binary sensor entity."""

    is_on_fn: Callable[[S400State], bool]


BINARY_SENSORS: tuple[S400BinarySensorEntityDescription, ...] = (
    S400BinarySensorEntityDescription(
        key="stable",
        translation_key="stable",
        is_on_fn=lambda s: s.stable,
    ),
    S400BinarySensorEntityDescription(
        key="connected",
        translation_key="connected",
        device_class=BinarySensorDeviceClass.CONNECTIVITY,
        entity_category=EntityCategory.DIAGNOSTIC,
        is_on_fn=lambda s: s.connected,
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: S400ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Xiaomi S400 Scale binary sensors from a config entry."""
    coordinator = entry.runtime_data
    async_add_entities(
        S400BinarySensorEntity(coordinator, entry, description)
        for description in BINARY_SENSORS
    )


class S400BinarySensorEntity(S400Entity, BinarySensorEntity):
    """Representation of a Xiaomi S400 Scale binary sensor."""

    entity_description: S400BinarySensorEntityDescription

    @property
    def is_on(self) -> bool:
        """Return true if the binary sensor is on."""
        return self.entity_description.is_on_fn(self.coordinator.state)
