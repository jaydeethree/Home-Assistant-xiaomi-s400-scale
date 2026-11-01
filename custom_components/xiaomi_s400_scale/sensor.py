"""Sensor platform for Xiaomi S400 Scale."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from homeassistant.components.sensor import (
    RestoreSensor,
    SensorDeviceClass,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_ADDRESS, UnitOfMass
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .coordinator import S400Coordinator, S400State

UNIT_OHM = "ohm"


@dataclass(frozen=True, kw_only=True)
class S400SensorEntityDescription(SensorEntityDescription):
    """Describe a Xiaomi Scale S400 sensor entity."""

    value_fn: Callable[[S400State], float | None]
    restore_fn: Callable[[S400State, float], None] | None = None
    extra_attrs_fn: Callable[[S400State], dict[str, Any] | None] | None = None


def _final_weight_extra_attrs(state: S400State) -> dict[str, Any] | None:
    attrs: dict[str, Any] = {}
    if state.profile_id is not None:
        attrs["profile_id"] = state.profile_id
    if state.device_timestamp is not None:
        attrs["device_timestamp"] = state.device_timestamp
    return attrs or None


def _restore_final_weight(state: S400State, value: float) -> None:
    if state.final_weight_kg is None:
        state.final_weight_kg = value
    if state.live_weight_kg is None:
        state.live_weight_kg = value


def _restore_impedance(state: S400State, value: float) -> None:
    if state.impedance_ohm is None:
        state.impedance_ohm = value


def _restore_impedance_low(state: S400State, value: float) -> None:
    if state.impedance_low_ohm is None:
        state.impedance_low_ohm = value


SENSORS: tuple[S400SensorEntityDescription, ...] = (
    S400SensorEntityDescription(
        key="live_weight",
        translation_key="live_weight",
        name="Live Weight",
        device_class=SensorDeviceClass.WEIGHT,
        native_unit_of_measurement=UnitOfMass.KILOGRAMS,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        icon="mdi:scale-bathroom",
        value_fn=lambda s: s.live_weight_kg,
    ),
    S400SensorEntityDescription(
        key="weight",
        translation_key="weight",
        name="Weight",
        device_class=SensorDeviceClass.WEIGHT,
        native_unit_of_measurement=UnitOfMass.KILOGRAMS,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        icon="mdi:scale-bathroom",
        value_fn=lambda s: s.final_weight_kg,
        restore_fn=_restore_final_weight,
        extra_attrs_fn=_final_weight_extra_attrs,
    ),
    S400SensorEntityDescription(
        key="impedance",
        translation_key="impedance",
        name="Impedance",
        native_unit_of_measurement=UNIT_OHM,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        icon="mdi:omega",
        value_fn=lambda s: s.impedance_ohm,
        restore_fn=_restore_impedance,
    ),
    S400SensorEntityDescription(
        key="impedance_low",
        translation_key="impedance_low",
        name="Low-Frequency Impedance",
        native_unit_of_measurement=UNIT_OHM,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        icon="mdi:omega",
        value_fn=lambda s: s.impedance_low_ohm,
        restore_fn=_restore_impedance_low,
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Xiaomi S400 Scale sensors from a config entry."""
    coordinator: S400Coordinator = hass.data[DOMAIN][entry.entry_id]
    async_add_entities(
        S400SensorEntity(coordinator, entry, description) for description in SENSORS
    )


class S400SensorEntity(RestoreSensor):
    """Representation of a Xiaomi S400 Scale sensor."""

    entity_description: S400SensorEntityDescription
    _attr_has_entity_name = True
    _attr_should_poll = False

    def __init__(
        self,
        coordinator: S400Coordinator,
        entry: ConfigEntry,
        description: S400SensorEntityDescription,
    ) -> None:
        """Initialize the sensor."""
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
        """Restore previous state and subscribe to coordinator updates."""
        await super().async_added_to_hass()
        if self.entity_description.restore_fn is not None:
            last_sensor_data = await self.async_get_last_sensor_data()
            if (
                last_sensor_data is not None
                and last_sensor_data.native_value is not None
            ):
                try:
                    val = float(last_sensor_data.native_value)
                    self.entity_description.restore_fn(self.coordinator.state, val)
                except (TypeError, ValueError):
                    pass

        self.async_on_remove(
            self.coordinator.async_add_listener(self._handle_coordinator_update)
        )

    @callback
    def _handle_coordinator_update(self) -> None:
        self.async_write_ha_state()

    @property
    def native_value(self) -> float | None:
        """Return the current sensor value."""
        return self.entity_description.value_fn(self.coordinator.state)

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        """Return additional state attributes."""
        if self.entity_description.extra_attrs_fn is not None:
            return self.entity_description.extra_attrs_fn(self.coordinator.state)
        return None
