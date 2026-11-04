"""Sensor platform for Xiaomi S400 Scale."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from homeassistant.components.sensor import (
    RestoreSensor,
    SensorDeviceClass,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.const import EntityCategory, UnitOfMass
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from . import S400ConfigEntry
from .coordinator import S400State
from .entity import S400Entity

UNIT_OHM = "ohm"


@dataclass(frozen=True, kw_only=True)
class S400SensorEntityDescription(SensorEntityDescription):
    """Describe a Xiaomi Scale S400 sensor entity."""

    value_fn: Callable[[S400State], float | datetime | None]
    restore_fn: Callable[[S400State, Any], None] | None = None


def _restore_final_weight(state: S400State, value: Any) -> None:
    if state.final_weight_kg is None:
        state.final_weight_kg = float(value)


def _restore_impedance(state: S400State, value: Any) -> None:
    if state.impedance_ohm is None:
        state.impedance_ohm = float(value)


def _restore_impedance_low(state: S400State, value: Any) -> None:
    if state.impedance_low_ohm is None:
        state.impedance_low_ohm = float(value)


def _restore_timestamp(state: S400State, value: Any) -> None:
    if state.device_timestamp is None:
        if isinstance(value, datetime):
            state.device_timestamp = int(value.timestamp())
        elif isinstance(value, (int, float)):
            state.device_timestamp = int(value)


def _timestamp_value(state: S400State) -> datetime | None:
    if state.device_timestamp is None or state.device_timestamp <= 0:
        return None
    return datetime.fromtimestamp(state.device_timestamp, tz=UTC)


SENSORS: tuple[S400SensorEntityDescription, ...] = (
    S400SensorEntityDescription(
        key="live_weight",
        translation_key="live_weight",
        device_class=SensorDeviceClass.WEIGHT,
        native_unit_of_measurement=UnitOfMass.KILOGRAMS,
        suggested_display_precision=1,
        value_fn=lambda s: s.live_weight_kg,
    ),
    S400SensorEntityDescription(
        key="weight",
        translation_key="weight",
        device_class=SensorDeviceClass.WEIGHT,
        native_unit_of_measurement=UnitOfMass.KILOGRAMS,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        value_fn=lambda s: s.final_weight_kg,
        restore_fn=_restore_final_weight,
    ),
    S400SensorEntityDescription(
        key="impedance",
        translation_key="impedance",
        native_unit_of_measurement=UNIT_OHM,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        value_fn=lambda s: s.impedance_ohm,
        restore_fn=_restore_impedance,
    ),
    S400SensorEntityDescription(
        key="impedance_low",
        translation_key="impedance_low",
        native_unit_of_measurement=UNIT_OHM,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=1,
        value_fn=lambda s: s.impedance_low_ohm,
        restore_fn=_restore_impedance_low,
    ),
    S400SensorEntityDescription(
        key="last_measurement",
        translation_key="last_measurement",
        device_class=SensorDeviceClass.TIMESTAMP,
        entity_category=EntityCategory.DIAGNOSTIC,
        value_fn=_timestamp_value,
        restore_fn=_restore_timestamp,
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: S400ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Xiaomi S400 Scale sensors from a config entry."""
    coordinator = entry.runtime_data
    async_add_entities(
        S400SensorEntity(coordinator, entry, description) for description in SENSORS
    )


class S400SensorEntity(S400Entity, RestoreSensor):
    """Representation of a Xiaomi S400 Scale sensor."""

    entity_description: S400SensorEntityDescription

    async def async_added_to_hass(self) -> None:
        """Restore previous state and subscribe to coordinator updates."""
        if self.entity_description.restore_fn is not None:
            last_sensor_data = await self.async_get_last_sensor_data()
            if (
                last_sensor_data is not None
                and last_sensor_data.native_value is not None
            ):
                try:
                    self.entity_description.restore_fn(
                        self.coordinator.state, last_sensor_data.native_value
                    )
                except (TypeError, ValueError):
                    pass

        await super().async_added_to_hass()

    @property
    def native_value(self) -> float | datetime | None:
        """Return the current sensor value."""
        return self.entity_description.value_fn(self.coordinator.state)
