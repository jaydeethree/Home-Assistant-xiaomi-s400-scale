"""Event platform for Xiaomi S400 Scale."""

from __future__ import annotations

from homeassistant.components.event import EventEntity, EventEntityDescription
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddConfigEntryEntitiesCallback

from .coordinator import S400ConfigEntry
from .entity import S400Entity

PARALLEL_UPDATES = 0
EVENT_MEASUREMENT_COMPLETED = "measurement_completed"

MEASUREMENT_EVENT_DESCRIPTION = EventEntityDescription(
    key="measurement",
    translation_key="measurement",
    event_types=[EVENT_MEASUREMENT_COMPLETED],
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: S400ConfigEntry,
    async_add_entities: AddConfigEntryEntitiesCallback,
) -> None:
    """Set up Xiaomi S400 Scale event entity from a config entry."""
    coordinator = entry.runtime_data
    async_add_entities(
        [S400MeasurementEventEntity(coordinator, MEASUREMENT_EVENT_DESCRIPTION)]
    )


class S400MeasurementEventEntity(S400Entity, EventEntity):
    """Event entity fired on each completed scale measurement."""

    async def async_added_to_hass(self) -> None:
        """Restore previous event state and subscribe to final measurements."""
        await super().async_added_to_hass()
        self.async_on_remove(
            self.coordinator.async_add_measurement_listener(
                self._handle_final_measurement
            )
        )

    @callback
    def _handle_final_measurement(self) -> None:
        """Emit a measurement_completed event with the full measurement payload."""
        state = self.coordinator.state
        self._trigger_event(
            EVENT_MEASUREMENT_COMPLETED,
            {
                "weight_kg": state.final_weight_kg,
                "impedance_ohm": state.impedance_ohm,
                "impedance_low_ohm": state.impedance_low_ohm,
                "profile_id": state.profile_id,
                "measured_at": (
                    state.last_measurement.isoformat()
                    if state.last_measurement is not None
                    else None
                ),
            },
        )
        self._last_state_key = self._get_state_key()
        self.async_write_ha_state()
