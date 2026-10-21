"""SCRUM-449: pure destination planning; a plan never authorizes Redis ACK."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from station_persist.validation import EventValidation, MeasurementValidation
from station_persist.validation import ValidationStatus as Status


class Destination(StrEnum):
    READING = "reading"
    INCONSISTENT = "inconsistent"
    QUARANTINE = "quarantine"


@dataclass(frozen=True, slots=True)
class PlannedField:
    # Zero is reserved for an event-level rejection (there may be no fields).
    ordinal: int
    destination: Destination
    measurement: MeasurementValidation | None
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ProcessingPlan:
    validation: EventValidation
    fields: tuple[PlannedField, ...]


def build_processing_plan(event: EventValidation) -> ProcessingPlan:
    """Preserve validation/origin, including invalid events with no measurements."""
    if event.reasons:
        return ProcessingPlan(
            event,
            (PlannedField(0, Destination.QUARANTINE, None, event.reasons),),
        )
    destinations = {
        Status.VALID: Destination.READING,
        Status.OUT_OF_RANGE: Destination.INCONSISTENT,
        Status.REJECTED: Destination.QUARANTINE,
    }
    return ProcessingPlan(
        event,
        tuple(
            PlannedField(i, destinations[field.status], field, field.reasons)
            for i, field in enumerate(event.fields, start=1)
        ),
    )
