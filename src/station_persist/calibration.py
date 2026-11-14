"""SCRUM-601: pure conversion of a raw reading into the sensor type unit."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import MAX_EMAX, MAX_PREC, MIN_EMIN, Context, Decimal

# value = raw * factor * gain + offset. The default context would round at 28
# digits; readings.value is unconstrained NUMERIC, so the arithmetic stays exact.
_EXACT = Context(prec=MAX_PREC, Emax=MAX_EMAX, Emin=MIN_EMIN)


class InvalidCalibrationError(ValueError):
    """The catalog holds a calibration that cannot be applied."""


@dataclass(frozen=True, slots=True)
class Calibration:
    """None leaves the reading unchanged (factor 1, gain 1, offset 0)."""

    factor: Decimal | None = None
    gain: Decimal | None = None
    offset: Decimal | None = None

    @property
    def identity(self) -> bool:
        return self.factor is None and self.gain is None and self.offset is None


def _multiplier(value: object, name: str) -> Decimal | None:
    number = _number(value, name)
    if number is not None and number == 0:
        raise InvalidCalibrationError(f"{name} must not be zero")
    return number


def _number(value: object, name: str) -> Decimal | None:
    if value is None:
        return None
    if type(value) not in (int, Decimal):
        raise InvalidCalibrationError(f"{name} must be an exact number")
    number = Decimal(value)
    if not number.is_finite():
        raise InvalidCalibrationError(f"{name} must be finite")
    return number


def effective_calibration(
    *,
    type_factor: object = None,
    type_gain: object = None,
    type_offset: object = None,
    sensor_gain: object = None,
    sensor_offset: object = None,
) -> Calibration:
    """Factor belongs to the type; sensor gain and offset override the type's."""
    return Calibration(
        factor=_multiplier(type_factor, "factor"),
        gain=_multiplier(sensor_gain if sensor_gain is not None else type_gain, "gain"),
        offset=_number(
            sensor_offset if sensor_offset is not None else type_offset, "offset"
        ),
    )


def apply_calibration(raw: Decimal, calibration: Calibration) -> Decimal:
    value = raw
    if calibration.factor is not None:
        value = _EXACT.multiply(value, calibration.factor)
    if calibration.gain is not None:
        value = _EXACT.multiply(value, calibration.gain)
    if calibration.offset is not None:
        value = _EXACT.add(value, calibration.offset)
    return value
