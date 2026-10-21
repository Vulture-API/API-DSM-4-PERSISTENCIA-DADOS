"""Read-only business validation. Results are not processing acknowledgements."""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from station_persist.relationships import (
    CatalogConnection,
    EventResolution,
    FieldResolution,
    UnrepresentableNumber,
    parse_json_decimal,
    resolve_entries,
)
from station_persist.stream import StreamEntry

# UTC datetime supported by the service, ending 9999-12-31T23:59:59Z.
_MAX_UNIX = 253402300799
_MAX_RECEIVED_MS = _MAX_UNIX * 1000 + 999
_STREAM_ID = re.compile(r"(0|[1-9][0-9]*)-(0|[1-9][0-9]*)")
_UINT64_MAX = 18446744073709551615


class _IntegerToken(str):
    pass


class CatalogConfigurationError(RuntimeError):
    """Schema/catalog must be repaired; retrying telemetry cannot fix it."""


class CatalogChangedError(ConnectionError):
    """Caller must retry resolution and validation with a consistent snapshot."""


class ValidationStatus(StrEnum):
    VALID = "valid"
    OUT_OF_RANGE = "out_of_range"
    REJECTED = "rejected"


class TimestampSource(StrEnum):
    PAYLOAD = "payload"
    RECEIVED_AT = "received_at"
    LEGACY_RECEIVED_AT = "legacy_received_at"


@dataclass(frozen=True, slots=True)
class TimestampValidation:
    unix_time: int | None = None
    received_at_ms: int | None = None
    source: TimestampSource | None = None
    reasons: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class MeasurementValidation:
    resolution: FieldResolution
    status: ValidationStatus
    value: Decimal | None = None
    min_value: Decimal | None = None
    max_value: Decimal | None = None
    reasons: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class EventValidation:
    resolution: EventResolution
    timestamp: TimestampValidation
    fields: tuple[MeasurementValidation, ...]

    @property
    def reasons(self) -> tuple[str, ...]:
        return self.resolution.reasons + self.timestamp.reasons


def stream_id_parts(value: str) -> tuple[int, int]:
    if not isinstance(value, str):
        raise ValueError("Stream ID must be a string")
    match = _STREAM_ID.fullmatch(value)
    if match is None or any(len(part) > 20 for part in match.groups()):
        raise ValueError("Stream ID must contain two unsigned 64-bit integers")
    parts = tuple(int(part) for part in match.groups())
    if any(part > _UINT64_MAX for part in parts):
        raise ValueError("Stream ID component exceeds unsigned 64-bit range")
    return parts


def exact_numeric(value: object) -> tuple[Decimal | None, str | None]:
    """No float conversion, context rounding, quantization or exponent expansion."""
    if isinstance(value, UnrepresentableNumber):
        return None, "numeric_not_representable"
    if type(value) not in (int, Decimal):
        return None, "invalid_numeric_type"
    number = Decimal(value)
    if not number.is_finite():
        return None, "non_finite_value"
    sign, digits, exponent = number.as_tuple()
    if not any(digits):
        return Decimal(0), None
    digits = list(digits)
    while digits[-1] == 0:
        digits.pop()
        exponent += 1
    # PostgreSQL unconstrained NUMERIC: 131072 integral and 16383 fractional digits.
    if max(0, len(digits) + exponent) > 131072 or max(0, -exponent) > 16383:
        return None, "numeric_not_representable"
    return Decimal((sign, tuple(digits), exponent)), None


def _timestamp(
    entry: StreamEntry, legacy_cutoff: tuple[int, int] | None
) -> TimestampValidation:
    raw_received = entry.fields.get("received_at", "")
    if not re.fullmatch(r"[0-9]{1,20}", raw_received):
        return TimestampValidation(reasons=("invalid_received_at",))
    received = int(raw_received)
    if received > _MAX_RECEIVED_MS:
        return TimestampValidation(reasons=("received_at_not_representable",))
    # Keep integer token identity without coercing 1.0, strings or huge integers.
    payload = json.loads(
        entry.fields["payload"], parse_int=_IntegerToken, parse_float=parse_json_decimal
    )
    if "unix_time" not in payload:
        return TimestampValidation(
            received // 1000, received, TimestampSource.RECEIVED_AT
        )
    raw_time = payload["unix_time"]
    if (
        raw_time is None
        and entry.fields.get("unix_time") == "null"
        and legacy_cutoff is not None
    ):
        try:
            legacy = stream_id_parts(entry.entry_id) <= legacy_cutoff
        except ValueError:
            return TimestampValidation(
                received_at_ms=received, reasons=("invalid_stream_id",)
            )
        if legacy:
            return TimestampValidation(
                received // 1000,
                received,
                TimestampSource.LEGACY_RECEIVED_AT,
                warnings=("legacy_timestamp_original_lost",),
            )
    if not isinstance(raw_time, _IntegerToken):
        return TimestampValidation(
            received_at_ms=received, reasons=("invalid_timestamp_type",)
        )
    digits = raw_time.lstrip("-").lstrip("0") or "0"
    if (
        (raw_time.startswith("-") and digits != "0")
        or len(digits) > 12
        or (len(digits) == 12 and digits > str(_MAX_UNIX))
    ):
        return TimestampValidation(
            received_at_ms=received, reasons=("timestamp_not_representable",)
        )
    return TimestampValidation(int(digits), received, TimestampSource.PAYLOAD)


async def validate_entries(
    entries: Sequence[StreamEntry],
    connection: CatalogConnection,
    *,
    legacy_max_id: str | None = None,
) -> tuple[EventValidation, ...]:
    """Caller owns connection/transaction. Prefer REPEATABLE READ for a whole batch."""
    cutoff = stream_id_parts(legacy_max_id) if legacy_max_id is not None else None
    resolved = await resolve_entries(entries, connection)
    ids = sorted(
        {
            field.sensor_id
            for event in resolved
            for field in event.fields
            if field.associated
        }
    )
    catalog = {}
    if ids:
        schema = await connection.fetch(
            "SELECT c.relname, a.attname, a.atttypid = 'numeric'::regtype AND a.atttypmod = -1 AS exact "
            "FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid "
            "WHERE (a.attrelid = to_regclass('readings') AND a.attname = 'value' "
            "OR a.attrelid = to_regclass('sensors') AND a.attname IN ('min_value','max_value')) "
            "AND NOT a.attisdropped"
        )
        if {(row["relname"], row["attname"]) for row in schema} != {
            ("readings", "value"),
            ("sensors", "min_value"),
            ("sensors", "max_value"),
        } or not all(row["exact"] for row in schema):
            raise CatalogConfigurationError(
                "Apply BANCO migration 016: unconstrained NUMERIC required"
            )
        rows = await connection.fetch(
            "SELECT id, station_id, min_value, max_value FROM sensors WHERE id = ANY($1::integer[])",
            ids,
        )
        for row in rows:
            if row["id"] in catalog:
                raise CatalogConfigurationError("Duplicate sensor in limit catalog")
            limits = []
            for key in ("min_value", "max_value"):
                limit = row[key]
                if limit is not None:
                    limit, reason = exact_numeric(limit)
                    if reason is not None:
                        raise CatalogConfigurationError(
                            "Non-finite or unrepresentable sensor limit"
                        )
                limits.append(limit)
            minimum, maximum = limits
            if minimum is not None and maximum is not None and minimum > maximum:
                raise CatalogConfigurationError("Sensor minimum exceeds maximum")
            catalog[row["id"]] = (row["station_id"], minimum, maximum)
        if set(ids) != catalog.keys():
            raise CatalogChangedError(
                "Sensor disappeared after relationship resolution"
            )
    events = []
    for event in resolved:
        time = (
            _timestamp(event.source, cutoff)
            if not event.reasons
            else TimestampValidation()
        )
        fields = []
        for field in event.fields:
            minimum = maximum = None
            if field.associated:
                station_id, minimum, maximum = catalog[field.sensor_id]
                if station_id != field.station_id:
                    raise CatalogChangedError("Sensor station changed after resolution")
            reasons = field.reasons + time.reasons
            number = None
            status = ValidationStatus.REJECTED
            if not reasons:
                number, reason = exact_numeric(field.value)
                if reason is not None:
                    reasons = (reason,)
                else:
                    status = ValidationStatus.VALID
                    if minimum is not None and number < minimum:
                        reasons = ("below_min",)
                    elif maximum is not None and number > maximum:
                        reasons = ("above_max",)
                    if reasons:
                        status = ValidationStatus.OUT_OF_RANGE
            fields.append(
                MeasurementValidation(field, status, number, minimum, maximum, reasons)
            )
        events.append(EventValidation(event, time, tuple(fields)))
    return tuple(events)
