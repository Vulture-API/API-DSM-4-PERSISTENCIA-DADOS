"""Catalog associations only: no measurement validation, writes or acknowledgement."""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from decimal import Decimal, DecimalException
from enum import StrEnum
from typing import Any, Protocol

from station_persist.stream import StreamEntry

type JSONValue = (
    None | bool | int | Decimal | str | list[JSONValue] | dict[str, JSONValue]
)


class RejectionReason(StrEnum):
    INVALID_PAYLOAD = "invalid_payload"
    INVALID_IDENTIFICATION = "invalid_identification"
    IDENTIFICATION_OUT_OF_RANGE = "identification_out_of_range"
    IDENTIFICATION_MISMATCH = "identification_mismatch"
    UNKNOWN_STATION = "unknown_station"
    AMBIGUOUS_STATION = "ambiguous_station"
    UNKNOWN_SENSOR = "unknown_sensor"
    INVALID_SENSOR_IDENTIFIER = "invalid_sensor_identifier"
    AMBIGUOUS_SENSOR = "ambiguous_sensor"
    DUPLICATE_SENSOR = "duplicate_sensor"
    INCOMPATIBLE_REFERENCE = "incompatible_reference"


@dataclass(frozen=True, slots=True)
class StationIdentity:
    kind: str
    value: int | str


@dataclass(frozen=True, slots=True)
class FieldResolution:
    local_identifier: str
    value: JSONValue
    station_id: int | None = None
    property_id: int | None = None
    sensor_id: int | None = None
    sensor_type_id: int | None = None
    reasons: tuple[RejectionReason, ...] = ()

    @property
    def associated(self) -> bool:
        return self.sensor_id is not None and not self.reasons


@dataclass(frozen=True, slots=True)
class EventResolution:
    source: StreamEntry
    station_id: int | None
    property_id: int | None
    fields: tuple[FieldResolution, ...]
    reasons: tuple[RejectionReason, ...] = ()


class CatalogConnection(Protocol):
    async def fetch(self, query: str, *args: Any) -> Sequence[Mapping[str, Any]]: ...


_MAC = re.compile(r"[0-9A-F]{2}([:-]?)(?:[0-9A-F]{2}\1){4}[0-9A-F]{2}")
_RESERVED = frozenset({"estacao_id", "unix_time"})


class UnrepresentableNumber(str):
    """Original numeric JSON token beyond even Decimal's exponent capacity."""


def parse_json_decimal(token: str) -> Decimal | UnrepresentableNumber:
    try:
        return Decimal(token)
    except DecimalException:
        return UnrepresentableNumber(token)


def _mac_identity(value: str) -> StationIdentity | None:
    if _MAC.fullmatch(value):
        return StationIdentity("mac", value.replace(":", "").replace("-", ""))
    return None


def station_identity(
    raw: object,
) -> tuple[StationIdentity | None, RejectionReason | None]:
    """Digit-only identities are legacy int4 IDs, never a fallback MAC."""
    if not isinstance(raw, str) or not raw.strip():
        return None, RejectionReason.INVALID_IDENTIFICATION
    value = raw.strip().upper()
    if value.isascii() and value.isdigit():
        digits = value.lstrip("0") or "0"
        if len(digits) > 10 or (len(digits) == 10 and digits > "2147483647"):
            return None, RejectionReason.IDENTIFICATION_OUT_OF_RANGE
        return StationIdentity("id", int(digits)), None
    mac = _mac_identity(value)
    if mac is not None:
        return mac, None
    return None, RejectionReason.INVALID_IDENTIFICATION


def _json_object(pairs: list[tuple[str, JSONValue]]) -> dict[str, JSONValue]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON key")
        result[key] = value
    return result


def _invalid_constant(value: str) -> None:
    raise ValueError("Non-JSON numeric constant")


def _integer(value: str) -> int | Decimal:
    # Python's int digit guard must not turn a received number into invalid JSON.
    return int(value) if len(value) < 4300 else Decimal(value)


def _invalid_sensor_identifier(value: str) -> bool:
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return True
    return "\x00" in value


def _prepare(entry: StreamEntry) -> tuple[EventResolution, StationIdentity | None]:
    try:
        payload = json.loads(
            entry.fields["payload"],
            parse_float=parse_json_decimal,
            parse_int=_integer,
            parse_constant=_invalid_constant,
            object_pairs_hook=_json_object,
        )
        if not isinstance(payload, dict):
            raise ValueError("Payload must be an object")
    except (KeyError, ValueError, RecursionError):
        return EventResolution(
            entry, None, None, (), (RejectionReason.INVALID_PAYLOAD,)
        ), None
    fields = tuple(
        FieldResolution(
            key,
            value,
            reasons=(RejectionReason.INVALID_SENSOR_IDENTIFIER,)
            if _invalid_sensor_identifier(key)
            else (),
        )
        for key, value in payload.items()
        if key not in _RESERVED
    )
    identity, reason = station_identity(entry.fields.get("estacao_id"))
    if reason is None and "estacao_id" in payload:
        payload_identity, reason = station_identity(payload["estacao_id"])
        if reason is None and payload_identity != identity:
            reason = RejectionReason.IDENTIFICATION_MISMATCH
    event = EventResolution(entry, None, None, fields)
    if reason is not None:
        return _reject_event(event, reason), None
    return event, identity


def _reject_event(event: EventResolution, reason: RejectionReason) -> EventResolution:
    return replace(
        event,
        reasons=(reason,),
        fields=tuple(replace(field, reasons=(reason,)) for field in event.fields),
    )


_STATIONS_SQL = """
SELECT id, property_id, mac_address FROM stations
WHERE id = ANY($1::integer[])
   OR upper(translate(btrim(mac_address), ':-', '')) = ANY($2::text[])
"""

_SENSORS_SQL = """
WITH requested AS (
    SELECT * FROM unnest($1::integer[], $2::text[])
        AS r(station_id, local_identifier)
)
SELECT r.station_id AS requested_station_id,
       r.local_identifier AS requested_identifier,
       s.id AS sensor_id, s.station_id, s.sensor_type_id
FROM requested r
JOIN sensors s ON s.station_id = r.station_id
             AND lower(s.local_identifier) = lower(r.local_identifier)
"""


async def resolve_entries(
    entries: Sequence[StreamEntry], connection: CatalogConnection
) -> tuple[EventResolution, ...]:
    """Resolve one batch using the caller's connection and transaction.

    Raw Stream fields remain in source; fractional JSON numbers use Decimal.
    Domain rejections are returned, while database exceptions propagate unchanged.
    No cache, transaction, ProcessingResult or persistence is owned here.
    """
    prepared = [_prepare(entry) for entry in entries]
    identities = {identity for _, identity in prepared if identity is not None}
    if not identities:
        return tuple(event for event, _ in prepared)
    rows = await connection.fetch(
        _STATIONS_SQL,
        sorted(identity.value for identity in identities if identity.kind == "id"),
        sorted(identity.value for identity in identities if identity.kind == "mac"),
    )
    stations = defaultdict(list)
    for row in rows:
        stations[StationIdentity("id", row["id"])].append(row)
        # The typed catalog column is always a MAC, even if all hex digits are 0-9.
        mac = _mac_identity(row["mac_address"].strip().upper())
        if mac is not None:
            stations[mac].append(row)

    events = []
    for event, identity in prepared:
        if identity is None:
            events.append(event)
            continue
        candidates = stations[identity]
        if not candidates:
            events.append(_reject_event(event, RejectionReason.UNKNOWN_STATION))
        elif len(candidates) != 1:
            events.append(_reject_event(event, RejectionReason.AMBIGUOUS_STATION))
        elif candidates[0]["property_id"] is None:
            events.append(_reject_event(event, RejectionReason.INCOMPATIBLE_REFERENCE))
        else:
            row = candidates[0]
            events.append(
                replace(event, station_id=row["id"], property_id=row["property_id"])
            )

    requests = sorted(
        {
            (event.station_id, field.local_identifier)
            for event in events
            if not event.reasons
            for field in event.fields
            if not field.reasons
        }
    )
    sensors = defaultdict(list)
    if requests:
        rows = await connection.fetch(
            _SENSORS_SQL,
            [station for station, _ in requests],
            [key for _, key in requests],
        )
        for row in rows:
            sensors[(row["requested_station_id"], row["requested_identifier"])].append(
                row
            )

    results = []
    for event in events:
        if event.reasons:
            results.append(event)
            continue
        fields = []
        for field in event.fields:
            field = replace(
                field, station_id=event.station_id, property_id=event.property_id
            )
            if field.reasons:
                fields.append(field)
                continue
            candidates = sensors[(event.station_id, field.local_identifier)]
            if not candidates:
                field = replace(field, reasons=(RejectionReason.UNKNOWN_SENSOR,))
            elif len(candidates) != 1:
                field = replace(field, reasons=(RejectionReason.AMBIGUOUS_SENSOR,))
            elif (
                candidates[0]["station_id"] != event.station_id
                or candidates[0]["sensor_type_id"] is None
            ):
                field = replace(
                    field, reasons=(RejectionReason.INCOMPATIBLE_REFERENCE,)
                )
            else:
                field = replace(
                    field,
                    sensor_id=candidates[0]["sensor_id"],
                    sensor_type_id=candidates[0]["sensor_type_id"],
                )
            fields.append(field)
        counts = Counter(field.sensor_id for field in fields if field.associated)
        results.append(
            replace(
                event,
                fields=tuple(
                    replace(field, reasons=(RejectionReason.DUPLICATE_SENSOR,))
                    if field.associated and counts[field.sensor_id] > 1
                    else field
                    for field in fields
                ),
            )
        )
    return tuple(results)
