"""SCRUM-451: atomic destinations and idempotency; Redis ACK belongs to caller."""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from station_persist.destinations import Destination, build_processing_plan
from station_persist.logging_config import log_event
from station_persist.stream import ProcessingResult, StreamEntry
from station_persist.validation import CatalogConfigurationError, validate_entries


class SourceIdentityError(RuntimeError):
    """An already committed source identity was reused with different content."""


def original_bytes(entry: StreamEntry) -> bytes:
    # Values are the original Redis strings, not parsed telemetry numbers. ASCII
    # escaping round-trips even NUL and lone surrogates without PostgreSQL JSONB.
    return json.dumps(
        dict(entry.fields), ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("ascii")


async def validate_storage_schema(conn: Any) -> None:
    """Read-only startup guard; migrations are always an explicit BANCO action."""
    required = {
        "telemetry_events": {
            "id",
            "source_id",
            "stream",
            "entry_id",
            "original_fields",
            "station_id",
            "measurement_unix_time",
            "received_at_ms",
            "timestamp_source",
            "warnings",
            "completed_at",
            "created_at",
        },
        "telemetry_results": {
            "event_id",
            "ordinal",
            "destination",
            "sensor_id",
            "reading_id",
            "reading_unix_time",
            "min_value",
            "max_value",
            "reasons",
            "quarantine_id",
        },
        "telemetry_quarantine": {
            "id",
            "event_id",
            "ordinal",
            "reasons",
            "status",
            "created_at",
            "resolved_at",
        },
        "readings": {"id", "sensor_id", "value", "unix_time", "data_consistent"},
        "occurrences": {
            "id",
            "type",
            "status",
            "station_id",
            "sensor_id",
            "reading_id",
            "value",
            "description",
            "detected_at",
        },
        "stations": {"id", "property_id", "last_communication_at"},
        "sensors": {"id", "station_id", "sensor_type_id", "min_value", "max_value"},
    }
    rows = await conn.fetch(
        "SELECT c.relname,a.attname,a.atttypid='numeric'::regtype AND a.atttypmod=-1 AS exact "
        "FROM pg_class c JOIN pg_attribute a ON a.attrelid=c.oid "
        "WHERE c.oid=ANY(ARRAY(SELECT to_regclass(t) FROM unnest($1::text[]) t)) "
        "AND a.attnum>0 AND NOT a.attisdropped",
        list(required),
    )
    found: dict[str, set[str]] = {}
    exact = set()
    for row in rows:
        found.setdefault(row["relname"], set()).add(row["attname"])
        if row["exact"]:
            exact.add((row["relname"], row["attname"]))
    indexes = await conn.fetch(
        "SELECT c.relname, ARRAY(SELECT a.attname::text FROM unnest(i.indkey) WITH ORDINALITY k(num,pos) "
        "JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.num ORDER BY k.pos) AS keys, "
        "pg_get_expr(i.indpred,i.indrelid) AS predicate FROM pg_index i "
        "JOIN pg_class c ON c.oid=i.indrelid "
        "WHERE i.indrelid=ANY(ARRAY(SELECT to_regclass(t) FROM unnest($1::text[]) t)) "
        "AND i.indisunique AND i.indisvalid AND i.indexprs IS NULL",
        ["readings", "telemetry_events", "occurrences"],
    )
    unique = {
        (row["relname"], tuple(row["keys"]))
        for row in indexes
        if row["predicate"] is None
    }
    occurrence_index = any(
        row["relname"] == "occurrences"
        and row["keys"] == ["sensor_id"]
        and row["predicate"] is not None
        and "OUT_OF_RANGE" in row["predicate"]
        and "OPEN" in row["predicate"]
        for row in indexes
    )
    if (
        any(
            not columns <= found.get(table, set())
            for table, columns in required.items()
        )
        or not {
            ("readings", "value"),
            ("sensors", "min_value"),
            ("sensors", "max_value"),
            ("occurrences", "value"),
        }
        <= exact
        or not {
            ("readings", ("sensor_id", "unix_time")),
            ("telemetry_events", ("source_id", "stream", "entry_id")),
        }
        <= unique
        or not occurrence_index
    ):
        raise CatalogConfigurationError(
            "Apply BANCO migrations 016 (both) and 017 before consume"
        )


async def _quarantine(
    conn: Any, event_id: int, field: Any, reasons: tuple[str, ...]
) -> None:
    quarantine_id = await conn.fetchval(
        "INSERT INTO telemetry_quarantine(event_id,ordinal,reasons) VALUES($1,$2,$3) RETURNING id",
        event_id,
        field.ordinal,
        list(reasons),
    )
    await conn.execute(
        "INSERT INTO telemetry_results(event_id,ordinal,destination,sensor_id,reasons,quarantine_id) "
        "VALUES($1,$2,'quarantine',$3,$4,$5)",
        event_id,
        field.ordinal,
        field.measurement.resolution.sensor_id if field.measurement else None,
        list(reasons),
        quarantine_id,
    )


async def _persist(
    conn: Any,
    entry: StreamEntry,
    source_id: str,
    stream: str,
    legacy_max_id: str | None,
) -> dict[str, int]:
    raw = original_bytes(entry)
    event_id = await conn.fetchval(
        "INSERT INTO telemetry_events(source_id,stream,entry_id,original_fields) VALUES($1,$2,$3,$4) "
        "ON CONFLICT(source_id,stream,entry_id) DO NOTHING RETURNING id",
        source_id,
        stream,
        entry.entry_id,
        raw,
    )
    counts = dict(readings=0, inconsistent=0, quarantine=0, duplicates=0)
    if event_id is None:
        existing = await conn.fetchrow(
            "SELECT original_fields,completed_at FROM telemetry_events "
            "WHERE source_id=$1 AND stream=$2 AND entry_id=$3 FOR UPDATE",
            source_id,
            stream,
            entry.entry_id,
        )
        if existing is None or existing["original_fields"] != raw:
            raise SourceIdentityError("Source identity collision; no acknowledgement")
        if existing["completed_at"] is None:
            raise CatalogConfigurationError(
                "Previously committed incomplete telemetry event"
            )
        counts["duplicates"] = 1
        return counts

    (event,) = await validate_entries([entry], conn, legacy_max_id=legacy_max_id)
    station_id = event.resolution.station_id
    if station_id is not None:
        # Lock station before sensors. The stronger station lock also avoids
        # lock-upgrade deadlocks when updating communication. Sensor FOR SHARE
        # protects non-key limits; changed rows abort the whole RR snapshot.
        await conn.fetch(
            "SELECT id FROM stations WHERE id=$1 FOR NO KEY UPDATE", station_id
        )
    sensor_ids = sorted(
        {f.resolution.sensor_id for f in event.fields if f.resolution.associated}
    )
    if sensor_ids:
        await conn.fetch(
            "SELECT id FROM sensors WHERE id=ANY($1::integer[]) ORDER BY id FOR SHARE",
            sensor_ids,
        )

    for field in build_processing_plan(event).fields:
        if field.destination == Destination.QUARANTINE:
            await _quarantine(conn, event_id, field, field.reasons)
            counts["quarantine"] += 1
            continue
        measurement = field.measurement
        sensor_id = measurement.resolution.sensor_id
        unix_time = event.timestamp.unix_time
        reading_id = await conn.fetchval(
            "INSERT INTO readings(sensor_id,value,unix_time,data_consistent) VALUES($1,$2,$3,$4) "
            "ON CONFLICT(sensor_id,unix_time) DO NOTHING RETURNING id",
            sensor_id,
            measurement.value,
            unix_time,
            field.destination == Destination.READING,
        )
        destination = field.destination.value
        if reading_id is None:
            previous = await conn.fetchrow(
                "SELECT id,value FROM readings WHERE sensor_id=$1 AND unix_time=$2 FOR SHARE",
                sensor_id,
                unix_time,
            )
            if previous["value"] != measurement.value:
                await _quarantine(conn, event_id, field, ("conflicting_measurement",))
                counts["quarantine"] += 1
                continue
            reading_id = previous["id"]
            destination = "duplicate"
            counts["duplicates"] += 1
        else:
            counts["readings"] += 1
            if field.destination == Destination.INCONSISTENT:
                counts["inconsistent"] += 1
                await conn.execute(
                    "INSERT INTO occurrences(type,station_id,sensor_id,reading_id,value,description,detected_at) "
                    "VALUES('OUT_OF_RANGE',$1,$2,$3,$4,$5,clock_timestamp() AT TIME ZONE 'UTC') "
                    "ON CONFLICT(sensor_id) WHERE type='OUT_OF_RANGE' AND status='OPEN' DO NOTHING",
                    station_id,
                    sensor_id,
                    reading_id,
                    measurement.value,
                    f"Sensor {sensor_id}: medição fora dos limites cadastrados ({','.join(field.reasons)}).",
                )
        await conn.execute(
            "INSERT INTO telemetry_results(event_id,ordinal,destination,sensor_id,reading_id,reading_unix_time,min_value,max_value,reasons) "
            "VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)",
            event_id,
            field.ordinal,
            destination,
            sensor_id,
            reading_id,
            unix_time,
            measurement.min_value,
            measurement.max_value,
            list(field.reasons),
        )

    # Validation can stop before timestamp parsing for an invalid payload. A
    # safely resolved station still communicated; independently check the receipt.
    received_raw = entry.fields.get("received_at", "")
    received = int(received_raw) if re.fullmatch(r"[0-9]{1,20}", received_raw) else None
    if received is not None and received > 253402300799999:
        received = None
    if station_id is not None and received is not None:
        instant = (
            datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=received)
        ).replace(tzinfo=None)
        await conn.execute(
            "UPDATE stations SET last_communication_at=GREATEST(last_communication_at,$2::timestamp) WHERE id=$1",
            station_id,
            instant,
        )
    await conn.execute(
        "UPDATE telemetry_events SET station_id=$2,measurement_unix_time=$3,received_at_ms=$4,timestamp_source=$5,warnings=$6,completed_at=clock_timestamp() WHERE id=$1",
        event_id,
        station_id,
        event.timestamp.unix_time,
        received,
        event.timestamp.source.value if event.timestamp.source else None,
        list(event.timestamp.warnings),
    )
    return counts


async def process_entry(
    entry: StreamEntry,
    pool: Any,
    *,
    source_id: str,
    stream: str,
    legacy_max_id: str | None = None,
) -> ProcessingResult:
    """One transaction per event. Exceptions/cancellation never signal completion.

    Caller retries transient failures, always invoking this whole transaction.
    This function performs no Redis operations.
    """
    if not source_id or not stream or any(c in source_id + stream for c in "\x00"):
        raise ValueError("Stable source_id and stream are required")
    if not entry.fields:
        raise CatalogConfigurationError(
            "Stream entry has no fields (possibly trimmed); retained pending"
        )
    start = time.monotonic()
    async with pool.acquire() as conn:
        async with conn.transaction(isolation="repeatable_read"):
            counts = await _persist(conn, entry, source_id, stream, legacy_max_id)
    # Transaction context exit (including COMMIT response) precedes completion.
    log_event(
        logging.getLogger("station_persist"),
        "event_committed",
        entry_id=entry.entry_id,
        elapsed_ms=round((time.monotonic() - start) * 1000, 3),
        **counts,
    )
    return ProcessingResult.COMPLETED
