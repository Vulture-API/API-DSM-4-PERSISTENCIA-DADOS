"""Manual replay of original quarantine; no Redis operations or payload edits."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
from dataclasses import dataclass
from importlib.metadata import version
from uuid import UUID

import asyncpg

from station_persist.destinations import Destination, build_processing_plan
from station_persist.logging_config import log_event
from station_persist.persistence import (
    lock_catalog,
    update_communication,
    write_measurement,
)
from station_persist.stream import StreamEntry
from station_persist.validation import (
    TimestampSource,
    TimestampValidation,
    validate_entries,
)


@dataclass(frozen=True, slots=True)
class ReplayOutcome:
    request_id: str
    attempt_id: int | None
    status: str
    accepted: int = 0
    rejected: int = 0


async def _snapshot(conn, quarantine_id, source_id, stream, *, locked=False):
    row = await conn.fetchrow(
        "SELECT q.id AS quarantine_id,q.ordinal,q.status AS quarantine_status,q.reasons,"
        "e.* FROM telemetry_quarantine q JOIN telemetry_events e ON e.id=q.event_id "
        "WHERE q.id=$1 AND e.source_id=$2 AND e.stream=$3"
        + (" FOR UPDATE OF e,q" if locked else ""),
        quarantine_id,
        source_id,
        stream,
    )
    if row is None:
        raise ValueError("Quarantine does not exist in configured source/stream")
    return row


async def _revalidate(conn, row):
    source = StreamEntry.from_redis(row["entry_id"], json.loads(row["original_fields"]))
    overrides = {}
    if row["measurement_unix_time"] is not None and row["timestamp_source"] is not None:
        overrides[source.entry_id] = TimestampValidation(
            row["measurement_unix_time"],
            row["received_at_ms"],
            TimestampSource(row["timestamp_source"]),
            warnings=tuple(row["warnings"]),
        )
    (event,) = await validate_entries(
        [source],
        conn,
        legacy_max_id=row["legacy_max_id"],
        timestamp_overrides=overrides,
    )
    return event


async def _targets(conn, row, event):
    handled = set(
        await conn.fetchval(
            "SELECT ARRAY(SELECT ordinal FROM telemetry_results WHERE event_id=$1 AND destination<>'quarantine' "
            "UNION SELECT ordinal FROM telemetry_replay_results WHERE event_id=$1 AND destination<>'rejected')",
            row["id"],
        )
    )
    conflicts = set(
        await conn.fetchval(
            "SELECT ARRAY(SELECT ordinal FROM telemetry_replay_results WHERE event_id=$1 AND 'conflicting_measurement'=ANY(reasons))",
            row["id"],
        )
    )
    plan = build_processing_plan(event)
    fields = [
        f
        for f in plan.fields
        if f.ordinal not in handled
        and (row["ordinal"] == 0 or f.ordinal in (0, row["ordinal"]))
    ]
    return fields, conflicts


async def _execute(conn, row, attempt_id):
    event = await _revalidate(conn, row)
    await lock_catalog(conn, event)
    fields, conflicts = await _targets(conn, row, event)
    accepted = rejected = 0
    for field in fields:
        ordinal = field.ordinal if row["ordinal"] == 0 else row["ordinal"]
        destination = "rejected"
        reading_id = sensor_id = None
        reasons = field.reasons
        if "conflicting_measurement" in row["reasons"] or ordinal in conflicts:
            reasons = ("conflicting_measurement",)
        elif field.destination != Destination.QUARANTINE:
            result = await write_measurement(
                conn, event.resolution.station_id, event.timestamp.unix_time, field
            )
            if result["destination"] == "quarantine":
                reasons = ("conflicting_measurement",)
            else:
                destination = result["destination"]
                reading_id = result["reading_id"]
                sensor_id = field.measurement.resolution.sensor_id
        await conn.execute(
            "INSERT INTO telemetry_replay_results(attempt_id,event_id,ordinal,destination,sensor_id,reading_id,reading_unix_time,"
            "min_value,max_value,reasons,timestamp_source,legacy_max_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
            attempt_id,
            row["id"],
            ordinal,
            destination,
            sensor_id,
            reading_id,
            event.timestamp.unix_time if reading_id is not None else None,
            field.measurement.min_value if field.measurement else None,
            field.measurement.max_value if field.measurement else None,
            list(reasons),
            event.timestamp.source.value if event.timestamp.source else None,
            row["legacy_max_id"],
        )
        accepted += int(destination != "rejected")
        rejected += int(destination == "rejected")
    await update_communication(conn, event.resolution.station_id, row["received_at_ms"])
    # If a target vanished because the catalog/event is now invalid, the event
    # rejection is represented above. Empty valid payloads are safely resolved.
    if rejected == 0:
        await conn.execute(
            "UPDATE telemetry_quarantine SET status='RESOLVED',resolved_at=clock_timestamp() WHERE id=$1 AND status='OPEN'",
            row["quarantine_id"],
        )
    status = (
        "PARTIAL" if accepted and rejected else "REJECTED" if rejected else "SUCCEEDED"
    )
    await conn.execute(
        "UPDATE telemetry_replay_attempts SET status=$2,finished_at=clock_timestamp() WHERE id=$1",
        attempt_id,
        status,
    )
    return accepted, rejected, status


async def _replay_once(
    pool,
    *,
    quarantine_id: int,
    request_id: str,
    operator: str,
    reason: str,
    source_id: str,
    stream: str,
    dry_run: bool = False,
) -> ReplayOutcome:
    request = UUID(request_id)
    if not operator.strip() or not reason.strip():
        raise ValueError("Operator and justification are required")
    if dry_run:
        async with (
            pool.acquire() as conn,
            conn.transaction(isolation="repeatable_read", readonly=True),
        ):
            row = await _snapshot(conn, quarantine_id, source_id, stream)
            event = await _revalidate(conn, row)
            fields, conflicts = await _targets(conn, row, event)
            accepted = rejected = 0
            for field in fields if row["quarantine_status"] != "RESOLVED" else []:
                blocked = (
                    field.destination == Destination.QUARANTINE
                    or "conflicting_measurement" in row["reasons"]
                    or field.ordinal in conflicts
                )
                if not blocked:
                    existing = await conn.fetchrow(
                        "SELECT value FROM readings WHERE sensor_id=$1 AND unix_time=$2",
                        field.measurement.resolution.sensor_id,
                        event.timestamp.unix_time,
                    )
                    blocked = (
                        existing is not None
                        and existing["value"] != field.measurement.value
                    )
                accepted += int(not blocked)
                rejected += int(blocked)
            return ReplayOutcome(str(request), None, "DRY_RUN", accepted, rejected)
    # Session lock spans durable request registration, business transaction and
    # failure audit. Connections release it on crash; request UUID stays durable.
    lock = int.from_bytes(
        hashlib.sha256(request.bytes).digest()[:8], "big", signed=True
    )
    async with pool.acquire() as conn:
        if not await conn.fetchval("SELECT pg_try_advisory_lock($1::bigint)", lock):
            return ReplayOutcome(str(request), None, "BUSY")
        attempt_id = None
        try:
            async with conn.transaction():
                await _snapshot(conn, quarantine_id, source_id, stream)
                await conn.execute(
                    "INSERT INTO telemetry_replay_requests(request_id,quarantine_id,operator_name,justification) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING",
                    request,
                    quarantine_id,
                    operator,
                    reason,
                )
                recorded = await conn.fetchrow(
                    "SELECT quarantine_id,operator_name,justification FROM telemetry_replay_requests WHERE request_id=$1",
                    request,
                )
                if tuple(recorded) != (quarantine_id, operator, reason):
                    raise ValueError(
                        "Request identity reused with different parameters"
                    )
                previous = await conn.fetchrow(
                    "SELECT id,status FROM telemetry_replay_attempts WHERE request_id=$1 ORDER BY id DESC LIMIT 1",
                    request,
                )
                if previous and previous["status"] in (
                    "SUCCEEDED",
                    "PARTIAL",
                    "REJECTED",
                ):
                    tally = await conn.fetchrow(
                        "SELECT count(*) FILTER(WHERE destination<>'rejected') AS accepted,count(*) FILTER(WHERE destination='rejected') AS rejected FROM telemetry_replay_results WHERE attempt_id=$1",
                        previous["id"],
                    )
                    return ReplayOutcome(
                        str(request),
                        previous["id"],
                        previous["status"],
                        tally["accepted"],
                        tally["rejected"],
                    )
                await conn.execute(
                    "UPDATE telemetry_replay_attempts SET status='INTERRUPTED',error_code='abandoned_attempt',finished_at=clock_timestamp() WHERE request_id=$1 AND status='RUNNING'",
                    request,
                )
                attempt_id = await conn.fetchval(
                    "INSERT INTO telemetry_replay_attempts(request_id,processor_version) VALUES($1,$2) RETURNING id",
                    request,
                    version("station-persist"),
                )
            async with conn.transaction(isolation="repeatable_read"):
                row = await _snapshot(
                    conn, quarantine_id, source_id, stream, locked=True
                )
                if row["quarantine_status"] == "RESOLVED":
                    await conn.execute(
                        "UPDATE telemetry_replay_attempts SET status='SUCCEEDED',finished_at=clock_timestamp() WHERE id=$1",
                        attempt_id,
                    )
                    accepted, rejected, status = 0, 0, "SUCCEEDED"
                else:
                    accepted, rejected, status = await _execute(conn, row, attempt_id)
            return ReplayOutcome(str(request), attempt_id, status, accepted, rejected)
        except BaseException as exc:
            if attempt_id is not None:
                status = (
                    "INTERRUPTED"
                    if isinstance(exc, asyncio.CancelledError)
                    else "TECHNICAL_FAILURE"
                )

                # If the COMMIT response was ambiguous, do not overwrite a terminal
                # committed result. A new connection can observe the durable state.
                error_code = type(exc).__name__

                async def audit():
                    sql = "UPDATE telemetry_replay_attempts SET status=$2,error_code=$3,finished_at=clock_timestamp() WHERE id=$1 AND status='RUNNING'"
                    if not conn.is_closed():
                        await conn.execute(sql, attempt_id, status, error_code)
                    else:
                        async with pool.acquire() as audit_conn:
                            await audit_conn.execute(
                                sql, attempt_id, status, error_code
                            )

                try:
                    await asyncio.wait_for(audit(), 5)
                except Exception:
                    pass  # RUNNING remains durable and is audited on next retry.
            raise
        finally:
            if not conn.is_closed():
                await conn.execute("SELECT pg_advisory_unlock($1::bigint)", lock)


async def replay_quarantine(pool, **kwargs) -> ReplayOutcome:
    """Retry complete audited transactions; keep the same logical request UUID.

    Bounded retries leave a durable failure on exhaustion. The operator can
    safely repeat the exact command; a committed terminal result is returned.
    """
    transient = (
        ConnectionError,
        OSError,
        TimeoutError,
        asyncpg.PostgresConnectionError,
        asyncpg.CannotConnectNowError,
        asyncpg.TooManyConnectionsError,
        asyncpg.SerializationError,
        asyncpg.DeadlockDetectedError,
        asyncpg.LockNotAvailableError,
        asyncpg.QueryCanceledError,
    )
    for attempt in range(3):
        try:
            outcome = await _replay_once(pool, **kwargs)
            log_event(
                logging.getLogger("station_persist"),
                "quarantine_replay",
                request_id=outcome.request_id,
                attempt_id=outcome.attempt_id,
                status=outcome.status,
                accepted_fields=outcome.accepted,
                rejected_fields=outcome.rejected,
            )
            return outcome
        except transient as exc:
            if attempt == 2:
                raise
            log_event(
                logging.getLogger("station_persist"),
                "replay_retry",
                error_type=type(exc).__name__,
                attempt=attempt + 1,
            )
            await asyncio.sleep(random.uniform(0.125, 0.25) * 2**attempt)
    raise AssertionError("unreachable")
