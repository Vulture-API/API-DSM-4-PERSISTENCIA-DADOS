"""Read-only status/quarantine diagnostics; replay is an explicit write command."""

from __future__ import annotations

import asyncio
import logging

import asyncpg
from redis.asyncio import Redis

from station_persist.logging_config import log_event
from station_persist.persistence import validate_storage_schema
from station_persist.replay import replay_quarantine


async def status_snapshot(settings, pool, redis):
    result = dict(
        stream=settings.stream,
        group=settings.group,
        lag=None,
        pending=None,
        oldest_pending_age_seconds=None,
        quarantine_open=None,
        oldest_quarantine_age_seconds=None,
        recovery_incidents=None,
        replay_attempts=None,
        committed_destinations=None,
        degraded=[],
    )
    try:
        groups = await redis.xinfo_groups(settings.stream)
        group = next((g for g in groups if g["name"] == settings.group), None)
        if group is None:
            result["degraded"].append("missing_group")
        else:
            result["lag"] = group.get("lag")
            pending = await redis.xpending(settings.stream, settings.group)
            result["pending"] = pending["pending"]
            if pending["min"]:
                seconds, micros = await redis.time()
                result["oldest_pending_age_seconds"] = max(
                    0, seconds + micros / 1e6 - int(pending["min"].split("-")[0]) / 1000
                )
            elif pending["pending"] == 0:
                result["oldest_pending_age_seconds"] = 0
    except Exception as exc:
        result["degraded"].append(f"redis:{type(exc).__name__}")
    try:
        if pool is None:
            raise ConnectionError("PostgreSQL unavailable")
        async with pool.acquire() as conn, conn.transaction(readonly=True):
            row = await conn.fetchrow(
                "SELECT count(*) AS n,extract(epoch FROM clock_timestamp()-min(q.created_at))::float8 AS age FROM telemetry_quarantine q JOIN telemetry_events e ON e.id=q.event_id WHERE e.source_id=$1 AND e.stream=$2 AND q.status='OPEN'",
                settings.source_id,
                settings.stream,
            )
            result["quarantine_open"] = row["n"]
            result["oldest_quarantine_age_seconds"] = row["age"] if row["n"] else 0
            result["recovery_incidents"] = await conn.fetchval(
                "SELECT count(*) FROM telemetry_recovery_incidents WHERE source_id=$1 AND stream=$2 AND durable_event_id IS NULL",
                settings.source_id,
                settings.stream,
            )
            result["replay_attempts"] = [
                dict(r)
                for r in await conn.fetch(
                    "SELECT a.status,count(*) AS count FROM telemetry_replay_attempts a JOIN telemetry_replay_requests r USING(request_id) JOIN telemetry_quarantine q ON q.id=r.quarantine_id JOIN telemetry_events e ON e.id=q.event_id WHERE e.source_id=$1 AND e.stream=$2 GROUP BY a.status",
                    settings.source_id,
                    settings.stream,
                )
            ]
            result["committed_destinations"] = [
                dict(r)
                for r in await conn.fetch(
                    "SELECT destination,count(*) AS count FROM ("
                    "SELECT r.destination FROM telemetry_results r JOIN telemetry_events e ON e.id=r.event_id "
                    "WHERE e.source_id=$1 AND e.stream=$2 AND e.completed_at IS NOT NULL "
                    "UNION ALL SELECT r.destination FROM telemetry_replay_results r JOIN telemetry_events e ON e.id=r.event_id "
                    "WHERE e.source_id=$1 AND e.stream=$2) destinations GROUP BY destination",
                    settings.source_id,
                    settings.stream,
                )
            ]
    except Exception as exc:
        result["degraded"].append(f"postgres:{type(exc).__name__}")
    return result


async def report_status(settings, pool, redis, stop):
    while not stop.is_set():
        snapshot = await status_snapshot(settings, pool, redis)
        log_event(logging.getLogger("station_persist"), "persist_status", **snapshot)
        try:
            await asyncio.wait_for(stop.wait(), settings.metrics_interval_seconds)
        except TimeoutError:
            pass


async def run_operation(
    settings,
    command,
    *,
    action=None,
    quarantine_id=None,
    request_id=None,
    operator=None,
    reason=None,
    limit=100,
    after_id=0,
    dry_run=False,
):
    if settings.source_id is None:
        raise ValueError("PERSIST_SOURCE_ID is required for operational commands")
    pool = redis = None
    try:
        if command == "status":
            redis = Redis.from_url(
                settings.redis_url.get_secret_value(),
                decode_responses=True,
                socket_timeout=settings.socket_timeout_seconds,
                socket_connect_timeout=settings.socket_timeout_seconds,
            )
        try:
            pool = await asyncpg.create_pool(
                settings.database_url.get_secret_value(),
                min_size=1,
                max_size=settings.pool_max_size,
                timeout=settings.socket_timeout_seconds,
                command_timeout=settings.socket_timeout_seconds,
            )
        except Exception:
            if command != "status":
                raise
        if command == "status":
            return await status_snapshot(settings, pool, redis)
        async with pool.acquire() as conn:
            await validate_storage_schema(conn)
        if action == "replay":
            outcome = await replay_quarantine(
                pool,
                quarantine_id=quarantine_id,
                request_id=request_id,
                operator=operator,
                reason=reason,
                source_id=settings.source_id,
                stream=settings.stream,
                dry_run=dry_run,
            )
            from dataclasses import asdict

            return asdict(outcome)
        async with pool.acquire() as conn, conn.transaction(readonly=True):
            if action == "list":
                rows = await conn.fetch(
                    "SELECT q.id,q.event_id,q.ordinal,q.status,q.reasons,q.created_at,q.resolved_at,e.entry_id FROM telemetry_quarantine q JOIN telemetry_events e ON e.id=q.event_id WHERE e.source_id=$1 AND e.stream=$2 AND q.id>$3 ORDER BY q.id LIMIT $4",
                    settings.source_id,
                    settings.stream,
                    after_id,
                    limit,
                )
                return {
                    "items": [dict(r) for r in rows],
                    "next_after_id": rows[-1]["id"] if len(rows) == limit else None,
                }
            row = await conn.fetchrow(
                "SELECT q.*,e.source_id,e.stream,e.entry_id FROM telemetry_quarantine q JOIN telemetry_events e ON e.id=q.event_id WHERE q.id=$1 AND e.source_id=$2 AND e.stream=$3",
                quarantine_id,
                settings.source_id,
                settings.stream,
            )
            if row is None:
                raise ValueError(
                    "Quarantine does not exist in configured source/stream"
                )
            attempts = await conn.fetch(
                "SELECT a.id,r.request_id,r.operator_name,r.justification,a.status,a.error_code,a.started_at,a.finished_at FROM telemetry_replay_requests r JOIN telemetry_replay_attempts a USING(request_id) WHERE r.quarantine_id=$1 ORDER BY a.id DESC LIMIT $2",
                quarantine_id,
                limit,
            )
            results = await conn.fetch(
                "SELECT rr.attempt_id,rr.ordinal,rr.destination,rr.sensor_id,rr.reading_id,rr.reading_unix_time,rr.reasons,rr.timestamp_source,rr.legacy_max_id "
                "FROM telemetry_replay_results rr JOIN telemetry_replay_attempts a ON a.id=rr.attempt_id "
                "JOIN telemetry_replay_requests r USING(request_id) WHERE r.quarantine_id=$1 "
                "ORDER BY rr.attempt_id DESC,rr.ordinal LIMIT $2",
                quarantine_id,
                limit,
            )
            return dict(row) | {
                "attempts": [dict(a) for a in attempts],
                "results": [dict(r) for r in results],
                "history_limit": limit,
            }
    finally:
        if redis is not None:
            await redis.aclose()
        if pool is not None:
            try:
                await asyncio.wait_for(pool.close(), settings.shutdown_timeout_seconds)
            except TimeoutError:
                pool.terminate()
