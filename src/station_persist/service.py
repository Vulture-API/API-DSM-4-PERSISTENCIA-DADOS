from __future__ import annotations

import asyncio
import logging
import signal

import asyncpg
from redis.asyncio import Redis

from station_persist.config import Settings
from station_persist.inspection import inspect_stream
from station_persist.logging_config import log_event
from station_persist.relationships import resolve_entries
from station_persist.stream import StreamEntry


def install_signal_handlers(stop: asyncio.Event) -> list[signal.Signals]:
    installed = []
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_running_loop().add_signal_handler(sig, stop.set)
            installed.append(sig)
        except NotImplementedError:  # Windows uses KeyboardInterrupt.
            pass
    return installed


async def run_inspection(
    settings: Settings,
    stop_event: asyncio.Event | None = None,
    *,
    resolve: bool = False,
) -> None:
    own_stop = stop_event is None
    stop = stop_event if stop_event is not None else asyncio.Event()
    installed = install_signal_handlers(stop) if own_stop else []
    logger = logging.getLogger("station_persist")
    redis = Redis.from_url(
        settings.redis_url.get_secret_value(),
        decode_responses=True,
        socket_timeout=settings.socket_timeout_seconds + settings.block_ms / 1000,
        socket_connect_timeout=settings.socket_timeout_seconds,
    )
    pool = None
    tasks = []
    try:
        await redis.ping()
        pool = await asyncpg.create_pool(
            settings.database_url.get_secret_value(),
            min_size=settings.pool_min_size,
            max_size=settings.pool_max_size,
            timeout=settings.socket_timeout_seconds,
            command_timeout=settings.socket_timeout_seconds,
        )
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")

        async def observe(entries: tuple[StreamEntry, ...]) -> None:
            async with pool.acquire() as conn:
                async with conn.transaction(isolation="repeatable_read", readonly=True):
                    results = await resolve_entries(entries, conn)
            for event in results:
                log_event(
                    logger,
                    "event_relationships",
                    entry_id=event.source.entry_id,
                    station_id=event.station_id,
                    property_id=event.property_id,
                    reasons=list(event.reasons),
                )
                for field in event.fields:
                    log_event(
                        logger,
                        "field_relationship",
                        entry_id=event.source.entry_id,
                        local_identifier=field.local_identifier.encode(
                            "utf-8", errors="backslashreplace"
                        ).decode("utf-8"),
                        station_id=field.station_id,
                        property_id=field.property_id,
                        sensor_id=field.sensor_id,
                        sensor_type_id=field.sensor_type_id,
                        associated=field.associated,
                        reasons=list(field.reasons),
                    )

        log_event(
            logger,
            "inspection_started",
            stream=settings.stream,
            writes_enabled=False,
            resolve_enabled=resolve,
        )
        worker = asyncio.create_task(
            inspect_stream(
                redis,
                stream=settings.stream,
                stop_event=stop,
                batch_size=settings.batch_size,
                block_ms=settings.block_ms,
                retry_initial_seconds=settings.retry_initial_seconds,
                retry_max_seconds=settings.retry_max_seconds,
                logger=logger,
                observer=observe if resolve else None,
            )
        )
        stopper = asyncio.create_task(stop.wait())
        tasks = [worker, stopper]
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        if worker in done:
            worker.result()
    except Exception as exc:
        log_event(logger, "inspection_failed", error_type=type(exc).__name__)
        raise
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        try:
            if pool is not None:
                try:
                    await asyncio.wait_for(
                        pool.close(), settings.shutdown_timeout_seconds
                    )
                except TimeoutError:
                    pool.terminate()
        finally:
            await redis.aclose()
            for sig in installed:
                asyncio.get_running_loop().remove_signal_handler(sig)
        log_event(logger, "inspection_stopped")
