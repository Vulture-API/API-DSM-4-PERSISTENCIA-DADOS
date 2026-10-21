"""Opt-in writes. Cross-consumer recovery and quarantine replay are SCRUM-530."""

from __future__ import annotations

import asyncio
import logging

import asyncpg
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from station_persist.config import Settings
from station_persist.logging_config import log_event
from station_persist.persistence import process_entry, validate_storage_schema
from station_persist.service import install_signal_handlers
from station_persist.stream import GroupConsumer

TRANSIENT_ERRORS = (
    ConnectionError,
    OSError,
    TimeoutError,
    RedisConnectionError,
    RedisTimeoutError,
    asyncpg.PostgresConnectionError,
    asyncpg.CannotConnectNowError,
    asyncpg.TooManyConnectionsError,
    asyncpg.SerializationError,
    asyncpg.DeadlockDetectedError,
    asyncpg.LockNotAvailableError,
    asyncpg.QueryCanceledError,
)


async def _pause(stop: asyncio.Event, delay: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), delay)
    except TimeoutError:
        pass


async def consume_loop(
    worker: GroupConsumer, settings: Settings, stop: asyncio.Event
) -> None:
    delay = settings.retry_initial_seconds
    while not stop.is_set():
        try:
            # Drain own pending entries before new messages. No XCLAIM/XAUTOCLAIM;
            # abandoned consumer identities remain for SCRUM-530.
            while not stop.is_set() and await worker.process_once(pending=True):
                pass
            if not stop.is_set():
                await worker.process_once()
            delay = settings.retry_initial_seconds
        except TRANSIENT_ERRORS as exc:
            log_event(
                logging.getLogger("station_persist"),
                "consumption_retry",
                error_type=type(exc).__name__,
                retry_in_seconds=delay,
            )
            await _pause(stop, delay)
            delay = min(delay * 2, settings.retry_max_seconds)


async def run_consumption(
    settings: Settings, stop_event: asyncio.Event | None = None
) -> None:
    if settings.source_id is None:
        raise ValueError("PERSIST_SOURCE_ID is required for consume")
    stop = stop_event if stop_event is not None else asyncio.Event()
    installed = install_signal_handlers(stop) if stop_event is None else []
    logger = logging.getLogger("station_persist")
    redis = Redis.from_url(
        settings.redis_url.get_secret_value(),
        decode_responses=True,
        socket_timeout=settings.socket_timeout_seconds + settings.block_ms / 1000,
        socket_connect_timeout=settings.socket_timeout_seconds,
    )
    pool = worker_task = stopper = None
    try:
        delay = settings.retry_initial_seconds
        while not stop.is_set():
            try:
                await redis.ping()
                pool = await asyncpg.create_pool(
                    settings.database_url.get_secret_value(),
                    min_size=settings.pool_min_size,
                    max_size=settings.pool_max_size,
                    timeout=settings.socket_timeout_seconds,
                    command_timeout=settings.socket_timeout_seconds,
                )
                break
            except TRANSIENT_ERRORS as exc:
                log_event(
                    logger,
                    "consumption_startup_retry",
                    error_type=type(exc).__name__,
                    retry_in_seconds=delay,
                )
                await _pause(stop, delay)
                delay = min(delay * 2, settings.retry_max_seconds)
        if pool is None:
            return

        async def process(entry):
            return await process_entry(
                entry,
                pool,
                source_id=settings.source_id,
                stream=settings.stream,
                legacy_max_id=settings.legacy_max_id,
            )

        worker = GroupConsumer(
            redis,
            stream=settings.stream,
            group=settings.group,
            consumer=settings.consumer,
            batch_size=settings.batch_size,
            block_ms=settings.block_ms,
            processor=process,
            stop_event=stop,
        )
        # Schema is checked BEFORE touching consumer groups, including retries.
        delay = settings.retry_initial_seconds
        while not stop.is_set():
            try:
                async with pool.acquire() as conn:
                    await validate_storage_schema(conn)
                await worker.ensure_group()
                break
            except TRANSIENT_ERRORS as exc:
                log_event(
                    logger,
                    "consumption_startup_retry",
                    error_type=type(exc).__name__,
                    retry_in_seconds=delay,
                )
                await _pause(stop, delay)
                delay = min(delay * 2, settings.retry_max_seconds)
        if stop.is_set():
            return
        log_event(
            logger, "consumption_started", stream=settings.stream, writes_enabled=True
        )
        worker_task = asyncio.create_task(consume_loop(worker, settings, stop))
        stopper = asyncio.create_task(stop.wait())
        done, _ = await asyncio.wait(
            [worker_task, stopper], return_when=asyncio.FIRST_COMPLETED
        )
        if worker_task in done:
            worker_task.result()
        else:
            try:
                await asyncio.wait_for(
                    asyncio.shield(worker_task), settings.shutdown_timeout_seconds
                )
            except TimeoutError:
                log_event(logger, "consumption_shutdown_timeout")
    except Exception as exc:
        log_event(logger, "consumption_failed", error_type=type(exc).__name__)
        raise
    finally:
        for task in (worker_task, stopper):
            if task is not None:
                task.cancel()
        await asyncio.gather(
            *(t for t in (worker_task, stopper) if t is not None),
            return_exceptions=True,
        )
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
        log_event(logger, "consumption_stopped")
