"""Opt-in writes with leased identities and bounded pending recovery."""

from __future__ import annotations

import asyncio
import logging
import random

import asyncpg
from redis.asyncio import Redis
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from station_persist.config import Settings
from station_persist.logging_config import log_event
from station_persist.persistence import process_entry, validate_storage_schema
from station_persist.recovery import ConsumerLease, PendingRecovery, missing_content
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
    worker: GroupConsumer,
    settings: Settings,
    stop: asyncio.Event,
    recovery=None,
    lease=None,
) -> None:
    delay = settings.retry_initial_seconds
    while not stop.is_set():
        try:
            # One bounded own/recovery/new batch per cycle; cursors progress past
            # failed entries so technical failures cannot starve later work.
            if lease is not None:
                await lease.check()
            await worker.process_once(pending=True)
            if recovery is not None and not stop.is_set():
                await recovery.once()
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
            await _pause(stop, random.uniform(delay / 2, delay))
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
    pool = worker_task = stopper = heartbeat = metrics_task = None
    lease = ConsumerLease(redis, settings)
    lease_stop = asyncio.Event()
    lease_acquired = False
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
                await _pause(stop, random.uniform(delay / 2, delay))
                delay = min(delay * 2, settings.retry_max_seconds)
        if pool is None:
            return

        async def process(entry):
            if not entry.fields:
                # The helper ACKs only after durable completion proof and incident.
                await missing_content(pool, redis, settings, entry.entry_id)
                from station_persist.stream import ProcessingResult

                return ProcessingResult.INCOMPLETE
            async with asyncio.timeout(settings.processing_timeout_seconds):
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
                await _pause(stop, random.uniform(delay / 2, delay))
                delay = min(delay * 2, settings.retry_max_seconds)
        if stop.is_set():
            return
        log_event(
            logger, "consumption_started", stream=settings.stream, writes_enabled=True
        )
        await lease.acquire()
        lease_acquired = True
        heartbeat = asyncio.create_task(lease.heartbeat(lease_stop))
        recovery = PendingRecovery(redis, pool, worker, settings, lease)
        from station_persist.operations import report_status

        metrics_task = asyncio.create_task(
            report_status(settings, pool, redis, lease_stop)
        )
        worker_task = asyncio.create_task(
            consume_loop(worker, settings, stop, recovery, lease)
        )
        stopper = asyncio.create_task(stop.wait())
        done, _ = await asyncio.wait(
            [worker_task, stopper, heartbeat], return_when=asyncio.FIRST_COMPLETED
        )
        if heartbeat in done:
            heartbeat.result()
        elif worker_task in done:
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
        lease_stop.set()
        for task in (worker_task, stopper, heartbeat, metrics_task):
            if task is not None:
                task.cancel()
        await asyncio.gather(
            *(
                t
                for t in (worker_task, stopper, heartbeat, metrics_task)
                if t is not None
            ),
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
            if lease_acquired:
                try:
                    await lease.release()
                except Exception:
                    log_event(logger, "lease_release_failed")
            await redis.aclose()
            for sig in installed:
                asyncio.get_running_loop().remove_signal_handler(sig)
        log_event(logger, "consumption_stopped")
