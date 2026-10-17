from __future__ import annotations

import asyncio
import logging
from typing import Any

from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from station_persist.logging_config import log_event
from station_persist.stream import StreamEntry


async def inspect_once(
    redis: Any,
    *,
    stream: str,
    last_id: str = "0-0",
    batch_size: int = 500,
    block_ms: int = 1000,
    logger: logging.Logger | None = None,
) -> str:
    logger = logger or logging.getLogger("station_persist")
    response = await redis.xread({stream: last_id}, count=batch_size, block=block_ms)
    for _stream, messages in response or []:
        for entry_id, fields in messages:
            entry = StreamEntry.from_redis(entry_id, fields)
            # Payload, credentials and arbitrary field values must not leak to logs.
            log_event(
                logger,
                "stream_entry_observed",
                entry_id=entry.entry_id,
                field_count=len(entry.fields),
            )
            last_id = entry.entry_id
    return last_id


async def inspect_stream(
    redis: Any,
    *,
    stream: str,
    stop_event: asyncio.Event,
    batch_size: int,
    block_ms: int,
    retry_initial_seconds: float,
    retry_max_seconds: float,
    logger: logging.Logger,
) -> None:
    last_id = "0-0"
    delay = retry_initial_seconds
    while not stop_event.is_set():
        try:
            last_id = await inspect_once(
                redis,
                stream=stream,
                last_id=last_id,
                batch_size=batch_size,
                block_ms=block_ms,
                logger=logger,
            )
            delay = retry_initial_seconds
        except (
            ConnectionError,
            OSError,
            TimeoutError,
            RedisConnectionError,
            RedisTimeoutError,
        ):
            log_event(logger, "inspection_retry", retry_in_seconds=delay)
            try:
                await asyncio.wait_for(stop_event.wait(), delay)
            except TimeoutError:
                pass
            delay = min(delay * 2, retry_max_seconds)
