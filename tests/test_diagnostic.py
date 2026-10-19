import asyncio
import logging
from unittest.mock import AsyncMock

import asyncpg
import pytest

from station_persist.inspection import inspect_once, inspect_stream


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        asyncpg.ConnectionDoesNotExistError(),
        asyncpg.CannotConnectNowError(),
        asyncpg.SerializationError(),
        TimeoutError(),
    ],
)
async def test_failed_catalog_batch_retries_from_same_cursor(error):
    redis = AsyncMock()
    stop = asyncio.Event()
    count = 0

    async def read(cursor, **kwargs):
        assert cursor == {"s": "0-0"}
        return [("s", [("1-0", {"payload": "one"}), ("2-0", {"payload": "two"})])]

    async def observe(entries):
        nonlocal count
        count += 1
        assert [entry.entry_id for entry in entries] == ["1-0", "2-0"]
        if count == 1:
            raise error
        stop.set()

    redis.xread.side_effect = read
    await inspect_stream(
        redis,
        stream="s",
        stop_event=stop,
        batch_size=2,
        block_ms=1,
        retry_initial_seconds=0.001,
        retry_max_seconds=0.002,
        logger=logging.getLogger("test"),
        observer=observe,
    )
    assert count == 2
    assert all(call[0] == "xread" for call in redis.mock_calls)


@pytest.mark.asyncio
async def test_empty_diagnostic_does_not_query_catalog():
    redis, observer = AsyncMock(), AsyncMock()
    redis.xread.return_value = []
    assert (
        await inspect_once(redis, stream="s", last_id="10-0", observer=observer)
        == "10-0"
    )
    observer.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        asyncpg.UndefinedTableError("missing stations"),
        asyncpg.UndefinedColumnError("missing property_id"),
        asyncpg.InvalidPasswordError("bad configuration"),
        asyncio.CancelledError(),
    ],
)
async def test_schema_config_errors_and_cancellation_are_not_rejections_or_retry(error):
    redis = AsyncMock()
    redis.xread.return_value = [("s", [("1-0", {})])]
    observer = AsyncMock(side_effect=error)
    with pytest.raises(type(error)):
        await inspect_stream(
            redis,
            stream="s",
            stop_event=asyncio.Event(),
            batch_size=1,
            block_ms=1,
            retry_initial_seconds=0.001,
            retry_max_seconds=0.002,
            logger=logging.getLogger("test"),
            observer=observer,
        )
    assert redis.xread.await_count == 1
