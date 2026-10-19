import asyncio
import logging
from unittest.mock import AsyncMock

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ResponseError

from station_persist.config import Settings
from station_persist.inspection import inspect_once, inspect_stream
from station_persist.stream import GroupConsumer, ProcessingResult, StreamEntry


@pytest.mark.parametrize(
    "kwargs",
    [
        {"pool_min_size": 5, "pool_max_size": 1},
        {"retry_initial_seconds": 5, "retry_max_seconds": 1},
        {"batch_size": 0},
        {"block_ms": 0},
        {"stream": ""},
    ],
)
def test_invalid_config(kwargs):
    with pytest.raises(ValueError):
        Settings(_env_file=None, database_url="postgresql://localhost/test", **kwargs)


def test_config_prefix_and_secrets(monkeypatch):
    monkeypatch.setenv(
        "PERSIST_DATABASE_URL", "postgresql://user:secret@localhost/test"
    )
    settings = Settings(_env_file=None)
    assert "secret" not in repr(settings)
    assert settings.consumer
    assert settings.stream == "telemetry:ingest"


def test_entry_preserves_invalid_and_legacy_fields():
    raw = {"unix_time": "null", "payload": '{"unix_time":"bad"}', "extra": "1"}
    entry = StreamEntry.from_redis("1-0", raw)
    raw["extra"] = "changed"
    assert entry.fields["extra"] == "1"
    assert entry.fields["payload"] == '{"unix_time":"bad"}'
    with pytest.raises(TypeError):
        entry.fields["extra"] = "x"
    assert not StreamEntry.from_redis("2-0", None).fields


def test_group_requires_processor_before_touching_redis():
    redis = AsyncMock()
    with pytest.raises(ValueError, match="processor"):
        GroupConsumer(redis, stream="s", group="g", consumer="c")
    assert redis.mock_calls == []


def consumer(redis, processor):
    return GroupConsumer(
        redis, stream="s", group="g", consumer="c", processor=processor
    )


@pytest.mark.asyncio
async def test_group_creation_and_existing_group():
    redis = AsyncMock()
    worker = consumer(redis, AsyncMock())
    await worker.ensure_group()
    redis.xgroup_create.assert_awaited_once_with("s", "g", id="0-0", mkstream=True)
    redis.xgroup_create.side_effect = ResponseError("BUSYGROUP exists")
    await worker.ensure_group()
    redis.xgroup_create.side_effect = ResponseError("NOAUTH")
    with pytest.raises(ResponseError, match="NOAUTH"):
        await worker.ensure_group()


@pytest.mark.asyncio
async def test_only_explicit_completed_outcome_acks():
    redis = AsyncMock()
    redis.xreadgroup.return_value = [
        ("s", [("1-0", {"payload": "old"}), ("2-0", {}), ("3-0", {})])
    ]
    processor = AsyncMock(
        side_effect=[ProcessingResult.COMPLETED, ProcessingResult.INCOMPLETE, True]
    )
    worker = consumer(redis, processor)
    assert await worker.process_once() == 1
    redis.xack.assert_awaited_once_with("s", "g", "1-0")
    assert processor.await_args_list[0].args[0].fields == {"payload": "old"}
    redis.xreadgroup.return_value = []
    await worker.process_once(pending=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [RuntimeError("failed"), asyncio.CancelledError()])
async def test_failure_or_cancellation_leaves_entry_pending(error):
    redis = AsyncMock()
    redis.xreadgroup.return_value = [("s", [("1-0", {"payload": "original"})])]
    worker = consumer(redis, AsyncMock(side_effect=error))
    with pytest.raises(type(error)):
        await worker.process_once()
    redis.xack.assert_not_awaited()


@pytest.mark.asyncio
async def test_empty_batch():
    redis = AsyncMock()
    redis.xreadgroup.return_value = []
    assert await consumer(redis, AsyncMock()).process_once() == 0
    redis.xack.assert_not_awaited()


@pytest.mark.asyncio
async def test_inspection_reads_without_groups_or_payload_logging(caplog):
    redis = AsyncMock()
    redis.xread.return_value = [
        ("s", [("1-0", {"payload": "PRIVATE", "unix_time": "null"})])
    ]
    with caplog.at_level(logging.INFO):
        last = await inspect_once(redis, stream="s")
    assert last == "1-0"
    assert "PRIVATE" not in caplog.text
    assert "PRIVATE" not in repr(caplog.records[0].structured_fields)
    assert [c[0] for c in redis.mock_calls] == ["xread"]
    redis.xread.return_value = []
    assert await inspect_once(redis, stream="s", last_id=last) == last


@pytest.mark.asyncio
async def test_inspection_retries_and_preserves_cursor():
    stop = asyncio.Event()
    redis = AsyncMock()
    calls = 0

    async def read(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            return [("s", [("1-0", {"payload": "legacy"})])]
        if calls == 2:
            raise RedisConnectionError("unavailable")
        assert args[0] == {"s": "1-0"}
        stop.set()
        return []

    redis.xread.side_effect = read
    await inspect_stream(
        redis,
        stream="s",
        stop_event=stop,
        batch_size=1,
        block_ms=1,
        retry_initial_seconds=0.001,
        retry_max_seconds=0.002,
        logger=logging.getLogger("test"),
    )
    assert calls == 3
