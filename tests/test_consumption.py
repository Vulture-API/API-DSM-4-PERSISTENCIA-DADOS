import asyncio
import json
from unittest.mock import AsyncMock, Mock

import asyncpg
import pytest
from test_service import resources as resources

from station_persist import __main__ as cli
from station_persist import consumption
from station_persist.config import Settings
from station_persist.persistence import original_bytes, process_entry
from station_persist.stream import GroupConsumer, StreamEntry


def settings(**kwargs):
    return Settings(
        _env_file=None,
        database_url="postgres://test",
        **(
            dict(
                source_id="stable-origin",
                retry_initial_seconds=0.001,
                retry_max_seconds=0.002,
            )
            | kwargs
        ),
    )


def test_original_stream_values_round_trip_without_json_numeric_conversion():
    fields = {
        "payload": '{"n":1e9999999999999999999,"bad":"\\u0000"}',
        "nul": "\x00\ud800",
    }
    assert json.loads(original_bytes(StreamEntry.from_redis("1-0", fields))) == fields


@pytest.mark.parametrize("source", ["", "  ", "bad\x00source"])
def test_invalid_source_configuration(source):
    with pytest.raises(ValueError):
        settings(source_id=source)


def test_cli_consume_and_inspection_flags(monkeypatch):
    config = settings()
    run = AsyncMock()
    monkeypatch.setattr(cli, "Settings", lambda: config)
    monkeypatch.setattr(cli, "run_consumption", run)
    cli.main(["consume"])
    run.assert_awaited_once_with(config)
    for flag in ("--resolve", "--validate"):
        with pytest.raises(SystemExit):
            cli.main(["consume", flag])


@pytest.mark.asyncio
async def test_no_source_or_trimmed_entry_cannot_ack():
    with pytest.raises(ValueError, match="PERSIST_SOURCE_ID"):
        await consumption.run_consumption(
            Settings(_env_file=None, database_url="postgres://test")
        )
    with pytest.raises(ValueError):
        await process_entry(
            StreamEntry.from_redis("1-0", {}), None, source_id="", stream="s"
        )
    with pytest.raises(RuntimeError, match="trimmed"):
        await process_entry(
            StreamEntry.from_redis("1-0", {}), None, source_id="origin", stream="s"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        asyncpg.SerializationError("retry"),
        asyncpg.DeadlockDetectedError("retry"),
        ConnectionError("retry"),
    ],
)
async def test_loop_retries_whole_pending_event(error):
    stop = asyncio.Event()
    worker = Mock()
    calls = []

    async def once(*, pending=False):
        calls.append(pending)
        if len(calls) == 1:
            raise error
        if not pending:
            stop.set()
        return 0

    worker.process_once = once
    await consumption.consume_loop(worker, settings(), stop)
    assert calls == [True, True, False]


@pytest.mark.asyncio
async def test_stop_prevents_next_admission():
    stop = asyncio.Event()
    stop.set()
    redis = AsyncMock()
    redis.xreadgroup.return_value = [("s", [("1-0", {"payload": "original"})])]
    process = AsyncMock()
    worker = GroupConsumer(
        redis, stream="s", group="g", consumer="c", processor=process, stop_event=stop
    )
    assert await worker.process_once() == 0
    process.assert_not_awaited()
    redis.xack.assert_not_awaited()


@pytest.mark.asyncio
async def test_runtime_transient_startup_then_fatal_worker_closes_resources(
    resources, monkeypatch
):
    redis, pool, _, create = resources
    create.side_effect = [asyncpg.CannotConnectNowError("transient"), pool]
    monkeypatch.setattr(consumption, "validate_storage_schema", AsyncMock())
    redis.xreadgroup.side_effect = RuntimeError("fatal worker")
    with pytest.raises(RuntimeError, match="fatal worker"):
        await consumption.run_consumption(settings(), asyncio.Event())
    assert create.await_count == 2
    pool.close.assert_awaited_once()
    redis.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_shutdown_cancels_overdue_worker_and_terminates_overdue_pool(
    resources, monkeypatch
):
    redis, pool, _, _ = resources
    stop = asyncio.Event()
    monkeypatch.setattr(consumption, "validate_storage_schema", AsyncMock())
    pool.terminate = Mock()

    async def blocked(*args, **kwargs):
        stop.set()
        await asyncio.Event().wait()

    redis.xreadgroup.side_effect = blocked
    pool.close.side_effect = TimeoutError
    await consumption.run_consumption(settings(shutdown_timeout_seconds=0.001), stop)
    pool.terminate.assert_called_once()
    redis.xack.assert_not_awaited()
    redis.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_stop_during_unavailable_startup_closes_redis(resources):
    redis, pool, _, _ = resources
    stop = asyncio.Event()

    async def unavailable():
        stop.set()
        raise ConnectionError("transient")

    redis.ping.side_effect = unavailable
    await consumption.run_consumption(settings(), stop)
    pool.acquire.assert_not_called()
    redis.aclose.assert_awaited_once()


@pytest.fixture(autouse=True)
def runtime_unit_support(monkeypatch):
    # Lease and operational queries have dedicated real Redis/PG integration tests.
    monkeypatch.setattr(consumption.ConsumerLease, "check", AsyncMock())
    from station_persist import operations

    monkeypatch.setattr(operations, "report_status", AsyncMock())
