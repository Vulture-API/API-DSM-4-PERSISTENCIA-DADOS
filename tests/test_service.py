import asyncio
import logging
from unittest.mock import AsyncMock, Mock

import pytest

from station_persist import __main__ as cli
from station_persist import service
from station_persist.config import Settings
from station_persist.logging_config import JsonFormatter, configure_logging


@pytest.fixture
def resources(monkeypatch):
    redis = AsyncMock()
    redis.xread.return_value = []
    pool = AsyncMock()
    conn = AsyncMock()
    pool.acquire = Mock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
    create = AsyncMock(return_value=pool)
    monkeypatch.setattr(service.Redis, "from_url", lambda *a, **k: redis)
    monkeypatch.setattr(service.asyncpg, "create_pool", create)
    return redis, pool, conn, create


@pytest.mark.asyncio
async def test_startup_read_only_and_closes(resources):
    redis, pool, conn, _ = resources
    stop = asyncio.Event()

    async def read(*a, **k):
        stop.set()
        return []

    redis.xread.side_effect = read
    await service.run_inspection(
        Settings(_env_file=None, database_url="postgresql://test"), stop
    )
    redis.ping.assert_awaited_once()
    conn.fetchval.assert_awaited_once_with("SELECT 1")
    assert not any("execute" in c[0] for c in conn.mock_calls)
    pool.close.assert_awaited_once()
    redis.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_startup_failure_closes_redis(resources):
    redis, _, _, create = resources
    create.side_effect = RuntimeError("database unavailable")
    with pytest.raises(RuntimeError):
        await service.run_inspection(
            Settings(_env_file=None, database_url="postgresql://test"), asyncio.Event()
        )
    redis.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_worker_failure_closes_both(resources):
    redis, pool, _, _ = resources
    redis.xread.side_effect = RuntimeError("fatal")
    with pytest.raises(RuntimeError):
        await service.run_inspection(
            Settings(_env_file=None, database_url="postgresql://test"), asyncio.Event()
        )
    pool.close.assert_awaited_once()
    redis.aclose.assert_awaited_once()


def test_cli_default_inspect(monkeypatch):
    settings = Settings(_env_file=None, database_url="postgresql://test")
    monkeypatch.setattr(cli, "Settings", lambda: settings)
    run = AsyncMock()
    monkeypatch.setattr(cli, "run_inspection", run)
    cli.main([])
    run.assert_awaited_once_with(settings)
    with pytest.raises(SystemExit):
        cli.main(["consume"])


def test_logging_json_without_payload():
    record = logging.LogRecord("test", logging.INFO, "", 0, "observed", (), None)
    record.structured_fields = {"entry_id": "1-0"}
    assert '"entry_id":"1-0"' in JsonFormatter().format(record)
    configure_logging("WARNING")
    assert logging.getLogger().level == logging.WARNING


@pytest.mark.asyncio
async def test_signal_registration(monkeypatch):
    loop = Mock()
    monkeypatch.setattr(asyncio, "get_running_loop", lambda: loop)
    assert len(service.install_signal_handlers(asyncio.Event())) == 2
    loop.add_signal_handler.side_effect = NotImplementedError
    assert service.install_signal_handlers(asyncio.Event()) == []
