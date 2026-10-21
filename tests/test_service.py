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


def test_cli_resolve(monkeypatch):
    settings = Settings(_env_file=None, database_url="postgresql://test")
    monkeypatch.setattr(cli, "Settings", lambda: settings)
    run = AsyncMock()
    monkeypatch.setattr(cli, "run_inspection", run)
    cli.main(["inspect", "--resolve"])
    run.assert_awaited_once_with(settings, resolve=True)


@pytest.mark.asyncio
async def test_diagnostic_snapshot_and_private_logs(resources, caplog):
    redis, _, conn, _ = resources
    stop = asyncio.Event()
    transaction = Mock()
    transaction.__aenter__ = AsyncMock()
    transaction.__aexit__ = AsyncMock()
    conn.transaction = Mock(return_value=transaction)
    conn.fetch.side_effect = [
        [dict(id=1, property_id=2, mac_address="AA:BB:CC:DD:EE:01")],
        [
            dict(
                requested_station_id=1,
                requested_identifier="temp",
                station_id=1,
                sensor_id=3,
                sensor_type_id=4,
            )
        ],
    ]

    async def read(*a, **k):
        stop.set()
        return [
            (
                "s",
                [
                    (
                        "1-0",
                        {
                            "estacao_id": "1",
                            "payload": '{"temp":"PRIVATE_MEASUREMENT"}',
                        },
                    )
                ],
            )
        ]

    redis.xread.side_effect = read
    with caplog.at_level(logging.INFO, logger="station_persist"):
        await service.run_inspection(
            Settings(_env_file=None, database_url="postgresql://test"),
            stop,
            resolve=True,
        )
    conn.transaction.assert_called_once_with(isolation="repeatable_read", readonly=True)
    records = [
        r.structured_fields for r in caplog.records if r.msg == "field_relationship"
    ]
    assert records == [
        dict(
            entry_id="1-0",
            local_identifier="temp",
            station_id=1,
            property_id=2,
            sensor_id=3,
            sensor_type_id=4,
            associated=True,
            reasons=[],
        )
    ]
    assert "PRIVATE_MEASUREMENT" not in repr([r.__dict__ for r in caplog.records])
    assert all(call[0] in {"ping", "xread", "aclose"} for call in redis.mock_calls)
    assert not any("execute" in call[0] for call in conn.mock_calls)


def test_cli_validate_includes_resolution(monkeypatch):
    settings = Settings(_env_file=None, database_url="postgresql://test")
    monkeypatch.setattr(cli, "Settings", lambda: settings)
    run = AsyncMock()
    monkeypatch.setattr(cli, "run_inspection", run)
    cli.main(["inspect", "--validate", "--resolve"])
    run.assert_awaited_once_with(settings, validate=True)
