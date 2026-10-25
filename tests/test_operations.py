import asyncio
import json
from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import ValidationError

from station_persist import __main__ as cli
from station_persist import operations
from station_persist.config import Settings


def settings(**overrides):
    return Settings(
        _env_file=None,
        database_url="postgresql://localhost/test",
        source_id="source",
        **overrides,
    )


@pytest.mark.parametrize(
    "overrides",
    [
        {"lease_ttl_seconds": 49},
        {"recovery_min_idle_ms": 59999},
        {"lease_refresh_seconds": 0},
        {"recovery_batch_size": 1001},
        {"processing_timeout_seconds": 0},
        {"metrics_interval_seconds": 0},
    ],
)
def test_invalid_recovery_configuration(overrides):
    with pytest.raises(ValidationError):
        settings(**overrides)


async def test_status_with_database_unavailable_keeps_unknowns(monkeypatch):
    redis = AsyncMock()
    redis.xinfo_groups.return_value = []
    monkeypatch.setattr(operations.Redis, "from_url", lambda *a, **kw: redis)
    monkeypatch.setattr(
        operations.asyncpg,
        "create_pool",
        AsyncMock(side_effect=ConnectionError("secret DSN")),
    )
    result = await operations.run_operation(settings(), "status")
    assert result["pending"] is None and result["quarantine_open"] is None
    assert result["degraded"] == ["missing_group", "postgres:ConnectionError"]
    assert "secret" not in json.dumps(result)
    redis.aclose.assert_awaited_once()


async def test_status_unknown_redis_lag_is_never_zero():
    redis = AsyncMock()
    redis.xinfo_groups.return_value = [{"name": "postgres-persister", "lag": None}]
    redis.xpending.return_value = {"pending": 0, "min": None}
    result = await operations.status_snapshot(settings(), None, redis)
    assert result["lag"] is None and result["pending"] == 0
    assert result["oldest_pending_age_seconds"] == 0
    redis.xinfo_groups.side_effect = ConnectionError("secret")
    result = await operations.status_snapshot(settings(), None, redis)
    assert result["pending"] is None
    assert result["degraded"] == ["redis:ConnectionError", "postgres:ConnectionError"]


async def test_periodic_metrics_emit_and_stop(monkeypatch):
    stop = asyncio.Event()
    snapshot = AsyncMock(return_value={"lag": None, "pending": 4})

    def log(*args, **kwargs):
        stop.set()

    monkeypatch.setattr(operations, "status_snapshot", snapshot)
    monkeypatch.setattr(operations, "log_event", log)
    await operations.report_status(settings(), Mock(), Mock(), stop)
    snapshot.assert_awaited_once()


@pytest.mark.parametrize(
    "argv",
    [
        ["quarantine"],
        ["quarantine", "show"],
        ["quarantine", "replay", "--id", "1"],
        ["status", "show"],
        ["quarantine", "list", "--limit", "0"],
        ["quarantine", "show", "--id", "-1"],
        ["inspect", "--dry-run"],
        ["status", "--validate"],
        ["quarantine", "list", "--after-id", "-1"],
    ],
)
def test_cli_rejects_invalid_operation_arguments(argv):
    with pytest.raises(SystemExit) as exc:
        cli.main(argv)
    assert exc.value.code == 2


def test_cli_status_prints_degraded_json(monkeypatch, capsys):
    monkeypatch.setattr(cli, "Settings", settings)
    monkeypatch.setattr(
        cli,
        "run_operation",
        AsyncMock(
            return_value={"pending": None, "degraded": ["redis:ConnectionError"]}
        ),
    )
    with pytest.raises(SystemExit) as exc:
        cli.main(["status"])
    assert exc.value.code == 1
    assert json.loads(capsys.readouterr().out)["pending"] is None


def test_cli_quarantine_replay_routes_explicit_audit_parameters(monkeypatch, capsys):
    monkeypatch.setattr(cli, "Settings", settings)
    operation = AsyncMock(return_value={"status": "DRY_RUN"})
    monkeypatch.setattr(cli, "run_operation", operation)
    cli.main(
        [
            "quarantine",
            "replay",
            "--id",
            "2",
            "--request-id",
            "request",
            "--operator",
            "op",
            "--reason",
            "fixed",
            "--dry-run",
        ]
    )
    assert operation.call_args.kwargs["operator"] == "op"
    assert operation.call_args.kwargs["dry_run"] is True
    assert json.loads(capsys.readouterr().out)["status"] == "DRY_RUN"


async def test_operational_command_requires_source_identity():
    with pytest.raises(ValueError, match="SOURCE_ID"):
        await operations.run_operation(
            settings().model_copy(update={"source_id": None}), "status"
        )
