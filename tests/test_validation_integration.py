import asyncio
import logging
import os
import uuid
from decimal import Decimal

import asyncpg
import pytest
from redis.asyncio import Redis
from test_catalog_integration import catalog as catalog
from test_catalog_integration import event

from station_persist import service
from station_persist.config import Settings
from station_persist.validation import (
    CatalogConfigurationError,
    exact_numeric,
    validate_entries,
)
from station_persist.validation import ValidationStatus as Status

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_INTEGRATION") != "1",
        reason="Disposable Redis/PostgreSQL required",
    ),
]


@pytest.mark.asyncio
async def test_real_validation_exact_values_limits_partial_results(catalog):
    conn, _ = catalog
    await conn.execute("UPDATE sensors SET min_value=0,max_value=50 WHERE id=20")
    source = event(
        payload='{"unix_time":1760000000,"temp":50.000000000000001,"humidity":true,"meta":23.45678901234567890123456789,"unknown":"original"}'
    )
    async with conn.transaction(isolation="repeatable_read", readonly=True):
        (result,) = await validate_entries([source], conn)
    assert result.resolution.source is source
    assert [f.status for f in result.fields] == [
        Status.OUT_OF_RANGE,
        Status.REJECTED,
        Status.VALID,
        Status.REJECTED,
    ]
    assert result.fields[0].reasons == ("above_max",)
    assert result.fields[1].resolution.value is True
    assert result.fields[3].resolution.value == "original"
    number = result.fields[2].value
    await conn.execute(
        "INSERT INTO readings(id,sensor_id,value,unix_time) VALUES(1,22,$1,1760000000)",
        number,
    )
    assert await conn.fetchval("SELECT value FROM readings WHERE id=1") == Decimal(
        "23.45678901234567890123456789"
    )
    for raw in ("1e131071", "1e-16383", "-123456789012345678901234567890.0123456789"):
        value, reason = exact_numeric(Decimal(raw))
        assert reason is None
        assert await conn.fetchval("SELECT $1::numeric", value) == Decimal(raw)
    for raw in ("1e131072", "1e-16384"):
        assert exact_numeric(Decimal(raw))[1] == "numeric_not_representable"
        with pytest.raises(asyncpg.NumericValueOutOfRangeError):
            await conn.fetchval("SELECT $1::text::numeric", raw)


@pytest.mark.asyncio
async def test_real_validation_snapshot_catalog_refresh_and_schema_failure(catalog):
    conn, dsn = catalog
    await conn.execute("UPDATE sensors SET max_value=10 WHERE id=20")
    writer = await asyncpg.connect(dsn)
    try:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            (first,) = await validate_entries([event()], conn)
            await writer.execute("UPDATE sensors SET max_value=20 WHERE id=20")
            (same,) = await validate_entries([event()], conn)
            assert first == same and first.fields[0].status == Status.OUT_OF_RANGE
        (new,) = await validate_entries([event()], conn)
        assert new.fields[0].status == Status.VALID
        await conn.execute("ALTER TABLE readings ALTER value TYPE decimal(10,2)")
        with pytest.raises(CatalogConfigurationError, match="migration 016"):
            await validate_entries([event()], conn)
        await conn.execute("ALTER TABLE sensors DROP COLUMN min_value")
        with pytest.raises(CatalogConfigurationError):
            await validate_entries([event()], conn)
    finally:
        await writer.close()


@pytest.mark.asyncio
async def test_real_validation_diagnostic_retries_backlog_follow_no_writes(
    catalog, monkeypatch, caplog
):
    conn, dsn = catalog
    redis = Redis.from_url(os.environ["TEST_REDIS_URL"], decode_responses=True)
    stream = f"validation:{uuid.uuid4().hex}"
    group = "existing"
    stop = asyncio.Event()
    observed = asyncio.Queue()
    original = service.validate_entries
    calls = []

    async def validate_checked(entries, db, **kwargs):
        assert await db.fetchval("SHOW transaction_read_only") == "on"
        assert await db.fetchval("SHOW transaction_isolation") == "repeatable read"
        calls.append([e.entry_id for e in entries])
        if len(calls) == 1:
            raise asyncpg.ConnectionDoesNotExistError("transient test failure")
        result = await original(entries, db, **kwargs)
        observed.put_nowait([e.entry_id for e in entries])
        return result

    monkeypatch.setattr(service, "validate_entries", validate_checked)
    task = None
    try:
        first = await redis.xadd(
            stream,
            dict(
                event(
                    payload='{"temp":23.456789012345678901,"meta":"PRIVATE_VALUE"}'
                ).fields
            ),
        )
        await redis.xgroup_create(stream, group, id="0-0")
        await redis.xreadgroup(group, "other", {stream: ">"})
        pending = await redis.xpending(stream, group)
        groups = await redis.xinfo_groups(stream)
        with caplog.at_level(logging.INFO, logger="station_persist"):
            task = asyncio.create_task(
                service.run_inspection(
                    Settings(
                        _env_file=None,
                        redis_url=os.environ["TEST_REDIS_URL"],
                        database_url=dsn,
                        stream=stream,
                        block_ms=10,
                        retry_initial_seconds=0.001,
                    ),
                    stop,
                    validate=True,
                )
            )
            assert await asyncio.wait_for(observed.get(), 5) == [first]
            second = await redis.xadd(
                stream, dict(event(payload='{"unix_time":null,"temp":1}').fields)
            )
            assert await asyncio.wait_for(observed.get(), 5) == [second]

            async def logged():
                while not any(
                    r.msg == "stream_entry_observed"
                    and r.structured_fields["entry_id"] == second
                    for r in caplog.records
                ):
                    await asyncio.sleep(0.001)

            await asyncio.wait_for(logged(), 5)
            stop.set()
            await asyncio.wait_for(task, 5)
        assert calls[:2] == [[first], [first]]
        assert await redis.xpending(stream, group) == pending
        after = await redis.xinfo_groups(stream)
        assert after[0].pop("lag") == groups[0].pop("lag") + 1
        assert after == groups
        assert len(await redis.xrange(stream)) == 2
        assert await conn.fetchval("SELECT count(*) FROM readings") == 0
        logs = repr([r.__dict__ for r in caplog.records])
        assert "PRIVATE_VALUE" not in logs and "23.456789012345678901" not in logs
        results = [
            r.structured_fields
            for r in caplog.records
            if r.msg == "measurement_validation"
        ]
        assert any(r["status"] == Status.VALID for r in results)
        assert any(r["reasons"] == ["invalid_timestamp_type"] for r in results)
    finally:
        stop.set()
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await redis.delete(stream)
        await redis.aclose()
