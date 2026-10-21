import asyncio
import json
import logging
import os
import uuid
from decimal import Decimal
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import asyncpg
import pytest
from redis.asyncio import Redis

from station_persist import consumption, persistence
from station_persist.config import Settings
from station_persist.persistence import (
    SourceIdentityError,
    process_entry,
    validate_storage_schema,
)
from station_persist.stream import GroupConsumer, ProcessingResult, StreamEntry
from station_persist.validation import CatalogConfigurationError

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_INTEGRATION") != "1", reason="Disposable services required"
    ),
]


@pytest.fixture
async def storage():
    # Use the versioned BANCO schema, not a parallel handwritten production schema.
    root = Path(
        os.getenv(
            "BANCO_SCHEMA_DIR", str(Path(__file__).parents[2] / "API-DSM-4-BANCO")
        )
    )
    schema = f"persist_{uuid.uuid4().hex}"
    dsn = os.environ["TEST_DATABASE_URL"]
    conn = await asyncpg.connect(dsn)
    pool = None
    try:
        await conn.execute(f'CREATE SCHEMA "{schema}"; SET search_path TO "{schema}"')
        for file in (
            "setup/schema-neon.sql",
            "migrations/015_role_permissions.sql",
            "migrations/016_occurrences.sql",
            "migrations/016_sensor_limits_and_reading_precision.sql",
            "migrations/017_telemetry_destinations_and_idempotency.sql",
        ):
            await conn.execute((root / file).read_text())
        await conn.execute("""
            INSERT INTO roles(id,name) VALUES(1,'test-owner');
            INSERT INTO users(id,role_id,name) VALUES(1,1,'owner');
            INSERT INTO properties(id,name,owner_user_id) VALUES(1,'property',1);
            INSERT INTO stations(id,property_id,mac_address,name) VALUES(1,1,'AA:BB:CC:DD:EE:01','station'),(2,1,'AA:BB:CC:DD:EE:02','other');
            INSERT INTO sensor_types(id,name,unit_of_measure) VALUES(1,'temperature','C');
            INSERT INTO sensors(id,station_id,sensor_type_id,local_identifier,min_value,max_value) VALUES
              (1,1,1,'temp',0,50),(2,1,1,'humidity',NULL,NULL),(3,2,1,'temp',NULL,NULL);
        """)
        parts = urlsplit(dsn)
        scoped = urlunsplit(
            parts._replace(
                query=urlencode(dict(parse_qsl(parts.query)) | {"search_path": schema})
            )
        )
        pool = await asyncpg.create_pool(scoped, min_size=1, max_size=8)
        yield conn, pool, scoped
    finally:
        if pool is not None:
            await pool.close()
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


def entry(
    payload='{"unix_time":1760000000,"temp":23.4567890123456789}',
    entry_id="1-0",
    **fields,
):
    return StreamEntry.from_redis(
        entry_id,
        dict(
            estacao_id="1",
            unix_time="1760000000",
            received_at="1760000000123",
            topic="estacoes/AA:BB:CC:DD:EE:01/dados",
            payload=payload,
        )
        | fields,
    )


async def persist(pool, event, **kwargs):
    return await process_entry(
        event, pool, source_id="test-source", stream="telemetry:ingest", **kwargs
    )


async def counts(conn):
    return tuple(
        [
            await conn.fetchval(f"SELECT count(*) FROM {table}")
            for table in (
                "readings",
                "occurrences",
                "telemetry_events",
                "telemetry_quarantine",
                "telemetry_results",
            )
        ]
    )


@pytest.mark.asyncio
async def test_real_mixed_destinations_exact_and_private_logs(storage, caplog):
    conn, pool, _ = storage
    source = entry(
        '{"unix_time":1760000000,"temp":50.000000000000001,"humidity":23.4567890123456789,"firmware":{"private":"PAYLOAD_SECRET"}}'
    )
    await validate_storage_schema(conn)
    with caplog.at_level(logging.INFO, logger="station_persist"):
        assert await persist(pool, source) is ProcessingResult.COMPLETED
    assert await counts(conn) == (2, 1, 1, 1, 3)
    readings = await conn.fetch(
        "SELECT sensor_id,value,data_consistent FROM readings ORDER BY sensor_id"
    )
    assert (
        readings[0]["value"] == Decimal("50.000000000000001")
        and readings[0]["data_consistent"] is False
    )
    assert (
        readings[1]["value"] == Decimal("23.4567890123456789")
        and readings[1]["data_consistent"] is True
    )
    assert await conn.fetchval("SELECT value FROM occurrences") == readings[0]["value"]
    assert await conn.fetchval("SELECT reasons FROM telemetry_quarantine") == [
        "unknown_sensor"
    ]
    assert json.loads(
        await conn.fetchval("SELECT original_fields FROM telemetry_events")
    ) == dict(source.fields)
    assert (
        await conn.fetchval(
            "SELECT last_communication_at::text FROM stations WHERE id=1"
        )
        == "2025-10-09 08:53:20.123"
    )
    assert (
        await conn.fetchval("SELECT count(*) FROM readings WHERE data_consistent") == 1
    )
    assert (
        "PAYLOAD_SECRET" not in caplog.text and "50.000000000000001" not in caplog.text
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "payload,fields,reason",
    [
        ("{invalid\x00\ud800", {}, "invalid_payload"),
        ('{"unix_time":null,"temp":1}', {}, "invalid_timestamp_type"),
        ('{"temp":true,"humidity":12}', {}, "invalid_numeric_type"),
        ('{"temp":1}', {"estacao_id": "999"}, "unknown_station"),
        ('{"temp":1}', {"received_at": "bad"}, "invalid_received_at"),
        (
            '{"temp":1e9999999999999999999999999999,"humidity":12}',
            {},
            "numeric_not_representable",
        ),
        ('{"bad\\u0000key":1,"humidity":12}', {}, "invalid_sensor_identifier"),
    ],
)
async def test_real_rejections_preserve_original(storage, payload, fields, reason):
    conn, pool, _ = storage
    source = entry(payload, **fields)
    await persist(pool, source)
    assert reason in await conn.fetchval("SELECT reasons FROM telemetry_quarantine")
    assert json.loads(
        await conn.fetchval("SELECT original_fields FROM telemetry_events")
    ) == dict(source.fields)
    assert await conn.fetchval("SELECT completed_at IS NOT NULL FROM telemetry_events")


@pytest.mark.asyncio
async def test_real_identity_conflict_duplicate_and_first_classification_wins(storage):
    conn, pool, _ = storage
    first = entry('{"unix_time":1760000000,"temp":60}')
    await persist(pool, first)
    await persist(pool, first)
    assert await counts(conn) == (1, 1, 1, 0, 1)
    await conn.execute(
        "UPDATE occurrences SET status='RESOLVED',resolved_at=now(); UPDATE sensors SET max_value=100 WHERE id=1"
    )
    await persist(
        pool, entry(first.fields["payload"], "2-0", received_at="1760000001123")
    )
    assert await counts(conn) == (1, 1, 2, 0, 2)
    assert await conn.fetchval("SELECT data_consistent FROM readings") is False
    assert (
        await conn.fetchval(
            "SELECT destination FROM telemetry_results ORDER BY event_id DESC LIMIT 1"
        )
        == "duplicate"
    )
    await persist(pool, entry('{"unix_time":1760000000,"temp":61}', "3-0"))
    assert await conn.fetchval("SELECT reasons FROM telemetry_quarantine") == [
        "conflicting_measurement"
    ]
    assert await conn.fetchval("SELECT value FROM readings") == 60
    with pytest.raises(SourceIdentityError):
        await persist(pool, entry('{"temp":99}', "1-0"))
    assert await counts(conn) == (1, 1, 3, 1, 3)
    assert (
        await conn.fetchval("SELECT count(*) FROM occurrences WHERE status='OPEN'") == 0
    )
    await persist(pool, entry('{"unix_time":1760000001,"temp":101}', "4-0"))
    assert (
        await conn.fetchval("SELECT count(*) FROM occurrences WHERE status='OPEN'") == 1
    )


@pytest.mark.asyncio
async def test_real_concurrent_readings_occurrences_and_conflicts(storage):
    conn, pool, _ = storage

    async def retry(source):
        for _ in range(20):
            try:
                return await persist(pool, source)
            except (asyncpg.SerializationError, asyncpg.DeadlockDetectedError):
                await asyncio.sleep(0.001)
        pytest.fail("Transaction did not converge")

    sources = [
        entry('{"unix_time":1760000000,"temp":60}', f"{i}-0") for i in range(1, 7)
    ]
    await asyncio.gather(*(retry(source) for source in sources + [sources[0]]))
    assert await counts(conn) == (1, 1, 6, 0, 6)
    await asyncio.gather(
        *(
            retry(entry(f'{{"unix_time":1760000000,"temp":{v}}}', f"{v}-0"))
            for v in (61, 62)
        )
    )
    assert await conn.fetchval("SELECT count(*) FROM telemetry_quarantine") == 2
    await asyncio.gather(
        *(
            retry(entry(f'{{"unix_time":{1760000000 + i},"temp":70}}', f"100-{i}"))
            for i in range(1, 4)
        )
    )
    assert await conn.fetchval("SELECT count(*) FROM readings") == 4
    assert await conn.fetchval("SELECT count(*) FROM occurrences") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "table,operation",
    [
        ("telemetry_quarantine", "INSERT"),
        ("occurrences", "INSERT"),
        ("telemetry_results", "INSERT"),
        ("telemetry_events", "UPDATE"),
        ("stations", "UPDATE"),
    ],
)
async def test_real_destination_failure_rolls_back_everything(
    storage, table, operation
):
    conn, pool, _ = storage
    await conn.execute(f"""
        CREATE FUNCTION fail_destination() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected technical failure'; END $$;
        CREATE TRIGGER fail_destination BEFORE {operation} ON {table} FOR EACH ROW EXECUTE FUNCTION fail_destination();
    """)
    with pytest.raises(asyncpg.RaiseError, match="injected"):
        await persist(pool, entry('{"temp":60,"humidity":true}'))
    assert await counts(conn) == (0, 0, 0, 0, 0)
    assert (
        await conn.fetchval("SELECT last_communication_at FROM stations WHERE id=1")
        is None
    )


@pytest.mark.asyncio
async def test_real_cancellation_rolls_back_and_commit_before_ack_is_replayable(
    storage, monkeypatch
):
    conn, pool, _ = storage
    entered = asyncio.Event()
    release = asyncio.Event()
    original = persistence._quarantine

    async def blocked(*args):
        entered.set()
        await release.wait()
        await original(*args)

    monkeypatch.setattr(persistence, "_quarantine", blocked)
    task = asyncio.create_task(persist(pool, entry('{"temp":1,"humidity":true}')))
    await asyncio.wait_for(entered.wait(), 3)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await counts(conn) == (0, 0, 0, 0, 0)
    monkeypatch.setattr(persistence, "_quarantine", original)

    redis = Redis.from_url(os.environ["TEST_REDIS_URL"], decode_responses=True)
    stream = f"commit:{uuid.uuid4().hex}"
    try:
        raw = dict(entry().fields)
        await redis.xadd(stream, raw)
        failures = [True]

        async def processor(source):
            result = await process_entry(
                source, pool, source_id="source", stream=stream
            )
            if failures.pop() if failures else False:
                raise ConnectionError("commit succeeded, response/ACK lost")
            return result

        worker = GroupConsumer(
            redis,
            stream=stream,
            group="g",
            consumer="c",
            block_ms=10,
            processor=processor,
        )
        await worker.ensure_group()
        with pytest.raises(ConnectionError):
            await worker.process_once()
        assert (await redis.xpending(stream, "g"))["pending"] == 1
        assert await counts(conn) == (1, 0, 1, 0, 1)
        assert await worker.process_once(pending=True) == 1
        assert (await redis.xpending(stream, "g"))["pending"] == 0
        assert await counts(conn) == (1, 0, 1, 0, 1)
    finally:
        await redis.delete(stream)
        await redis.aclose()


@pytest.mark.asyncio
async def test_real_communication_timestamp_legacy_and_extreme_precision(storage):
    conn, pool, _ = storage
    await persist(
        pool,
        entry('{"unix_time":null,"temp":1}', "1-0", unix_time="null"),
        legacy_max_id="1-0",
    )
    assert (
        await conn.fetchval("SELECT timestamp_source FROM telemetry_events")
        == "legacy_received_at"
    )
    assert await conn.fetchval("SELECT warnings FROM telemetry_events") == [
        "legacy_timestamp_original_lost"
    ]
    # Invalid measurement timestamp still proves communication; milliseconds retained.
    await persist(
        pool, entry('{"unix_time":null,"temp":2}', "2-0", received_at="1760000001123")
    )
    latest = await conn.fetchval(
        "SELECT last_communication_at FROM stations WHERE id=1"
    )
    await persist(
        pool,
        entry(
            '{"humidity":1.2345678901234567890123456789}',
            "3-0",
            received_at="00000001760000000123",
        ),
    )
    assert (
        await conn.fetchval("SELECT last_communication_at FROM stations WHERE id=1")
        == latest
    )
    assert await conn.fetchval(
        "SELECT value FROM readings WHERE sensor_id=2"
    ) == Decimal("1.2345678901234567890123456789")
    await persist(pool, entry('{"unix_time":1760000010,"temp":1e309}', "4-0"))
    assert await conn.fetchval(
        "SELECT value FROM occurrences WHERE status='OPEN'"
    ) == Decimal("1e309")
    with pytest.raises(asyncpg.NumericValueOutOfRangeError):
        await conn.fetchval(
            "SELECT value::float8 FROM readings WHERE unix_time=1760000010"
        )


@pytest.mark.asyncio
async def test_real_consume_backlog_new_entries_schema_guard_and_shutdown(storage):
    conn, _, scoped = storage
    redis = Redis.from_url(os.environ["TEST_REDIS_URL"], decode_responses=True)
    stream = f"consume:{uuid.uuid4().hex}"
    settings = Settings(
        _env_file=None,
        database_url=scoped,
        redis_url=os.environ["TEST_REDIS_URL"],
        source_id="test-source",
        stream=stream,
        group="test",
        consumer="stable",
        block_ms=10,
    )
    stop = asyncio.Event()
    task = None

    async def await_count(expected):
        async with asyncio.timeout(4):
            while (
                await conn.fetchval("SELECT count(*) FROM telemetry_events") != expected
                or (await redis.xpending(stream, "test"))["pending"]
            ):
                await asyncio.sleep(0.01)

    try:
        await redis.xadd(stream, dict(entry().fields))
        task = asyncio.create_task(consumption.run_consumption(settings, stop))
        # Wait for group creation without assuming scheduler order.
        async with asyncio.timeout(4):
            while not await redis.xinfo_groups(stream):
                await asyncio.sleep(0.01)
        await await_count(1)
        await redis.xadd(
            stream, dict(entry('{"unix_time":1760000001,"temp":24}').fields)
        )
        await await_count(2)
        stop.set()
        await asyncio.wait_for(task, 3)
        assert await conn.fetchval("SELECT count(*) FROM readings") == 2
        await conn.execute("DROP INDEX uq_readings_sensor_unix_time")
        with pytest.raises(CatalogConfigurationError):
            await consumption.run_consumption(
                settings.model_copy(update={"group": "must-not-exist"}), asyncio.Event()
            )
        assert [g["name"] for g in await redis.xinfo_groups(stream)] == ["test"]
    finally:
        stop.set()
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await redis.delete(stream)
        await redis.aclose()


@pytest.mark.asyncio
async def test_real_catalog_locked_until_all_destinations_commit(storage, monkeypatch):
    conn, pool, scoped = storage
    entered, release = asyncio.Event(), asyncio.Event()
    original = persistence._quarantine

    async def held(*args):
        entered.set()
        await release.wait()
        await original(*args)

    monkeypatch.setattr(persistence, "_quarantine", held)
    processing = asyncio.create_task(
        persist(pool, entry('{"temp":60,"humidity":true}'))
    )
    await asyncio.wait_for(entered.wait(), 3)
    writer = await asyncpg.connect(scoped)
    changing = asyncio.create_task(
        writer.execute(
            "UPDATE sensors SET max_value=100,station_id=2,local_identifier='moved' WHERE id=1"
        )
    )
    try:
        async with asyncio.timeout(3):
            while not await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')",
                writer.get_server_pid(),
            ):
                await asyncio.sleep(0.01)
        assert not changing.done()
        release.set()
        await processing
        await changing
        assert await conn.fetchval("SELECT data_consistent FROM readings") is False
        assert (
            await conn.fetchval(
                "SELECT max_value FROM telemetry_results WHERE ordinal=1"
            )
            == 50
        )
        assert await conn.fetchval("SELECT station_id FROM occurrences") == 1
    finally:
        release.set()
        await asyncio.gather(processing, changing, return_exceptions=True)
        await writer.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("force_timeout", [False, True])
async def test_real_shutdown_drains_active_only_or_rolls_back_on_deadline(
    storage, monkeypatch, force_timeout
):
    conn, _, scoped = storage
    redis = Redis.from_url(os.environ["TEST_REDIS_URL"], decode_responses=True)
    stream = f"shutdown:{uuid.uuid4().hex}"
    entered, release, stop = asyncio.Event(), asyncio.Event(), asyncio.Event()
    original = persistence._quarantine

    async def held(*args):
        entered.set()
        await release.wait()
        await original(*args)

    monkeypatch.setattr(persistence, "_quarantine", held)
    settings = Settings(
        _env_file=None,
        database_url=scoped,
        redis_url=os.environ["TEST_REDIS_URL"],
        source_id="stable",
        stream=stream,
        group="g",
        consumer="c",
        block_ms=10,
        shutdown_timeout_seconds=0.02 if force_timeout else 2,
    )
    task = None
    try:
        await redis.xadd(stream, dict(entry('{"temp":1,"humidity":true}').fields))
        await redis.xadd(
            stream, dict(entry('{"unix_time":1760000001,"temp":2}').fields)
        )
        task = asyncio.create_task(consumption.run_consumption(settings, stop))
        await asyncio.wait_for(entered.wait(), 3)
        stop.set()
        if not force_timeout:
            release.set()
        await asyncio.wait_for(task, 3)
        assert await conn.fetchval("SELECT count(*) FROM readings") == (
            0 if force_timeout else 1
        )
        assert (await redis.xpending(stream, "g"))["pending"] == (
            2 if force_timeout else 1
        )
        assert await conn.fetchval("SELECT count(*) FROM telemetry_events") == (
            0 if force_timeout else 1
        )
        # The same identity resumes its pending entries on restart.
        monkeypatch.setattr(persistence, "_quarantine", original)
        stop = asyncio.Event()
        task = asyncio.create_task(consumption.run_consumption(settings, stop))
        async with asyncio.timeout(4):
            while (await redis.xpending(stream, "g"))["pending"]:
                await asyncio.sleep(0.01)
        stop.set()
        await asyncio.wait_for(task, 3)
        assert await conn.fetchval("SELECT count(*) FROM readings") == 2
    finally:
        release.set()
        stop.set()
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await redis.delete(stream)
        await redis.aclose()
