import asyncio
import logging
import os
import uuid
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import asyncpg
import pytest
from redis.asyncio import Redis

from station_persist import service
from station_persist.config import Settings
from station_persist.relationships import RejectionReason as Reason
from station_persist.relationships import resolve_entries
from station_persist.stream import StreamEntry

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_INTEGRATION") != "1",
        reason="RUN_INTEGRATION=1 requires disposable Redis/PostgreSQL",
    ),
]


@pytest.fixture
async def catalog():
    """Isolated subset of versioned BANCO columns/constraints, never production."""
    schema = f"test_{uuid.uuid4().hex}"
    dsn = os.environ["TEST_DATABASE_URL"]
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(f'CREATE SCHEMA "{schema}"')
        await conn.execute(f'SET search_path TO "{schema}"')
        await conn.execute("""
            CREATE TABLE properties(id integer PRIMARY KEY);
            CREATE TABLE stations(
                id integer PRIMARY KEY,
                property_id integer NOT NULL REFERENCES properties,
                mac_address varchar(50) NOT NULL UNIQUE
            );
            CREATE TABLE sensor_types(id integer PRIMARY KEY);
            CREATE TABLE sensors(
                id integer PRIMARY KEY,
                station_id integer NOT NULL REFERENCES stations,
                sensor_type_id integer NOT NULL REFERENCES sensor_types,
                local_identifier varchar(50) NOT NULL,
                UNIQUE(station_id, local_identifier)
            );
            CREATE TABLE readings(
                id bigint PRIMARY KEY,
                sensor_id integer NOT NULL REFERENCES sensors,
                value decimal(10,2), unix_time bigint, data_consistent boolean
            );
            INSERT INTO properties VALUES (10), (11);
            INSERT INTO stations VALUES
                (1,10,'AA:BB:CC:DD:EE:01'), (2,11,'AA:BB:CC:DD:EE:02');
            INSERT INTO sensor_types VALUES (100), (101);
            INSERT INTO sensors VALUES
                (20,1,100,'temp'), (21,1,101,'humidity'), (22,1,100,'meta'),
                (30,2,100,'temp'), (31,2,101,'only_there'),
                (40,1,100,'Case'), (41,1,100,'case');
        """)
        parts = urlsplit(dsn)
        query = dict(parse_qsl(parts.query)) | {"search_path": schema}
        scoped_dsn = urlunsplit(parts._replace(query=urlencode(query)))
        yield conn, scoped_dsn
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


def event(identity="1", payload='{"temp":12}', entry_id="1-0"):
    return StreamEntry.from_redis(
        entry_id,
        {
            "estacao_id": identity,
            "payload": payload,
            "unix_time": "null",
            "received_at": "1234567890000",
            "topic": "estacoes/AA:BB:CC:DD:EE:02/dados",
            "legacy": "original",
        },
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "identity", ["1", "001", "AA:BB:CC:DD:EE:01", "aa-bb-cc-dd-ee-01", "aabbccddee01"]
)
async def test_real_catalog_id_mac_case_and_station_scope(catalog, identity):
    conn, _ = catalog
    entries = [
        event(
            identity,
            '{"TEMP":"invalid number","humidity":null,"only_there":2,"extra":{"version":1}}',
        ),
        event("2"),
    ]
    async with conn.transaction(readonly=True):
        first, second = await resolve_entries(entries, conn)
    assert (first.station_id, first.property_id) == (1, 10)
    assert first.source is entries[0]
    assert first.fields[0].sensor_id == 20
    assert first.fields[0].value == "invalid number"
    assert first.fields[1].sensor_id == 21 and first.fields[1].value is None
    assert first.fields[2].reasons == (Reason.UNKNOWN_SENSOR,)
    assert first.fields[3].value == {"version": 1}
    assert first.fields[3].reasons == (Reason.UNKNOWN_SENSOR,)
    assert second.fields[0].sensor_id == 30 and second.property_id == 11


@pytest.mark.asyncio
async def test_real_catalog_ambiguities_partial_rejections_and_no_sql_injection(
    catalog,
):
    conn, _ = catalog
    async with conn.transaction(readonly=True):
        (result,) = await resolve_entries(
            [
                event(
                    payload='{"Case":5,"temp":1,"TEMP":2,"humidity":[1,true],"x\\";DROP TABLE sensors;--":3}'
                )
            ],
            conn,
        )
    fields = {field.local_identifier: field for field in result.fields}
    assert fields["Case"].reasons == (Reason.AMBIGUOUS_SENSOR,)
    assert (
        fields["temp"].reasons == fields["TEMP"].reasons == (Reason.DUPLICATE_SENSOR,)
    )
    assert fields["humidity"].associated and fields["humidity"].value == [1, True]
    assert fields['x";DROP TABLE sensors;--'].reasons == (Reason.UNKNOWN_SENSOR,)
    assert await conn.fetchval("SELECT count(*) FROM sensors") == 7
    await conn.execute("INSERT INTO stations VALUES (3,10,'aa-bb-cc-dd-ee-01')")
    mac, legacy = await resolve_entries([event("aabbccddee01"), event("1")], conn)
    assert mac.reasons == (Reason.AMBIGUOUS_STATION,)
    assert legacy.fields[0].sensor_id == 20


@pytest.mark.asyncio
async def test_real_catalog_rejections_and_database_error(catalog):
    conn, _ = catalog
    results = await resolve_entries(
        [
            event("9"),
            event("123456789012"),
            event("bad"),
            event("1", '{"estacao_id":"2","temp":3}'),
            event(payload="[]"),
        ],
        conn,
    )
    assert [r.reasons for r in results] == [
        (r,)
        for r in [
            Reason.UNKNOWN_STATION,
            Reason.IDENTIFICATION_OUT_OF_RANGE,
            Reason.INVALID_IDENTIFICATION,
            Reason.IDENTIFICATION_MISMATCH,
            Reason.INVALID_PAYLOAD,
        ]
    ]
    await conn.execute("ALTER TABLE sensors RENAME TO hidden_sensors")
    with pytest.raises(asyncpg.UndefinedTableError):
        await resolve_entries([event()], conn)


@pytest.mark.asyncio
async def test_real_catalog_snapshot_and_changes_visible_on_next_transaction(catalog):
    conn, dsn = catalog
    writer = await asyncpg.connect(dsn)
    try:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            (before,) = await resolve_entries([event()], conn)
            await writer.execute("UPDATE stations SET property_id=11 WHERE id=1")
            await writer.execute(
                "UPDATE sensors SET local_identifier='changed' WHERE id=20"
            )
            (same_snapshot,) = await resolve_entries([event()], conn)
            assert before == same_snapshot
        (later,) = await resolve_entries([event()], conn)
        assert later.property_id == 11
        assert later.fields[0].reasons == (Reason.UNKNOWN_SENSOR,)
        (changed,) = await resolve_entries([event(payload='{"changed":true}')], conn)
        assert changed.fields[0].sensor_id == 20
    finally:
        await writer.close()


@pytest.mark.asyncio
async def test_real_diagnostic_backlog_follow_shutdown_no_writes_ack_or_groups(
    catalog, monkeypatch, caplog
):
    conn, dsn = catalog
    stream, group = f"test:{uuid.uuid4().hex}", uuid.uuid4().hex
    url = os.environ["TEST_REDIS_URL"]
    redis = Redis.from_url(url, decode_responses=True)
    stop, observed = asyncio.Event(), asyncio.Queue()
    original = service.resolve_entries

    async def resolve_checked(entries, db):
        assert await db.fetchval("SHOW transaction_read_only") == "on"
        assert await db.fetchval("SHOW transaction_isolation") == "repeatable read"
        results = await original(entries, db)
        await observed.put([entry.entry_id for entry in entries])
        return results

    monkeypatch.setattr(service, "resolve_entries", resolve_checked)
    monkeypatch.setattr(service.Redis, "from_url", lambda *a, **k: redis)
    task = None
    try:
        first_event = event(payload='{"temp":"PRIVATE_VALUE","humidity":false}')
        first = await redis.xadd(stream, dict(first_event.fields))
        await redis.xgroup_create(stream, group, id="0-0")
        await redis.xreadgroup(group, "other", {stream: ">"})
        pending_before = await redis.xpending(stream, group)
        groups_before = await redis.xinfo_groups(stream)
        counts = [
            await conn.fetchval(f"SELECT count(*) FROM {table}")
            for table in ("stations", "sensors", "readings")
        ]
        with caplog.at_level(logging.INFO, logger="station_persist"):
            task = asyncio.create_task(
                service.run_inspection(
                    Settings(
                        _env_file=None,
                        redis_url=url,
                        database_url=dsn,
                        stream=stream,
                        block_ms=10,
                    ),
                    stop,
                    resolve=True,
                )
            )
            assert await asyncio.wait_for(observed.get(), 5) == [first]
            second = await redis.xadd(
                stream, dict(event(payload='{"unknown":{}}').fields)
            )
            assert await asyncio.wait_for(observed.get(), 5) == [second]

            async def wait_for_logged_batch():
                while not any(
                    r.msg == "stream_entry_observed"
                    and r.structured_fields["entry_id"] == second
                    for r in caplog.records
                ):
                    await asyncio.sleep(0.001)

            await asyncio.wait_for(wait_for_logged_batch(), 5)
            stop.set()
            await asyncio.wait_for(task, 5)
        assert await redis.xpending(stream, group) == pending_before
        groups_after = await redis.xinfo_groups(stream)
        # Appending the second fixture event changes lag without a group mutation.
        assert groups_after[0].pop("lag") == groups_before[0].pop("lag") + 1
        assert groups_after == groups_before
        assert len(await redis.xrange(stream)) == 2
        assert counts == [
            await conn.fetchval(f"SELECT count(*) FROM {table}")
            for table in ("stations", "sensors", "readings")
        ]
        logs = repr([r.__dict__ for r in caplog.records])
        assert "PRIVATE_VALUE" not in logs
        assert any(
            r.msg == "field_relationship" and r.structured_fields.get("sensor_id") == 20
            for r in caplog.records
        )
        assert any(
            r.msg == "field_relationship"
            and r.structured_fields.get("reasons") == [Reason.UNKNOWN_SENSOR]
            for r in caplog.records
        )
        # A second independent Stream remains group-free during resolution.
        no_group_stream = stream + ":ungrouped"
        try:
            await redis.xadd(no_group_stream, dict(first_event.fields))
            stop = asyncio.Event()
            task = asyncio.create_task(
                service.run_inspection(
                    Settings(
                        _env_file=None,
                        redis_url=url,
                        database_url=dsn,
                        stream=no_group_stream,
                        block_ms=10,
                    ),
                    stop,
                    resolve=True,
                )
            )
            await asyncio.wait_for(observed.get(), 5)
            stop.set()
            await asyncio.wait_for(task, 5)
            assert await redis.xinfo_groups(no_group_stream) == []
        finally:
            await redis.delete(no_group_stream)
    finally:
        stop.set()
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await redis.delete(stream)
        await redis.aclose()


@pytest.mark.asyncio
async def test_real_numeric_mac_catalog_column_does_not_become_legacy_id(catalog):
    conn, _ = catalog
    await conn.execute("INSERT INTO stations VALUES (3,10,'001122334455')")
    await conn.execute("INSERT INTO sensors VALUES (50,3,100,'temp')")
    mac, numeric = await resolve_entries(
        [event("00:11:22:33:44:55"), event("001122334455")], conn
    )
    assert mac.station_id == 3 and mac.fields[0].sensor_id == 50
    assert numeric.reasons == (
        Reason.UNKNOWN_STATION,
    )  # ID 1122334455, no MAC fallback.
