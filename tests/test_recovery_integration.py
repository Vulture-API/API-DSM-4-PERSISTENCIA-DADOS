"""SCRUM-530 fault scenarios against real Redis and the versioned BANCO schema."""

import asyncio
import os
from uuid import uuid4

import asyncpg
import pytest
from redis.asyncio import Redis
from test_persistence_integration import entry, persist
from test_persistence_integration import storage as storage_fixture

from station_persist import consumption, replay
from station_persist.config import Settings
from station_persist.operations import run_operation, status_snapshot
from station_persist.persistence import process_entry
from station_persist.recovery import (
    ConsumerLease,
    LeaseLostError,
    PendingRecovery,
    lease_key,
    missing_content,
)
from station_persist.stream import GroupConsumer, ProcessingResult

storage = storage_fixture

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_INTEGRATION") != "1", reason="Disposable services required"
    ),
]


@pytest.fixture
async def recovery_env(storage):
    conn, pool, scoped = storage
    redis = Redis.from_url(os.environ["TEST_REDIS_URL"], decode_responses=True)
    stream = f"recovery:{uuid4().hex}"
    settings = Settings(
        _env_file=None,
        database_url=scoped,
        redis_url=os.environ["TEST_REDIS_URL"],
        source_id="test-source",
        stream=stream,
        group="g",
        consumer="replacement",
        block_ms=10,
        recovery_min_idle_ms=200,
        lease_ttl_seconds=0.15,
        lease_refresh_seconds=0.02,
        processing_timeout_seconds=0.08,
        recovery_interval_seconds=0.01,
        recovery_batch_size=2,
    )
    lease = ConsumerLease(redis, settings)
    await lease.acquire()
    try:
        yield conn, pool, redis, settings, lease
    finally:
        await lease.release()
        await redis.delete(stream)
        await redis.aclose()


async def setup_pending(redis, settings, *, consumer="dead", fields=None, count=1):
    await redis.xgroup_create(settings.stream, settings.group, id="0-0", mkstream=True)
    ids = [
        await redis.xadd(settings.stream, fields or dict(entry().fields))
        for _ in range(count)
    ]
    await redis.xreadgroup(
        settings.group, consumer, {settings.stream: ">"}, count=count
    )
    # Simulate idle time deterministically; no long sleep or unavailable broker.
    await redis.xclaim(settings.stream, settings.group, consumer, 0, ids, idle=1000)
    return ids


def worker_for(pool, redis, settings, *, processor=None):
    async def processor_default(event):
        return await process_entry(
            event, pool, source_id=settings.source_id, stream=settings.stream
        )

    return GroupConsumer(
        redis,
        stream=settings.stream,
        group=settings.group,
        consumer=settings.consumer,
        processor=processor or processor_default,
        block_ms=10,
        batch_size=10,
    )


async def test_real_recovery_claims_abandoned_but_protects_live_owner(recovery_env):
    conn, pool, redis, s, lease = recovery_env
    ids = await setup_pending(redis, s, count=2)
    await redis.set(lease_key(s.stream, s.group, "dead"), "live", px=10000)
    recovery = PendingRecovery(redis, pool, worker_for(pool, redis, s), s, lease)
    assert await recovery.once() == 0
    assert (await redis.xpending(s.stream, s.group))["pending"] == 2
    await redis.delete(lease_key(s.stream, s.group, "dead"))
    recovery.cursor = "-"
    recovery.next_scan = 0
    assert await recovery.once() == 2
    assert await conn.fetchval("SELECT count(*) FROM readings") == 1
    assert await conn.fetchval("SELECT count(*) FROM telemetry_events") == 2
    assert (await redis.xpending(s.stream, s.group))["pending"] == 0
    assert len(ids) == 2


async def test_real_missing_content_never_disappears_without_durable_proof(
    recovery_env,
):
    conn, pool, redis, s, lease = recovery_env
    ids = await setup_pending(redis, s, count=2)
    rows = await redis.xrange(s.stream, ids[1], ids[1])
    from station_persist.stream import StreamEntry

    await process_entry(
        StreamEntry.from_redis(*rows[0]), pool, source_id=s.source_id, stream=s.stream
    )
    await redis.xdel(s.stream, *ids)
    recovery = PendingRecovery(redis, pool, worker_for(pool, redis, s), s, lease)
    assert await recovery.once() == 1
    pending = await redis.xpending_range(s.stream, s.group, "-", "+", 10)
    assert [p["message_id"] for p in pending] == ids[:1]
    incidents = await conn.fetch(
        "SELECT entry_id,durable_event_id FROM telemetry_recovery_incidents ORDER BY entry_id"
    )
    assert [i["entry_id"] for i in incidents] == ids
    assert (
        incidents[0]["durable_event_id"] is None
        and incidents[1]["durable_event_id"] is not None
    )
    # An unresolved tombstone cannot block valid new traffic.
    await redis.xadd(s.stream, dict(entry('{"unix_time":1760000001,"temp":21}').fields))
    assert await recovery.worker.process_once() == 1
    assert (await redis.xpending(s.stream, s.group))["pending"] == 1
    await missing_content(pool, redis, s, ids[0])
    assert (
        await conn.fetchval(
            "SELECT max(observations) FROM telemetry_recovery_incidents"
        )
        == 2
    )


async def test_real_missing_content_audit_failure_does_not_ack(recovery_env):
    conn, pool, redis, s, _ = recovery_env
    (identifier,) = await setup_pending(redis, s)
    await redis.xdel(s.stream, identifier)
    await conn.execute("DROP TABLE telemetry_recovery_incidents")
    with pytest.raises(asyncpg.UndefinedTableError):
        await missing_content(pool, redis, s, identifier)
    assert (await redis.xpending(s.stream, s.group))["pending"] == 1


async def test_real_lease_fencing_and_token_safe_release(recovery_env):
    _, _, redis, s, lease = recovery_env
    other = ConsumerLease(redis, s)
    with pytest.raises(LeaseLostError):
        await other.acquire()
    await other.release()
    await lease.check()
    stop = asyncio.Event()
    heartbeat = asyncio.create_task(lease.heartbeat(stop))
    await asyncio.sleep(0.03)
    assert await redis.pttl(lease.key) > 0
    await redis.set(lease.key, "new-owner", px=10000)
    with pytest.raises(LeaseLostError):
        await asyncio.wait_for(heartbeat, 0.3)
    with pytest.raises(LeaseLostError):
        await lease.check()
    await lease.release()
    assert await redis.get(lease.key) == "new-owner"
    await redis.delete(lease.key)


async def test_real_failing_pending_does_not_starve_same_batch(recovery_env):
    _, pool, redis, s, lease = recovery_env
    ids = await setup_pending(redis, s, count=3)
    seen = []

    async def processor(event):
        seen.append(event.entry_id)
        if event.entry_id == ids[0]:
            raise ConnectionError("transient")
        return ProcessingResult.COMPLETED

    worker = worker_for(pool, redis, s, processor=processor)
    recovery = PendingRecovery(redis, pool, worker, s, lease)
    with pytest.raises(ConnectionError):
        await recovery.once()
    assert await recovery.once() == 2  # continues after the failed first entry
    assert seen == ids
    # Own PEL follows the same per-attempt cursor rule.
    await redis.xclaim(s.stream, s.group, s.consumer, 0, ids[:1])
    new = await redis.xadd(s.stream, dict(entry().fields))
    await redis.xreadgroup(s.group, s.consumer, {s.stream: ">"})
    with pytest.raises(ConnectionError):
        await worker.process_once(pending=True)
    assert await worker.process_once(pending=True) == 1
    assert seen[-1] == new


async def test_real_runtime_lease_loss_rolls_back_active_transaction(
    recovery_env, monkeypatch
):
    conn, _, redis, s, lease = recovery_env
    await lease.release()  # runtime acquires its own instance token
    entered = asyncio.Event()
    from station_persist import persistence

    original = persistence._quarantine

    async def held(*args):
        entered.set()
        await asyncio.Event().wait()
        await original(*args)

    monkeypatch.setattr(persistence, "_quarantine", held)
    await redis.xadd(s.stream, dict(entry('{"temp":1,"humidity":true}').fields))
    task = asyncio.create_task(consumption.run_consumption(s, asyncio.Event()))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        await redis.set(lease.key, "new-token", px=10000)
        with pytest.raises(LeaseLostError):
            await asyncio.wait_for(task, 3)
        assert await conn.fetchval("SELECT count(*) FROM telemetry_events") == 0
        assert (await redis.xpending(s.stream, s.group))["pending"] == 1
        assert await redis.get(lease.key) == "new-token"
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await redis.delete(lease.key)


async def do_replay(pool, qid, **kwargs):
    return await replay.replay_quarantine(
        pool,
        quarantine_id=qid,
        request_id=kwargs.pop("request_id", str(uuid4())),
        operator="operator",
        reason="catalog corrected",
        source_id="test-source",
        stream="telemetry:ingest",
        **kwargs,
    )


async def test_real_replay_unknown_sensor_immutable_and_request_idempotency(storage):
    conn, pool, _ = storage
    await persist(
        pool, entry('{"unix_time":1760000000,"temp":20,"new":23.1234567890123456789}')
    )
    original = await conn.fetchrow("SELECT * FROM telemetry_events")
    q = await conn.fetchrow("SELECT * FROM telemetry_quarantine")
    await conn.execute(
        "INSERT INTO sensors(id,station_id,sensor_type_id,local_identifier) VALUES(10,1,1,'new')"
    )
    request = str(uuid4())
    dry = await do_replay(pool, q["id"], request_id=request, dry_run=True)
    assert (dry.accepted, dry.rejected) == (1, 0)
    assert await conn.fetchval("SELECT count(*) FROM telemetry_replay_requests") == 0
    outcome = await do_replay(pool, q["id"], request_id=request)
    assert (outcome.status, outcome.accepted, outcome.rejected) == ("SUCCEEDED", 1, 0)
    assert await do_replay(pool, q["id"], request_id=request) == outcome
    assert await conn.fetchval("SELECT count(*) FROM readings") == 2
    assert await conn.fetchrow("SELECT * FROM telemetry_events") == original
    assert (await conn.fetchrow("SELECT * FROM telemetry_quarantine"))["reasons"] == q[
        "reasons"
    ]
    assert await conn.fetchval("SELECT status FROM telemetry_quarantine") == "RESOLVED"
    assert await conn.fetchval("SELECT count(*) FROM telemetry_replay_attempts") == 1
    from decimal import Decimal

    assert await conn.fetchval(
        "SELECT value FROM readings WHERE sensor_id=10"
    ) == Decimal("23.1234567890123456789")
    with pytest.raises(ValueError, match="different parameters"):
        await replay.replay_quarantine(
            pool,
            quarantine_id=q["id"],
            request_id=request,
            operator="different",
            reason="catalog corrected",
            source_id="test-source",
            stream="telemetry:ingest",
        )
    with pytest.raises(ValueError, match="configured source"):
        await replay.replay_quarantine(
            pool,
            quarantine_id=q["id"],
            request_id=str(uuid4()),
            operator="op",
            reason="fix",
            source_id="other",
            stream="telemetry:ingest",
        )


async def test_real_replay_partial_event_skips_previously_accepted_fields(storage):
    conn, pool, _ = storage
    await persist(
        pool,
        entry('{"unix_time":null,"a":60,"b":24}', estacao_id="99", unix_time="null"),
        legacy_max_id="1-0",
    )
    qid = await conn.fetchval("SELECT id FROM telemetry_quarantine")
    await conn.execute(
        "INSERT INTO stations(id,property_id,name,mac_address) VALUES(99,1,'added','AA:BB:CC:DD:EE:99'); INSERT INTO sensors(id,station_id,sensor_type_id,local_identifier,min_value,max_value) VALUES(10,99,1,'a',0,50)"
    )
    result = await do_replay(pool, qid)
    assert (result.status, result.accepted, result.rejected) == ("PARTIAL", 1, 1)
    assert await conn.fetchval("SELECT unix_time FROM readings") == 1760000000
    assert (
        await conn.fetchval(
            "SELECT last_communication_at::text FROM stations WHERE id=99"
        )
        == "2025-10-09 08:53:20.123"
    )
    assert await conn.fetchval("SELECT data_consistent FROM readings") is False
    assert await conn.fetchval("SELECT count(*) FROM occurrences") == 1
    # If the first sensor is renamed, revalidation must NOT process its ordinal again.
    await conn.execute(
        "UPDATE sensors SET local_identifier='renamed' WHERE id=10; INSERT INTO sensors(id,station_id,sensor_type_id,local_identifier) VALUES(11,99,1,'b')"
    )
    dry = await do_replay(pool, qid, dry_run=True)
    assert (dry.accepted, dry.rejected) == (1, 0)
    result = await do_replay(pool, qid)
    assert (result.status, result.accepted, result.rejected) == ("SUCCEEDED", 1, 0)
    assert await conn.fetchval("SELECT count(*) FROM readings") == 2
    assert (
        await conn.fetchval(
            "SELECT count(*) FROM telemetry_replay_results WHERE ordinal=1"
        )
        == 1
    )
    assert await conn.fetchval(
        "SELECT bool_and(legacy_max_id='1-0' AND timestamp_source='legacy_received_at') FROM telemetry_replay_results"
    )


async def test_real_replay_conflict_never_overwrites_and_invalid_payload_stays_rejected(
    storage,
):
    conn, pool, _ = storage
    await persist(pool, entry('{"temp":20}'))
    await persist(pool, entry('{"temp":21}', "2-0"))
    qid = await conn.fetchval("SELECT id FROM telemetry_quarantine")
    dry = await do_replay(pool, qid, dry_run=True)
    assert (dry.accepted, dry.rejected) == (0, 1)
    assert (await do_replay(pool, qid)).status == "REJECTED"
    assert await conn.fetchval("SELECT value FROM readings") == 20
    await persist(pool, entry('{"unix_time":null,"temp":20}', "3-0"))
    qid = await conn.fetchval("SELECT max(id) FROM telemetry_quarantine")
    assert (await do_replay(pool, qid)).status == "REJECTED"
    assert await conn.fetchval("SELECT count(*) FROM readings") == 1


@pytest.mark.parametrize("cancel", [False, True])
async def test_real_replay_failure_is_audited_and_same_request_can_retry(
    storage, monkeypatch, cancel
):
    conn, pool, _ = storage
    await persist(pool, entry('{"new":25}'))
    qid = await conn.fetchval("SELECT id FROM telemetry_quarantine")
    await conn.execute(
        "INSERT INTO sensors(id,station_id,sensor_type_id,local_identifier) VALUES(10,1,1,'new')"
    )
    entered = asyncio.Event()
    original = replay.write_measurement

    async def fail(*args):
        entered.set()
        if cancel:
            await asyncio.Event().wait()
        raise RuntimeError("technical failure")

    monkeypatch.setattr(replay, "write_measurement", fail)
    request = str(uuid4())
    task = asyncio.create_task(do_replay(pool, qid, request_id=request))
    await asyncio.wait_for(entered.wait(), 3)
    if cancel:
        task.cancel()
    with pytest.raises(asyncio.CancelledError if cancel else RuntimeError):
        await task
    assert await conn.fetchval("SELECT count(*) FROM readings") == 0
    assert await conn.fetchval("SELECT count(*) FROM telemetry_replay_results") == 0
    assert await conn.fetchval("SELECT status FROM telemetry_replay_attempts") == (
        "INTERRUPTED" if cancel else "TECHNICAL_FAILURE"
    )
    monkeypatch.setattr(replay, "write_measurement", original)
    assert (await do_replay(pool, qid, request_id=request)).status == "SUCCEEDED"
    assert await conn.fetchval("SELECT count(*) FROM telemetry_replay_attempts") == 2


async def test_real_replay_concurrent_request_is_busy_and_response_loss_does_not_repeat(
    storage, monkeypatch
):
    conn, pool, _ = storage
    await persist(pool, entry('{"new":25}'))
    qid = await conn.fetchval("SELECT id FROM telemetry_quarantine")
    await conn.execute(
        "INSERT INTO sensors(id,station_id,sensor_type_id,local_identifier) VALUES(10,1,1,'new')"
    )
    entered, release = asyncio.Event(), asyncio.Event()
    original = replay.write_measurement

    async def held(*args):
        entered.set()
        await release.wait()
        return await original(*args)

    monkeypatch.setattr(replay, "write_measurement", held)
    request = str(uuid4())
    first = asyncio.create_task(do_replay(pool, qid, request_id=request))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        assert (await do_replay(pool, qid, request_id=request)).status == "BUSY"
    finally:
        release.set()
    result = await first
    assert await do_replay(pool, qid, request_id=request) == result
    assert await conn.fetchval("SELECT count(*) FROM readings") == 1


async def test_real_status_quarantine_metadata_and_no_redis_for_replay(
    recovery_env, monkeypatch
):
    conn, pool, redis, s, _ = recovery_env
    (identifier,) = await setup_pending(redis, s)
    from station_persist.stream import StreamEntry

    await process_entry(
        StreamEntry.from_redis(identifier, dict(entry('{"bad":true}').fields)),
        pool,
        source_id=s.source_id,
        stream=s.stream,
    )
    qid = await conn.fetchval("SELECT id FROM telemetry_quarantine")
    snapshot = await status_snapshot(s, pool, redis)
    assert snapshot["pending"] == 1 and snapshot["quarantine_open"] == 1
    assert snapshot["oldest_pending_age_seconds"] >= 0 and not snapshot["degraded"]

    def no_redis(*args, **kwargs):
        raise AssertionError("quarantine must be PostgreSQL-only")

    monkeypatch.setattr("station_persist.operations.Redis.from_url", no_redis)
    listed = await run_operation(s, "quarantine", action="list", limit=1)
    assert listed["items"][0]["id"] == qid and listed["next_after_id"] == qid
    shown = await run_operation(s, "quarantine", action="show", quarantine_id=qid)
    assert shown["reasons"] == ["unknown_sensor"] and shown["attempts"] == []
    result = await run_operation(
        s,
        "quarantine",
        action="replay",
        quarantine_id=qid,
        request_id=str(uuid4()),
        operator="op",
        reason="check",
        dry_run=True,
    )
    assert result["status"] == "DRY_RUN"
    assert await conn.fetchval("SELECT count(*) FROM telemetry_replay_requests") == 0


async def test_real_replay_two_requests_for_same_event_serialize(storage):
    conn, pool, _ = storage
    await persist(pool, entry('{"new":25}'))
    qid = await conn.fetchval("SELECT id FROM telemetry_quarantine")
    await conn.execute(
        "INSERT INTO sensors(id,station_id,sensor_type_id,local_identifier) VALUES(10,1,1,'new')"
    )
    results = await asyncio.gather(do_replay(pool, qid), do_replay(pool, qid))
    assert all(r.status == "SUCCEEDED" for r in results)
    assert sum(r.accepted for r in results) == 1
    assert await conn.fetchval("SELECT count(*) FROM readings") == 1
    assert await conn.fetchval("SELECT count(*) FROM telemetry_replay_results") == 1


async def test_real_replay_ambiguous_commit_retries_without_repeating_effects(storage):
    conn, pool, _ = storage
    await persist(pool, entry('{"new":25}'))
    qid = await conn.fetchval("SELECT id FROM telemetry_quarantine")
    await conn.execute(
        "INSERT INTO sensors(id,station_id,sensor_type_id,local_identifier) VALUES(10,1,1,'new')"
    )
    lost = [True]

    class AmbiguousTransaction:
        def __init__(self, tx):
            self.tx = tx

        async def __aenter__(self):
            return await self.tx.__aenter__()

        async def __aexit__(self, *args):
            result = await self.tx.__aexit__(*args)
            if args[0] is None and lost:
                lost.pop()
                raise ConnectionError("COMMIT response lost after durable success")
            return result

    class Connection:
        def __init__(self, conn):
            self.conn = conn

        def __getattr__(self, name):
            return getattr(self.conn, name)

        def transaction(self, **kwargs):
            tx = self.conn.transaction(**kwargs)
            return (
                AmbiguousTransaction(tx)
                if kwargs.get("isolation") == "repeatable_read"
                else tx
            )

    class Acquisition:
        async def __aenter__(self):
            self.ctx = pool.acquire()
            return Connection(await self.ctx.__aenter__())

        async def __aexit__(self, *args):
            return await self.ctx.__aexit__(*args)

    class Pool:
        def acquire(self):
            return Acquisition()

    result = await do_replay(Pool(), qid)
    assert result.status == "SUCCEEDED" and result.accepted == 1
    assert await conn.fetchval("SELECT count(*) FROM telemetry_replay_attempts") == 1
    assert (
        await conn.fetchval("SELECT status FROM telemetry_replay_attempts")
        == "SUCCEEDED"
    )
    assert await conn.fetchval("SELECT count(*) FROM readings") == 1
