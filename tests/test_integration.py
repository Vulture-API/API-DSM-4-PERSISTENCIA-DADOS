import asyncio
import os
import uuid

import asyncpg
import pytest
from redis.asyncio import Redis

from station_persist.config import Settings
from station_persist.inspection import inspect_once
from station_persist.service import run_inspection
from station_persist.stream import GroupConsumer, ProcessingResult

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_INTEGRATION") != "1",
        reason="RUN_INTEGRATION=1 requires disposable Redis/PostgreSQL",
    ),
]


@pytest.mark.asyncio
async def test_real_inspection_no_writes_groups_or_pending_changes():
    url = os.environ["TEST_REDIS_URL"]
    dsn = os.environ["TEST_DATABASE_URL"]
    stream, group = f"test:{uuid.uuid4().hex}", uuid.uuid4().hex
    redis = Redis.from_url(url, decode_responses=True)
    conn = await asyncpg.connect(dsn)
    try:
        original = {
            "payload": '{"estacao_id":"e-1","unix_time":"bad"}',
            "unix_time": "null",
            "extra": "legacy",
        }
        first = await redis.xadd(stream, original)
        await redis.xgroup_create(stream, group, id="0-0")
        await redis.xreadgroup(group, "existing", {stream: ">"})
        pending_before = await redis.xpending(stream, group)
        assert await inspect_once(redis, stream=stream, block_ms=1) == first
        second = await redis.xadd(stream, {"payload": "new"})
        assert (
            await inspect_once(redis, stream=stream, last_id=first, block_ms=1)
            == second
        )
        before_db = await conn.fetchval(
            "SELECT count(*) FROM pg_tables WHERE schemaname='public'"
        )
        stop = asyncio.Event()
        task = asyncio.create_task(
            run_inspection(
                Settings(
                    _env_file=None,
                    redis_url=url,
                    database_url=dsn,
                    stream=stream,
                    block_ms=10,
                ),
                stop,
            )
        )
        await asyncio.sleep(0.15)
        stop.set()
        await asyncio.wait_for(task, 3)
        assert await redis.xpending(stream, group) == pending_before
        assert len(await redis.xinfo_groups(stream)) == 1
        assert await redis.xrange(stream) == [
            (first, original),
            (second, {"payload": "new"}),
        ]
        assert (
            await conn.fetchval(
                "SELECT count(*) FROM pg_tables WHERE schemaname='public'"
            )
            == before_db
        )
    finally:
        await redis.delete(stream)
        await redis.aclose()
        await conn.close()


@pytest.mark.asyncio
async def test_real_group_backlog_ack_gated_by_processor_completion():
    redis = Redis.from_url(os.environ["TEST_REDIS_URL"], decode_responses=True)
    stream, group = f"test:{uuid.uuid4().hex}", uuid.uuid4().hex
    outcome = ProcessingResult.INCOMPLETE

    async def process(entry):
        assert entry.fields == {"unix_time": "null", "payload": "legacy"}
        return outcome

    try:
        first = await redis.xadd(stream, {"unix_time": "null", "payload": "legacy"})
        worker = GroupConsumer(
            redis,
            stream=stream,
            group=group,
            consumer="test",
            processor=process,
            block_ms=1,
        )
        await worker.ensure_group()
        await worker.ensure_group()
        assert await worker.process_once() == 0
        assert (await redis.xpending(stream, group))["pending"] == 1
        outcome = ProcessingResult.COMPLETED
        assert await worker.process_once(pending=True) == 1
        assert (await redis.xpending(stream, group))["pending"] == 0
        assert (await redis.xrange(stream))[0][0] == first
    finally:
        await redis.delete(stream)
        await redis.aclose()
