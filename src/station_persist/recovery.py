"""Bounded claims with live-owner protection and durable missing-content audit."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from uuid import uuid4

from station_persist.logging_config import log_event
from station_persist.stream import StreamEntry

RENEW = "if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('PEXPIRE',KEYS[1],ARGV[2]) else return 0 end"
RELEASE = "if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) else return 0 end"
CLAIM = """
if redis.call('GET',KEYS[3])~=ARGV[5] then return {-3} end
local p=redis.call('XPENDING',KEYS[1],ARGV[1],ARGV[3],ARGV[3],1)
if #p==0 or p[1][2]~=ARGV[4] or p[1][3]<tonumber(ARGV[6]) then return {0} end
if redis.call('EXISTS',KEYS[2])==1 then return {0} end
if #redis.call('XRANGE',KEYS[1],ARGV[3],ARGV[3],'COUNT',1)==0 then return {-1} end
return redis.call('XCLAIM',KEYS[1],ARGV[1],ARGV[2],ARGV[6],ARGV[3])
"""


class LeaseLostError(RuntimeError):
    pass


def lease_key(stream: str, group: str, consumer: str) -> str:
    digest = hashlib.sha256(
        json.dumps([stream, group, consumer], ensure_ascii=True).encode()
    ).hexdigest()
    return f"persist:lease:{digest}"


class ConsumerLease:
    def __init__(self, redis, settings):
        self.redis, self.settings = redis, settings
        self.key = lease_key(settings.stream, settings.group, settings.consumer)
        self.token = uuid4().hex

    async def acquire(self):
        if not await self.redis.set(
            self.key,
            self.token,
            nx=True,
            px=int(self.settings.lease_ttl_seconds * 1000),
        ):
            raise LeaseLostError("Consumer identity already has a live lease")

    async def check(self):
        if await self.redis.get(self.key) != self.token:
            raise LeaseLostError("Consumer lease lost; stop admissions")

    async def heartbeat(self, stop):
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), self.settings.lease_refresh_seconds)
                return
            except TimeoutError:
                pass
            if not await self.redis.eval(
                RENEW,
                1,
                self.key,
                self.token,
                int(self.settings.lease_ttl_seconds * 1000),
            ):
                raise LeaseLostError("Consumer lease lost during renewal")

    async def release(self):
        await self.redis.eval(RELEASE, 1, self.key, self.token)


async def missing_content(pool, redis, settings, entry_id):
    async with pool.acquire() as conn, conn.transaction():
        event = await conn.fetchrow(
            "SELECT id,completed_at FROM telemetry_events WHERE source_id=$1 AND stream=$2 AND entry_id=$3",
            settings.source_id,
            settings.stream,
            entry_id,
        )
        durable_id = (
            event["id"] if event and event["completed_at"] is not None else None
        )
        await conn.execute(
            "INSERT INTO telemetry_recovery_incidents(source_id,stream,group_name,entry_id,reason,durable_event_id) "
            "VALUES($1,$2,$3,$4,'missing_stream_content',$5) ON CONFLICT(source_id,stream,group_name,entry_id,reason) "
            "DO UPDATE SET last_seen_at=clock_timestamp(),observations=telemetry_recovery_incidents.observations+1,"
            "durable_event_id=EXCLUDED.durable_event_id",
            settings.source_id,
            settings.stream,
            settings.group,
            entry_id,
            durable_id,
        )
    log_event(
        logging.getLogger("station_persist"),
        "missing_stream_content",
        entry_id=entry_id,
        durable_copy=durable_id is not None,
        level=logging.WARNING if durable_id is not None else logging.ERROR,
    )
    if durable_id is not None:
        return int(await redis.xack(settings.stream, settings.group, entry_id))
    return 0


class PendingRecovery:
    def __init__(self, redis, pool, worker, settings, lease):
        self.redis, self.pool, self.worker, self.settings, self.lease = (
            redis,
            pool,
            worker,
            settings,
            lease,
        )
        self.cursor = "-"
        self.next_scan = 0.0

    async def once(self):
        now = asyncio.get_running_loop().time()
        if now < self.next_scan:
            return 0
        await self.lease.check()
        rows = await self.redis.xpending_range(
            self.settings.stream,
            self.settings.group,
            self.cursor,
            "+",
            self.settings.recovery_batch_size,
        )
        # Do not use XPENDING IDLE here: its filtered scan may traverse the
        # entire PEL to find a few idle items. Lua checks idle for each candidate
        # from this strictly bounded, cursor-paginated batch instead.
        if not rows:
            self.cursor = "-"
        self.next_scan = (
            now + self.settings.recovery_interval_seconds if not rows else now
        )
        confirmed = 0
        for row in rows:
            if self.worker.stop_event is not None and self.worker.stop_event.is_set():
                break
            self.cursor = f"({row['message_id']}"
            owner = row["consumer"]
            response = await self.redis.eval(
                CLAIM,
                3,
                self.settings.stream,
                lease_key(self.settings.stream, self.settings.group, owner),
                self.lease.key,
                self.settings.group,
                self.settings.consumer,
                row["message_id"],
                owner,
                self.lease.token,
                self.settings.recovery_min_idle_ms,
            )
            if response == [-3]:
                raise LeaseLostError("Lease lost before recovery")
            if response == [-1]:
                confirmed += await missing_content(
                    self.pool, self.redis, self.settings, row["message_id"]
                )
            elif response and response != [0]:
                entry_id, flat = response[0]
                fields = dict(zip(flat[::2], flat[1::2], strict=True))
                confirmed += await self.worker.process_entries(
                    [StreamEntry.from_redis(entry_id, fields)]
                )
                log_event(
                    logging.getLogger("station_persist"),
                    "pending_recovered",
                    entry_id=entry_id,
                )
        return confirmed
