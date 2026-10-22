from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any

from redis.exceptions import ResponseError


@dataclass(frozen=True, slots=True)
class StreamEntry:
    entry_id: str
    fields: Mapping[str, str]

    @classmethod
    def from_redis(cls, entry_id: str, fields: dict[str, str] | None) -> StreamEntry:
        # A trimmed entry has no fields. Preserve it as such; never auto-ACK it.
        return cls(entry_id, MappingProxyType(dict(fields or {})))


class ProcessingResult(Enum):
    COMPLETED = "completed"
    INCOMPLETE = "incomplete"


Processor = Callable[[StreamEntry], Awaitable[ProcessingResult]]


class GroupConsumer:
    """Future processors must signal durable completion explicitly (SCRUM-451)."""

    def __init__(
        self,
        redis: Any,
        *,
        stream: str,
        group: str,
        consumer: str,
        batch_size: int = 500,
        block_ms: int = 1000,
        processor: Processor | None = None,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        if processor is None:
            raise ValueError(
                "A processor is required before group consumption can start"
            )
        self.redis, self.stream = redis, stream
        self.group, self.consumer = group, consumer
        self.batch_size, self.block_ms = batch_size, block_ms
        self.processor = processor
        self.stop_event = stop_event
        self.pending_cursor = "0-0"

    async def ensure_group(self) -> None:
        try:
            await self.redis.xgroup_create(
                self.stream, self.group, id="0-0", mkstream=True
            )
        except ResponseError as exc:
            if not str(exc).startswith("BUSYGROUP"):
                raise

    async def process_once(self, *, pending: bool = False) -> int:
        response = await self.redis.xreadgroup(
            self.group,
            self.consumer,
            {self.stream: self.pending_cursor if pending else ">"},
            count=self.batch_size,
            block=None if pending else self.block_ms,
        )
        entries = [
            StreamEntry.from_redis(entry_id, fields)
            for _stream, messages in response or []
            for entry_id, fields in messages
        ]
        if pending and not entries:
            self.pending_cursor = "0-0"
        return await self.process_entries(entries, pending=pending)

    async def process_entries(self, entries, *, pending=False):
        confirmed = 0
        for entry in entries:
            if self.stop_event is not None and self.stop_event.is_set():
                break
            if pending:
                # Advance only to the attempted entry. A failing first item must
                # not starve the rest of its batch on every scan.
                self.pending_cursor = entry.entry_id
            outcome = await self.processor(entry)
            if outcome is ProcessingResult.COMPLETED:
                confirmed += int(
                    await self.redis.xack(self.stream, self.group, entry.entry_id)
                )
        return confirmed
