import json
import os
from decimal import Decimal

import pytest
from test_persistence_integration import counts, entry, persist
from test_persistence_integration import storage as storage

from station_persist.persistence import validate_storage_schema
from station_persist.stream import ProcessingResult
from station_persist.validation import CatalogConfigurationError

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_INTEGRATION") != "1", reason="Disposable services required"
    ),
]


async def reading(conn, sensor_id):
    return await conn.fetchrow(
        "SELECT value,data_consistent FROM readings WHERE sensor_id=$1", sensor_id
    )


@pytest.mark.asyncio
async def test_real_reading_is_stored_converted_and_original_payload_is_kept(storage):
    conn, pool, _ = storage
    await conn.execute(
        "UPDATE sensor_types SET factor=3.6, gain=2, offset_value=1 WHERE id=1;"
        "UPDATE sensors SET gain=0.000805860805, offset_value=-0.125 WHERE id=2"
    )
    source = entry('{"unix_time":1760000000,"temp":10,"humidity":4095}')
    await validate_storage_schema(conn)
    assert await persist(pool, source) is ProcessingResult.COMPLETED

    # temp: calibração do tipo (10 × 3,6 × 2 + 1). humidity: fator do tipo com
    # ganho e offset do próprio sensor (4095 × 3,6 × 0,000805860805 − 0,125).
    assert (await reading(conn, 1))["value"] == Decimal("73")
    assert (await reading(conn, 2))["value"] == Decimal("11.75499998731")
    assert json.loads(
        await conn.fetchval("SELECT original_fields FROM telemetry_events")
    ) == dict(source.fields)


@pytest.mark.asyncio
async def test_real_limits_use_the_converted_value(storage):
    conn, pool, _ = storage
    await conn.execute("UPDATE sensors SET gain=0.5 WHERE id=1")
    inside = entry('{"unix_time":1760000000,"temp":100}', "1-0")
    outside = entry('{"unix_time":1760000060,"temp":102}', "2-0")
    assert await persist(pool, inside) is ProcessingResult.COMPLETED
    assert await persist(pool, outside) is ProcessingResult.COMPLETED

    rows = await conn.fetch(
        "SELECT value,data_consistent FROM readings WHERE sensor_id=1 ORDER BY unix_time"
    )
    assert [(row["value"], row["data_consistent"]) for row in rows] == [
        (Decimal("50"), True),
        (Decimal("51"), False),
    ]
    assert await conn.fetchval("SELECT value FROM occurrences") == Decimal("51")


@pytest.mark.asyncio
async def test_real_redelivery_after_calibration_change_keeps_the_stored_reading(
    storage,
):
    conn, pool, _ = storage
    await conn.execute("UPDATE sensors SET gain=2 WHERE id=1")
    payload = '{"unix_time":1760000000,"temp":10}'
    assert await persist(pool, entry(payload, "1-0")) is ProcessingResult.COMPLETED
    # Mesma medição chegando de novo com a mesma calibração: duplicata.
    assert await persist(pool, entry(payload, "2-0")) is ProcessingResult.COMPLETED
    assert await counts(conn) == (1, 0, 2, 0, 2)

    await conn.execute("UPDATE sensors SET gain=3 WHERE id=1")
    assert await persist(pool, entry(payload, "3-0")) is ProcessingResult.COMPLETED

    assert (await reading(conn, 1))["value"] == Decimal("20")
    assert await counts(conn) == (1, 0, 3, 1, 3)
    assert await conn.fetchval("SELECT reasons FROM telemetry_quarantine") == [
        "conflicting_measurement"
    ]


@pytest.mark.asyncio
async def test_real_new_reading_after_calibration_change_uses_the_new_calibration(
    storage,
):
    conn, pool, _ = storage
    await conn.execute("UPDATE sensors SET gain=2 WHERE id=1")
    first = entry('{"unix_time":1760000000,"temp":10}', "1-0")
    assert await persist(pool, first) is ProcessingResult.COMPLETED
    await conn.execute("UPDATE sensors SET gain=NULL, offset_value=5 WHERE id=1")
    second = entry('{"unix_time":1760000060,"temp":10}', "2-0")
    assert await persist(pool, second) is ProcessingResult.COMPLETED

    rows = await conn.fetch(
        "SELECT value FROM readings WHERE sensor_id=1 ORDER BY unix_time"
    )
    assert [row["value"] for row in rows] == [Decimal("20"), Decimal("15")]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ddl",
    [
        "ALTER TABLE sensors DROP COLUMN gain",
        "ALTER TABLE sensor_types DROP COLUMN offset_value",
        "ALTER TABLE sensor_types DROP CONSTRAINT sensor_types_calibration_valid;"
        "ALTER TABLE sensor_types ALTER factor TYPE decimal(10,2)",
    ],
)
async def test_real_schema_guard_requires_the_calibration_migration(storage, ddl):
    conn, _, _ = storage
    await validate_storage_schema(conn)
    await conn.execute(ddl)
    with pytest.raises(CatalogConfigurationError, match="021"):
        await validate_storage_schema(conn)
