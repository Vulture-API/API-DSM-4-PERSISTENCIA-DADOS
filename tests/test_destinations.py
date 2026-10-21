from decimal import Decimal

import pytest
from test_validation import db, entry

from station_persist.destinations import Destination, build_processing_plan
from station_persist.stream import ProcessingResult
from station_persist.validation import validate_entries


@pytest.mark.asyncio
async def test_mixed_plan_preserves_original_and_exact_values():
    source = entry('{"temp":1,"humidity":20.1234567890123456789,"meta":true}')
    (validation,) = await validate_entries(
        [source], db(("temp", "humidity", "meta"), maximum=Decimal(10))
    )
    plan = build_processing_plan(validation)
    assert plan.validation is validation
    assert plan.validation.resolution.source is source
    assert [f.destination for f in plan.fields] == [
        Destination.READING,
        Destination.INCONSISTENT,
        Destination.QUARANTINE,
    ]
    assert [f.ordinal for f in plan.fields] == [1, 2, 3]
    assert plan.fields[1].measurement.value == Decimal("20.1234567890123456789")
    assert plan.fields[2].measurement.resolution.value is True
    assert plan is not ProcessingResult.COMPLETED


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", ["{", "[]", '{"unix_time":null,"temp":1}'])
async def test_whole_event_rejection_has_durable_destination(payload):
    (validation,) = await validate_entries([entry(payload)], db())
    plan = build_processing_plan(validation)
    assert len(plan.fields) == 1
    assert plan.fields[0].ordinal == 0
    assert plan.fields[0].destination == Destination.QUARANTINE
    assert plan.fields[0].reasons == validation.reasons
    assert plan.fields[0].measurement is None


@pytest.mark.asyncio
async def test_empty_telemetry_preserves_event_for_communication():
    (validation,) = await validate_entries([entry("{}")], db(()))
    plan = build_processing_plan(validation)
    assert plan.fields == ()
    assert plan.validation.timestamp.received_at_ms == 1234567890123
