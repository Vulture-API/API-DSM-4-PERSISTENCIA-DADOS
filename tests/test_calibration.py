from decimal import Decimal, localcontext

import pytest
from test_validation import db, entry

from station_persist.calibration import (
    Calibration,
    InvalidCalibrationError,
    apply_calibration,
    effective_calibration,
)
from station_persist.validation import (
    CatalogConfigurationError,
    validate_entries,
)
from station_persist.validation import (
    ValidationStatus as Status,
)


def test_absent_calibration_keeps_the_raw_reading():
    calibration = effective_calibration()
    assert calibration.identity
    assert apply_calibration(Decimal("23.456"), calibration) == Decimal("23.456")


def test_value_is_raw_times_factor_times_gain_plus_offset():
    calibration = effective_calibration(
        type_factor=Decimal("3.6"), type_gain=Decimal("0.5"), type_offset=Decimal("-2")
    )
    assert apply_calibration(Decimal("10"), calibration) == Decimal("16")


@pytest.mark.parametrize(
    "sensor,expected",
    [
        (dict(), Calibration(Decimal("2"), Decimal("3"), Decimal("4"))),
        (
            dict(sensor_gain=Decimal("5")),
            Calibration(Decimal("2"), Decimal("5"), Decimal("4")),
        ),
        (
            dict(sensor_offset=Decimal("-1")),
            Calibration(Decimal("2"), Decimal("3"), Decimal("-1")),
        ),
        (
            dict(sensor_gain=Decimal("5"), sensor_offset=Decimal("0")),
            Calibration(Decimal("2"), Decimal("5"), Decimal("0")),
        ),
    ],
)
def test_sensor_gain_and_offset_override_the_type_and_factor_stays(sensor, expected):
    calibration = effective_calibration(
        type_factor=Decimal("2"),
        type_gain=Decimal("3"),
        type_offset=Decimal("4"),
        **sensor,
    )
    assert calibration == expected


def test_sensor_calibration_applies_without_any_type_calibration():
    calibration = effective_calibration(
        sensor_gain=Decimal("0.5"), sensor_offset=Decimal("-0.125")
    )
    assert apply_calibration(Decimal("1000"), calibration) == Decimal("499.875")


def test_negative_offset_and_fractional_gain():
    calibration = effective_calibration(
        type_gain=Decimal("0.000805860805"), type_offset=Decimal("-12.5")
    )
    assert apply_calibration(Decimal("4095"), calibration) == Decimal("-9.200000003525")


def test_conversion_is_exact_beyond_the_default_decimal_precision():
    raw = Decimal("1.2345678901234567890123456789012345678901234567890")
    gain = Decimal("3.0000000000000000000000000000000000000000000000001")
    with localcontext() as context:
        context.prec = 5
        value = apply_calibration(raw, effective_calibration(type_gain=gain))
    assert value == Decimal(
        "3.70370367037037036703703703670370370367037037036712345678901234567890123456789012345678901234567890"
    )


def test_integer_calibration_from_the_catalog_is_accepted():
    calibration = effective_calibration(type_factor=2, sensor_offset=1)
    assert apply_calibration(Decimal("1.5"), calibration) == Decimal("4.0")


@pytest.mark.parametrize(
    "calibration",
    [
        dict(type_factor=Decimal("0")),
        dict(type_gain=Decimal("0")),
        dict(sensor_gain=Decimal("0.00")),
        dict(type_factor=Decimal("NaN")),
        dict(sensor_gain=Decimal("Infinity")),
        dict(type_offset=Decimal("-Infinity")),
        dict(sensor_offset=Decimal("NaN")),
        dict(type_gain=1.5),
        dict(sensor_offset="1"),
    ],
)
def test_calibration_that_cannot_be_applied_is_refused(calibration):
    with pytest.raises(InvalidCalibrationError):
        effective_calibration(**calibration)


@pytest.mark.asyncio
async def test_reading_without_calibration_is_stored_as_received():
    (event,) = await validate_entries([entry('{"temp":23.456}')], db())
    (field,) = event.fields
    assert (field.status, field.value, field.raw_value) == (
        Status.VALID,
        Decimal("23.456"),
        Decimal("23.456"),
    )


@pytest.mark.asyncio
async def test_reading_is_converted_and_keeps_the_raw_value():
    conn = db(type_factor=Decimal("3.6"), gain=Decimal("0.5"), type_offset=Decimal("1"))
    (event,) = await validate_entries([entry('{"temp":10}')], conn)
    (field,) = event.fields
    assert (field.status, field.value, field.raw_value) == (
        Status.VALID,
        Decimal("19"),
        Decimal("10"),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "raw,status,reasons",
    [
        ("100", Status.VALID, ()),
        ("0", Status.VALID, ()),
        ("101", Status.OUT_OF_RANGE, ("above_max",)),
        ("-1", Status.OUT_OF_RANGE, ("below_min",)),
    ],
)
async def test_limits_are_checked_on_the_converted_value(raw, status, reasons):
    conn = db(
        minimum=Decimal("0"),
        maximum=Decimal("50"),
        gain=Decimal("0.5"),
    )
    (event,) = await validate_entries([entry(f'{{"temp":{raw}}}')], conn)
    (field,) = event.fields
    assert (field.status, field.reasons) == (status, reasons)


@pytest.mark.asyncio
async def test_rejected_reading_stays_rejected_with_calibration():
    conn = db(gain=Decimal("2"))
    (event,) = await validate_entries([entry('{"temp":"23"}')], conn)
    (field,) = event.fields
    assert (field.status, field.value, field.raw_value) == (
        Status.REJECTED,
        None,
        None,
    )
    assert field.reasons == ("invalid_numeric_type",)


@pytest.mark.asyncio
async def test_converted_value_that_does_not_fit_is_rejected():
    conn = db(gain=Decimal("1e131072"))
    (event,) = await validate_entries([entry('{"temp":10}')], conn)
    (field,) = event.fields
    assert (field.status, field.reasons) == (
        Status.REJECTED,
        ("numeric_not_representable",),
    )
    assert field.value is None and field.raw_value == Decimal("10")


@pytest.mark.asyncio
async def test_each_sensor_of_the_event_uses_its_own_calibration():
    conn = db(keys=("temp", "lux"))
    rows = list(conn.fetch.side_effect)
    rows[3][1]["gain"] = Decimal("0.25")
    conn.fetch.side_effect = rows
    (event,) = await validate_entries([entry('{"temp":20,"lux":400}')], conn)
    assert [field.value for field in event.fields] == [Decimal("20"), Decimal("100")]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "calibration",
    [dict(gain=Decimal("0")), dict(type_factor=Decimal("NaN")), dict(type_gain=0.5)],
)
async def test_invalid_calibration_in_the_catalog_is_a_configuration_error(calibration):
    with pytest.raises(CatalogConfigurationError, match="calibration"):
        await validate_entries([entry()], db(**calibration))


@pytest.mark.asyncio
async def test_schema_without_the_calibration_columns_asks_for_the_migration():
    conn = db()
    rows = list(conn.fetch.side_effect)
    rows[2] = rows[2][:3]
    conn.fetch.side_effect = rows
    with pytest.raises(CatalogConfigurationError, match="migration 021"):
        await validate_entries([entry()], conn)
