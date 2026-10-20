from decimal import Decimal, localcontext
from unittest.mock import AsyncMock

import asyncpg
import pytest

from station_persist.config import Settings
from station_persist.stream import StreamEntry
from station_persist.validation import (
    CatalogChangedError,
    CatalogConfigurationError,
    TimestampSource,
    exact_numeric,
    stream_id_parts,
    validate_entries,
)
from station_persist.validation import (
    ValidationStatus as Status,
)


def entry(
    payload='{"temp":23.456}', received="1234567890123", entry_id="10-2", **extra
):
    fields = dict(
        estacao_id="1",
        unix_time="null",
        received_at=received,
        topic="estacoes/ignored/dados",
        payload=payload,
    )
    fields.update(extra)
    return StreamEntry.from_redis(entry_id, fields)


def db(keys=("temp",), minimum=None, maximum=None):
    conn = AsyncMock()
    conn.fetch.side_effect = [
        [dict(id=1, property_id=2, mac_address="AA:BB:CC:DD:EE:01")],
        [
            dict(
                requested_station_id=1,
                requested_identifier=k,
                sensor_id=i + 10,
                station_id=1,
                sensor_type_id=3,
            )
            for i, k in enumerate(keys)
        ],
        [
            dict(relname=table, attname=column, exact=True)
            for table, column in (
                ("readings", "value"),
                ("sensors", "min_value"),
                ("sensors", "max_value"),
            )
        ],
        [
            dict(id=i + 10, station_id=1, min_value=minimum, max_value=maximum)
            for i, _ in enumerate(keys)
        ],
    ]
    return conn


@pytest.mark.parametrize(
    "raw,valid",
    [
        ("0-0", True),
        ("9-99", True),
        ("18446744073709551615-18446744073709551615", True),
        ("01-0", False),
        ("0-", False),
        ("1", False),
        ("-1-0", False),
        ("18446744073709551616-0", False),
        ("9" * 30 + "-0", False),
    ],
)
def test_cutoff_validation(raw, valid):
    if valid:
        assert stream_id_parts(raw) == tuple(map(int, raw.split("-")))
        assert (
            Settings(
                _env_file=None, database_url="postgres://test", legacy_max_id=raw
            ).legacy_max_id
            == raw
        )
    else:
        with pytest.raises(ValueError):
            stream_id_parts(raw)
        with pytest.raises(ValueError):
            Settings(_env_file=None, database_url="postgres://test", legacy_max_id=raw)
    assert (
        Settings(
            _env_file=None, database_url="postgres://test", legacy_max_id=""
        ).legacy_max_id
        is None
    )


@pytest.mark.parametrize("value", [True, False, None, "23.45", [], {}, 1.1])
def test_no_numeric_coercion(value):
    assert exact_numeric(value) == (None, "invalid_numeric_type")


@pytest.mark.parametrize("value", ["NaN", "sNaN", "Infinity", "-Infinity"])
def test_non_finite(value):
    assert exact_numeric(Decimal(value)) == (None, "non_finite_value")


@pytest.mark.parametrize(
    "raw,reason",
    [
        ("1e131071", None),
        ("1e131072", "numeric_not_representable"),
        ("1e-16383", None),
        ("1e-16384", "numeric_not_representable"),
        ("0e-1000000", None),
        ("-12345678901234567890.1234567890123456789", None),
    ],
)
def test_exact_numeric_storage_limits_without_context_rounding(raw, reason):
    original = Decimal(raw)
    with localcontext() as ctx:
        ctx.prec = 3
        value, error = exact_numeric(original)
    assert error == reason
    if error is None:
        assert value == original


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "value,status,reason",
    [
        ("0", Status.VALID, None),
        ("50", Status.VALID, None),
        ("-0.001", Status.OUT_OF_RANGE, "below_min"),
        ("50.0000000000000001", Status.OUT_OF_RANGE, "above_max"),
        ("23.456789012345678901", Status.VALID, None),
    ],
)
async def test_inclusive_exact_limits(value, status, reason):
    original = entry('{"temp":' + value + "}")
    (result,) = await validate_entries(
        [original], db(minimum=Decimal(0), maximum=Decimal(50))
    )
    field = result.fields[0]
    assert field.status == status and field.reasons == (
        () if reason is None else (reason,)
    )
    assert field.value == Decimal(value)
    assert result.resolution.source is original
    assert result.timestamp.source == TimestampSource.RECEIVED_AT
    assert result.timestamp.unix_time == 1234567890


@pytest.mark.asyncio
async def test_partial_results_and_original_values():
    original = entry(
        '{"unix_time":1760000000,"temp":23.456,"humidity":true,"extra":"PRIVATE"}'
    )
    (result,) = await validate_entries([original], db(("temp", "humidity")))
    assert [f.status for f in result.fields] == [
        Status.VALID,
        Status.REJECTED,
        Status.REJECTED,
    ]
    assert result.fields[1].resolution.value is True
    assert result.fields[2].resolution.value == "PRIVATE"
    assert result.fields[2].reasons == ("unknown_sensor",)
    assert result.timestamp.source == TimestampSource.PAYLOAD
    assert result.timestamp.unix_time == 1760000000
    assert result.resolution.source.fields == original.fields


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "raw,received,reason",
    [
        ("null", "1234", "invalid_timestamp_type"),
        ("true", "1234", "invalid_timestamp_type"),
        ('"1760000000"', "1234", "invalid_timestamp_type"),
        ("1.0", "1234", "invalid_timestamp_type"),
        ("1e0", "1234", "invalid_timestamp_type"),
        ("-1", "1234", "timestamp_not_representable"),
        ("253402300800", "1234", "timestamp_not_representable"),
        ("9" * 5000, "1234", "timestamp_not_representable"),
        ("1", "bad", "invalid_received_at"),
        ("1", "-1", "invalid_received_at"),
        ("1", "253402300800000", "received_at_not_representable"),
    ],
)
async def test_timestamp_rejections(raw, received, reason):
    (result,) = await validate_entries(
        [entry('{"unix_time":' + raw + ',"temp":1}', received)], db()
    )
    assert result.timestamp.reasons == (reason,)
    assert result.fields[0].status == Status.REJECTED
    assert result.fields[0].resolution.value == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("raw", ["0", "253402300799"])
async def test_timestamp_utc_boundaries(raw):
    (result,) = await validate_entries(
        [entry('{"unix_time":' + raw + ',"temp":1}')], db()
    )
    assert result.timestamp.unix_time == int(raw)
    assert result.fields[0].status == Status.VALID


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "entry_id,cutoff,legacy",
    [
        ("9-999", "10-2", True),
        ("10-2", "10-2", True),
        ("10-3", "10-2", False),
        ("10-2", None, False),
    ],
)
async def test_explicit_legacy_cutoff_inclusive(entry_id, cutoff, legacy):
    (result,) = await validate_entries(
        [entry('{"unix_time":null,"temp":1}', entry_id=entry_id)],
        db(),
        legacy_max_id=cutoff,
    )
    if legacy:
        assert result.timestamp.unix_time == 1234567890
        assert result.timestamp.warnings == ("legacy_timestamp_original_lost",)
        assert result.timestamp.source == TimestampSource.LEGACY_RECEIVED_AT
    else:
        assert result.timestamp.reasons == ("invalid_timestamp_type",)
    assert result.fields[0].status == (Status.VALID if legacy else Status.REJECTED)


@pytest.mark.asyncio
async def test_invalid_legacy_source_does_not_guess():
    (result,) = await validate_entries(
        [entry('{"unix_time":null,"temp":1}', entry_id="bad")],
        db(),
        legacy_max_id="10-2",
    )
    assert result.timestamp.reasons == ("invalid_stream_id",)
    (result,) = await validate_entries(
        [entry('{"unix_time":null,"temp":1}', unix_time="0")],
        db(),
        legacy_max_id="10-2",
    )
    assert result.timestamp.source is None
    assert result.timestamp.reasons == ("invalid_timestamp_type",)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "limits",
    [(Decimal("NaN"), None), (Decimal(2), Decimal(1)), (Decimal("1e131072"), None)],
)
async def test_invalid_catalog_is_technical(limits):
    with pytest.raises(CatalogConfigurationError):
        await validate_entries([entry()], db(minimum=limits[0], maximum=limits[1]))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rows",
    [
        [],
        [dict(id=10, station_id=2, min_value=None, max_value=None)],
        [dict(id=10, station_id=1, min_value=None, max_value=None)] * 2,
    ],
)
async def test_catalog_changed_or_duplicated_remains_technical(rows):
    conn = db()
    conn.fetch.side_effect = list(conn.fetch.side_effect)[:3] + [rows]
    with pytest.raises((CatalogChangedError, CatalogConfigurationError)):
        await validate_entries([entry()], conn)


@pytest.mark.asyncio
async def test_schema_failure_unknown_station_and_empty_batch():
    conn = db()
    conn.fetch.side_effect = list(conn.fetch.side_effect)[:3] + [
        asyncpg.UndefinedColumnError("min_value")
    ]
    with pytest.raises(asyncpg.UndefinedColumnError):
        await validate_entries([entry()], conn)
    conn = AsyncMock()
    conn.fetch.return_value = []
    (result,) = await validate_entries([entry()], conn)
    assert result.reasons == ("unknown_station",)
    assert result.fields[0].status == Status.REJECTED
    assert conn.fetch.await_count == 1
    conn = AsyncMock()
    assert await validate_entries([], conn) == ()
    conn.fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_numeric_rejection_in_batch_without_limits():
    (result,) = await validate_entries([entry('{"temp":1e131072}')], db())
    assert result.fields[0].reasons == ("numeric_not_representable",)
    assert result.fields[0].resolution.value == Decimal("1e131072")


@pytest.mark.asyncio
async def test_unrepresentable_exponent_rejects_only_affected_measurement():
    (result,) = await validate_entries(
        [entry('{"temp":1e999999999999999999999999,"humidity":23.456}')],
        db(("temp", "humidity")),
    )
    assert result.fields[0].reasons == ("numeric_not_representable",)
    assert result.fields[0].resolution.value == "1e999999999999999999999999"
    assert result.fields[1].status == Status.VALID
    assert result.fields[1].value == Decimal("23.456")


def test_stream_id_non_string_is_configuration_error():
    with pytest.raises(ValueError, match="string"):
        stream_id_parts(1)
