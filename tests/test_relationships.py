import json
from decimal import Decimal
from unittest.mock import AsyncMock

import asyncpg
import pytest

from station_persist.relationships import (
    RejectionReason as Reason,
)
from station_persist.relationships import resolve_entries, station_identity
from station_persist.stream import StreamEntry


def entry(identity="1", payload=None, *, raw=None, entry_id="1-0"):
    return StreamEntry.from_redis(
        entry_id,
        {
            "estacao_id": identity,
            "unix_time": "null",
            "received_at": "1234567890000",
            "topic": "estacoes/WRONG/dados",
            "payload": raw if raw is not None else json.dumps(payload or {}),
            "legacy": "preserved",
        },
    )


def station(id=1, mac="AA:BB:CC:DD:EE:01", property_id=10):
    return dict(id=id, mac_address=mac, property_id=property_id)


def sensor(key="temp", id=20, station_id=1, requested_station_id=1, type_id=30):
    return dict(
        requested_station_id=requested_station_id,
        requested_identifier=key,
        sensor_id=id,
        station_id=station_id,
        sensor_type_id=type_id,
    )


@pytest.mark.parametrize(
    ("raw", "kind", "value"),
    [
        ("001", "id", 1),
        ("0", "id", 0),
        ("2147483647", "id", 2147483647),
        (" aa:bb:CC:dd:ee:01 ", "mac", "AABBCCDDEE01"),
        ("aa-bb-cc-dd-ee-01", "mac", "AABBCCDDEE01"),
        ("aabbccddee01", "mac", "AABBCCDDEE01"),
    ],
)
def test_identity_supported(raw, kind, value):
    identity, reason = station_identity(raw)
    assert reason is None
    assert (identity.kind, identity.value) == (kind, value)


@pytest.mark.parametrize(
    "raw", [None, 1, "", " ", "a:b:c:d:e:f", "AA:BB-CC:DD:EE:01", "-1", "1.0", "１２３"]
)
def test_invalid_identification(raw):
    assert station_identity(raw) == (None, Reason.INVALID_IDENTIFICATION)


@pytest.mark.parametrize(
    "raw", ["2147483648", "0002147483648", "123456789012", "9" * 10000]
)
def test_id_overflow_never_falls_back_to_mac(raw):
    assert station_identity(raw) == (None, Reason.IDENTIFICATION_OUT_OF_RANGE)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "raw", ["[]", "null", '"text"', "{", '{"temp":NaN}', '{"temp":1,"temp":2}']
)
async def test_invalid_payload_is_structured(raw):
    conn = AsyncMock()
    (result,) = await resolve_entries([entry(raw=raw)], conn)
    assert result.reasons == (Reason.INVALID_PAYLOAD,)
    assert result.source.fields["payload"] == raw
    conn.fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_payload_or_identity_and_empty_batch():
    conn = AsyncMock()
    assert await resolve_entries([], conn) == ()
    (result,) = await resolve_entries([StreamEntry.from_redis("1-0", {})], conn)
    assert result.reasons == (Reason.INVALID_PAYLOAD,)
    (result,) = await resolve_entries(
        [StreamEntry.from_redis("2-0", {"payload": '{"temp":12}'})], conn
    )
    assert result.reasons == (Reason.INVALID_IDENTIFICATION,)
    assert result.fields[0].value == 12
    conn.fetch.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload_identity", "reason"),
    [
        ("2", Reason.IDENTIFICATION_MISMATCH),
        (None, Reason.INVALID_IDENTIFICATION),
        ("2147483648", Reason.IDENTIFICATION_OUT_OF_RANGE),
    ],
)
async def test_payload_identification_rejects_without_choosing_station(
    payload_identity, reason
):
    conn = AsyncMock()
    (result,) = await resolve_entries(
        [entry(payload={"estacao_id": payload_identity, "temp": 3})], conn
    )
    assert result.reasons == (reason,)
    assert result.fields[0].reasons == (reason,)
    assert result.station_id is None
    conn.fetch.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "identity,payload_identity", [("1", "001"), ("aabbccddee01", "AA-BB-CC-DD-EE-01")]
)
async def test_equivalent_identifications(identity, payload_identity):
    conn = AsyncMock()
    conn.fetch.side_effect = [[station()]]
    (result,) = await resolve_entries(
        [entry(identity, {"estacao_id": payload_identity, "unix_time": {"bad": True}})],
        conn,
    )
    assert result.station_id == 1 and result.property_id == 10
    assert result.fields == () and not result.reasons
    assert conn.fetch.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rows,reason",
    [
        ([], Reason.UNKNOWN_STATION),
        ([station(), station(2, "aa-bb-cc-dd-ee-01")], Reason.AMBIGUOUS_STATION),
        ([station(property_id=None)], Reason.INCOMPATIBLE_REFERENCE),
    ],
)
async def test_station_rejections(rows, reason):
    conn = AsyncMock()
    conn.fetch.return_value = rows
    (result,) = await resolve_entries([entry("aabbccddee01", {"temp": 1})], conn)
    assert result.reasons == (reason,)
    assert result.fields[0].reasons == (reason,)
    assert conn.fetch.await_count == 1


@pytest.mark.asyncio
async def test_batch_preserves_provenance_all_values_and_partial_results():
    original = entry(
        raw='{"estacao_id":"001","unix_time":"invalid","temp":1.234567890123456789,"meta":{"firmware":[null,true,"PRIVATE"]},"unknown":"bad","Temp":false}'
    )
    conn = AsyncMock()
    conn.fetch.side_effect = [
        [station()],
        [sensor(), sensor("Temp"), sensor("meta", id=21)],
    ]
    (result,) = await resolve_entries([original], conn)
    assert result.source is original and result.source.fields["legacy"] == "preserved"
    assert result.station_id == 1 and result.property_id == 10 and not result.reasons
    fields = {field.local_identifier: field for field in result.fields}
    assert fields["temp"].value == Decimal("1.234567890123456789")
    assert fields["Temp"].value is False
    assert (
        fields["temp"].reasons == fields["Temp"].reasons == (Reason.DUPLICATE_SENSOR,)
    )
    assert not fields["temp"].associated
    assert fields["meta"].associated
    assert fields["meta"].value == {"firmware": [None, True, "PRIVATE"]}
    assert fields["meta"].sensor_type_id == 30
    assert fields["unknown"].reasons == (Reason.UNKNOWN_SENSOR,)
    assert all(
        field.station_id == 1 and field.property_id == 10 for field in result.fields
    )
    query, ids, keys = conn.fetch.await_args.args
    assert "s.station_id = r.station_id" in query
    assert "lower(s.local_identifier) = lower(r.local_identifier)" in query
    assert ids == [1] * 4
    assert keys == ["Temp", "meta", "temp", "unknown"]
    assert all(call[0] == "fetch" for call in conn.mock_calls)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rows,reason",
    [
        ([sensor(), sensor(id=21)], Reason.AMBIGUOUS_SENSOR),
        ([sensor(station_id=2)], Reason.INCOMPATIBLE_REFERENCE),
        ([sensor(type_id=None)], Reason.INCOMPATIBLE_REFERENCE),
    ],
)
async def test_ambiguous_or_incompatible_sensor(rows, reason):
    conn = AsyncMock()
    conn.fetch.side_effect = [[station()], rows]
    (result,) = await resolve_entries([entry(payload={"temp": "not numeric"})], conn)
    assert result.fields[0].reasons == (reason,)
    assert result.fields[0].value == "not numeric"
    assert result.fields[0].sensor_id is None


@pytest.mark.asyncio
async def test_db_failure_remains_technical_and_no_persistent_cache():
    conn = AsyncMock()
    error = asyncpg.ConnectionDoesNotExistError("disconnected")
    conn.fetch.side_effect = error
    with pytest.raises(asyncpg.ConnectionDoesNotExistError) as exc:
        await resolve_entries([entry()], conn)
    assert exc.value is error
    conn.fetch.side_effect = [[station()], []]
    (first,) = await resolve_entries([entry()], conn)
    (second,) = await resolve_entries([entry()], conn)
    assert first.station_id == 1
    assert second.reasons == (Reason.UNKNOWN_STATION,)


@pytest.mark.asyncio
async def test_non_postgres_key_and_unbounded_json_number_remain_data_rejections():
    conn = AsyncMock()
    conn.fetch.side_effect = [[station()], [sensor()]]
    source = entry(
        raw='{"bad\\u0000key":true,"surrogate\\ud800":null,"temp":' + "9" * 5000 + "}"
    )
    (result,) = await resolve_entries([source], conn)
    assert (
        result.fields[0].reasons
        == result.fields[1].reasons
        == (Reason.INVALID_SENSOR_IDENTIFIER,)
    )
    assert result.fields[2].associated
    assert result.fields[2].value == Decimal("9" * 5000)
    assert result.source is source
    assert conn.fetch.await_args.args[2] == ["temp"]
