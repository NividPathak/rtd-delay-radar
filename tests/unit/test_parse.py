from pathlib import Path

import pytest
from google.transit import gtfs_realtime_pb2 as gtfs

from src.bronze.parse import parse_snapshot
from src.common.schemas import RECORD_SCHEMAS

FIXTURES = Path(__file__).parent.parent / "fixtures"


def load(name: str) -> bytes:
    return (FIXTURES / f"{name}.pb").read_bytes()


@pytest.mark.parametrize("feed", ["trip_updates", "vehicle_positions", "alerts"])
def test_record_keys_match_schema(feed: str) -> None:
    records = parse_snapshot(feed, load(feed))
    assert records
    expected = set(RECORD_SCHEMAS[feed].fieldNames())
    assert all(set(r) == expected for r in records)


def test_trip_updates_fixture_values() -> None:
    records = parse_snapshot("trip_updates", load("trip_updates"))
    assert len(records) == 19
    first = records[0]
    assert first["trip_id"] == "116034942"
    assert first["route_id"] == "104L"
    assert first["start_date"] == "20261005"
    assert first["feed_timestamp"] > 1_700_000_000
    stop = first["stop_time_updates"][0]
    assert stop["stop_sequence"] == 2
    assert stop["arrival_time"] == 1791280560
    assert stop["arrival_delay"] is None  # RTD does not send delay fields


def test_skipped_stops_keep_their_status() -> None:
    records = parse_snapshot("trip_updates", load("trip_updates"))
    statuses = {s["schedule_relationship"] for r in records for s in r["stop_time_updates"]}
    assert "SKIPPED" in statuses


def test_vehicle_without_trip_has_null_trip_id() -> None:
    records = parse_snapshot("vehicle_positions", load("vehicle_positions"))
    assert len(records) == 95
    assert sum(r["trip_id"] is not None for r in records) == 7
    assert all(r["latitude"] is not None for r in records)


def test_alerts_flatten_text_and_entities() -> None:
    records = parse_snapshot("alerts", load("alerts"))
    first = records[0]
    assert first["cause"] == "CONSTRUCTION"
    assert first["effect"] == "NO_SERVICE"
    assert first["header_text"]
    assert first["informed_entities"][0]["route_id"] == "7"
    assert first["active_periods"][0]["end"] is None


def test_unset_fields_are_none_not_zero() -> None:
    feed = gtfs.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.timestamp = 100
    entity = feed.entity.add(id="e1")
    entity.vehicle.vehicle.id = "v1"
    record = parse_snapshot("vehicle_positions", feed.SerializeToString())[0]
    assert record["direction_id"] is None
    assert record["latitude"] is None
    assert record["vehicle_id"] == "v1"


def test_entities_of_other_types_are_ignored() -> None:
    assert parse_snapshot("alerts", load("trip_updates")) == []
