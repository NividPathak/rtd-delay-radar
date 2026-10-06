from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from src.bronze.ingest import parse_binary_files
from src.pipelines.silver_transforms import (
    alert_route_versions,
    clean_vehicle_positions,
    dedupe_stop_updates,
    dedupe_vehicle_positions,
    explode_stop_time_updates,
    gtfs_seconds,
    latest_version,
    only_version,
    scheduled_stops,
    select_static,
    service_day_start_epoch,
    with_scheduled_delay,
)

FIXTURES = Path(__file__).parent.parent / "fixtures"
STATIC = FIXTURES / "static"


def bronze(spark, feed: str):
    content = (FIXTURES / f"{feed}.pb").read_bytes()
    files = spark.createDataFrame([(f"/x/{feed}.pb", content)], "path string, content binary")
    return parse_binary_files(files, feed)


def static(spark, file: str):
    raw = spark.read.option("header", True).csv(str(STATIC / "version=*" / f"{file}.txt"))
    return select_static(raw, file)


@pytest.fixture(scope="module")
def schedule(spark):
    return scheduled_stops(
        static(spark, "stop_times"), static(spark, "trips"), static(spark, "routes")
    )


def test_gtfs_seconds_handles_hours_past_midnight(spark) -> None:
    df = spark.createDataFrame([("08:01:02",), ("25:30:00",)], "t string")
    assert [r.s for r in df.select(gtfs_seconds(F.col("t")).alias("s")).collect()] == [
        28862,
        91800,
    ]


def test_service_day_start_on_normal_and_dst_days(spark) -> None:
    df = spark.createDataFrame([("2026-10-05",), ("2026-11-01",)], "d string").select(
        service_day_start_epoch(F.to_date("d")).alias("epoch")
    )
    normal, dst_end = [r.epoch for r in df.collect()]
    # Normal day: midnight MDT = 06:00 UTC.
    assert normal == int(datetime.fromisoformat("2026-10-05T06:00:00+00:00").timestamp())
    # DST ends 2026-11-01: noon MST (19:00 UTC) minus 12h = 07:00 UTC, not midnight MDT.
    assert dst_end == int(datetime.fromisoformat("2026-11-01T07:00:00+00:00").timestamp())


def test_select_static_adds_version_from_path(spark) -> None:
    routes = static(spark, "routes")
    assert routes.select("gtfs_version").distinct().first().gtfs_version == "test"
    assert set(routes.columns) == {
        "route_id",
        "route_short_name",
        "route_long_name",
        "route_type",
        "gtfs_version",
    }


def test_latest_version_picks_newest_start_date(spark) -> None:
    info = spark.createDataFrame(
        [("old", "20260501"), ("new", "20260927")], "gtfs_version string, feed_start_date string"
    )
    rows = spark.createDataFrame([("old", 1), ("new", 2)], "gtfs_version string, x int")
    assert [r.x for r in only_version(rows, latest_version(info)).collect()] == [2]


def test_scheduled_stops_marks_rail_and_bus(schedule) -> None:
    modes = {r.route_id: r.mode for r in schedule.select("route_id", "mode").distinct().collect()}
    assert modes["A"] == "rail"
    assert modes["15"] == "bus"


def test_stop_updates_join_schedule_and_compute_delay(spark, schedule) -> None:
    updates = dedupe_stop_updates(explode_stop_time_updates(bronze(spark, "trip_updates")))
    silver = with_scheduled_delay(updates, schedule)
    assert silver.count() == 697
    with_arrival = silver.filter("predicted_arrival_ts is not null")
    assert with_arrival.filter("scheduled_arrival_ts is null").count() == 0
    worst = with_arrival.agg(F.max(F.abs("arrival_delay_s")).alias("m")).first().m
    assert worst < 2 * 60 * 60  # every fixture delay is inside the +-2h expectation


def test_wrong_rtd_start_date_is_shifted_by_one_day(spark, schedule) -> None:
    # In the fixture, RTD reports start_date 20261005 for trips that run on Oct 6.
    updates = explode_stop_time_updates(bronze(spark, "trip_updates"))
    row = (
        with_scheduled_delay(updates, schedule)
        .filter("trip_id = '116037952' and stop_sequence = 40")
        .first()
    )
    assert row.start_date == "20261005"
    assert row.service_date_shift_days == 1
    assert str(row.service_date) == "2026-10-06"
    assert row.arrival_delay_s == 0  # predicted 03:26 MDT, scheduled 03:26


def test_dedupe_stop_updates_removes_repeats(spark) -> None:
    updates = explode_stop_time_updates(bronze(spark, "trip_updates"))
    doubled = updates.unionByName(updates)
    assert dedupe_stop_updates(doubled).count() == updates.count()


def test_clean_vehicle_positions_flags_service(spark) -> None:
    vp = clean_vehicle_positions(bronze(spark, "vehicle_positions"))
    assert vp.filter("in_service").count() == 7
    assert vp.filter("vehicle_ts is null").count() == 0
    assert dedupe_vehicle_positions(vp.unionByName(vp)).count() == vp.count()


def test_alert_route_versions_one_row_per_route_stop(spark) -> None:
    alerts = alert_route_versions(bronze(spark, "alerts"))
    first = alerts.filter("entity_id = '53131'").first()
    assert first.route_id == "7"
    assert first.cause == "CONSTRUCTION"
    assert first.first_seen_ts == first.last_seen_ts
    assert first.active_start_ts is not None
