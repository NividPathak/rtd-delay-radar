"""Lakeflow Declarative Pipeline: silver tables.

Wires the tested functions in `src/pipelines/silver_transforms.py` together and adds
data quality expectations. Pipeline settings (see resources/rtd.pipeline.yml):
    rtd.bronze_schema   e.g. rtd.bronze or rtd.dev_bronze
    rtd.static_path     volume folder with version=<feed_version>/<file>.txt
    rtd.code_root       deployed repo root, added to sys.path so `src` can be imported
"""

import sys

from pyspark import pipelines as dp
from pyspark.sql import DataFrame

sys.path.insert(0, spark.conf.get("rtd.code_root"))  # noqa: F821

from src.common import config  # noqa: E402
from src.pipelines import silver_transforms as T  # noqa: E402

BRONZE = spark.conf.get("rtd.bronze_schema")  # noqa: F821
STATIC_PATH = spark.conf.get("rtd.static_path")  # noqa: F821
BBOX = config.DENVER_BBOX
MAX_DELAY = config.MAX_ABS_DELAY_SECONDS


def read_static(file: str) -> DataFrame:
    raw = spark.read.option("header", True).csv(f"{STATIC_PATH}/version=*/{file}.txt")  # noqa: F821
    return T.select_static(raw, file)


# ---- Static GTFS reference tables (latest schedule version) ----


@dp.materialized_view(comment="Latest static GTFS version, chosen by feed_start_date.")
def gtfs_current_version() -> DataFrame:
    return T.latest_version(read_static("feed_info"))


@dp.materialized_view(comment="Routes from the latest static GTFS, with bus or rail mode.")
def gtfs_routes() -> DataFrame:
    routes = T.only_version(read_static("routes"), spark.read.table("gtfs_current_version"))  # noqa: F821
    return routes.withColumn("mode", T.route_mode(routes.route_type.cast("int")))


@dp.materialized_view(comment="Stops from the latest static GTFS.")
@dp.expect("stop_in_denver_area", f"stop_lat BETWEEN {BBOX['min_lat']} AND {BBOX['max_lat']}")
def gtfs_stops() -> DataFrame:
    stops = T.only_version(read_static("stops"), spark.read.table("gtfs_current_version"))  # noqa: F821
    return stops.selectExpr(
        "stop_id", "stop_name", "cast(stop_lat as double) as stop_lat",
        "cast(stop_lon as double) as stop_lon", "parent_station", "gtfs_version",
    )  # fmt: skip


@dp.materialized_view(comment="One row per scheduled trip stop, with route, direction, mode.")
def gtfs_scheduled_stops() -> DataFrame:
    version = spark.read.table("gtfs_current_version")  # noqa: F821
    return T.scheduled_stops(
        T.only_version(read_static("stop_times"), version).drop("gtfs_version"),
        T.only_version(read_static("trips"), version).drop("gtfs_version"),
        T.only_version(read_static("routes"), version).drop("gtfs_version"),
    )


# ---- Realtime silver tables ----


@dp.table(comment="One row per trip, stop, and feed snapshot, with schedule and delay.")
@dp.expect_or_drop("trip_id_not_null", "trip_id IS NOT NULL")
@dp.expect_or_drop(
    "delay_within_2h", f"arrival_delay_s IS NULL OR abs(arrival_delay_s) <= {MAX_DELAY}"
)
@dp.expect("matched_schedule", "scheduled_arrival_ts IS NOT NULL")
@dp.expect("start_date_not_shifted", "coalesce(service_date_shift_days, 0) = 0")
def stop_time_updates() -> DataFrame:
    updates = T.explode_stop_time_updates(
        spark.readStream.table(f"{BRONZE}.trip_updates")  # noqa: F821
    ).withWatermark("feed_ts", "2 hours")
    schedule = spark.read.table("gtfs_scheduled_stops")  # noqa: F821
    return T.with_scheduled_delay(T.dedupe_stop_updates(updates), schedule)


@dp.table(comment="Vehicle positions, deduplicated, with an in_service flag.")
@dp.expect_or_drop("vehicle_id_not_null", "vehicle_id IS NOT NULL")
@dp.expect_or_drop(
    "position_in_denver_area",
    f"latitude BETWEEN {BBOX['min_lat']} AND {BBOX['max_lat']} "
    f"AND longitude BETWEEN {BBOX['min_lon']} AND {BBOX['max_lon']}",
)
def vehicle_positions() -> DataFrame:
    bronze = spark.readStream.table(f"{BRONZE}.vehicle_positions")  # noqa: F821
    cleaned = T.clean_vehicle_positions(bronze).withWatermark("vehicle_ts", "2 hours")
    return T.dedupe_vehicle_positions(cleaned)


@dp.materialized_view(comment="One row per alert and affected route/stop, first and last seen.")
def alerts() -> DataFrame:
    return T.alert_route_versions(spark.read.table(f"{BRONZE}.alerts"))  # noqa: F821
