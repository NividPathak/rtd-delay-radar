"""Pure DataFrame transformations for the silver layer.

Each function takes DataFrames and returns a DataFrame, so it can be tested on a
local Spark session. The Lakeflow pipeline in `transformations/silver.py` only wires
these together and adds data quality expectations.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from src.common import config
from src.common.schemas import STATIC_COLUMNS

RAIL_ROUTE_TYPES = [0, 1, 2]  # GTFS: tram/light rail, subway, commuter rail
SECONDS_IN_12_HOURS = 12 * 60 * 60

# ---- Static GTFS ----


def select_static(raw: DataFrame, file: str) -> DataFrame:
    """Keep the named columns of one static file plus its version from the file path."""
    version = F.regexp_extract(F.col("_metadata.file_path"), r"version=([^/]+)/", 1)
    return raw.select(*STATIC_COLUMNS[file], version.alias("gtfs_version"))


def latest_version(feed_info: DataFrame) -> DataFrame:
    """One row: the gtfs_version with the newest feed_start_date."""
    return (
        feed_info.orderBy(F.desc("feed_start_date"), F.desc("gtfs_version"))
        .select("gtfs_version")
        .limit(1)
    )


def only_version(df: DataFrame, version: DataFrame) -> DataFrame:
    """Keep rows from the given single-row version DataFrame."""
    return df.join(version, "gtfs_version", "inner")


def gtfs_seconds(hms: Column) -> Column:
    """GTFS "HH:MM:SS" to seconds. Hours can go past 24 for trips after midnight."""
    parts = F.split(hms, ":")
    return (
        parts.getItem(0).cast("int") * 3600
        + parts.getItem(1).cast("int") * 60
        + parts.getItem(2).cast("int")
    )


def route_mode(route_type: Column) -> Column:
    """'rail' for light rail and commuter rail, 'bus' for everything else."""
    return F.when(route_type.isin(RAIL_ROUTE_TYPES), "rail").otherwise("bus")


def scheduled_stops(stop_times: DataFrame, trips: DataFrame, routes: DataFrame) -> DataFrame:
    """One row per scheduled (trip_id, stop_sequence) with route and direction attached."""
    stops = stop_times.select(
        "trip_id",
        F.col("stop_sequence").cast("int").alias("stop_sequence"),
        "stop_id",
        gtfs_seconds(F.col("arrival_time")).alias("scheduled_arrival_secs"),
        gtfs_seconds(F.col("departure_time")).alias("scheduled_departure_secs"),
    )
    trip_info = trips.select(
        "trip_id", "route_id", "service_id", F.col("direction_id").cast("int").alias("direction_id")
    )
    route_info = routes.select(
        "route_id",
        "route_short_name",
        F.col("route_type").cast("int").alias("route_type"),
    ).withColumn("mode", route_mode(F.col("route_type")))
    return stops.join(trip_info, "trip_id").join(route_info, "route_id")


# ---- Realtime feeds ----


def explode_stop_time_updates(bronze: DataFrame) -> DataFrame:
    """Bronze trip updates (one row per trip) to one row per trip, stop, and snapshot."""
    return bronze.select(
        "trip_id",
        F.col("route_id").alias("rt_route_id"),
        "start_date",
        "vehicle_id",
        F.timestamp_seconds("feed_timestamp").alias("feed_ts"),
        F.explode("stop_time_updates").alias("stu"),
        "ingest_ts",
    ).select(
        "trip_id",
        "rt_route_id",
        "start_date",
        "vehicle_id",
        "feed_ts",
        F.col("stu.stop_sequence").alias("stop_sequence"),
        F.col("stu.stop_id").alias("stop_id"),
        F.col("stu.arrival_time").alias("predicted_arrival_epoch"),
        F.col("stu.departure_time").alias("predicted_departure_epoch"),
        F.col("stu.schedule_relationship").alias("stop_schedule_relationship"),
        "ingest_ts",
    )


def dedupe_stop_updates(df: DataFrame) -> DataFrame:
    """Drop repeated (trip_id, stop_id, feed_ts) rows. Needs a watermark when streaming."""
    return df.dropDuplicates(["trip_id", "stop_id", "feed_ts"])


def service_day_start_epoch(service_date: Column) -> Column:
    """GTFS service day reference time: noon local time minus 12 hours, as epoch seconds.

    This equals midnight on normal days, and stays correct on daylight saving days
    (the GTFS spec defines times from "noon minus 12h" for this reason).
    """
    noon_local = F.to_timestamp(F.concat(service_date.cast("string"), F.lit(" 12:00:00")))
    noon_utc = F.to_utc_timestamp(noon_local, config.LOCAL_TIMEZONE)
    return F.unix_timestamp(noon_utc) - SECONDS_IN_12_HOURS


def service_date_shift_days(start_date: Column, predicted: Column, sched_secs: Column) -> Column:
    """Whole days RTD's `start_date` is off by, judged from the prediction (-1, 0, or +1).

    RTD's TripUpdate feed reports the previous day's start_date for some early-morning
    trips (the vehicle feed has the right date). Real delays are within hours, while a
    wrong date shifts the delay by about 24 hours, so rounding to whole days finds it.
    Null when there is no prediction or no schedule match.
    """
    raw_delay = predicted - (service_day_start_epoch(start_date) + sched_secs)
    days = F.round(raw_delay / 86400).cast("int")
    return F.when(F.abs(days) <= 1, days).otherwise(F.lit(0))


def with_scheduled_delay(updates: DataFrame, schedule: DataFrame) -> DataFrame:
    """Join each stop update to its schedule and compute arrival delay in seconds.

    Left join: updates with no matching schedule row keep null schedule fields, so the
    pipeline can measure the match rate with an expectation instead of hiding them.
    """
    joined = updates.join(schedule, ["trip_id", "stop_sequence"], "left")
    rt_start_date = F.to_date("start_date", "yyyyMMdd")
    shift = service_date_shift_days(
        rt_start_date, F.col("predicted_arrival_epoch"), F.col("scheduled_arrival_secs")
    )
    service_date = F.date_add(rt_start_date, F.coalesce(F.col("service_date_shift_days"), F.lit(0)))
    scheduled_arrival_epoch = service_day_start_epoch(F.col("service_date")) + F.col(
        "scheduled_arrival_secs"
    )
    return (
        joined.withColumn("service_date_shift_days", shift)
        .withColumn("service_date", service_date)
        .withColumn("scheduled_arrival_ts", F.timestamp_seconds(scheduled_arrival_epoch))
        .withColumn("predicted_arrival_ts", F.timestamp_seconds("predicted_arrival_epoch"))
        .withColumn("arrival_delay_s", F.col("predicted_arrival_epoch") - scheduled_arrival_epoch)
        .drop("predicted_arrival_epoch", "scheduled_arrival_secs", "scheduled_departure_secs")
    )


def clean_vehicle_positions(bronze: DataFrame) -> DataFrame:
    """Cast timestamps, flag in-service vehicles, and drop repeated position reports."""
    return (
        bronze.select(
            "vehicle_id",
            "trip_id",
            "route_id",
            "latitude",
            "longitude",
            "bearing",
            "current_status",
            "stop_id",
            "occupancy_status",
            F.timestamp_seconds("vehicle_timestamp").alias("vehicle_ts"),
            F.timestamp_seconds("feed_timestamp").alias("feed_ts"),
            "ingest_ts",
        )
        .withColumn("in_service", F.col("trip_id").isNotNull())
        .dropDuplicates(["vehicle_id", "vehicle_ts"])
    )


def alert_route_versions(bronze: DataFrame) -> DataFrame:
    """One row per alert and informed route/stop, with first and last time it was seen."""
    exploded = bronze.select(
        "entity_id",
        "cause",
        "effect",
        "header_text",
        F.explode_outer("informed_entities").alias("ie"),
        F.timestamp_seconds("feed_timestamp").alias("feed_ts"),
        F.aggregate(
            "active_periods", F.lit(None).cast("long"), lambda acc, p: F.least(acc, p.start)
        ).alias("active_start_epoch"),
    )
    return exploded.groupBy(
        "entity_id",
        F.col("ie.route_id").alias("route_id"),
        F.col("ie.stop_id").alias("stop_id"),
        "cause",
        "effect",
        "header_text",
    ).agg(
        F.min("feed_ts").alias("first_seen_ts"),
        F.max("feed_ts").alias("last_seen_ts"),
        F.timestamp_seconds(F.min("active_start_epoch")).alias("active_start_ts"),
    )
