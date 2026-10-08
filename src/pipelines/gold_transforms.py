"""Pure DataFrame transformations for the gold layer.

`stop_arrivals` turns the stream of predictions in silver into one labelled row per
trip and stop. `route_delay_hourly` aggregates those labels for the dashboard and EDA.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common import config

STATIC_ATTRIBUTES = [
    "rt_route_id",
    "route_id",
    "route_short_name",
    "route_type",
    "mode",
    "direction_id",
    "stop_id",
    "scheduled_arrival_ts",
]


def stop_arrivals(stop_updates: DataFrame) -> DataFrame:
    """One row per (service_date, trip_id, stop_sequence) with an observed arrival time.

    The observed arrival is RTD's last prediction for that stop before the stop dropped
    out of the feed (RTD removes a stop once the vehicle passes it). `label_lead_s` is how
    far ahead of the arrival that last prediction was made. Small means the vehicle was
    about to arrive, so the label is reliable. `is_observed` marks those rows.
    """
    predicted = stop_updates.filter(F.col("predicted_arrival_ts").isNotNull())
    last = F.max(F.struct("feed_ts", "predicted_arrival_ts")).alias("last")
    grouped = predicted.groupBy("service_date", "trip_id", "stop_sequence").agg(
        last,
        F.min("feed_ts").alias("first_seen_ts"),
        F.count(F.lit(1)).alias("n_snapshots"),
        *[F.max(c).alias(c) for c in STATIC_ATTRIBUTES],
    )
    actual = F.col("last.predicted_arrival_ts")
    lead = F.unix_timestamp(actual) - F.unix_timestamp(F.col("last.feed_ts"))
    return grouped.select(
        "service_date",
        "trip_id",
        "stop_sequence",
        *STATIC_ATTRIBUTES,
        actual.alias("actual_arrival_ts"),
        F.col("last.feed_ts").alias("last_seen_ts"),
        "first_seen_ts",
        "n_snapshots",
        (F.unix_timestamp(actual) - F.unix_timestamp("scheduled_arrival_ts")).alias("delay_s"),
        lead.alias("label_lead_s"),
        (lead <= config.LABEL_MAX_LEAD_SECONDS).alias("is_observed"),
        F.current_timestamp().alias("processed_ts"),
    )


def route_delay_hourly(arrivals: DataFrame) -> DataFrame:
    """Observed delay per route, service date, and local hour of the scheduled arrival."""
    local_ts = F.from_utc_timestamp("scheduled_arrival_ts", config.LOCAL_TIMEZONE)
    observed = arrivals.filter("is_observed").select(
        "*",
        F.hour(local_ts).alias("hour_local"),
        F.date_format(local_ts, "E").alias("day_of_week"),
        F.dayofweek(local_ts).between(2, 6).alias("is_weekday"),
    )
    late = (F.col("delay_s") > config.LATE_THRESHOLD_SECONDS).cast("int")
    return observed.groupBy(
        "service_date",
        "day_of_week",
        "is_weekday",
        "hour_local",
        "route_id",
        "route_short_name",
        "mode",
    ).agg(
        F.count(F.lit(1)).alias("n_arrivals"),
        F.avg("delay_s").alias("avg_delay_s"),
        F.percentile_approx("delay_s", 0.5).alias("median_delay_s"),
        F.avg(late).alias("share_late_5min"),
        F.max("processed_ts").alias("processed_ts"),
    )
