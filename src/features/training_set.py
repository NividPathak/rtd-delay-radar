"""Build the training set: one row per (anchor stop, horizon) with features known at t0.

An example: a vehicle has just reached stop A (its observed arrival, from gold.stop_arrivals).
The prediction time t0 is when that arrival became known. The label is the observed delay
at stop A+K, K stops ahead. Every feature uses only information available at t0. The
leakage check for each feature is in docs/decisions.md.
"""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from src.common import config

TRIP_KEY = ["service_date", "trip_id"]
# One example = one anchor stop on one trip at one horizon.
KEY_TYPES = [
    ("service_date", "date"),
    ("trip_id", "string"),
    ("anchor_stop_sequence", "int"),
    ("horizon", "int"),
]
EXAMPLE_KEY = [c for c, _ in KEY_TYPES]


def observed(arrivals: DataFrame) -> DataFrame:
    """Observed arrivals with `known_ts`: the moment the arrival became known."""
    return arrivals.filter("is_observed").withColumn(
        "known_ts", F.greatest("actual_arrival_ts", "last_seen_ts")
    )


def with_delay_trend(obs: DataFrame, n_stops: int = config.TREND_STOPS) -> DataFrame:
    """Delay now minus delay `n_stops` observed stops earlier on the same trip."""
    earlier = Window.partitionBy(*TRIP_KEY).orderBy("stop_sequence")
    return obs.withColumn(
        "delay_trend_s", F.col("delay_s") - F.lag("delay_s", n_stops).over(earlier)
    )


def anchor_target_pairs(obs: DataFrame, horizons: list[int] = config.HORIZONS) -> DataFrame:
    """Pair each anchor stop with the observed stop K scheduled stops later on the same trip."""
    anchors = obs.select(
        *TRIP_KEY,
        "route_id",
        "route_short_name",
        "mode",
        "direction_id",
        F.col("stop_sequence").alias("anchor_stop_sequence"),
        F.col("delay_s").alias("current_delay_s"),
        "delay_trend_s",
        F.col("scheduled_arrival_ts").alias("anchor_scheduled_ts"),
        F.col("known_ts").alias("prediction_ts"),
        F.explode(F.array(*[F.lit(h) for h in horizons])).alias("horizon"),
    ).withColumn("target_stop_sequence", F.col("anchor_stop_sequence") + F.col("horizon"))
    targets = obs.select(
        *TRIP_KEY,
        F.col("stop_sequence").alias("target_stop_sequence"),
        F.col("stop_id").alias("target_stop_id"),
        F.col("scheduled_arrival_ts").alias("target_scheduled_ts"),
        F.col("actual_arrival_ts").alias("target_actual_ts"),
        F.col("delay_s").alias("target_delay_s"),
    )
    return (
        anchors.join(targets, [*TRIP_KEY, "target_stop_sequence"])
        .filter(F.col("target_actual_ts") > F.col("prediction_ts"))  # target still in the future
        .withColumn(
            "scheduled_gap_s",
            F.unix_timestamp("target_scheduled_ts") - F.unix_timestamp("anchor_scheduled_ts"),
        )
    )


def local(ts: str) -> Column:
    return F.from_utc_timestamp(ts, config.LOCAL_TIMEZONE)


def with_time_features(examples: DataFrame) -> DataFrame:
    """Local hour, weekday, weekend and holiday flags at prediction time."""
    t0 = local("prediction_ts")
    return (
        examples.withColumn("hour_local", F.hour(t0))
        .withColumn("day_of_week", F.dayofweek(t0))
        .withColumn("is_weekend", F.dayofweek(t0).isin(1, 7).cast("int"))
        .withColumn("is_holiday", F.to_date(t0).cast("string").isin(config.HOLIDAYS).cast("int"))
        .withColumn("target_hour_local", F.hour(local("target_scheduled_ts")))
    )


def with_stops_remaining(examples: DataFrame, scheduled_stops: DataFrame) -> DataFrame:
    """Scheduled stops left on the trip after the anchor (known from the static schedule)."""
    last_stop = scheduled_stops.groupBy("trip_id").agg(F.max("stop_sequence").alias("last_seq"))
    return (
        examples.join(last_stop, "trip_id", "left")
        .withColumn("stops_remaining", F.col("last_seq") - F.col("anchor_stop_sequence"))
        .drop("last_seq")
    )


def with_historical_delay(
    examples: DataFrame, obs: DataFrame, days: int = config.HIST_DAYS
) -> DataFrame:
    """Average observed delay for the target's route, stop, and local hour over the
    previous `days` service dates. The current service date is excluded, so no
    same-day (possibly future) arrivals leak in."""
    daily = (
        obs.withColumn("hour", F.hour(local("scheduled_arrival_ts")))
        .groupBy("route_id", "stop_id", "hour", "service_date")
        .agg(F.sum("delay_s").alias("sum_delay"), F.count(F.lit(1)).alias("n"))
    )
    ex = examples.alias("ex")
    d = daily.alias("d")
    cond = [
        F.col("ex.route_id") == F.col("d.route_id"),
        F.col("ex.target_stop_id") == F.col("d.stop_id"),
        F.col("ex.target_hour_local") == F.col("d.hour"),
        F.col("d.service_date") >= F.date_sub(F.col("ex.service_date"), days),
        F.col("d.service_date") < F.col("ex.service_date"),
    ]
    hist = (
        ex.join(d, cond)
        .groupBy(*[F.col(f"ex.{k}").alias(k) for k in EXAMPLE_KEY])
        .agg(
            (F.sum("d.sum_delay") / F.sum("d.n")).alias("hist_avg_delay_s"),
            F.sum("d.n").alias("hist_n"),
        )
    )
    return examples.join(hist, EXAMPLE_KEY, "left").fillna({"hist_n": 0})


def with_vehicle_ahead(examples: DataFrame, obs: DataFrame) -> DataFrame:
    """Delay of the most recent other vehicle on the same route and direction at the
    target stop, counting only arrivals already known before t0 (an as-of join)."""
    events = obs.select(
        "route_id",
        "direction_id",
        "stop_id",
        F.col("known_ts").alias("ts"),
        F.lit(0).alias("is_example"),
        F.col("delay_s").alias("ev_delay"),
        F.col("known_ts").alias("ev_ts"),
        *[F.lit(None).cast(t).alias(c) for c, t in KEY_TYPES],
    )
    queries = examples.select(
        "route_id",
        "direction_id",
        F.col("target_stop_id").alias("stop_id"),
        F.col("prediction_ts").alias("ts"),
        F.lit(1).alias("is_example"),
        F.lit(None).cast("long").alias("ev_delay"),
        F.lit(None).cast("timestamp").alias("ev_ts"),
        *[F.col(c) for c, _ in KEY_TYPES],
    )
    # Examples sort before events at the same timestamp, so only strictly earlier events count.
    w = (
        Window.partitionBy("route_id", "direction_id", "stop_id")
        .orderBy("ts", F.desc("is_example"))
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    asof = (
        events.unionByName(queries)
        .withColumn("ahead_delay_s", F.last("ev_delay", ignorenulls=True).over(w))
        .withColumn("ahead_ts", F.last("ev_ts", ignorenulls=True).over(w))
        .filter("is_example = 1")
        .select(
            *EXAMPLE_KEY,
            "ahead_delay_s",
            (F.unix_timestamp("ts") - F.unix_timestamp("ahead_ts")).alias("ahead_age_s"),
        )
    )
    return examples.join(asof, EXAMPLE_KEY, "left")


def with_alert_flag(examples: DataFrame, alerts: DataFrame) -> DataFrame:
    """1 if a route-wide alert (names the route, not one stop) was in the feed at t0.

    Stop-level alerts (mostly long construction closures of single stops) are left out:
    they cover most routes most of the time, so they would carry almost no signal."""
    a = alerts.filter(F.col("stop_id").isNull()).select(
        F.col("route_id").alias("a_route"),
        F.col("first_seen_ts").alias("a_first"),
        F.col("last_seen_ts").alias("a_last"),
    )
    hits = (
        examples.join(
            F.broadcast(a),
            (F.col("route_id") == F.col("a_route"))
            & F.col("prediction_ts").between(F.col("a_first"), F.col("a_last")),
        )
        .select(*EXAMPLE_KEY)
        .distinct()
        .withColumn("alert_active", F.lit(1))
    )
    return examples.join(hits, EXAMPLE_KEY, "left").fillna({"alert_active": 0})


def with_rtd_prediction(examples: DataFrame, stop_updates: DataFrame) -> DataFrame:
    """RTD's predicted delay for the target stop from the latest snapshot at or before t0."""
    preds = stop_updates.select(
        "service_date",
        "trip_id",
        F.col("stop_sequence").alias("target_stop_sequence"),
        "feed_ts",
        "arrival_delay_s",
    ).filter("arrival_delay_s IS NOT NULL")
    latest = (
        examples.select(*EXAMPLE_KEY, "target_stop_sequence", "prediction_ts")
        .join(preds, ["service_date", "trip_id", "target_stop_sequence"])
        .filter(F.col("feed_ts") <= F.col("prediction_ts"))
        .groupBy(*EXAMPLE_KEY)
        .agg(F.max(F.struct("feed_ts", "arrival_delay_s")).alias("p"))
        .select(*EXAMPLE_KEY, F.col("p.arrival_delay_s").alias("rtd_pred_s"))
    )
    return examples.join(latest, EXAMPLE_KEY, "left")


def build_training_set(
    arrivals: DataFrame, stop_updates: DataFrame, scheduled_stops: DataFrame, alerts: DataFrame
) -> DataFrame:
    """All steps together. Inputs are gold.stop_arrivals, silver.stop_time_updates,
    silver.gtfs_scheduled_stops, and silver.alerts."""
    obs = with_delay_trend(observed(arrivals))
    examples = with_time_features(anchor_target_pairs(obs))
    examples = with_stops_remaining(examples, scheduled_stops)
    examples = with_historical_delay(examples, obs)
    examples = with_vehicle_ahead(examples, obs)
    examples = with_alert_flag(examples, alerts)
    examples = with_rtd_prediction(examples, stop_updates)
    return examples.withColumn("persistence_pred_s", F.col("current_delay_s")).withColumn(
        "processed_ts", F.current_timestamp()
    )
