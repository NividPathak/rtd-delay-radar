from datetime import date, datetime, timedelta

from pyspark.sql import Row

from src.features.training_set import (
    anchor_target_pairs,
    build_training_set,
    observed,
    with_alert_flag,
    with_delay_trend,
    with_historical_delay,
    with_rtd_prediction,
    with_time_features,
    with_vehicle_ahead,
)

DAY = date(2026, 10, 6)
T = datetime(2026, 10, 6, 16, 0)  # 10:00 Denver
ALERT_SCHEMA = "route_id string, stop_id string, first_seen_ts timestamp, last_seen_ts timestamp"


def arrival(trip: str, seq: int, delay: int, minute: int, day: date = DAY, **kw) -> Row:
    """An observed arrival `minute` minutes after T (shifted by whole days for other dates)."""
    shift = timedelta(days=(day - DAY).days)
    actual = T + shift + timedelta(minutes=minute)
    base = dict(
        service_date=day,
        trip_id=trip,
        stop_sequence=seq,
        stop_id=f"s{seq}",
        route_id="15",
        route_short_name="15",
        mode="bus",
        direction_id=0,
        scheduled_arrival_ts=actual - timedelta(seconds=delay),
        actual_arrival_ts=actual,
        last_seen_ts=actual - timedelta(seconds=30),
        delay_s=delay,
        is_observed=True,
    )
    base.update(kw)
    return Row(**base)


def trip_rows(trip: str, delays: list[int], start_minute: int = 0, day: date = DAY) -> list[Row]:
    return [arrival(trip, i + 1, d, start_minute + 2 * i, day) for i, d in enumerate(delays)]


def obs_df(spark, rows):
    return with_delay_trend(observed(spark.createDataFrame(rows)))


def test_pairs_use_scheduled_stop_offset_and_future_targets(spark) -> None:
    obs = obs_df(spark, trip_rows("t1", [60, 90, 120, 150, 180, 240]))
    pairs = anchor_target_pairs(obs, horizons=[1, 5])
    rows = {(r.anchor_stop_sequence, r.horizon): r for r in pairs.collect()}
    assert rows[(1, 5)].target_delay_s == 240
    assert rows[(1, 5)].current_delay_s == 60
    assert (2, 5) not in rows  # stop 7 does not exist
    assert all(r.target_actual_ts > r.prediction_ts for r in rows.values())


def test_delay_trend_uses_three_stops_back(spark) -> None:
    obs = obs_df(spark, trip_rows("t1", [0, 30, 60, 120]))
    trend = {r.stop_sequence: r.delay_trend_s for r in obs.collect()}
    assert trend[4] == 120
    assert trend[3] is None


def test_time_features_are_local(spark) -> None:
    obs = obs_df(spark, trip_rows("t1", [0, 0]))
    row = with_time_features(anchor_target_pairs(obs, [1])).first()
    assert row.hour_local == 10
    assert row.day_of_week == 3  # Tuesday
    assert row.is_weekend == 0


def test_historical_delay_excludes_today(spark) -> None:
    yesterday = DAY - timedelta(days=1)
    rows = trip_rows("old", [100, 100], day=yesterday) + trip_rows("t1", [0, 30])
    obs = obs_df(spark, rows)
    ex = with_time_features(anchor_target_pairs(obs, [1])).filter("trip_id = 't1'")
    row = with_historical_delay(ex, obs).first()
    assert row.hist_avg_delay_s == 100  # yesterday only, not today's 30
    assert row.hist_n == 1


def test_vehicle_ahead_only_counts_arrivals_known_before_t0(spark) -> None:
    # Trip "ahead" reaches stop 2 at minute 1 (known by t0). Trip "late" reaches it at minute 9.
    rows = [
        arrival("ahead", 2, 300, 1),
        arrival("late", 2, 999, 9),
        arrival("t1", 1, 0, 3),
        arrival("t1", 2, 0, 5),
    ]
    obs = obs_df(spark, rows)
    ex = anchor_target_pairs(obs, [1])
    row = with_vehicle_ahead(ex, obs).filter("trip_id = 't1'").first()
    assert row.ahead_delay_s == 300
    assert row.ahead_age_s > 0


def test_alert_flag_requires_alert_live_at_t0(spark) -> None:
    obs = obs_df(spark, trip_rows("t1", [0, 0]))
    ex = anchor_target_pairs(obs, [1])
    t0 = ex.first().prediction_ts
    alerts = spark.createDataFrame(
        [
            Row(
                route_id="15", stop_id=None, first_seen_ts=t0 - timedelta(hours=1), last_seen_ts=t0
            ),
            Row(
                route_id="15", stop_id="s9", first_seen_ts=t0 - timedelta(hours=1), last_seen_ts=t0
            ),
            Row(
                route_id="15",
                stop_id=None,
                first_seen_ts=t0 + timedelta(minutes=1),
                last_seen_ts=t0 + timedelta(hours=1),
            ),
        ],
        ALERT_SCHEMA,
    )
    assert with_alert_flag(ex, alerts).first().alert_active == 1
    future_only = alerts.filter(f"first_seen_ts > '{t0}'")
    assert with_alert_flag(ex, future_only).first().alert_active == 0
    stop_level_only = alerts.filter("stop_id IS NOT NULL")
    assert with_alert_flag(ex, stop_level_only).first().alert_active == 0


def test_rtd_prediction_uses_latest_snapshot_at_or_before_t0(spark) -> None:
    obs = obs_df(spark, trip_rows("t1", [0, 0]))
    ex = anchor_target_pairs(obs, [1])
    t0 = ex.first().prediction_ts
    updates = spark.createDataFrame(
        [
            Row(
                service_date=DAY,
                trip_id="t1",
                stop_sequence=2,
                feed_ts=t0 - timedelta(seconds=60),
                arrival_delay_s=40,
            ),
            Row(service_date=DAY, trip_id="t1", stop_sequence=2, feed_ts=t0, arrival_delay_s=50),
            Row(
                service_date=DAY,
                trip_id="t1",
                stop_sequence=2,
                feed_ts=t0 + timedelta(seconds=30),
                arrival_delay_s=999,
            ),
        ]
    )
    assert with_rtd_prediction(ex, updates).first().rtd_pred_s == 50


def test_build_training_set_end_to_end(spark) -> None:
    obs_rows = trip_rows("t1", [60, 90, 120, 150, 180, 240])
    arrivals = spark.createDataFrame(obs_rows)
    schedule = spark.createDataFrame([Row(trip_id="t1", stop_sequence=s) for s in range(1, 9)])
    alerts = spark.createDataFrame([("99", None, T, T)], ALERT_SCHEMA)
    updates = spark.createDataFrame(
        [Row(service_date=DAY, trip_id="t1", stop_sequence=6, feed_ts=T, arrival_delay_s=200)]
    )
    ts = build_training_set(arrivals, updates, schedule, alerts)
    row = ts.filter("anchor_stop_sequence = 1 AND horizon = 5").first()
    assert row.target_delay_s == 240
    assert row.persistence_pred_s == 60
    assert row.stops_remaining == 7
    assert row.alert_active == 0
    assert row.rtd_pred_s == 200  # snapshot at T is at or before t0, so it is usable
