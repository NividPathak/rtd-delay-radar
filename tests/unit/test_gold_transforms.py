from datetime import date, datetime

from pyspark.sql import Row

from src.pipelines.gold_transforms import route_delay_hourly, stop_arrivals

SCHEDULED = datetime(2026, 10, 6, 23, 0, 0)  # 17:00 Denver (MDT, UTC-6), a Tuesday


def update(feed_ts: datetime, predicted: datetime | None, stop_sequence: int = 5, **kw) -> Row:
    base = dict(
        service_date=date(2026, 10, 6),
        trip_id="t1",
        stop_sequence=stop_sequence,
        rt_route_id="15",
        route_id="15",
        route_short_name="15",
        route_type=3,
        mode="bus",
        direction_id=0,
        stop_id="s5",
        scheduled_arrival_ts=SCHEDULED,
        feed_ts=feed_ts,
        predicted_arrival_ts=predicted,
    )
    base.update(kw)
    return Row(**base)


def arrivals(spark, rows):
    return stop_arrivals(spark.createDataFrame(rows))


def test_last_prediction_before_stop_drops_out_is_the_label(spark) -> None:
    rows = [
        update(datetime(2026, 10, 6, 22, 50), datetime(2026, 10, 6, 23, 1)),
        update(datetime(2026, 10, 6, 22, 59), datetime(2026, 10, 6, 23, 3)),
        update(datetime(2026, 10, 6, 23, 2), datetime(2026, 10, 6, 23, 3, 30)),
    ]
    row = arrivals(spark, rows).first()
    assert row.actual_arrival_ts == datetime(2026, 10, 6, 23, 3, 30)
    assert row.delay_s == 210
    assert row.label_lead_s == 90
    assert row.is_observed
    assert row.n_snapshots == 3
    assert row.first_seen_ts == datetime(2026, 10, 6, 22, 50)


def test_label_from_far_ahead_is_not_observed(spark) -> None:
    # Last snapshot 10 minutes before arrival: a collection gap or a trip still running.
    row = arrivals(
        spark, [update(datetime(2026, 10, 6, 22, 53), datetime(2026, 10, 6, 23, 3))]
    ).first()
    assert row.label_lead_s == 600
    assert not row.is_observed


def test_one_row_per_trip_stop_and_skipped_stops_dropped(spark) -> None:
    rows = [
        update(datetime(2026, 10, 6, 22, 59), datetime(2026, 10, 6, 23, 0), stop_sequence=5),
        update(datetime(2026, 10, 6, 23, 4), datetime(2026, 10, 6, 23, 5), stop_sequence=6),
        update(datetime(2026, 10, 6, 23, 4), None, stop_sequence=7),
    ]
    assert sorted(r.stop_sequence for r in arrivals(spark, rows).collect()) == [5, 6]


def test_route_delay_hourly_uses_local_hour_and_observed_only(spark) -> None:
    rows = [
        update(datetime(2026, 10, 6, 22, 59), datetime(2026, 10, 6, 23, 1), stop_sequence=1),
        update(datetime(2026, 10, 6, 23, 6), datetime(2026, 10, 6, 23, 7), stop_sequence=2),
        update(datetime(2026, 10, 6, 22, 30), datetime(2026, 10, 6, 23, 30), stop_sequence=3),
    ]
    hourly = route_delay_hourly(arrivals(spark, rows)).collect()
    assert len(hourly) == 1
    row = hourly[0]
    assert row.hour_local == 17
    assert row.is_weekday
    assert row.day_of_week == "Tue"
    assert row.n_arrivals == 2  # the 60-minute-lead row is not observed
    assert row.avg_delay_s == 240  # (60 + 420) / 2
    assert row.share_late_5min == 0.5
