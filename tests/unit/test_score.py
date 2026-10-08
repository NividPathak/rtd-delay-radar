from datetime import date, datetime, timedelta

from pyspark.sql import Row
from pyspark.sql import functions as F

from src.ml.score import live_vehicles, monitoring, predictions_long

DAY = date(2026, 10, 6)
NOW = datetime(2026, 10, 6, 16, 30)


def example(**kw) -> Row:
    base = dict(
        service_date=DAY,
        trip_id="t1",
        anchor_stop_sequence=3,
        horizon=5,
        target_stop_sequence=8,
        target_stop_id="s8",
        route_id="15",
        route_short_name="15",
        mode="bus",
        prediction_ts=NOW,
        target_scheduled_ts=NOW + timedelta(minutes=10),
        current_delay_s=60,
        persistence_pred_s=60,
        rtd_pred_s=90,
    )
    base.update(kw)
    return Row(**base)


def test_predictions_long_one_row_per_predictor(spark) -> None:
    df = spark.createDataFrame([example()])
    rows = {r.predictor: r for r in predictions_long(df, F.lit(NOW), None).collect()}
    assert set(rows) == {"persistence", "rtd"}
    assert rows["rtd"].predicted_delay_s == 90
    assert rows["rtd"].predicted_arrival_ts == NOW + timedelta(minutes=10, seconds=90)
    assert rows["persistence"].model_version is None


def test_predictions_long_includes_model_when_present(spark) -> None:
    df = spark.createDataFrame([example()]).withColumn("model_pred_s", F.lit(75.0))
    rows = {r.predictor: r for r in predictions_long(df, F.lit(NOW), "3").collect()}
    assert rows["model"].predicted_delay_s == 75
    assert rows["model"].model_version == "3"


def test_predictions_long_drops_missing_rtd(spark) -> None:
    df = spark.createDataFrame(
        [example(rtd_pred_s=None)],
        "service_date date, trip_id string, anchor_stop_sequence int, horizon int, "
        "target_stop_sequence int, target_stop_id string, route_id string, "
        "route_short_name string, mode string, prediction_ts timestamp, "
        "target_scheduled_ts timestamp, current_delay_s long, persistence_pred_s long, "
        "rtd_pred_s long",
    )
    preds = predictions_long(df, F.lit(NOW), None)
    assert [r.predictor for r in preds.collect()] == ["persistence"]


def test_monitoring_scores_against_observed_arrivals(spark) -> None:
    preds = predictions_long(
        spark.createDataFrame([example(), example(trip_id="t2")]), F.lit(NOW), None
    )
    arrivals = spark.createDataFrame(
        [
            Row(service_date=DAY, trip_id="t1", stop_sequence=8, delay_s=120, is_observed=True),
            Row(service_date=DAY, trip_id="t2", stop_sequence=8, delay_s=0, is_observed=False),
        ]
    )
    rows = {r.predictor: r for r in monitoring(preds, arrivals).collect()}
    assert rows["persistence"].n == 1  # t2's label is not trusted, so it is not scored
    assert rows["persistence"].mae_s == 60
    assert rows["rtd"].mae_s == 30


def test_live_vehicles_latest_recent_position_with_next_stop_delay(spark) -> None:
    positions = spark.createDataFrame(
        [
            Row(
                vehicle_id="v1",
                trip_id="t1",
                route_id="15",
                latitude=39.7,
                longitude=-105.0,
                vehicle_ts=NOW - timedelta(minutes=2),
                in_service=True,
            ),
            Row(
                vehicle_id="v1",
                trip_id="t1",
                route_id="15",
                latitude=39.6,
                longitude=-105.1,
                vehicle_ts=NOW - timedelta(minutes=5),
                in_service=True,
            ),
            Row(
                vehicle_id="v2",
                trip_id="t2",
                route_id="15",
                latitude=39.8,
                longitude=-105.2,
                vehicle_ts=NOW - timedelta(hours=1),
                in_service=True,
            ),
            Row(
                vehicle_id="v3",
                trip_id=None,
                route_id=None,
                latitude=39.9,
                longitude=-105.3,
                vehicle_ts=NOW,
                in_service=False,
            ),
        ]
    )
    updates = spark.createDataFrame(
        [
            Row(trip_id="t1", feed_ts=NOW, stop_sequence=6, arrival_delay_s=180, mode="bus"),
            Row(trip_id="t1", feed_ts=NOW, stop_sequence=7, arrival_delay_s=240, mode="bus"),
            Row(
                trip_id="t1",
                feed_ts=NOW - timedelta(minutes=1),
                stop_sequence=5,
                arrival_delay_s=0,
                mode="bus",
            ),
        ]
    )
    routes = spark.createDataFrame([Row(route_id="15", route_short_name="15")])
    rows = live_vehicles(positions, updates, routes, F.lit(NOW)).collect()
    assert [r.vehicle_id for r in rows] == ["v1"]  # v2 is stale, v3 is not in service
    assert rows[0].latitude == 39.7
    assert rows[0].delay_s == 180  # next stop in the newest snapshot
    assert rows[0].delay_min == 3
