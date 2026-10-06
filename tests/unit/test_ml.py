import random
from datetime import date, timedelta

import pytest
from pyspark.sql import Row

from src.ml.evaluate import error_metrics, metrics_dict, time_split
from src.ml.model import gbt_pipeline, linear_pipeline, predict_delay, prepare

START = date(2026, 10, 6)
SCHEMA = (
    "service_date date, mode string, route_id string, horizon int, current_delay_s long, "
    "target_delay_s long, delay_trend_s long, anchor_stop_sequence int, stops_remaining int, "
    "scheduled_gap_s long, hour_local int, day_of_week int, is_weekend int, is_holiday int, "
    "hist_avg_delay_s double, hist_n long, ahead_delay_s long, ahead_age_s long, "
    "alert_active int, direction_id int, persistence_pred_s long"
)


def example(day: int, horizon: int, current: int, target: int, mode: str = "bus") -> Row:
    return Row(
        service_date=START + timedelta(days=day),
        mode=mode,
        route_id="15" if mode == "bus" else "A",
        horizon=horizon,
        current_delay_s=current,
        target_delay_s=target,
        delay_trend_s=None,
        anchor_stop_sequence=3,
        stops_remaining=20,
        scheduled_gap_s=120 * horizon,
        hour_local=8,
        day_of_week=3,
        is_weekend=0,
        is_holiday=0,
        hist_avg_delay_s=None,
        hist_n=0,
        ahead_delay_s=None,
        ahead_age_s=None,
        alert_active=0,
        direction_id=0,
        persistence_pred_s=current,
    )


@pytest.fixture(scope="module")
def synthetic(spark):
    # Delay grows by 10 s per stop ahead, plus noise. Persistence misses the growth.
    rng = random.Random(0)
    rows = []
    for day in range(10):
        for _ in range(60):
            h = rng.choice([1, 5, 10])
            cur = rng.randint(-60, 300)
            rows.append(example(day, h, cur, cur + 10 * h + rng.randint(-5, 5)))
    return spark.createDataFrame(rows, SCHEMA)


def test_time_split_is_by_date_and_flags_short_data(synthetic) -> None:
    train, test, info = time_split(synthetic, START, START + timedelta(days=9), test_days=7)
    assert info.preliminary
    assert "only 10 days" in info.reason
    assert info.test_days == 3  # shortened to a third of the data
    max_train = train.agg({"service_date": "max"}).first()[0]
    min_test = test.agg({"service_date": "min"}).first()[0]
    assert max_train < min_test


def test_time_split_full_data_is_not_preliminary(synthetic) -> None:
    _, _, info = time_split(synthetic, START, START + timedelta(days=27), test_days=7)
    assert not info.preliminary
    assert info.test_days == 7


def test_error_metrics_by_mode_and_horizon(spark) -> None:
    df = spark.createDataFrame(
        [
            Row(mode="bus", horizon=1, target_delay_s=100, p=110),
            Row(mode="bus", horizon=1, target_delay_s=100, p=70),
            Row(mode="rail", horizon=5, target_delay_s=0, p=None),
        ]
    )
    m = metrics_dict(error_metrics(df, "p"))
    assert m["mae_s.bus.h1"] == 20
    assert m["rmse_s.bus.h1"] == pytest.approx((500) ** 0.5)
    assert m["n.all"] == 2  # missing predictions are excluded, not counted as zero


@pytest.mark.parametrize("make", [linear_pipeline, lambda: gbt_pipeline(max_iter=10)])
def test_models_beat_persistence_on_learnable_signal(synthetic, make) -> None:
    train, test, _ = time_split(synthetic, START, START + timedelta(days=9))
    model = make().fit(prepare(train))
    scored = predict_delay(model, prepare(test), "model_pred_s")
    model_mae = metrics_dict(error_metrics(scored, "model_pred_s"))["mae_s.all"]
    persistence_mae = metrics_dict(error_metrics(scored, "persistence_pred_s"))["mae_s.all"]
    assert model_mae < persistence_mae
