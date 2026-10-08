"""Time-based split and error metrics. No random splits and no shuffling, ever."""

from dataclasses import dataclass
from datetime import date, timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common import config


@dataclass
class SplitInfo:
    """What the split covered, and whether the result can be reported."""

    first_date: date
    last_date: date
    test_start: date
    n_days: int
    test_days: int
    preliminary: bool
    reason: str


def time_split(
    df: DataFrame, first_date: date, last_date: date, test_days: int = config.TEST_DAYS
) -> tuple[DataFrame, DataFrame, SplitInfo]:
    """Train on service dates before the last `test_days` days, test on those days.

    With too little data the split still runs (so the pipeline can be tested), but the
    result is marked preliminary with the reason.
    """
    n_days = (last_date - first_date).days + 1
    test_len = min(test_days, max(1, n_days // 3))
    test_start = last_date - timedelta(days=test_len - 1)
    reasons = []
    if n_days < config.MIN_DAYS_FOR_RESULTS:
        reasons.append(f"only {n_days} days of data (need {config.MIN_DAYS_FOR_RESULTS})")
    if test_len < test_days:
        reasons.append(f"held-out window is {test_len} days (need {test_days})")
    info = SplitInfo(
        first_date=first_date,
        last_date=last_date,
        test_start=test_start,
        n_days=n_days,
        test_days=test_len,
        preliminary=bool(reasons),
        reason="; ".join(reasons) or "full data",
    )
    train = df.filter(F.col("service_date") < F.lit(test_start))
    test = df.filter(F.col("service_date") >= F.lit(test_start))
    return train, test, info


def error_metrics(df: DataFrame, pred_col: str, label_col: str = "target_delay_s") -> DataFrame:
    """MAE and RMSE in seconds, overall and by mode and horizon. Rows with no prediction
    are excluded and counted, so baselines with gaps (RTD) are compared fairly."""
    err = F.col(pred_col) - F.col(label_col)
    scored = df.filter(F.col(pred_col).isNotNull())
    aggs = [
        F.count(F.lit(1)).alias("n"),
        F.avg(F.abs(err)).alias("mae_s"),
        F.sqrt(F.avg(err * err)).alias("rmse_s"),
    ]
    by_group = scored.groupBy("mode", "horizon").agg(*aggs)
    overall = scored.agg(*aggs).select(
        F.lit("all").alias("mode"), F.lit(0).alias("horizon"), "n", "mae_s", "rmse_s"
    )
    return overall.unionByName(by_group).withColumn("predictor", F.lit(pred_col))


def metrics_dict(metrics: DataFrame) -> dict[str, float]:
    """Flatten small metric rows into MLflow-style keys like mae_s.bus.h5."""
    out = {}
    for r in metrics.collect():  # at most a few dozen rows
        suffix = "all" if r.mode == "all" else f"{r.mode}.h{r.horizon}"
        out[f"mae_s.{suffix}"] = float(r.mae_s)
        out[f"rmse_s.{suffix}"] = float(r.rmse_s)
        out[f"n.{suffix}"] = float(r.n)
    return out
