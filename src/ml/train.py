"""Job entry point: baselines, linear model, and GBT on a time split, all logged to MLflow.

Usage (on Databricks):
    rtd-train --start 2026-10-06 --end 2026-10-27 --git-sha <commit>
A full retrain is this one command with a new date range.

Runs on less than 21 days of data, or without a full held-out week, are pipeline tests:
they are tagged data_status=preliminary and never get the `champion` alias.
"""

import argparse
from types import ModuleType

from pyspark.ml import Pipeline
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common import config
from src.features.build import date_range
from src.ml.evaluate import SplitInfo, error_metrics, metrics_dict, time_split
from src.ml.model import gbt_pipeline, linear_pipeline, predict_delay, prepare

BASELINES = ["persistence_pred_s", "rtd_pred_s"]
CANDIDATES: dict[str, tuple[Pipeline, dict]] = {
    "linear": (linear_pipeline(reg_param=0.1), {"model": "linear", "reg_param": 0.1}),
    "gbt_d4": (gbt_pipeline(max_depth=4, max_iter=50), {"model": "gbt", "max_depth": 4}),
    "gbt_d6": (gbt_pipeline(max_depth=6, max_iter=50), {"model": "gbt", "max_depth": 6}),
}


def run_tags(info: SplitInfo, git_sha: str) -> dict[str, str]:
    return {
        "data_status": "preliminary" if info.preliminary else "full",
        "data_status_reason": info.reason,
        "data_start": str(info.first_date),
        "data_end": str(info.last_date),
        "test_start": str(info.test_start),
        "git_sha": git_sha,
    }


def results_rows(metrics: DataFrame, run_id: str, info: SplitInfo) -> DataFrame:
    return metrics.select(
        F.lit(run_id).alias("run_id"),
        "predictor",
        "mode",
        "horizon",
        "n",
        "mae_s",
        "rmse_s",
        F.lit("preliminary" if info.preliminary else "full").alias("data_status"),
        F.lit(str(info.first_date)).alias("data_start"),
        F.lit(str(info.last_date)).alias("data_end"),
        F.current_timestamp().alias("processed_ts"),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Train and evaluate delay models")
    parser.add_argument("--start", default="", help="first service date (default: earliest)")
    parser.add_argument("--end", default="", help="last service date (default: latest)")
    parser.add_argument("--schema-prefix", default="")
    parser.add_argument("--git-sha", default="unknown")
    args = parser.parse_args()

    import mlflow
    from databricks.sdk.runtime import spark

    table = config.table_name("gold", "training_set", args.schema_prefix)
    first, last = date_range(spark, table, args.start, args.end)
    data = spark.table(table)
    data = prepare(data.filter(F.col("service_date").between(F.lit(first), F.lit(last))))
    train, test, info = time_split(data, first, last)
    print(f"split: {info}")

    mlflow.set_registry_uri("databricks-uc")
    user = spark.sql("SELECT current_user()").first()[0]
    mlflow.set_experiment(f"/Users/{user}/rtd_delay_radar")
    tags = run_tags(info, args.git_sha)
    results = []

    with mlflow.start_run(run_name="baselines", tags=tags) as run:
        for name in BASELINES:
            m = error_metrics(test, name)
            mlflow.log_metrics({f"{name}.{k}": v for k, v in metrics_dict(m).items()})
            results.append(results_rows(m, run.info.run_id, info))

    if train.isEmpty():
        print(f"no training days before {info.test_start}: baselines logged, models skipped")
        save_results(results, args.schema_prefix)
        return

    best_name, best_mae, best_model = None, float("inf"), None
    for name, (pipeline, params) in CANDIDATES.items():
        with mlflow.start_run(run_name=name, tags=tags) as run:
            mlflow.log_params(params)
            model = pipeline.fit(train)
            scored = predict_delay(model, test, f"{name}_pred_s")
            m = error_metrics(scored, f"{name}_pred_s")
            flat = metrics_dict(m)
            mlflow.log_metrics(flat)
            results.append(results_rows(m, run.info.run_id, info))
            if flat["mae_s.all"] < best_mae:
                best_name, best_mae, best_model = name, flat["mae_s.all"], (model, run.info.run_id)
                best_scored = scored.withColumnRenamed(f"{name}_pred_s", "model_pred_s")
    print(f"best model: {best_name} (MAE {best_mae:.1f} s, {tags['data_status']})")

    save_results(results, args.schema_prefix)
    save_test_predictions(best_scored, best_model[1], args.schema_prefix)
    register(mlflow, best_model, train, info)


PREDICTION_COLUMNS = [
    "service_date",
    "trip_id",
    "anchor_stop_sequence",
    "horizon",
    "route_short_name",
    "mode",
    "hour_local",
    "is_weekend",
    "current_delay_s",
    "delay_trend_s",
    "has_ahead",
    "alert_active",
    "target_delay_s",
    "persistence_pred_s",
    "rtd_pred_s",
    "model_pred_s",
]


def save_test_predictions(scored: DataFrame, run_id: str, schema_prefix: str) -> None:
    """Overwrite gold.test_predictions with the best model's test-week predictions."""
    table = config.table_name("gold", "test_predictions", schema_prefix)
    out = scored.select(*PREDICTION_COLUMNS, F.lit(run_id).alias("run_id"))
    out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)


def save_results(results: list[DataFrame], schema_prefix: str) -> None:
    """Append this run's metrics to gold.model_results (a few dozen rows)."""
    combined = results[0]
    for r in results[1:]:
        combined = combined.unionByName(r)
    table = config.table_name("gold", "model_results", schema_prefix)
    combined.write.format("delta").mode("append").saveAsTable(table)


def register(mlflow: ModuleType, best: tuple, train: DataFrame, info: SplitInfo) -> None:
    """Register the best model in Unity Catalog. Only full-data runs may become champion."""
    from mlflow.models import infer_signature

    model, run_id = best
    sample = train.limit(5).toPandas()  # five rows, for the model signature only
    signature = infer_signature(sample, sample[["current_delay_s"]])
    with mlflow.start_run(run_id=run_id):
        logged = mlflow.spark.log_model(
            model,
            artifact_path="model",
            signature=signature,
            registered_model_name=config.MODEL_NAME,
            dfs_tmpdir=f"{config.CHECKPOINT_VOLUME_PATH}/mlflow_tmp",
        )
    client = mlflow.MlflowClient()
    version = logged.registered_model_version
    client.set_model_version_tag(
        config.MODEL_NAME, version, "data_status", "preliminary" if info.preliminary else "full"
    )
    if info.preliminary:
        print(f"registered {config.MODEL_NAME} v{version} as preliminary, no champion alias")
    else:
        client.set_registered_model_alias(config.MODEL_NAME, "champion", version)
        print(f"registered {config.MODEL_NAME} v{version} with alias champion")


if __name__ == "__main__":
    main()
