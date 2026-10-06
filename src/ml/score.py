"""Job entry point: score active trips, monitor past predictions, refresh the live map.

Runs after each pipeline update in the rtd_refresh job:
  1. gold.delay_predictions (append): for each active trip, predicted delay 1, 5, 10, and
     20 stops ahead from persistence, RTD, and the champion model (once one exists).
  2. gold.prediction_monitoring (overwrite): daily MAE and RMSE of every predictor against
     observed arrivals, for the last 30 days.
  3. gold.live_vehicles (overwrite): latest position and delay of each vehicle in service.

Usage (on Databricks):
    rtd-score --schema-prefix dev_
"""

import argparse
from datetime import timedelta
from types import ModuleType

from pyspark.ml import PipelineModel
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from src.common import config
from src.features.training_set import build_live_examples

PREDICTION_KEY = ["service_date", "trip_id", "anchor_stop_sequence", "horizon"]


def predictions_long(examples: DataFrame, now_ts: Column, model_version: str | None) -> DataFrame:
    """One row per example and predictor. `examples` may hold a model_pred_s column."""
    predictors = {"persistence": "persistence_pred_s", "rtd": "rtd_pred_s"}
    if "model_pred_s" in examples.columns:
        predictors["model"] = "model_pred_s"
    stacked = F.explode(
        F.array(
            *[
                F.struct(F.lit(name).alias("predictor"), F.col(col).cast("double").alias("pred"))
                for name, col in predictors.items()
            ]
        )
    )
    return (
        examples.select(
            *PREDICTION_KEY,
            "target_stop_sequence",
            "target_stop_id",
            "route_id",
            "route_short_name",
            "mode",
            "prediction_ts",
            "target_scheduled_ts",
            "current_delay_s",
            stacked.alias("p"),
        )
        .select(
            "*",
            F.col("p.predictor").alias("predictor"),
            F.col("p.pred").alias("predicted_delay_s"),
        )
        .drop("p")
        .filter(F.col("predicted_delay_s").isNotNull())
        .withColumn(
            "predicted_arrival_ts",
            F.timestamp_seconds(
                F.unix_timestamp("target_scheduled_ts") + F.col("predicted_delay_s").cast("long")
            ),
        )
        .withColumn("model_version", F.lit(model_version).cast("string"))
        .withColumn("scored_at_ts", now_ts)
        .withColumn("processed_ts", F.current_timestamp())
    )


def monitoring(predictions: DataFrame, arrivals: DataFrame) -> DataFrame:
    """Daily error of every predictor once the target arrival has been observed."""
    actual = arrivals.filter("is_observed").select(
        "service_date",
        "trip_id",
        F.col("stop_sequence").alias("target_stop_sequence"),
        F.col("delay_s").alias("actual_delay_s"),
    )
    err = F.col("predicted_delay_s") - F.col("actual_delay_s")
    return (
        predictions.join(actual, ["service_date", "trip_id", "target_stop_sequence"])
        .groupBy("service_date", "predictor", "model_version", "mode", "horizon")
        .agg(
            F.count(F.lit(1)).alias("n"),
            F.avg(F.abs(err)).alias("mae_s"),
            F.sqrt(F.avg(err * err)).alias("rmse_s"),
        )
        .withColumn("processed_ts", F.current_timestamp())
    )


def live_vehicles(
    positions: DataFrame,
    stop_updates: DataFrame,
    routes: DataFrame,
    now_ts: Column,
    max_age_s: int = config.LIVE_VEHICLE_MAX_AGE_S,
) -> DataFrame:
    """Latest position of each in-service vehicle, with its trip's delay at the next stop."""
    recent = positions.filter(
        F.col("in_service")
        & (F.unix_timestamp(now_ts) - F.unix_timestamp("vehicle_ts") <= max_age_s)
    )
    latest_pos = Window.partitionBy("vehicle_id").orderBy(F.desc("vehicle_ts"))
    vehicles = recent.withColumn("_r", F.row_number().over(latest_pos)).filter("_r = 1").drop("_r")
    # Delay at the trip's next stop in its latest snapshot: the lowest stop_sequence listed.
    next_stop = Window.partitionBy("trip_id").orderBy(F.desc("feed_ts"), "stop_sequence")
    trip_delay = (
        stop_updates.filter(F.col("arrival_delay_s").isNotNull())
        .withColumn("_r", F.row_number().over(next_stop))
        .filter("_r = 1")
        .select("trip_id", F.col("arrival_delay_s").alias("delay_s"), "mode")
    )
    return (
        vehicles.join(trip_delay, "trip_id", "left")
        .join(routes.select("route_id", "route_short_name"), "route_id", "left")
        .select(
            "vehicle_id",
            "trip_id",
            "route_id",
            "route_short_name",
            "mode",
            "latitude",
            "longitude",
            "vehicle_ts",
            "delay_s",
            (F.col("delay_s") / 60).alias("delay_min"),
            F.current_timestamp().alias("processed_ts"),
        )
    )


def recent(df: DataFrame, column: str, days: int) -> DataFrame:
    return df.filter(F.col(column) >= F.date_sub(F.current_date(), days))


def load_champion(mlflow_module: ModuleType) -> tuple[PipelineModel | None, str | None]:
    """Return (model, version) for the champion alias, or (None, None) if there is none."""
    client = mlflow_module.MlflowClient()
    try:
        version = client.get_model_version_by_alias(config.MODEL_NAME, "champion").version
    except Exception:
        return None, None
    uri = f"models:/{config.MODEL_NAME}@champion"
    model = mlflow_module.spark.load_model(
        uri, dfs_tmpdir=f"{config.CHECKPOINT_VOLUME_PATH}/mlflow_tmp"
    )
    return model, str(version)


def main() -> None:
    parser = argparse.ArgumentParser(description="Score active trips and monitor predictions")
    parser.add_argument("--schema-prefix", default="")
    args = parser.parse_args()

    import mlflow
    from databricks.sdk.runtime import spark

    from src.ml.model import predict_delay, prepare

    p = args.schema_prefix

    def table(schema: str, name: str) -> DataFrame:
        return spark.table(config.table_name(schema, name, p))

    stop_updates = recent(table("silver", "stop_time_updates"), "service_date", 1)
    now = stop_updates.agg(F.max("feed_ts")).first()[0]  # one value: newest snapshot
    if now is None:
        print("no recent stop updates, nothing to score")
        return
    now_ts = F.lit(now).cast("timestamp")
    hist_start = now.date() - timedelta(days=config.HIST_DAYS + 1)
    arrivals = table("gold", "stop_arrivals").filter(F.col("service_date") >= F.lit(hist_start))

    examples = build_live_examples(
        arrivals,
        stop_updates,
        table("silver", "gtfs_scheduled_stops"),
        table("silver", "alerts"),
        now_ts,
    )
    mlflow.set_registry_uri("databricks-uc")
    model, version = load_champion(mlflow)
    if model is None:
        print("no champion model yet: writing persistence and RTD predictions only")
    else:
        examples = predict_delay(model, prepare(examples), "model_pred_s")
        print(f"scoring with {config.MODEL_NAME} v{version} (champion)")

    preds = predictions_long(examples, now_ts, version)
    preds.write.format("delta").mode("append").saveAsTable(
        config.table_name("gold", "delay_predictions", p)
    )

    history = recent(table("gold", "delay_predictions"), "service_date", config.MONITORING_DAYS)
    monitoring(history, arrivals).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(config.table_name("gold", "prediction_monitoring", p))

    live = live_vehicles(
        recent(table("silver", "vehicle_positions"), "vehicle_ts", 1),
        stop_updates,
        table("silver", "gtfs_routes"),
        now_ts,
    )
    live.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        config.table_name("gold", "live_vehicles", p)
    )
    print(f"scored at {now} UTC")


if __name__ == "__main__":
    main()
