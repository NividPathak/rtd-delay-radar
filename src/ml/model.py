"""Model definitions. Both models predict the change in delay from now to the target stop
(the residual over persistence), then add the current delay back. A model that learns
nothing therefore falls back to the persistence baseline instead of something worse."""

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

NUMERIC_FEATURES = [
    "current_delay_s",
    "delay_trend_s",
    "anchor_stop_sequence",
    "stops_remaining",
    "horizon",
    "scheduled_gap_s",
    "hour_local",
    "day_of_week",
    "is_weekend",
    "is_holiday",
    "hist_avg_delay_s",
    "hist_n",
    "has_hist",
    "ahead_delay_s",
    "ahead_age_s",
    "has_ahead",
    "alert_active",
    "direction_id",
]
CATEGORICAL_FEATURES = ["mode", "route_id"]
RESIDUAL = "residual_s"
NO_AHEAD_AGE_S = 3600


def prepare(df: DataFrame) -> DataFrame:
    """Fill missing feature values explicitly, with a flag where missing means something."""
    return (
        df.withColumn("has_hist", (F.col("hist_n") > 0).cast("int"))
        .withColumn("has_ahead", F.col("ahead_delay_s").isNotNull().cast("int"))
        .fillna(
            {
                "delay_trend_s": 0,
                "hist_avg_delay_s": 0,
                "ahead_delay_s": 0,
                "ahead_age_s": NO_AHEAD_AGE_S,
                "stops_remaining": 0,
                "direction_id": -1,
            }
        )
    ).transform(with_residual)


def with_residual(df: DataFrame) -> DataFrame:
    """Training target: change in delay. Live scoring rows have no label, so no residual."""
    if "target_delay_s" not in df.columns:
        return df
    return df.withColumn(RESIDUAL, F.col("target_delay_s") - F.col("current_delay_s"))


def feature_stages(one_hot: bool) -> list:
    """Index categoricals (one-hot encode them for the linear model) and assemble a vector."""
    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        for c in CATEGORICAL_FEATURES
    ]
    if one_hot:
        encoder = OneHotEncoder(
            inputCols=[f"{c}_idx" for c in CATEGORICAL_FEATURES],
            outputCols=[f"{c}_ohe" for c in CATEGORICAL_FEATURES],
            handleInvalid="keep",
        )
        cat_cols = [f"{c}_ohe" for c in CATEGORICAL_FEATURES]
        stages = [*indexers, encoder]
    else:
        cat_cols = [f"{c}_idx" for c in CATEGORICAL_FEATURES]
        stages = list(indexers)
    assembler = VectorAssembler(inputCols=NUMERIC_FEATURES + cat_cols, outputCol="features")
    return [*stages, assembler]


def linear_pipeline(reg_param: float = 0.1, elastic_net: float = 0.0) -> Pipeline:
    lr = LinearRegression(
        featuresCol="features", labelCol=RESIDUAL, regParam=reg_param, elasticNetParam=elastic_net
    )
    return Pipeline(stages=[*feature_stages(one_hot=True), lr])


def gbt_pipeline(max_depth: int = 5, max_iter: int = 50, step_size: float = 0.1) -> Pipeline:
    gbt = GBTRegressor(
        featuresCol="features",
        labelCol=RESIDUAL,
        maxDepth=max_depth,
        maxIter=max_iter,
        stepSize=step_size,
        maxBins=256,  # route_id has about 130 values
        seed=42,
    )
    return Pipeline(stages=[*feature_stages(one_hot=False), gbt])


def predict_delay(model: PipelineModel, df: DataFrame, out_col: str) -> DataFrame:
    """Predicted delay at the target = current delay + predicted change."""
    scored = model.transform(df)
    return scored.withColumn(out_col, F.col("current_delay_s") + F.col("prediction")).drop(
        "prediction", "features"
    )
