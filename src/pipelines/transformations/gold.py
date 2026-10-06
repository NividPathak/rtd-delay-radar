"""Lakeflow Declarative Pipeline: gold tables.

Reads the silver tables of the same pipeline and publishes to the gold schema
(pipeline setting rtd.gold_schema, e.g. rtd.gold or rtd.dev_gold).
"""

import sys

from pyspark import pipelines as dp
from pyspark.sql import DataFrame

sys.path.insert(0, spark.conf.get("rtd.code_root"))  # noqa: F821

from src.common import config  # noqa: E402
from src.pipelines import gold_transforms as G  # noqa: E402

GOLD = spark.conf.get("rtd.gold_schema")  # noqa: F821


@dp.materialized_view(
    name=f"{GOLD}.stop_arrivals",
    comment="One row per trip and stop: scheduled time, observed arrival, delay in seconds.",
)
@dp.expect("label_is_observed", "is_observed")
@dp.expect("delay_within_2h", f"abs(delay_s) <= {config.MAX_ABS_DELAY_SECONDS}")
def stop_arrivals() -> DataFrame:
    return G.stop_arrivals(spark.read.table("stop_time_updates"))  # noqa: F821


@dp.materialized_view(
    name=f"{GOLD}.route_delay_hourly",
    comment="Observed delay per route, service date, and local hour, for the dashboard.",
)
def route_delay_hourly() -> DataFrame:
    return G.route_delay_hourly(spark.read.table(f"{GOLD}.stop_arrivals"))  # noqa: F821
