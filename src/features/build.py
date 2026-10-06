"""Job entry point: build gold.training_set for a date range.

Usage (on Databricks):
    rtd-build-features --start 2026-10-06 --end 2026-10-27
    rtd-build-features                       # every service date available
Only the given service dates are replaced in the table, so ranges can be rebuilt safely.
"""

import argparse
from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import config
from src.features.training_set import build_training_set


def date_range(spark: SparkSession, arrivals_table: str, start: str, end: str) -> tuple[date, date]:
    """Use the given dates, or fall back to the first and last service date in gold."""
    if start and end:
        return date.fromisoformat(start), date.fromisoformat(end)
    row = spark.table(arrivals_table).agg(F.min("service_date"), F.max("service_date")).first()
    return (
        date.fromisoformat(start) if start else row[0],
        date.fromisoformat(end) if end else row[1],
    )


def between(df: DataFrame, first: date, last: date) -> DataFrame:
    return df.filter(F.col("service_date").between(F.lit(first), F.lit(last)))


def write_range(spark: SparkSession, df: DataFrame, table: str, first: date, last: date) -> None:
    """Replace only the rows for these service dates (or create the table)."""
    writer = df.write.format("delta").mode("overwrite")
    if spark.catalog.tableExists(table):
        writer = writer.option("replaceWhere", f"service_date BETWEEN '{first}' AND '{last}'")
    writer.saveAsTable(table)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build gold.training_set")
    parser.add_argument("--start", default="", help="first service date, YYYY-MM-DD")
    parser.add_argument("--end", default="", help="last service date, YYYY-MM-DD")
    parser.add_argument("--schema-prefix", default="")
    args = parser.parse_args()

    from databricks.sdk.runtime import spark

    p = args.schema_prefix
    arrivals_table = config.table_name("gold", "stop_arrivals", p)
    first, last = date_range(spark, arrivals_table, args.start, args.end)
    history_start = first - timedelta(days=config.HIST_DAYS)  # for the 7-day history feature
    training_set = build_training_set(
        arrivals=between(spark.table(arrivals_table), history_start, last),
        stop_updates=between(
            spark.table(config.table_name("silver", "stop_time_updates", p)), first, last
        ),
        scheduled_stops=spark.table(config.table_name("silver", "gtfs_scheduled_stops", p)),
        alerts=spark.table(config.table_name("silver", "alerts", p)),
    )
    target = config.table_name("gold", "training_set", p)
    write_range(spark, between(training_set, first, last), target, first, last)
    print(f"training set {first} to {last} -> {target}")


if __name__ == "__main__":
    main()
