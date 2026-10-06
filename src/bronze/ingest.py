"""Bronze ingestion: Auto Loader picks up new raw .pb files and appends parsed rows to Delta.

One bronze table per feed, one row per feed entity. Runs with trigger(availableNow=True):
it processes every file that arrived since the last run, then stops. The checkpoint
remembers which files were already ingested, so each file is processed exactly once.

Usage (on Databricks):
    rtd-bronze --schema-prefix dev_ --date 2026-10-06    # one day, dev tables
    rtd-bronze                                           # all new files, prod tables
"""

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType

from src.bronze.parse import parse_snapshot
from src.common import config
from src.common.schemas import RECORD_SCHEMAS


def parse_binary_files(files: DataFrame, feed_name: str) -> DataFrame:
    """Turn a binaryFile DataFrame (path, content) into one row per feed entity.

    Adds `source_file` (where the row came from) and `ingest_ts` (when bronze saw it).
    """
    record_schema = RECORD_SCHEMAS[feed_name]
    parse_udf = F.udf(
        lambda content: parse_snapshot(feed_name, bytes(content)), ArrayType(record_schema)
    )
    return (
        files.select(F.col("path").alias("source_file"), parse_udf("content").alias("records"))
        .select("source_file", F.explode("records").alias("record"))
        .select("record.*", "source_file")
        .withColumn("ingest_ts", F.current_timestamp())
    )


def source_path(feed_name: str, date: str | None) -> str:
    """Volume folder to watch. A date limits dev runs to one day of files."""
    base = f"{config.RAW_VOLUME_PATH}/feed={feed_name}"
    return f"{base}/date={date}/" if date else f"{base}/"


def ingest_feed(spark: SparkSession, feed_name: str, schema_prefix: str, date: str | None) -> None:
    """Run one availableNow Auto Loader stream for one feed and wait for it to finish."""
    files = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("pathGlobFilter", "*.pb")
        .load(source_path(feed_name, date))
    )
    target = config.table_name("bronze", feed_name, schema_prefix)
    checkpoint = f"{config.CHECKPOINT_VOLUME_PATH}/{schema_prefix}bronze_{feed_name}"
    query = (
        parse_binary_files(files, feed_name)
        .writeStream.option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(target)
    )
    query.awaitTermination()
    print(f"bronze {feed_name}: done -> {target}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest raw RTD files into bronze tables")
    parser.add_argument("--schema-prefix", default="", help='"dev_" for the dev target')
    parser.add_argument("--date", default=None, help="only this UTC date, YYYY-MM-DD")
    args = parser.parse_args()

    from databricks.sdk.runtime import spark

    for feed_name in config.FEEDS:
        ingest_feed(spark, feed_name, args.schema_prefix, args.date)


if __name__ == "__main__":
    main()
