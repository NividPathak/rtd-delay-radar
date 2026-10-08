import os
from pathlib import Path

import pytest

HOMEBREW_JAVA_17 = Path("/opt/homebrew/opt/openjdk@17")


@pytest.fixture(scope="session")
def spark():
    """A small local Spark session shared by all tests."""
    if "JAVA_HOME" not in os.environ and HOMEBREW_JAVA_17.exists():
        os.environ["JAVA_HOME"] = str(HOMEBREW_JAVA_17)
    from pyspark import __version__ as pyspark_version
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("rtd-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        # from_protobuf is in a separate Spark module locally; Databricks includes it.
        .config("spark.jars.packages", f"org.apache.spark:spark-protobuf_2.13:{pyspark_version}")
        .getOrCreate()
    )
    yield session
    session.stop()
