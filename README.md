# RTD Delay Radar

Real-time transit delay prediction for Denver RTD on Databricks Free Edition and Apache Spark.

A streaming lakehouse that ingests live RTD GTFS-Realtime feeds, builds bronze, silver, and gold Delta tables, and trains a model that predicts stop-level arrival delays.

**Status:** Milestone 0 (setup) in progress. See [PROGRESS.md](PROGRESS.md) and [PROJECT_PLAN.md](PROJECT_PLAN.md).

## Limitations

- Runs on Databricks Free Edition: serverless compute only, with a daily usage quota.
- Streaming runs in scheduled bursts using `trigger(availableNow=True)`, not as an always-on stream.
- Feed polling runs outside Databricks (laptop or GitHub Actions) and uploads raw files to a Unity Catalog volume.

## Results

Pending full data.
