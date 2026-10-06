# RTD Delay Radar

Real-time transit delay prediction for Denver RTD on Databricks Free Edition and Apache Spark.

A streaming lakehouse that ingests live RTD GTFS-Realtime feeds, builds bronze, silver, and gold Delta tables, and trains a model that predicts stop-level arrival delays.

**Status:** M0 (setup and spikes) complete. The collector has been running since 2026-10-06. See [PROGRESS.md](PROGRESS.md) and [PROJECT_PLAN.md](PROJECT_PLAN.md).

## Architecture

```
RTD GTFS-RT feeds --> collector (laptop, every 60 s) --> /Volumes/rtd/landing/raw/
                                                              |
                         bronze --> silver --> gold --> model  (Databricks, hourly bursts; coming in M2+)
```

Details and reasoning: [docs/architecture.md](docs/architecture.md). Design choices: [docs/decisions.md](docs/decisions.md).

## Setup

Requires Python 3.11, [uv](https://docs.astral.sh/uv/), and the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html).

```bash
uv sync                                    # install dependencies
databricks auth login --host <workspace>   # browser OAuth, no tokens in files
uv run python -m src.common.setup_catalog  # create catalog rtd, schemas, raw volume (once)
databricks bundle deploy                   # deploy jobs to the dev target
```

Run the collector:

```bash
uv run python -m src.collector.run --once  # one poll cycle
nohup caffeinate -i .venv/bin/python -m src.collector.run >> data/logs/collector.log 2>&1 &
uv run python -m src.collector.health      # date range and gaps over 5 minutes
```

Tests and lint:

```bash
uv run ruff check && uv run pytest
```

## Data

RTD GTFS-Realtime feeds, used under RTD's GTFS-Realtime license agreement. Raw data is not committed to this repo.

## Limitations

- Runs on Databricks Free Edition: serverless compute only, with a daily usage quota.
- Streaming runs in scheduled bursts using `trigger(availableNow=True)`, not as an always-on stream.
- Feed polling runs outside Databricks (laptop, with GitHub Actions planned as backup) and uploads raw files to a Unity Catalog volume. Collection gaps are measured and documented, not hidden.

## Results

Pending full data.
