# RTD Delay Radar

Real-time transit delay prediction for Denver RTD on Databricks Free Edition and Apache Spark.

A streaming lakehouse that ingests live RTD GTFS-Realtime feeds, builds bronze, silver, and gold Delta tables, and trains a model that predicts stop-level arrival delays.

**Status:** M0 complete. M1 (collector) code complete, waiting on 48 hours of continuous data. The collector has been running since 2026-10-06. See [PROGRESS.md](PROGRESS.md) and [PROJECT_PLAN.md](PROJECT_PLAN.md).

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
uv run python -m src.collector.run --once        # one poll cycle
sh ops/install_collector_agent.sh                # laptop: launchd keeps it running and awake
uv run python -m src.collector.health --volume   # date range and gaps over 5 minutes, from the volume
```

Backup runner: `.github/workflows/collector.yml` checks the volume every 15 minutes and collects for 55 minutes when the laptop stops delivering. It needs repo secrets `DATABRICKS_HOST` and `DATABRICKS_TOKEN`.

Tests and lint:

```bash
uv run ruff check && uv run pytest
```

## Data

RTD GTFS-Realtime feeds, used under RTD's GTFS-Realtime license agreement. Raw data is not committed to this repo.

## Limitations

- Runs on Databricks Free Edition: serverless compute only, with a daily usage quota.
- Streaming runs in scheduled bursts using `trigger(availableNow=True)`, not as an always-on stream.
- Feed polling runs outside Databricks (laptop, with a GitHub Actions backup) and uploads raw files to a Unity Catalog volume. Collection gaps are measured and documented, not hidden.

## Results

Pending full data.
