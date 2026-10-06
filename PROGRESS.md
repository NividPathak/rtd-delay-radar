# Progress

## Current milestone

M2, bronze and silver (branch `m2-bronze-silver`, stacked on `m1-collector`). Verified end to end on one day in the dev target. Waiting on: user OK to deploy the prod target with its 2-hour schedule (that is the M2 "Done when").

Previous:

M1, collector (branch `m1-collector`). Code complete. Waiting on: GitHub secrets for the backup runner, then 48 hours of data with no gaps over 5 minutes.

## Key dates

- **Collector started:** 2026-10-06 08:57 UTC (02:57 Denver).
- **Earliest full retrain:** 2026-10-27 (collector start + 21 days), only if collection has no long gaps. Any model before then is a pipeline test tagged `data_status=preliminary`.
- **M1 "Done when" check:** 48 hours of continuous data, earliest 2026-10-08 09:00 UTC.

## Collector health (last check)

- 2026-10-06 09:19 UTC: continuous since 08:57:37 UTC, no gaps over 5 minutes. Local and volume counts match.
- Laptop: launchd agent `com.rtd-delay-radar.collector` (auto-restart, `caffeinate -w` keeps the Mac from idle sleep). Log at `data/logs/collector.log`.
- Check: `uv run python -m src.collector.health --volume --days 3`.
- Stop: `launchctl bootout gui/$(id -u)/com.rtd-delay-radar.collector`. Start again: `sh ops/install_collector_agent.sh`.
- Backup: `.github/workflows/collector.yml`, inactive until `DATABRICKS_HOST` and `DATABRICKS_TOKEN` secrets are added.

## M1 checklist

- [x] Polls three feeds every 60 seconds, skips duplicates by header timestamp.
- [x] Writes raw `.pb` files under `feed=<name>/date=YYYY-MM-DD/hour=HH/` and uploads to the volume.
- [x] Retries with backoff, logs failures, never crashes on one bad poll.
- [x] Auto-restart on the laptop (launchd), tested by killing the process.
- [x] GitHub Actions backup runner (`collector.yml`).
- [ ] User adds GitHub secrets, backup runner tested with a manual run.
- [x] Unit tests (25 total).
- [ ] 48 hours of continuous data with no gaps over 5 minutes (earliest 2026-10-08 09:00 UTC).
- [x] `docs/architecture.md` and `docs/interview_prep.md` M1 entries.

## M2 checklist

- [x] Bronze: Auto Loader binaryFile, `from_protobuf` decode, availableNow, checkpoints in `rtd.landing.checkpoints`.
- [x] Static GTFS (routes, stops, trips, stop_times, calendar) uploaded by version and loaded as silver materialized views.
- [x] Silver pipeline: dedupe, type casts, schedule join, DST-safe delay, RTD start_date correction.
- [x] Expectations with pass rates (dev run on 2026-10-06): trip_id, delay ±2h, schedule match 100%; date correction 99.93%; vehicle position in Denver area 99.88%.
- [x] Unit tests (52) including a Spark vs Python decoder cross-check on real snapshots.
- [x] `docs/architecture.md` and `docs/interview_prep.md` M2 entries.
- [ ] Deploy prod target and confirm one scheduled run (needs user OK, first prod run reads all history so far).

## First-time setup checklist

- [x] Step 1. Machine check: macOS 26.5 (arm64), git 2.52, Python 3.11, gh 2.91, uv 0.11. Java not installed (needed later for local PySpark tests).
- [x] Step 2. GitHub repo: https://github.com/NividPathak/rtd-delay-radar, `main` pushed.
- [x] Step 3. Databricks CLI v1.19.0 installed via Homebrew.
- [x] Step 4. CLI authenticated with OAuth (profile `DEFAULT`), `databricks current-user me` verified.
- [x] Step 5. User confirmed the workspace is Free Edition (2026-10-06).
- [x] Step 6. User read and accepted the RTD GTFS-Realtime license (2026-10-06).
- [x] Step 7. Python 3.11 venv via `uv`, `pyproject.toml`, ruff and pytest pass.
- [x] Step 8. Setup recorded. M0 started.

## M0 checklist

- [x] Repo on GitHub with CLAUDE.md and the plan.
- [x] Catalog `rtd`, schemas `landing`, `bronze`, `silver`, `gold`, `ml`, volume `rtd.landing.raw`.
- [x] Asset Bundle with `dev` and `prod` targets; `m0_spike_job` deployed to `dev`.
- [x] Spike 1 (local parse) and Spike 2 (outbound from serverless) written up in `docs/decisions.md`.
- [x] Collector built, tested (19 unit tests), and running.
- [x] CI (ruff + pytest) on pull requests.
- [x] `docs/architecture.md` and `docs/interview_prep.md` M0 entries.

## Session log

### 2026-10-06
- Done: first-time setup, all of M0. The collector was built early and is running.
- Findings: RTD feed URLs moved to `open-data.rtd-denver.com`. SQL `CREATE CATALOG` was needed on Free Edition because the REST API needs a storage root. Serverless can reach RTD. One transient `Errno 5` reading bundle files on the first job run.
- Next (M1): GitHub Actions collector as backup runner, auto-restart for the laptop collector (launchd), confirm 48 hours with no gaps over 5 minutes.
- M1: added `--minutes`, volume health check, freshness check, launchd agent, backup workflow. Found and fixed `caffeinate` not holding a sleep assertion.
- Open problems:
  - Closing the lid still sleeps the Mac. The backup runner covers it once secrets are added.
  - Java runtime missing for local Spark tests (decide before M2).
  - Watch for the `Errno 5` workspace file read error in M2. Fallback is packaging `src/` as a wheel.

### 2026-10-06 (later): M2
- Done: bronze and silver code, bundle job and pipeline, verified on one day in dev.
- Problems fixed along the way: Python UDF out of memory (switched to `from_protobuf`), pipeline could not import `src` (added `rtd.code_root` to `sys.path`), duplicate `stop_id` column, RTD start_date one day behind for early trips, RTD static download rejected Python's default User-Agent.
- Quota note: the first failed silver run retried itself for about 12 minutes. Retries are now off (`pipelines.numUpdateRetryAttempts: 0`).
- Size note: one day is about 15 million silver stop update rows. Three weeks is roughly 300 million. Gold (M3) should reduce this to one row per trip and stop.
- Next: user OK for prod deploy, then M3 once M1 is merged.
