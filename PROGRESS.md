# Progress

## Current milestone

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
