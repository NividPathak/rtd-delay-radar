# Progress

## Current milestone

M6, CI/CD and write-up (branch `m6-polish`). M1 to M5 merged to `main` through PRs #2 to #6 on 2026-10-08, each after CI passed. M1 was merged before its 48-hour condition so the backup runner could run from `main` (see `docs/decisions.md`).

M6 remaining:
- [x] `deploy.yml`: deploy prod on push to `main` after lint and tests.
- [x] `ci.yml`: bundle validate on pull requests (when secrets exist).
- [x] README rewrite, `docs/results.md` findings and paid-compute section.
- [ ] GitHub secrets (user) so the backup collector and deploy workflow run.
- [ ] Dashboard screenshots and a demo GIF (needs Chrome signed in to the workspace).
- [ ] Pin the repo on the GitHub profile (user).
- [ ] Final results after the 2026-10-27 retrain.

Previous:

M5, scoring and dashboard (branch `m5-scoring`, stacked on `m4-model`). Deployed to prod. "Done when" (dashboard updates on its own after a scheduled run) is waiting for the next scheduled prod run, plus a visual check of the dashboard in the workspace.

- Prod dashboard: https://dbc-5b0fa231-e20d.cloud.databricks.com/dashboardsv3/01f1c1d682841d109d9a2ea5d7029325/published?w=7474646683731620

Previous:

M4, features and model (branch `m4-model`, stacked on `m3-gold`). Code complete and pipeline-tested in dev. **No results until the full retrain.** "Done when" (results table on a held-out week vs both baselines) needs 3 weeks of data.

Previous:

M3, gold (branch `m3-gold`, stacked on `m2-bronze-silver` and `m1-collector`). Code done and verified in dev. Prod is deployed with a 2-hour schedule (first run succeeded in 5.8 min). Prod gold is waiting on the next scheduled run after a Free Edition `RESOURCE_EXHAUSTED` error.

Previous:

M1, collector (branch `m1-collector`). Code complete. Waiting on: GitHub secrets for the backup runner, then 48 hours of data with no gaps over 5 minutes.

## Key dates

- **Full retrain command (run on or after 2026-10-27):**
  `databricks bundle run -t prod rtd_train --params start=2026-10-06,end=2026-10-27`

- **Collector started:** 2026-10-06 08:57 UTC (02:57 Denver).
- **Earliest full retrain:** 2026-10-27 (collector start + 21 days), only if collection has no long gaps. Any model before then is a pipeline test tagged `data_status=preliminary`.
- **M1 "Done when" check:** 48 hours of continuous data, earliest 2026-10-08 09:00 UTC.

## Collector health (last check)

- 2026-10-08 06:00 UTC: 45 hours collected since 2026-10-06 08:57 UTC, 21 gaps over 5 minutes, 12.2 hours missing (uptime 72.8%). On 2026-10-07: 17 gaps between 01:29 and 18:39 UTC, about 9 hours, all laptop sleep. No gaps from 18:39 UTC to 06:00 UTC on 2026-10-08.
- M5 check: every scheduled prod run since 2026-10-06 23:49 UTC appended predictions and refreshed live vehicles (16 batches by 05:48 UTC on 2026-10-08).

Earlier:

- 2026-10-06 21:26 UTC: 566 snapshots per feed since 08:57:37 UTC. **4 gaps over 5 minutes, about 3 hours in total.** Cause: the Mac slept with the lid closed (`pmset -g log` shows "Clamshell" sleep at 13:25 UTC). The collector itself never crashed. The backup runner was not active yet (no GitHub secrets).

| Gap (UTC, 2026-10-06) | Length | Cause |
|---|---|---|
| 13:24 to 14:12 | 48 min | lid closed, Mac asleep |
| 14:12 to 14:30 | 18 min | Mac asleep (dark wakes only) |
| 14:32 to 15:26 | 54 min | Mac asleep |
| 19:40 to 20:44 | 64 min | Mac asleep |

- The M1 "48 hours with no gap over 5 minutes" clock restarts after the backup runner is active.
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

## M5 checklist

- [x] `rtd-score` task after silver/gold in `rtd_refresh`: predictions for active trips (persistence, RTD, champion model when it exists).
- [x] `gold.prediction_monitoring`: daily MAE/RMSE per predictor, mode, horizon against observed arrivals.
- [x] `gold.live_vehicles` for the map.
- [x] AI/BI dashboard in the bundle: live delay map, worst routes now, prediction vs actual, error over time, error by horizon.
- [x] Dev: score task succeeded (baselines only, no champion). All 6 dashboard queries succeed on dev gold.
- [x] Prod deployed (rtd_refresh now has bronze, silver, score).
- [ ] Visual check of the dashboard (needs the user signed in to the workspace in the browser).
- [ ] Confirm a scheduled prod run refreshes the dashboard tables on its own.
- [ ] Model predictions on the dashboard (after the champion exists, 2026-10-27 earliest).

## M4 checklist

- [x] `gold.training_set` builder: anchor stop to K stops ahead (1, 5, 10, 20), features known at `t0`, both baselines.
- [x] Leakage check for every feature in `docs/decisions.md`, with unit tests for the as-of joins.
- [x] Time split by service date, MAE/RMSE by mode and horizon, preliminary flag (< 21 days or < 7 held-out days).
- [x] Linear and GBT (residual over persistence), MLflow logging with date range and git commit, UC registration without champion for preliminary runs.
- [x] `rtd_train` job (features then train), one command with a date range.
- [x] `notebooks/error_analysis.py` and `gold.test_predictions`.
- [x] Pipeline test in dev (2026-10-06 only): baselines logged as preliminary, models skipped (no training days). Feature coverage checked; `alert_active` narrowed to route-wide alerts.
- [ ] Full retrain on 3 weeks with a held-out week (2026-10-27 earliest).
- [ ] Champion alias only after that run.

## M3 checklist

- [x] `gold.stop_arrivals`: one row per trip and stop, observed arrival from the last prediction, `is_observed` when made at most 2 minutes before arrival.
- [x] `gold.route_delay_hourly`: observed delay by route, date, local hour, weekday flag.
- [x] `notebooks/eda_delays.py`: label quality, distributions, time of day, worst routes, the 5 pm query, delay along a trip.
- [x] Verified in dev on 2026-10-06: 96% to 98% observed labels in hours with continuous collection.
- [x] `docs/architecture.md` and `docs/interview_prep.md` M3 entries.
- [ ] Prod gold built. The first prod gold update failed with Free Edition `RESOURCE_EXHAUSTED` (too much serverless compute running at once). Not retried. The next scheduled run (every 2 h) will build it.
- [ ] "Done when": answer the 5 pm weekday question with one query (section 5 of the notebook). Needs observed weekday data at 17:00 Denver, so it needs a full day with no collection gaps around 5 pm.

## M2 checklist

- [x] Bronze: Auto Loader binaryFile, `from_protobuf` decode, availableNow, checkpoints in `rtd.landing.checkpoints`.
- [x] Static GTFS (routes, stops, trips, stop_times, calendar) uploaded by version and loaded as silver materialized views.
- [x] Silver pipeline: dedupe, type casts, schedule join, DST-safe delay, RTD start_date correction.
- [x] Expectations with pass rates (dev run on 2026-10-06): trip_id, delay ±2h, schedule match 100%; date correction 99.93%; vehicle position in Denver area 99.88%.
- [x] Unit tests (52) including a Spark vs Python decoder cross-check on real snapshots.
- [x] `docs/architecture.md` and `docs/interview_prep.md` M2 entries.
- [x] Prod target deployed (user OK 2026-10-06). First run: bronze 172 s, silver 171 s, success. Schedule every 2 hours, unpaused.

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

### 2026-10-06 (evening): M3
- Done: gold transforms with tests, gold views in the pipeline, EDA notebook, docs. Dev gold verified.
- Prod: first scheduled-target run succeeded (5.8 min). The prod gold update then failed with `RESOURCE_EXHAUSTED` because the SQL warehouse was still running. Warehouse stopped, nothing retried.
- Next: confirm the next scheduled prod run builds gold; answer the 5 pm query once a gap-free weekday afternoon is collected; GitHub secrets for the backup collector (still the top priority for data quality).

### 2026-10-06 (night): M4
- Done: training set builder with as-of joins, evaluation, models, training job, error analysis notebook, M4 docs. Pipeline test in dev passed end to end.
- Found: `alert_active` was 1 for 83% of examples because of stop-level construction alerts. Narrowed to route-wide alerts.
- Corrected: the evening `RESOURCE_EXHAUSTED` was my manual run colliding with the first scheduled prod run, which succeeded and built prod gold.
- Next: daily pipeline tests as days accumulate (first model training possible once there are 2+ service dates); GitHub secrets for the backup collector; full retrain on 2026-10-27.

### 2026-10-06 (late night): M5
- Done: live scoring with shared feature code, monitoring, live vehicles, dashboard JSON and bundle resource, M5 docs. Dev score run succeeded; prod deployed.
- Next: check the dashboard visually; confirm the next scheduled prod run updates it; GitHub secrets for the backup collector (still open).

### 2026-10-08: merges and M6
- Merged M1 to M5 (PRs #2 to #6) after CI. Deleted the feature branches.
- Added deploy and bundle-validate workflows, rewrote the README, added findings to `docs/results.md`, M6 docs.
- Open: GitHub secrets, dashboard screenshots/GIF, pinning the repo, 48-hour gap-free check, full retrain 2026-10-27.
