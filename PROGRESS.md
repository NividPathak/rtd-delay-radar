# Progress

## Current milestone

M0 complete (branch `m0-setup`, PR open). Next: M1, collector hardening and 48 hours of continuous data.

## Key dates

- **Collector started:** 2026-10-06 08:57 UTC (02:57 Denver).
- **Earliest full retrain:** 2026-10-27 (collector start + 21 days), only if collection has no long gaps. Any model before then is a pipeline test tagged `data_status=preliminary`.
- **M1 "Done when" check:** 48 hours of continuous data, earliest 2026-10-08 09:00 UTC.

## Collector health (last check)

- 2026-10-06 09:02 UTC: 6 snapshots per feed, 08:57:37 to 09:02:08 UTC, no gaps over 5 minutes, no errors in the log.
- Runs as a background process: `caffeinate -i .venv/bin/python -m src.collector.run`, log at `data/logs/collector.log`.
- Check: `uv run python -m src.collector.health`. Stop: `pkill -f src.collector.run`.

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
- Open problems:
  - Laptop collector stops if the lid closes or the Mac restarts. Needs launchd or the GitHub Actions backup soon.
  - Java runtime missing for local Spark tests (decide before M2).
  - Watch for the `Errno 5` workspace file read error in M2. Fallback is packaging `src/` as a wheel.
