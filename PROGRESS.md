# Progress

## Current milestone

M0, setup and spikes (branch `m0-setup`). Collector is built first.

## First-time setup checklist

- [x] Step 1. Machine check: macOS 26.5 (arm64), git 2.52, Python 3.11, gh 2.91, uv 0.11. Java not installed (needed later for local PySpark tests).
- [x] Step 2. GitHub repo: https://github.com/NividPathak/rtd-delay-radar, `main` pushed.
- [x] Step 3. Databricks CLI v1.19.0 installed via Homebrew.
- [x] Step 4. CLI authenticated with OAuth (profile `DEFAULT`), `databricks current-user me` verified.
- [x] Step 5. User confirmed the workspace is Free Edition (2026-10-06).
- [x] Step 6. User read and accepted the RTD GTFS-Realtime license (2026-10-06).
- [x] Step 7. Python 3.11 venv via `uv`, `pyproject.toml`, ruff and pytest pass.
- [x] Step 8. Setup recorded. M0 started.

## Session log

### 2026-10-06
- Done: setup steps 1 to 4 and 7. Repo skeleton matches the plan's layout.
- User confirmed Free Edition and accepted the RTD license.
- Next: M0 with the collector first.
- Open problems: Java runtime missing for local Spark tests (decide before M2).
