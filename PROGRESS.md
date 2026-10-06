# Progress

## Current milestone

First-time setup (blocked on user confirmations). Next: M0, setup and spikes.

## First-time setup checklist

- [x] Step 1. Machine check: macOS 26.5 (arm64), git 2.52, Python 3.11, gh 2.91, uv 0.11. Java not installed (needed later for local PySpark tests).
- [x] Step 2. GitHub repo: https://github.com/NividPathak/rtd-delay-radar, `main` pushed.
- [x] Step 3. Databricks CLI v1.19.0 installed via Homebrew.
- [x] Step 4. CLI authenticated with OAuth (profile `DEFAULT`), `databricks current-user me` verified.
- [ ] Step 5. Confirm the workspace is Free Edition, not a trial. Waiting on user.
- [ ] Step 6. Accept RTD GTFS-Realtime license. Waiting on user. No feed fetched yet.
- [x] Step 7. Python 3.11 venv via `uv`, `pyproject.toml`, ruff and pytest pass.
- [ ] Step 8. Record and start M0.

## Session log

### 2026-10-06
- Done: setup steps 1 to 4 and 7. Repo skeleton matches the plan's layout.
- Next: user confirms Free Edition and RTD license, then start M0 with the collector first.
- Open problems: Java runtime missing for local Spark tests (decide before M2).
