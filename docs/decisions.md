# Decisions

Each entry: the options, the choice, the reason.

## 2026-10-06: RTD feed URLs moved

- **Options:** URLs in PROJECT_PLAN.md (`www.rtd-denver.com/files/gtfs-rt/...`) or the URLs on RTD's current GTFS-RT page.
- **Choice:** `https://open-data.rtd-denver.com/files/gtfs-rt/rtd/{TripUpdate,VehiclePosition,Alerts}.pb`, kept only in `src/common/config.py`.
- **Reason:** RTD moved its feeds in Fall 2025. The plan's URLs are out of date.

## 2026-10-06: Spike 1, local download and parse

- **Result:** All three feeds download and parse with `gtfs-realtime-bindings`. GTFS-RT version 2.0. Header timestamps were 5 to 6 seconds old when fetched.
- **Late-evening sample sizes (about 22:54 Denver time):** trip updates 19 KB, 19 entities. Vehicle positions 6 KB, 95 entities. Alerts 72 KB, 129 entities.
- **Findings that affect later milestones:**
  - Trip updates give absolute predicted `arrival.time` and `departure.time`, not a `delay` field. Delay must be computed against the static schedule (M2/M3).
  - Some stop time updates have `schedule_relationship: SKIPPED`. Silver must handle them.
  - Many vehicle positions have no `trip_id` (vehicles not in service). Silver should filter or flag them.
- These snapshots are saved as offline test fixtures in `tests/fixtures/` (104 KB total).

## 2026-10-06: Collector runs before the rest of M0

- **Options:** Finish M0 in plan order (bundle, spike 2) first, or build and start the collector first.
- **Choice:** Collector first.
- **Reason:** History only exists if the collector is running. CLAUDE.md asks for it as early as possible. The rest of M0 does not depend on it.

## 2026-10-06: Partition raw files by UTC, not Denver time

- **Options:** `date=`/`hour=` folders in UTC or in America/Denver local time.
- **Choice:** UTC, taken from the feed header timestamp.
- **Reason:** UTC has no daylight saving jumps, so no duplicate or missing hour folders in March and November. Local time features (hour of day, weekday) are computed later in silver.

## 2026-10-06: Duplicate detection uses the header timestamp

- **Choice:** Skip a snapshot when its `header.timestamp` matches the last one saved for that feed.
- **Reason:** RTD regenerates feeds about every 30 seconds. Polling every 60 seconds can still see the same snapshot twice if the feed stalls, and storing it twice adds nothing.

## 2026-10-06: Use the standard library for HTTP

- **Options:** `requests` or `urllib.request`.
- **Choice:** `urllib.request`.
- **Reason:** One GET per feed does not need an extra dependency, and `requests` is not in the approved tech stack.

## 2026-10-06: Creating the `rtd` catalog on Free Edition

- **Problem:** The catalogs REST API (`databricks catalogs create`) failed with "Metastore storage root URL does not exist. Default Storage is enabled in your account."
- **Options:** Ask the user to create the catalog in the UI, run SQL `CREATE CATALOG` on a SQL warehouse, or use the built-in `workspace` catalog.
- **Choice:** SQL `CREATE CATALOG IF NOT EXISTS rtd` on the Serverless Starter Warehouse, in `src/common/setup_catalog.py`.
- **Reason:** Databricks docs say serverless workspaces create catalogs on default storage through SQL or the UI, not through the REST API. SQL keeps setup in code and keeps the `rtd` naming from the plan. Schemas and the volume are created through the SDK, which works on default storage.
- **Why not in the Asset Bundle:** Bundle-managed schemas get destroyed with `bundle destroy` and get renamed in `dev` mode. The raw volume holds data that cannot be re-downloaded, so it is created once by this script and is not managed by the bundle.

## 2026-10-06: Spike 2, outbound access from serverless compute

- **Test:** `m0_spike_job` (deployed by the bundle) runs `notebooks/spike_outbound_access.py` on serverless compute and fetches each feed once.
- **Result:** Outbound access works on this Free Edition workspace. All three feeds returned HTTP 200 (run on 2026-10-06 at 03:01 Denver time).
- **First attempt:** Failed before any network call with `OSError: [Errno 5] Input/output error` while importing `src/common/config.py` from the synced bundle files. The file was uploaded correctly (checked with `databricks workspace export`). The unchanged rerun passed, so this looks like a transient workspace file read error right after deploy. Watch for it in M2. If it comes back, package `src/` as a wheel the way the official bundle template does.

## 2026-10-06: Keep the collector outside Databricks even though outbound works

- **Options:** Poll RTD from a Databricks job, or from an external Python process (laptop, with GitHub Actions as backup).
- **Choice:** External collector.
- **Reason:** Polling must happen every 60 seconds. Every job run on serverless compute pays startup time and uses the daily quota, and CLAUDE.md caps job schedules at once per hour. A tiny external process polls for free and only uploads files. Databricks then processes the files in hourly `availableNow` bursts. Spike 2 shows a Databricks fallback is possible if the laptop collector has long outages, but it would be expensive on quota.

## 2026-10-06: Laptop collector runs under `caffeinate`

- **Choice:** For now the collector runs as a background process wrapped in `caffeinate -i`, logging to `data/logs/collector.log`.
- **Limits:** `caffeinate -i` stops idle sleep but not sleep from closing the lid. A restart or crash stops collection until it is started again. Gaps are reported by `python -m src.collector.health` and are not hidden.
- **Next:** M1 adds the GitHub Actions runner as a backup and could add a launchd agent so the collector restarts on its own.

## 2026-10-06: `caffeinate -i <command>` did not keep the Mac awake

- **Problem:** Running `caffeinate -i python -m src.collector.run` left a Python process with the same PID and no sleep assertion. `pmset -g assertions` showed nothing for the collector. So the M0 laptop collector was never actually protected from idle sleep.
- **Fix:** Launch through `/bin/sh -c 'caffeinate -i -w $$ & exec python -m src.collector.run'`. `caffeinate -w <pid>` holds the assertion until that PID exits, and `exec` turns the shell into the collector with the same PID. Verified with `pmset -g assertions`: "caffeinate asserting on behalf of Process ID <collector pid>".
- **Still true:** Closing the lid sleeps the Mac anyway. The backup runner covers that.

## 2026-10-06: launchd agent for the laptop collector

- **Options:** A `nohup` background process, a launchd agent, or a cron job.
- **Choice:** launchd agent (`ops/rtd-collector.plist.template`, installed by `ops/install_collector_agent.sh`).
- **Reason:** launchd is the macOS service manager. `KeepAlive` restarts the collector within 30 seconds if it exits, and `RunAtLoad` starts it again at login after a reboot. Tested: killing the process led to a new PID and polling resumed.

## 2026-10-06: GitHub Actions runner as a backup, not a second always-on collector

- **Options:** (a) GitHub Actions collects all the time alongside the laptop. (b) GitHub Actions collects only when the laptop is not delivering. (c) No backup.
- **Choice:** (b). Every 15 minutes the workflow runs `src/collector/freshness.py`. If the newest snapshot in the volume is under 5 minutes old, it exits. If not, it collects for 55 minutes.
- **Reason:** A laptop sleeps when the lid closes. (a) would double every upload for no gain and spend about 24 runner-hours a day. (b) costs about 30 seconds per check while the laptop is healthy. The `concurrency` group stops two backup runs from overlapping.
- **Known limits:** GitHub can delay scheduled runs by several minutes or more under load, so a laptop outage can still leave a gap of about 15 to 30 minutes before the backup starts. Gaps are measured with `python -m src.collector.health --volume` and documented.
- **Auth:** The runner needs a Databricks personal access token in the repo secret `DATABRICKS_TOKEN` (plus `DATABRICKS_HOST`). The token is created by the user and never written to the repo. A service principal with OAuth would be cleaner. Revisit in M6 if Free Edition allows it.
