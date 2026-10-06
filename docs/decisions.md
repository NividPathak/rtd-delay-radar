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

## 2026-10-06: Static GTFS source and versioning

- **Source:** `https://www.rtd-denver.com/files/gtfs/google_transit.zip` (RTD's GTFS page links the same license agreement already accepted for GTFS-Realtime).
- **Choice:** `src/collector/static_gtfs.py` keeps 7 of the 9 files (not `shapes.txt` or `agency.txt`) and uploads them to `raw/static/version=<feed_version>/`. A version already in the volume is skipped. Silver uses the version with the newest `feed_start_date`.
- **Reason:** RTD changes schedules about three times a year. Keeping each version means a later model can join each day to the schedule that was in effect, instead of silently using today's. For now silver uses only the latest version. Revisit when the first schedule change happens during collection.
- **User-Agent:** RTD's download server returned 403 to Python's default `Python-urllib` User-Agent. All requests now send `rtd-delay-radar/0.1 (+repo URL)`, which honestly identifies the project.

## 2026-10-06: Dev target writes to separate schemas

- **Options:** Dev and prod share the bronze/silver tables (dev just filters by date), or dev writes to its own schemas.
- **Choice:** The bundle variable `schema_prefix` is `dev_` on the dev target, so dev writes to `rtd.dev_bronze`, `rtd.dev_silver`, `rtd.dev_gold` with its own checkpoints. Dev also sets `dev_date` so Auto Loader only reads one day of files.
- **Reason:** A dev run with a date filter would otherwise mark files as processed in the shared checkpoint, and prod would never pick up the rest. Separate schemas keep experiments away from the real tables.

## 2026-10-06: Bronze decodes protobuf with Spark `from_protobuf`, not a Python UDF

- **What happened:** The first dev run failed with `[UDF_PYSPARK_ERROR.OOM] Python worker exited unexpectedly (crashed) due to running out of memory`. A daytime TripUpdate snapshot is about 0.6 MB with 741 trips and 21,064 stop updates. Parsed into Python dicts it takes about 9 MB, and Spark sends rows to a Python UDF in batches, so one batch needed close to 1 GB in the serverless Python sandbox. The overnight fixtures were too small to show this.
- **Options:** (a) Keep the UDF and shrink batches through Spark configs (serverless only allows a short list of configs). (b) A Python UDTF that yields rows one at a time. (c) Spark's built-in `from_protobuf`, which decodes in the JVM.
- **Choice:** (c). The schema comes from the `gtfs-realtime-bindings` package as a serialized `FileDescriptorSet` (`binaryDescriptorSet`), so no `.proto` or `.desc` file has to be maintained.
- **Checked:** On a 0.6 MB daytime snapshot both decoders give 741 trips and 21,064 stop updates. A unit test compares every field of the Spark output with the pure-Python parser on all three fixtures. The only differences were two unset enums, where `from_protobuf` fills in the GTFS-RT spec default (`SCHEDULED`, `IN_TRANSIT_TO`). The Python parser now applies the same defaults and stays as a test oracle.
- **Local tests** load the `spark-protobuf` jar through `spark.jars.packages`. It is part of Apache Spark and is built into Databricks.

## 2026-10-06: RTD TripUpdate `start_date` is a day behind for early-morning trips

- **Found:** In the 02:54 Denver fixture, every trip update says `start_date=20261005`, but the trips run on Oct 6. The static schedule starts trip 116037952 at `02:55:00` (service day Oct 6), and the vehicle feed reports `start_date=20261006` for the same trips. Using RTD's date made every delay exactly 24 hours too large.
- **Options:** Drop these rows, trust the vehicle feed's date (only some trips have vehicles), or correct the date from the data.
- **Choice:** `service_date_shift_days = round(raw_delay / 86400)`, limited to -1, 0, or +1. Real delays are within hours, while a wrong date shifts the delay by about 24 hours, so rounding to whole days finds the error without touching real delays. The shift is kept as a column, and the pipeline expectation `start_date_not_shifted` reports how often RTD's date was wrong.
- **Leakage check:** The correction uses only the current prediction and the static schedule, both known at prediction time.

## 2026-10-06: Scheduled times use GTFS "noon minus 12 hours"

- **Choice:** Scheduled time = (noon local time on the service date, minus 12 hours) + the `HH:MM:SS` offset from `stop_times.txt`.
- **Reason:** The GTFS spec defines times this way so they stay correct on daylight saving days. Midnight would put every scheduled time on 2026-11-01 (DST ends) one hour off. A unit test covers that date.

## 2026-10-06: Silver table types

- **`stop_time_updates`, `vehicle_positions`:** streaming tables. They read bronze incrementally and deduplicate with a 2-hour watermark on the event time (`feed_ts`, `vehicle_ts`). The event time is part of the dedupe key, so Spark can discard old dedupe state.
- **`alerts`:** materialized view. An alert repeats in every snapshot, so silver keeps one row per alert and affected route/stop with `first_seen_ts` and `last_seen_ts`. That needs a group-by over all history, which a materialized view refreshes incrementally.
- **Static GTFS (`gtfs_*`):** materialized views over the CSV files, latest version only.
- **Expectations:** drop rows with a null `trip_id`, a delay outside ±2 hours, a null `vehicle_id`, or a position outside the Denver area. Warn only (keep the row, count failures) for `matched_schedule` and `start_date_not_shifted`, so pass rates show up in the pipeline's event log.

## 2026-10-06: Refresh every 2 hours

- **Options:** Every hour or every 2 hours (the plan allows 1 to 2).
- **Choice:** Every 2 hours in prod, until one run's quota cost is measured. The dev target's schedule is paused by development mode.

## 2026-10-06: How an "actual arrival" is defined

- **Problem:** RTD publishes predictions, not observed arrival times. The plan offers two label sources: the last TripUpdate before arrival, or a vehicle position crossing the stop.
- **Options:** (a) The last prediction for the stop before RTD drops it from the feed (RTD removes a stop once the vehicle passes it). (b) Match vehicle positions to stop coordinates and detect when the vehicle passes.
- **Choice:** (a), with a reliability rule. `label_lead_s` = observed arrival minus the time of that last snapshot. The label counts (`is_observed`) only when the last prediction was made at most 120 seconds before the arrival.
- **Reason:** Only 7 of 95 vehicles in the first sample carried a trip id, so (b) would label very few stops. A prediction made a minute before arrival is very close to the true time, and the 120-second rule rejects labels where collection stopped early (gaps) or the trip was still running.
- **Check (dev, 2026-10-06):** 96% to 98% of trip-stops are observed in hours with continuous collection. It drops to 10% to 42% in hours with collection gaps, and to 0% for trips still running at refresh time. So label coverage depends on collector uptime.
- **Limitation:** The label is still RTD's estimate, made at most 2 minutes out. Comparing the model to "RTD's own prediction" at short horizons has to keep this in mind, because at very short horizons RTD's prediction and the label are nearly the same thing.

## 2026-10-06: Gold tables are materialized views in the same pipeline

- **Choice:** `gold.stop_arrivals` and `gold.route_delay_hourly` are materialized views published from the silver pipeline to the gold schema (`rtd.gold_schema` setting).
- **Reason:** A trip-stop's label changes until the vehicle passes the stop, so gold has to be recomputed from silver, not appended once. A materialized view handles that, and Databricks refreshes it incrementally when it can. Keeping it in the same pipeline makes the silver-to-gold dependency explicit and refreshes both in one update.
- **Cost watch:** Silver grows by about 15 million rows a day. If the gold refresh gets slow or expensive as history grows, limit the recompute to recent service dates and freeze older ones.
- **Pipeline name:** The display name is now `rtd_silver_gold`. The bundle resource key stays `rtd_silver`, because changing the key would delete and recreate the pipeline and its tables.

## 2026-10-06: Free Edition limit on running serverless compute at the same time

- **What happened:** The prod pipeline update failed at startup with `RESOURCE_EXHAUSTED: You've hit the limit for serverless compute for free usage. Stop or delete existing serverless compute to free up capacity.` At that moment the SQL warehouse (used for label checks) was still running, and a dev pipeline update had just finished. Nothing was retried, per the quota rule.
- **Reading:** Free Edition caps how much serverless compute can run at once, separate from the daily quota. Running a dev pipeline, a prod job, and the SQL warehouse close together hits it.
- **Correction (later the same evening):** The job history shows the first *scheduled* prod run started at 21:48 UTC, the same minute as my manual prod pipeline update. So the collision was mainly my manual run against the scheduler. That scheduled run succeeded and built prod gold.
- **Rules from now on:** Run one Databricks workload at a time. Stop the SQL warehouse right after ad hoc queries (`databricks warehouses stop`). Never start dev runs while a prod run is in progress. If the scheduled job keeps hitting this limit, move it from every 2 hours to every 3 hours and record that here.

## 2026-10-06: The prediction problem, exactly

- **Example:** a vehicle has just reached stop A on trip T. The prediction time `t0` is when that arrival became known in our data: the later of the observed arrival and the last snapshot that still listed stop A.
- **Label:** the observed delay at stop A+K (K scheduled stops later on the same trip), for K in {1, 5, 10, 20}. Only targets whose arrival is after `t0` are kept.
- **Model target:** the change in delay from A to A+K (the residual over persistence). The prediction is current delay + predicted change, so a model that learns nothing falls back to persistence.

## 2026-10-06: Leakage check for every feature

Each feature must be computable at `t0` from data that existed at `t0`. Unit tests in `tests/unit/test_training_set.py` cover the starred items with cases where future data is present and must be ignored.

| Feature | Source | Why it does not leak |
|---|---|---|
| `current_delay_s` | delay at anchor stop A | A's arrival defines `t0`. It is known by definition. |
| `delay_trend_s` | delay at A minus delay 3 observed stops earlier | Earlier stops were reached before A. |
| route, direction, mode, `anchor_stop_sequence`, `horizon` | trip and schedule | Fixed before the trip starts. |
| `stops_remaining`, `scheduled_gap_s` | static schedule | Published in advance. |
| `hour_local`, `day_of_week`, `is_weekend`, `is_holiday` | clock time at `t0` | Calendar facts. |
| `hist_avg_delay_s`, `hist_n` * | observed delays for the target's route, stop, and hour over the 7 previous service dates | The current service date is excluded, so no same-day arrivals (some of which are after `t0`) are used. Test: today's value is ignored. |
| `ahead_delay_s`, `ahead_age_s` * | most recent other vehicle on the same route and direction at the target stop | As-of join on `known_ts < t0`. Ties sort the example first, so an arrival known at exactly `t0` is not used. Test: a later vehicle's delay is ignored. |
| `alert_active` * | silver.alerts | Alert must have been in the feed at `t0` (`first_seen_ts <= t0 <= last_seen_ts`). Test: an alert first seen after `t0` does not count. |
| `rtd_pred_s` (baseline, not a feature) * | RTD's prediction for stop A+K | Latest snapshot with `feed_ts <= t0`. Test: a snapshot after `t0` is ignored. |

- **Not used as features:** anything from the target row other than the label, any snapshot after `t0`, and the target's observed arrival.
- **Split:** by service date. Train on earlier dates, test on the last 7. Never random, never shuffled. Rows from one trip never appear in both sets, because a trip belongs to one service date.
- **Remaining risk:** the 7-day history uses `is_observed` labels from earlier days. Those were computed with full knowledge of those days, which is fine because those days are entirely in the past at `t0`.

## 2026-10-06: Models and tuning

- **Models:** Spark ML `LinearRegression` (one-hot route and mode, L2 penalty 0.1) and `GBTRegressor` (depth 4 and 6, 50 trees). Both predict the residual over persistence.
- **Why Spark ML:** serverless environment version 4+ supports `pyspark.ml` and `mlflow.spark`, and it is in the approved stack. Serverless caps a model at 100 MB, which these trees stay well under.
- **Tuning:** a deliberately small grid (two tree depths). Choosing among them on the test week is a mild form of test-set reuse. When there is enough data, the plan is to hold out the last week of the training period as validation and keep the test week untouched. That needs at least 3 weeks of data.
- **Missing values:** filled explicitly (0, or 3600 s for "no vehicle ahead") with `has_hist` and `has_ahead` flags so the model can tell a real 0 from a missing value.

## 2026-10-06: When results count

- `src/ml/evaluate.py` marks a run preliminary when there are fewer than 21 days of data or the held-out window is shorter than 7 days. Preliminary runs are tagged `data_status=preliminary` in MLflow, the model version gets the same tag, and it never gets the `champion` alias.
- With a single service date there are no training days at all. The job then logs the baselines and skips the models instead of training on nothing.
- Earliest full retrain: **2026-10-27** (collector start 2026-10-06 plus 21 days), and only if collection gaps are small by then.

## 2026-10-06: `alert_active` counts only route-wide alerts

- **Found in the first training set (dev, one day):** `alert_active` was 1 for about 83% of examples. RTD keeps more than 100 long-running alerts, most of them single-stop closures for construction, so nearly every route always "had an alert". A flag that is almost always on carries little signal.
- **Choice:** Count only alerts whose informed entity names the route but no specific stop (route-wide: detours, reduced service). A stop-level feature at the target stop could be added later if error analysis shows alerts matter.
- **Other coverage on the same day:** RTD prediction available for 100% of examples, vehicle ahead about 92%, delay trend about 89%, 7-day history 0% (it needs earlier days).
