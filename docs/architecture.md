# Architecture

What we built in each milestone, and why, in plain language.

## M0. Setup, spikes, and the collector

### What exists now

```
RTD feeds (protobuf, every ~30 s)
      |
      v
Collector on the laptop (src/collector/run.py, every 60 s)
      |   saves to data/raw/ locally, then uploads
      v
/Volumes/rtd/landing/raw/feed=<name>/date=YYYY-MM-DD/hour=HH/<name>_<ts>.pb
```

- **Unity Catalog:** catalog `rtd` with schemas `landing`, `bronze`, `silver`, `gold`, `ml`, and a managed volume `rtd.landing.raw` for raw files. They are created once by `src/common/setup_catalog.py`.
- **Asset Bundle:** `databricks.yml` plus `resources/`. It has `dev` and `prod` targets. It deploys one job today (`m0_spike_job`). Every later job and pipeline is added here, never by clicking in the UI.
- **CI:** GitHub Actions runs `ruff` and `pytest` on every pull request.

### The collector, step by step

1. **Fetch** each feed with `urllib`. If the request fails, retry after 2, 4, then 8 seconds (exponential backoff), then give up for this cycle.
2. **Read the header timestamp** from the protobuf. It says when RTD built the snapshot.
3. **Skip duplicates.** If the timestamp matches the last one saved for that feed, nothing new happened, so nothing is stored.
4. **Save locally** first, under a path partitioned by feed, UTC date, and UTC hour. The local copy is a backup if the upload fails.
5. **Upload** the same bytes to the Unity Catalog volume with the Databricks SDK. If the upload fails, the file goes into a queue and is retried at the start of the next cycle.
6. **Isolate failures.** Each feed is wrapped in its own error handler, so one broken feed never stops the other two or crashes the loop.

`python -m src.collector.health` reads the saved file names and reports, for each feed, how many snapshots exist, the first and last timestamp, and every gap longer than 5 minutes.

### Why it is built this way

- **Why the collector runs outside Databricks.** RTD feeds only show the present moment, so someone has to ask every minute. A Databricks job that runs every minute would start serverless compute 1,440 times a day and burn the Free Edition quota. A tiny Python process on a laptop does the polling for free. Databricks only processes the files later, in hourly batches. Spike 2 showed Databricks *can* reach RTD, so this is a cost decision, not a network workaround.
- **Why save raw protobuf files instead of parsing first.** Raw files are the source of truth. If a parsing bug turns up in M2, bronze can be rebuilt from the raw files. Data that was never saved can never be recovered.
- **Why partition by UTC.** Denver switches clocks twice a year. Local-time folders would get a repeated hour in November and a missing hour in March. UTC never jumps.
- **Why the collector came before the rest of the pipeline.** Code can be written any day. Past delays cannot be downloaded later. Every hour the collector is off is data lost for good.

### Spikes

- **Spike 1 (local parse):** All three feeds download and parse. Trip updates give predicted arrival times but no `delay` field, so delays will be computed against the static schedule in later milestones.
- **Spike 2 (outbound from serverless):** Works. Details in `docs/decisions.md`.

## M1. Collector that keeps running

### What exists now

```
                    every 60 s                         upload
Laptop  (launchd agent, caffeinate) --> RTD feeds --> /Volumes/rtd/landing/raw/
                                                          ^
GitHub Actions (every 15 min):                            |
  freshness check: newest snapshot < 5 min old? --yes--> exit
                                               --no---> collect for 55 min
```

- **Laptop runner.** A launchd agent starts the collector at login and restarts it within 30 seconds if it exits. A `caffeinate -w <pid>` helper stops the Mac from idle sleep while the collector runs.
- **Backup runner.** `.github/workflows/collector.yml` asks the volume one question every 15 minutes: "Did anything arrive in the last 5 minutes?" If yes, the laptop is fine and the run ends. If no, it runs the same collector code for 55 minutes.
- **Health checks.** `src/collector/health.py --volume` lists the files in the volume and reports the date range and every gap longer than 5 minutes. The file name holds the header timestamp, so no file has to be opened.

### Why it is built this way

- **One collector, two places to run it.** The GitHub runner uses exactly the same Python code as the laptop. Only the auth differs: OAuth through the Databricks CLI on the laptop, a token in GitHub secrets in CI.
- **Backup, not duplicate.** Running both all the time would upload every file twice. Checking freshness first means GitHub only works when it is needed.
- **Uploads are idempotent.** If both runners happen to save the same snapshot, the file name is identical (`<feed>_<header timestamp>.pb`) and the second upload just overwrites the first. No duplicates downstream.
- **Gaps are measured, not hidden.** Lid-closed sleep and GitHub schedule delays can still leave short gaps. The health report finds them, and they are listed in PROGRESS.md and later in the README.

## M2. Bronze and silver

### What exists now

```
/Volumes/rtd/landing/raw/feed=*/...pb
      |  rtd_refresh job, task 1: bronze (Auto Loader, availableNow)
      v
rtd.bronze.trip_updates | vehicle_positions | alerts        one row per feed entity
      |  rtd_refresh job, task 2: rtd_silver pipeline (Lakeflow, serverless)
      v
rtd.silver.stop_time_updates    one row per trip, stop, snapshot; scheduled vs predicted delay
rtd.silver.vehicle_positions    deduplicated, in_service flag
rtd.silver.alerts               one row per alert and affected route/stop, first/last seen
rtd.silver.gtfs_*               static schedule (latest version): routes, stops, scheduled stops
```

The `dev` bundle target runs the same code against `rtd.dev_bronze` and `rtd.dev_silver`, and only reads one day of files.

### Bronze, step by step

1. **Auto Loader** (`cloudFiles`, format `binaryFile`) lists new `.pb` files in the volume. The checkpoint in `/Volumes/rtd/landing/checkpoints/` remembers which files were already loaded, so every file is ingested exactly once.
2. **`from_protobuf`** decodes each file into a `FeedMessage` struct inside the JVM. The protobuf schema comes from the `gtfs-realtime-bindings` package.
3. **Explode** the entities, keep each feed's fields with explicit types, and add `source_file` and `ingest_ts`.
4. **`trigger(availableNow=True)`** processes everything new, then stops. The job runs every 2 hours.

### Silver, step by step

1. **Static schedule.** Read the static GTFS files from the volume, keep the newest version, and build `gtfs_scheduled_stops`: one row per trip and stop with the scheduled time in seconds, the route, the direction, and bus or rail.
2. **Stop updates.** Explode each trip update into one row per stop, drop repeats of the same (trip, stop, snapshot), and join to the schedule on (trip_id, stop_sequence).
3. **Delay.** Scheduled time = (noon local time on the service date, minus 12 hours) + the GTFS time offset. That is the GTFS definition, and it stays correct on daylight saving days. Delay = RTD's predicted arrival minus the scheduled arrival.
4. **RTD's date bug.** For early-morning trips, RTD's trip update `start_date` is one day behind. The pipeline detects the whole-day offset, corrects it, and records the correction in `service_date_shift_days`.
5. **Expectations** drop rows with a missing trip or vehicle id, a delay outside ±2 hours, or a position outside the Denver area. Two warn-only expectations count unmatched schedule rows and date corrections. All pass rates are in the pipeline event log.

### First results on one day (2026-10-06, dev target)

- 7.7 million stop update rows from about 12 hours of snapshots. Every row matched the schedule. 0.07% needed the date correction.
- Delay of RTD's *prediction* against the schedule: bus median 3 s (p90 178 s), rail median 64 s (p90 224 s). These are predictions, not observed arrivals. Actual-arrival labels are built in M3.

### Why it is built this way

- **Why `availableNow` instead of an always-on stream.** An always-on stream keeps serverless compute running 24 hours a day, and the Free Edition quota would run out. `availableNow` uses the same Structured Streaming code, checkpoints, and exactly-once guarantees, but runs in bursts: process everything new, then shut down. The cost is freshness. Data is up to 2 hours old, which is fine for training a model.
- **Why bronze is a job and silver is a pipeline.** Bronze needs `from_protobuf` and file-level control of Auto Loader, which is plain Structured Streaming. Silver is mostly joins, deduplication, and quality rules, which is what Lakeflow pipelines do well: they manage dependencies between tables and record expectation results without extra code.
- **Why not a Python UDF for parsing.** It was the first version. On daytime snapshots it ran out of memory in the serverless Python sandbox. `from_protobuf` runs in the JVM and handles them easily.
- **Why the transformations are plain functions.** Every silver step is a function that takes and returns DataFrames, tested on a local Spark session with real RTD snapshots. The pipeline file only wires them together, so almost all logic is tested without touching Databricks.

## M3. Gold tables and labels

### What exists now

```
rtd.silver.stop_time_updates   (every prediction, every snapshot)
      |  group by (service_date, trip_id, stop_sequence), keep the last prediction
      v
rtd.gold.stop_arrivals         one row per trip and stop: scheduled, observed arrival, delay_s
      |  observed labels only, by route, date, local hour
      v
rtd.gold.route_delay_hourly    avg and median delay, share more than 5 minutes late
```

`notebooks/eda_delays.py` explores both tables: label quality, delay distribution, time of day, worst routes, the 5 pm question, and delay growth along a trip.

### How a label is made

1. RTD keeps an upcoming stop in each trip update until the vehicle passes it, then drops it.
2. For each trip and stop, gold keeps RTD's **last** prediction before the stop disappeared. That is the observed arrival.
3. `label_lead_s` says how far ahead of the arrival that last prediction was made. If it is at most 2 minutes, the vehicle was about to arrive, so the label is trusted (`is_observed`).
4. `delay_s` = observed arrival minus scheduled arrival.

### What the first day showed

- In hours when the collector ran continuously, 96% to 98% of trip-stops got a trusted label. In hours with gaps (the laptop slept), only 10% to 42% did. The labels are only as good as the collector's uptime.
- Observed delay medians on the first day: bus 69 s, rail 70 s. One day is not a result. These numbers will be reported properly after three weeks.

### Why it is built this way

- **Why not use vehicle positions for labels.** Most vehicles in the feed do not carry a trip id, so matching positions to stops would label few arrivals. The last-prediction method labels almost every stop when collection is continuous.
- **Why a materialized view.** A label keeps changing until the vehicle passes the stop, so gold must be recomputed rather than appended to. A materialized view recomputes from silver on each pipeline update, incrementally when it can.
- **Why `route_delay_hourly` uses local time.** Riders and the dashboard think in Denver time ("5 pm on weekdays"), so the hour is taken from the scheduled arrival converted to America/Denver.

## M4. Features and model

### What exists now

```
gold.stop_arrivals + silver.stop_time_updates + silver.gtfs_scheduled_stops + silver.alerts
      |  rtd_train job, task 1: rtd-build-features --start --end
      v
gold.training_set      one row per (anchor stop, horizon): features at t0, label, baselines
      |  rtd_train job, task 2: rtd-train --start --end
      v
time split by service date -> baselines -> linear, GBT (depth 4, 6) -> MLflow
      |
      +--> gold.model_results     MAE and RMSE per predictor, mode, horizon, with data_status
      +--> gold.test_predictions  best model's held-out predictions (for error analysis)
      +--> rtd.ml.delay_model     Unity Catalog model (champion alias only for full-data runs)
```

The job is not scheduled. A full retrain is one command with a date range.

### How an example is built

1. **Anchor:** a vehicle has just reached stop A. The prediction time `t0` is when that arrival became known.
2. **Target:** the same trip's observed delay K scheduled stops later, for K in 1, 5, 10, 20, as long as that arrival is after `t0`.
3. **Features** use only what was known at `t0`: current delay, the trend over the last 3 stops, route and direction, how many stops are left, time of day and day type, the average delay at the target stop and hour over the previous 7 days, the delay of the last vehicle on the same route that already reached the target stop, and whether a route-wide alert was live.
4. **Baselines:** persistence (delay stays the same) and RTD's own prediction for the target stop from the latest snapshot before `t0`.

### Why it is built this way

- **Why the split is by time.** Delays on one day are correlated: a snowstorm or a crash affects every trip that day. A random split would put trips from the same day in both train and test, and the model would look good by memorising the day. Splitting by service date means the model is always tested on days it has never seen, like in real use.
- **Why predict the change, not the delay.** Persistence is a strong baseline. Predicting the change in delay means a model that learns nothing ends up equal to persistence, not worse. Every gain is a gain over the baseline.
- **Why as-of joins.** Features like "the vehicle ahead" and "RTD's prediction" must be the latest value *before* `t0`. A plain join would pick up values from after `t0` and leak the future. The as-of joins and their tests are what keep the evaluation honest.
- **Why nothing is reported yet.** Any model trained on less than 3 weeks is a pipeline test. The code marks those runs preliminary and refuses to give them the `champion` alias.

## M5. Scoring and dashboard

### What exists now

```
rtd_refresh (every 2 h):  bronze -> silver and gold pipeline -> score
                                                                  |
          +-------------------------------+-----------------------+
          v                               v                       v
gold.delay_predictions         gold.prediction_monitoring   gold.live_vehicles
(append: every active trip,    (daily MAE/RMSE per          (latest position and
 1/5/10/20 stops ahead,         predictor once arrivals      delay per vehicle)
 persistence, RTD, model*)      are observed)
          \______________________________|_______________________/
                                         v
                       AI/BI dashboard "RTD Delay Radar"
     live delay map | worst routes now | prediction vs actual | error over time
```

\* The model's predictions appear once `rtd.ml.delay_model@champion` exists (after the full retrain).

### Why it is built this way

- **Scoring right after the pipeline.** The freshest data exists right after silver and gold update, so scoring is the last task of the same job. No separate schedule is needed, and the quota cost stays in one run.
- **Same feature code for training and scoring.** `src/features/training_set.py` builds features for both. The only difference is where the target comes from: an observed arrival for training, the static schedule for live scoring. Sharing the code means the model sees features computed the same way in both places (no training/serving skew).
- **Baselines are scored too.** Monitoring compares the model with persistence and RTD on the same predictions every day. That is the honest way to show whether the model keeps its advantage after deployment.
- **The dashboard updates by itself.** Its queries read gold tables, which every scheduled run refreshes. Opening the dashboard runs those queries on the SQL warehouse.

## M6. CI/CD and write-up

### What exists now

```
pull request ──> ci.yml:     ruff, pytest (local Spark), bundle validate -t prod
push to main ──> deploy.yml: ruff, pytest, bundle validate, bundle deploy -t prod
every 15 min ──> collector.yml: backup collector when the volume goes stale
```

- Every milestone was merged through a pull request after CI passed (PRs #1 to #6).
- `README.md` explains the project, architecture, findings, limitations, and setup. `docs/results.md` keeps model results marked "pending full data" until the 3-week retrain.

### Why it is built this way

- **Deploy from `main` only.** Prod always matches `main`, and nothing reaches prod without passing lint and tests first. Before this, prod was deployed from a laptop, which is fine for building but not for keeping prod reproducible.
- **Secrets stay in GitHub.** The Databricks host and token are repository secrets, never in files. Every Databricks step skips cleanly when they are missing, so forks and early runs do not fail.
- **Tests run without Databricks.** The CI unit tests run on a local Spark session with real RTD snapshots, so every pull request is checked without spending any Databricks quota.
