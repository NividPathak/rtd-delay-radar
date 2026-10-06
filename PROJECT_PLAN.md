# RTD Delay Radar: Project Plan

Real-time transit delay prediction for Denver RTD on Databricks and Apache Spark.

## 1. What this project is

A streaming lakehouse that ingests live RTD bus and rail feeds, cleans them into Delta tables, and predicts how late a vehicle will be at its upcoming stops. A dashboard shows live delays and model accuracy.

**The one-line pitch for your resume:**
"Built a streaming lakehouse on Databricks that ingests live RTD transit feeds with Spark Structured Streaming and predicts stop-level arrival delays, beating RTD's own persistence baseline by X%."

You fill in X at the end. That number is the whole point of the project.

## 2. Skills this puts on your resume

| Skill | Where it shows up |
|---|---|
| Apache Spark Structured Streaming | Auto Loader ingestion, stream-to-Delta |
| Delta Lake and medallion architecture | Bronze, silver, gold tables |
| Lakeflow Declarative Pipelines | Silver and gold layers with data quality expectations |
| Unity Catalog | Tables, volumes, model registry |
| MLflow | Experiment tracking, model registry, model versions |
| Feature engineering at scale | Window functions over streaming history |
| Databricks Asset Bundles | Everything deployed as code |
| CI/CD | GitHub Actions for tests and deploys |
| Protobuf and API ingestion | GTFS-Realtime parsing |

## 3. Data sources

All free and public. Read and accept RTD's GTFS-Realtime license agreement before you start. Link is on the RTD real-time feeds page.

**Live feeds (protobuf, refresh roughly every 30 to 60 seconds):**
- Trip updates: `https://www.rtd-denver.com/files/gtfs-rt/TripUpdate.pb`
- Vehicle positions: `https://www.rtd-denver.com/files/gtfs-rt/VehiclePosition.pb`
- Service alerts: `https://www.rtd-denver.com/files/gtfs-rt/Alerts.pb`

**Static schedule (zip of CSVs):** RTD GTFS schedule dataset. Gives routes, stops, trips, stop_times. Download link is on the same RTD page.

**Weather (optional, milestone 4):** Open-Meteo hourly history for Denver. No API key needed.

RTD changed its feed URLs in Fall 2025. If a URL fails, check the RTD real-time feeds page for the current ones.

## 4. Constraints you must design around

Databricks Free Edition is serverless only. It has a daily usage quota. If you exceed it, compute shuts off for the rest of the day. Accounts can be deleted after long inactivity. It is for non-commercial use.

This shapes three decisions:

1. **No always-on streams.** Use Structured Streaming with `trigger(availableNow=True)` on a schedule. It is still real streaming code with checkpoints and exactly-once semantics. It just runs in bursts. Interviewers accept this. Say "incremental streaming with availableNow triggers" and explain the cost tradeoff.
2. **Polling happens outside Databricks.** A small Python collector polls RTD every 60 seconds and uploads raw files to a Unity Catalog volume. Serverless compute on Free Edition may block outbound calls to RTD. Milestone 0 tests this. The external collector works either way.
3. **Keep jobs small.** Schedule the pipeline every 1 to 2 hours, not every minute. Protect the quota.

## 5. Architecture

```
RTD GTFS-RT feeds (protobuf)
        |
        v
[Collector]  Python script, polls every 60s
        |    runs on your laptop or GitHub Actions
        v
Unity Catalog Volume  /Volumes/rtd/landing/raw/...   (raw .pb files, partitioned by date/hour)
        |
        v
[Bronze]  Auto Loader (binaryFile) -> parse protobuf -> Delta
        |    one row per feed entity, raw fields kept, ingest timestamp added
        v
[Silver]  Lakeflow Declarative Pipeline
        |    dedupe, type casting, join to static GTFS, data quality expectations
        v
[Gold]    stop_arrivals (actual vs scheduled), route_delay_hourly, training_set
        |
        +--> [ML]  features -> train -> MLflow -> Unity Catalog model registry
        |              |
        |              v
        |         batch scoring job writes predictions table
        v
[Dashboard]  Databricks AI/BI dashboard: live delays, worst routes, model error over time
```

## 6. The ML problem, stated precisely

**Question:** A vehicle is at stop A right now. How many seconds late will it be at a stop K stops ahead?

**Label:** Actual arrival delay at the downstream stop. You get this from the last TripUpdate for that trip and stop before the vehicle arrives, or from vehicle position crossing the stop.

**Baseline to beat:** Persistence. "Delay at the future stop equals the delay right now." This is a strong baseline. Beating it by even 10 to 15 percent on MAE is a real result.

**Second baseline:** RTD's own prediction in the TripUpdate feed at the same moment.

**Features:**
- Current delay, delay trend over last 3 stops
- Route, direction, stop sequence, stops remaining
- Hour of day, day of week, holiday flag
- Historical average delay for this route, stop, and hour (rolling 7 days)
- Delay of the vehicle ahead on the same route
- Active service alert on the route (yes or no)
- Weather: temperature, precipitation, snow (optional)

**Models:** Start with linear regression. Then gradient boosted trees (Spark MLlib GBT or LightGBM). Log every run to MLflow.

**Split:** By time. Train on earlier weeks, test on the latest week. Never shuffle. Random splits leak future information and will get you caught in an interview.

**Metrics:** MAE and RMSE in seconds. Report by route type (bus vs rail) and by prediction horizon (5, 10, 20 stops ahead).

## 7. Milestones

Eight weeks at 6 to 8 hours a week. Start the collector in week 1 and leave it running. The model needs at least 3 weeks of history.

### M0. Setup and spikes (week 1)
- Create Databricks Free Edition account. Verify with LinkedIn for higher limits.
- Create GitHub repo. Add CLAUDE.md and this plan. First commit and push.
- Install Databricks CLI. Authenticate. Initialize an Asset Bundle.
- Create catalog `rtd` with schemas `landing`, `bronze`, `silver`, `gold`, `ml`.
- Spike 1: Download each feed locally and parse it with `gtfs-realtime-bindings`. Print 5 records.
- Spike 2: From a Databricks notebook, try to fetch a feed URL. Record whether outbound access works.
- **Done when:** Repo is on GitHub, bundle deploys an empty job, both spikes are written up in `docs/decisions.md`.

### M1. Collector (weeks 1 to 2)
- Python script that polls three feeds every 60 seconds.
- Skips duplicate snapshots using the feed header timestamp.
- Writes raw `.pb` files, uploads to the volume under `feed=<name>/date=YYYY-MM-DD/hour=HH/`.
- Retries with backoff. Logs failures. Never crashes on one bad poll.
- GitHub Actions workflow as a second runner option.
- Unit tests with saved sample feeds.
- **Done when:** 48 hours of continuous data sits in the volume with no gaps longer than 5 minutes.

### M2. Bronze and silver (weeks 2 to 3)
- Bronze: Auto Loader reads binary files, a UDF parses protobuf into rows. `availableNow` trigger. Checkpoints in the volume.
- Load static GTFS (routes, stops, trips, stop_times) as reference tables.
- Silver: Lakeflow Declarative Pipeline. Dedupe on (trip_id, stop_id, feed_timestamp). Cast types. Join to static GTFS.
- Add expectations: non-null trip_id, valid lat/lon within the Denver bounding box, delay within plus or minus 2 hours.
- **Done when:** Pipeline runs end to end on a schedule and the expectations report shows pass rates.

### M3. Gold tables and labels (week 4)
- `gold.stop_arrivals`: one row per trip and stop with scheduled time, actual time, delay in seconds.
- `gold.route_delay_hourly`: aggregates for the dashboard.
- Exploratory notebook: delay distribution, worst routes, time-of-day patterns, rail vs bus.
- **Done when:** You can answer "what is the latest route in Denver at 5 pm on weekdays" with one query.

### M4. Features and model (weeks 5 to 6)
- Build `gold.training_set` with the features in section 6. Use Spark window functions.
- Persistence baseline and RTD-prediction baseline first. Record their MAE.
- Train linear model, then GBT. Tune with a small search. Log everything to MLflow.
- Register the best model in Unity Catalog with an alias `champion`.
- Error analysis notebook: where does the model lose to the baseline and why.
- **Done when:** A results table compares all models against both baselines on a held-out week.

### M5. Scoring and dashboard (week 7)
- Batch scoring job loads the `champion` model and writes `gold.delay_predictions` after each pipeline run.
- Monitoring table: daily MAE of predictions vs actuals once trips complete.
- AI/BI dashboard with four views: live delay map, worst routes now, prediction vs actual, model error over time.
- **Done when:** The dashboard updates on its own after a scheduled run.

### M6. Polish and publish (week 8)
- GitHub Actions: lint and unit tests on every pull request, bundle validate, deploy on merge to main.
- README with architecture diagram, screenshots, results table, setup steps, and limitations.
- Short writeup in `docs/results.md` with findings and what you would do with paid compute.
- Record a 2 minute demo video or GIF of the dashboard.
- Pin the repo on your GitHub profile.
- **Done when:** A stranger can understand the project in 60 seconds from the README.

## 8. Repo structure

```
rtd-delay-radar/
├── CLAUDE.md
├── PROJECT_PLAN.md
├── PROGRESS.md
├── README.md
├── databricks.yml              # Asset Bundle root
├── resources/                  # job and pipeline definitions (YAML)
├── src/
│   ├── collector/              # polling script, uploader
│   ├── bronze/                 # Auto Loader + protobuf parsing
│   ├── pipelines/              # Lakeflow Declarative Pipeline (silver, gold)
│   ├── features/               # feature engineering
│   ├── ml/                     # training, evaluation, scoring
│   └── common/                 # config, schemas, utilities
├── notebooks/                  # exploration and error analysis only
├── tests/
│   ├── fixtures/               # small saved .pb samples
│   └── unit/
├── docs/
│   ├── architecture.md
│   ├── decisions.md
│   └── results.md
├── .github/workflows/          # ci.yml, deploy.yml, collector.yml
├── pyproject.toml
└── .gitignore
```

## 9. Risks

| Risk | What to do |
|---|---|
| Free Edition quota runs out mid-day | Schedule pipeline every 1 to 2 hours. Develop on small date ranges. |
| Not enough data for the model | Start the collector in week 1. Do not wait for M2. |
| Laptop collector has gaps when it sleeps | Use the GitHub Actions runner as backup. Document gaps honestly. |
| RTD feed URLs change | Keep URLs in one config file. Check RTD's page if polls fail. |
| Model cannot beat persistence | Still a valid result. Report it by horizon. It usually wins at longer horizons. |
| Free Edition account deleted for inactivity | All code lives in GitHub. Raw data can be re-collected. |

## 10. Stretch goals

Pick one if you finish early.
- Add an LLM summary of service alerts using `ai_query` and use it as a feature.
- Serve the model behind an endpoint and build a small Databricks App to query it.
- Add Bustang feeds for intercity routes.
- Compare Spark MLlib GBT vs LightGBM on training time and accuracy.

## 11. Resume bullets to earn

Fill in the numbers when you finish.
- Built a streaming lakehouse on Databricks ingesting N million live RTD transit records with Spark Structured Streaming, Auto Loader, and Delta Lake in a medallion architecture.
- Engineered time-window features in PySpark and trained gradient boosted models tracked in MLflow, reducing stop-level delay MAE by X% over the persistence baseline.
- Automated deployment with Databricks Asset Bundles and GitHub Actions CI/CD, with data quality enforced through Lakeflow pipeline expectations.
