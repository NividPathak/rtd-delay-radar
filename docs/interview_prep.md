# Interview Prep

Questions each milestone prepares you for, with short answers in your own words.

## M0. Setup, spikes, and the collector

**1. Why does your collector run outside Databricks?**
The feeds only show the present, so they have to be polled every minute. Running a Databricks job every minute would start serverless compute about 1,440 times a day and use up the Free Edition daily quota. A small Python process polls for free and just uploads files. Databricks processes them in hourly batches. I tested that Databricks can reach RTD, so this is a cost choice, not a network workaround.

**2. How do you avoid storing the same data twice?**
Every GTFS-Realtime feed has a header timestamp saying when it was generated. The collector remembers the last timestamp per feed and skips a snapshot if it has not changed. The timestamp is also in the file name, so a re-upload overwrites the same file instead of creating a duplicate.

**3. What happens when RTD or Databricks is down?**
Fetches retry with exponential backoff (2, 4, 8 seconds). Each feed is handled separately, so one failing feed does not stop the others. Files are saved locally before uploading. A failed upload is queued and retried next cycle. Gaps that still happen are measured by a health script and documented, not hidden.

**4. Why store raw protobuf files rather than parsed rows?**
The raw files are the source of truth and cannot be downloaded again later. If I find a parsing bug, I can rebuild every downstream table from the raw layer. That is the point of a bronze/landing layer in a medallion architecture.

**5. Why partition the files by UTC date and hour?**
Partitioning lets later jobs read only the hours they need, which matters with a compute quota. UTC avoids daylight saving problems: local time repeats an hour in November and skips one in March. Local hour-of-day is still computed later as a feature.

## M1. Collector that keeps running

**1. How do you keep a laptop-based collector reliable?**
A launchd agent (the macOS service manager) restarts it if it crashes and starts it at login. `caffeinate -w` stops idle sleep while it runs. For times the laptop is closed, a GitHub Actions workflow checks every 15 minutes whether new files have arrived in the volume. If not, it takes over for 55 minutes.

**2. Why not just run the GitHub Actions collector all the time?**
It would upload every snapshot twice and use runner time for nothing. A freshness check costs about 30 seconds and only starts collecting when the main runner has stopped delivering.

**3. If both runners upload the same snapshot, do you get duplicates?**
No. The file name is the feed name plus the feed's header timestamp, so both runners produce the same path and the second upload overwrites the first. The write is idempotent by design.

**4. How do you know if you lost data?**
A health script lists the files in the volume, reads the timestamps from the file names, and reports every gap longer than 5 minutes. Gaps are documented, never filled with made-up data.

**5. Something you found that surprised you?**
`caffeinate -i <command>` did not actually prevent sleep in my setup: the process had no sleep assertion. I checked with `pmset -g assertions` and switched to `caffeinate -w <pid>`, which ties the assertion to the collector's PID. Lesson: verify the mechanism, not just that the command ran.

## M2. Bronze and silver

**1. Why `trigger(availableNow=True)` instead of a continuous stream?**
Free Edition has a daily compute quota, and an always-on stream would use it up. `availableNow` is still Structured Streaming with checkpoints and exactly-once processing. It processes everything that arrived since the last run, then stops. I run it every 2 hours. The tradeoff is freshness: up to 2 hours of latency, which is fine for building training data.

**2. How does Auto Loader avoid processing a file twice?**
It records every file it has ingested in the checkpoint. On the next run it only picks up files it has not seen. The checkpoint is per target and per feed, so the dev and prod runs never interfere.

**3. Your first bronze version ran out of memory. What happened and how did you fix it?**
I parsed protobuf in a Python UDF. A daytime snapshot is only 0.6 MB, but it has 21,000 stop updates and becomes about 9 MB of Python objects. Spark sends UDF rows in batches, so one batch needed nearly 1 GB in the serverless Python sandbox. I switched to Spark's built-in `from_protobuf`, which decodes in the JVM, and kept the Python parser as a test oracle. A unit test checks that both give identical output on real snapshots.

**4. How do you compute the scheduled arrival time from GTFS?**
GTFS gives times like `25:30:00` relative to the service day, not clock times. The spec defines the reference as noon local time minus 12 hours, which is midnight on normal days but stays correct on daylight saving days. I add the offset in seconds to that reference. A unit test checks 2026-11-01, when DST ends.

**5. What data quality problem did you find in RTD's feed?**
For early-morning trips, RTD's trip update `start_date` is one day behind. The static schedule and the vehicle feed both show the trip runs the next day. Left alone, every early-morning delay would be off by 24 hours. Real delays are within hours, so I round the raw delay to whole days to detect the error, correct the date, and keep a column recording the correction. A warn-only expectation reports how often it happens: 0.07% of rows on the first day.

## M3. Gold tables and labels

**1. RTD only publishes predictions. Where do your labels come from?**
RTD drops a stop from a trip update once the vehicle passes it. The last prediction before that happens is very close to the real arrival, so I use it as the observed arrival. I only trust it if that last prediction was made at most 2 minutes before the arrival time, which I record as `label_lead_s`.

**2. How do you know the labels are good?**
I measured the share of trusted labels per hour. With continuous collection it is 96% to 98%. During collection gaps it drops to 10% to 42%, which shows the rule correctly rejects labels where data is missing instead of silently using stale predictions.

**3. What is the weakness of this label?**
It is still RTD's own estimate, just made very close to arrival. At very short horizons, comparing a model to RTD's prediction is almost comparing RTD to itself. That is why results are split by prediction horizon.

**4. Why is gold a materialized view and not a streaming table?**
A trip-stop's label changes until the vehicle passes the stop. A streaming table appends rows and never revisits them. A materialized view recomputes from silver on each update, so labels are always based on the latest data.

**5. How would you answer "which route is latest at 5 pm on weekdays"?**
One query on `gold.route_delay_hourly`: filter `is_weekday` and `hour_local = 17`, take the arrival-weighted average delay per route, and require a minimum number of arrivals so a route with three trips does not top the list.

## M4. Features and model

**1. Why did you split train and test by time and not randomly?**
Delays on the same day are correlated (weather, incidents, events). A random split puts trips from the same day in both sets, so the model can effectively memorise the day and the test score looks better than real use. Splitting by service date tests the model only on days it has never seen.

**2. How did you make sure no feature leaks the future?**
Every example has a prediction time `t0`. Each feature is defined as "the latest value known before `t0`". The tricky ones (the vehicle ahead, RTD's prediction, alerts, 7-day history) use as-of joins or exclude the current day, and each has a unit test where future data is present and must be ignored. The full table is in `docs/decisions.md`.

**3. What are your baselines and why two?**
Persistence: the delay stays what it is now. It is simple and strong. RTD's own prediction: what riders already see in the app. Beating persistence shows the model learned something. Comparing with RTD shows whether it is useful in practice.

**4. Why does your model predict the change in delay?**
So that a model that learns nothing collapses to persistence instead of something worse. It also makes the target smaller and better behaved, which helps the linear model.

**5. You only had one day of data when you built this. What did you do?**
I built and tested the whole pipeline but did not report any number. The code marks runs with under 21 days or no full held-out week as preliminary, tags them in MLflow, and never gives them the champion alias. The first real results come from the retrain on 2026-10-27.

## M5. Scoring and dashboard

**1. How do you avoid training/serving skew?**
Training and live scoring call the same feature function. The only difference is the target: in training it is an observed arrival, in scoring it comes from the schedule because the vehicle has not arrived yet. Features like the vehicle ahead or the 7-day history are computed by the same code in both cases.

**2. What does your scoring job do if there is no approved model?**
It writes predictions from the two baselines and logs that no champion exists. When a model gets the `champion` alias after a full-data retrain, the next run loads it and adds its predictions with the model version. I never promote a model trained on too little data just to fill the dashboard.

**3. How do you monitor the model after deployment?**
Every prediction is stored. Once the target stop is reached and the label is trusted, the monitoring step joins them and computes daily MAE and RMSE per predictor, mode, and horizon. The dashboard plots model error next to the baselines over time, so drift or a broken feature shows up as the model's line moving toward or above persistence.

**4. How "live" is the dashboard?**
Up to about 2 hours behind, because the pipeline runs in `availableNow` bursts every 2 hours to stay within the Free Edition quota. The dashboard says so in its header and shows a "data as of" time. With paid compute I would run the same code on a shorter schedule; the code would not change.

**5. Why is the dashboard a JSON file in Git?**
So it is deployed by the Asset Bundle like everything else, reviewed in pull requests, and identical between dev and prod. The bundle sets which schema the dashboard reads, so one file serves both targets.

## M6. CI/CD and write-up

**1. How does code get to production?**
Through a pull request. CI runs lint, unit tests on a local Spark session, and validates the Databricks bundle. After merge, a GitHub Actions workflow runs the tests again and deploys the prod target with `databricks bundle deploy`. Jobs, the pipeline, and the dashboard are all defined in the bundle, so nothing is created by hand.

**2. How do you test Spark code without a cluster?**
Every transformation is a pure function from DataFrames to a DataFrame, tested on a local Spark session with small real RTD snapshots saved in the repo. Pipeline files only wire those functions together. Databricks is only used to verify end to end.

**3. What would you change with a bigger budget?**
Refresh every 10 to 15 minutes instead of every 2 hours (same code, different trigger), move polling into Databricks to remove laptop gaps, join each day to the schedule version in effect, and serve the model behind an endpoint.

**4. What went wrong during the project?**
Laptop sleep cost about 27% of the first two days of data. GitHub only runs scheduled workflows from the default branch, so the backup collector could not run until I merged it, which conflicted with my own "merge only when done" rule. I documented the conflict and merged early on purpose. I also hit the Free Edition limit on concurrent compute by running a manual job at the same minute as a scheduled one.

**5. How do you keep the project honest?**
Preliminary models are tagged and never promoted. Results stay "pending full data" until there are 3 weeks and a held-out week. Collection gaps are measured and published in the README. If the model loses to a baseline somewhere, the results page will say so.
