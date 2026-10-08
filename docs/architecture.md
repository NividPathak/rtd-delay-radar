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
