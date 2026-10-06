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
