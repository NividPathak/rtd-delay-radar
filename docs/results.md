# Results

**Status: pending full data.**

The collector started on 2026-10-06. The plan needs at least 3 weeks of history and a full held-out week before any number is reported. The earliest full retrain is **2026-10-27**, and only if collection gaps are small by then (see PROGRESS.md for the gap log).

Runs before that date are pipeline tests. They are tagged `data_status=preliminary` in MLflow and in `gold.model_results`, and they are not reported here or in the README.

## How results will be produced

```bash
databricks bundle run -t prod rtd_train --params start=2026-10-06,end=2026-10-27
```

This builds `gold.training_set`, splits by service date (last 7 days held out), computes both baselines on the held-out week, trains the linear model and two GBT models, logs everything to MLflow, and appends one row per predictor, mode, and horizon to `gold.model_results`.

## Findings so far (not model results)

These come from building and running the pipeline, not from a trained model.

- **RTD's trip update `start_date` is one day behind for early-morning trips.** The static schedule and the vehicle feed agree on the correct date. Left uncorrected, every early-morning delay would be off by 24 hours. 0.07% of stop updates on the first day needed the correction.
- **Labels depend on collector uptime.** With continuous collection, 96% to 98% of trip-stops get a trusted observed arrival. During collection gaps that falls to 10% to 42%, because the last prediction before arrival was never seen.
- **Most RTD alerts are long-running single-stop closures.** Counting any alert on a route flagged 83% of examples, so the feature only counts route-wide alerts.
- **Daytime snapshots are large.** A single 0.6 MB trip update snapshot holds about 21,000 stop updates, which broke a Python UDF on serverless. JVM-side `from_protobuf` handles it.
- **Collection uptime so far is 72.8%** (2026-10-06 to 2026-10-08), almost all lost to laptop sleep.

## What will be reported

- MAE and RMSE in seconds for persistence, RTD's prediction, the linear model, and the best GBT.
- Split by bus vs rail and by horizon (1, 5, 10, and 20 stops ahead).
- Where the model loses to either baseline, stated plainly, with the error analysis from `notebooks/error_analysis.py`.
- Known caveat: labels are RTD's last prediction made at most 2 minutes before arrival. At 1 stop ahead, RTD's prediction and the label are close to the same thing, so RTD is expected to be very hard to beat there.

## What I would do with paid compute

- **Shorter refresh interval.** The same `availableNow` code on a 10 to 15 minute schedule, or a continuous trigger for bronze. Nothing in the transformations changes; only the trigger and the schedule do.
- **Collect inside Databricks.** Spike 2 showed serverless can reach RTD. A small always-on job would replace the laptop and remove sleep gaps.
- **Schedule versions by date.** Join each service date to the static schedule that was in effect, instead of the latest one.
- **Larger tuning.** A proper validation week inside the training period and a wider search (tree depth, learning rate, LightGBM), with the test week untouched.
- **Online serving.** A Model Serving endpoint and a small app that answers "how late will my bus be at stop X", using the same feature function.
- **Weather and events.** Open-Meteo hourly weather and a stadium event calendar as features, both known in advance or at prediction time.
