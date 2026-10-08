# Results

**Status: pending full data.**

The collector started on 2026-10-06. The plan needs at least 3 weeks of history and a full held-out week before any number is reported. The earliest full retrain is **2026-10-27**, and only if collection gaps are small by then (see PROGRESS.md for the gap log).

Runs before that date are pipeline tests. They are tagged `data_status=preliminary` in MLflow and in `gold.model_results`, and they are not reported here or in the README.

## How results will be produced

```bash
databricks bundle run -t prod rtd_train --params start=2026-10-06,end=2026-10-27
```

This builds `gold.training_set`, splits by service date (last 7 days held out), computes both baselines on the held-out week, trains the linear model and two GBT models, logs everything to MLflow, and appends one row per predictor, mode, and horizon to `gold.model_results`.

## What will be reported

- MAE and RMSE in seconds for persistence, RTD's prediction, the linear model, and the best GBT.
- Split by bus vs rail and by horizon (1, 5, 10, and 20 stops ahead).
- Where the model loses to either baseline, stated plainly, with the error analysis from `notebooks/error_analysis.py`.
- Known caveat: labels are RTD's last prediction made at most 2 minutes before arrival. At 1 stop ahead, RTD's prediction and the label are close to the same thing, so RTD is expected to be very hard to beat there.
