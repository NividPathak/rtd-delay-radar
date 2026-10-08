# Databricks notebook source
# MAGIC %md
# MAGIC # Error analysis: where does the model lose to the baselines, and why?
# MAGIC
# MAGIC Reads `gold.test_predictions` (the best model's held-out predictions, written by the
# MAGIC training job) and `gold.model_results`. Check `data_status` first: preliminary runs are
# MAGIC pipeline tests, not results.

# COMMAND ----------

dbutils.widgets.text("gold_schema", "rtd.gold")
GOLD = dbutils.widgets.get("gold_schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Results table: every predictor on the held-out window

# COMMAND ----------

display(
    spark.sql(f"""
    SELECT data_status, data_start, data_end, predictor, mode, horizon, n,
           round(mae_s, 1) AS mae_s, round(rmse_s, 1) AS rmse_s
    FROM {GOLD}.model_results
    -- one training job writes all its rows in one statement, so they share processed_ts
    WHERE processed_ts = (SELECT max(processed_ts) FROM {GOLD}.model_results)
    ORDER BY mode, horizon, mae_s
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Model vs persistence vs RTD, by horizon and mode
# MAGIC `model_gain_pct` > 0 means the model beats persistence. Rows where RTD has no prediction
# MAGIC are excluded from all three columns so the comparison uses the same examples.

# COMMAND ----------

display(
    spark.sql(f"""
    SELECT mode, horizon, count(*) AS n,
           round(avg(abs(persistence_pred_s - target_delay_s)), 1) AS persistence_mae,
           round(avg(abs(rtd_pred_s - target_delay_s)), 1) AS rtd_mae,
           round(avg(abs(model_pred_s - target_delay_s)), 1) AS model_mae,
           round(100 * (1 - avg(abs(model_pred_s - target_delay_s))
                          / avg(abs(persistence_pred_s - target_delay_s))), 1) AS model_gain_pct
    FROM {GOLD}.test_predictions
    WHERE rtd_pred_s IS NOT NULL
    GROUP BY mode, horizon
    ORDER BY mode, horizon
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Where the model loses to persistence
# MAGIC Slices with at least 200 examples where the model's MAE is worse than persistence.

# COMMAND ----------

display(
    spark.sql(f"""
    WITH slices AS (
      SELECT route_short_name, mode, horizon,
             CASE WHEN current_delay_s < 0 THEN 'early'
                  WHEN current_delay_s < 120 THEN 'on time (0-2 min)'
                  WHEN current_delay_s < 600 THEN 'late (2-10 min)'
                  ELSE 'very late (10+ min)' END AS current_state,
             count(*) AS n,
             avg(abs(persistence_pred_s - target_delay_s)) AS persistence_mae,
             avg(abs(model_pred_s - target_delay_s)) AS model_mae
      FROM {GOLD}.test_predictions
      GROUP BY 1, 2, 3, 4
    )
    SELECT *, round(model_mae - persistence_mae, 1) AS model_minus_persistence
    FROM slices
    WHERE n >= 200 AND model_mae > persistence_mae
    ORDER BY model_minus_persistence DESC
    LIMIT 25
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Does the vehicle-ahead feature help?
# MAGIC Compare error with and without a known vehicle ahead.

# COMMAND ----------

display(
    spark.sql(f"""
    SELECT horizon, has_ahead, count(*) AS n,
           round(avg(abs(persistence_pred_s - target_delay_s)), 1) AS persistence_mae,
           round(avg(abs(model_pred_s - target_delay_s)), 1) AS model_mae
    FROM {GOLD}.test_predictions
    GROUP BY horizon, has_ahead
    ORDER BY horizon, has_ahead
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Error by local hour (rush hours vs off-peak)

# COMMAND ----------

display(
    spark.sql(f"""
    SELECT hour_local, mode, count(*) AS n,
           round(avg(abs(persistence_pred_s - target_delay_s)), 1) AS persistence_mae,
           round(avg(abs(model_pred_s - target_delay_s)), 1) AS model_mae
    FROM {GOLD}.test_predictions
    WHERE is_weekend = 0
    GROUP BY hour_local, mode
    ORDER BY mode, hour_local
    """)
)
