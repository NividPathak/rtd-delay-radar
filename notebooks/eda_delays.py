# Databricks notebook source
# MAGIC %md
# MAGIC # RTD delays: exploratory analysis
# MAGIC
# MAGIC Reads the gold tables. Every query is aggregated in Spark and returns a small result,
# MAGIC so nothing large is collected to the notebook. Set `gold_schema` to `rtd.dev_gold`
# MAGIC to explore the dev target.
# MAGIC
# MAGIC **Labels:** `delay_s` is observed arrival minus scheduled arrival. Only rows with
# MAGIC `is_observed` (last prediction made at most 2 minutes before arrival) are used.

# COMMAND ----------

dbutils.widgets.text("gold_schema", "rtd.gold")
GOLD = dbutils.widgets.get("gold_schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Label quality
# MAGIC How many trip stops have a reliable label, and how far ahead the last prediction was.

# COMMAND ----------

display(
    spark.sql(f"""
    SELECT mode,
           count(*) AS trip_stops,
           round(avg(CAST(is_observed AS INT)), 3) AS share_observed,
           percentile_approx(label_lead_s, 0.5) AS median_lead_s,
           min(service_date) AS first_date,
           max(service_date) AS last_date
    FROM {GOLD}.stop_arrivals
    GROUP BY mode
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Delay distribution, bus vs rail
# MAGIC Positive means late. Percentiles show the tails better than the mean.

# COMMAND ----------

display(
    spark.sql(f"""
    SELECT mode,
           count(*) AS arrivals,
           round(avg(delay_s)) AS mean_s,
           percentile_approx(delay_s, array(0.05, 0.25, 0.5, 0.75, 0.95)) AS p05_p25_p50_p75_p95,
           round(avg(CAST(delay_s > 300 AS INT)), 3) AS share_late_5min,
           round(avg(CAST(delay_s < -60 AS INT)), 3) AS share_early_1min
    FROM {GOLD}.stop_arrivals
    WHERE is_observed
    GROUP BY mode
    """)
)

# COMMAND ----------

display(
    spark.sql(f"""
    SELECT mode, floor(delay_s / 60) AS delay_min, count(*) AS arrivals
    FROM {GOLD}.stop_arrivals
    WHERE is_observed AND delay_s BETWEEN -600 AND 1800
    GROUP BY mode, floor(delay_s / 60)
    ORDER BY mode, delay_min
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Time of day
# MAGIC Average delay by local hour, weekdays vs weekends.

# COMMAND ----------

display(
    spark.sql(f"""
    SELECT is_weekday, hour_local, mode,
           sum(n_arrivals) AS arrivals,
           round(sum(avg_delay_s * n_arrivals) / sum(n_arrivals)) AS avg_delay_s
    FROM {GOLD}.route_delay_hourly
    GROUP BY is_weekday, hour_local, mode
    ORDER BY is_weekday, mode, hour_local
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Worst routes
# MAGIC Routes with at least 200 observed arrivals, ranked by average delay.

# COMMAND ----------

display(
    spark.sql(f"""
    SELECT route_short_name, mode,
           sum(n_arrivals) AS arrivals,
           round(sum(avg_delay_s * n_arrivals) / sum(n_arrivals)) AS avg_delay_s,
           round(sum(share_late_5min * n_arrivals) / sum(n_arrivals), 3) AS share_late_5min
    FROM {GOLD}.route_delay_hourly
    GROUP BY route_short_name, mode
    HAVING sum(n_arrivals) >= 200
    ORDER BY avg_delay_s DESC
    LIMIT 20
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. The M3 question: latest route in Denver at 5 pm on weekdays

# COMMAND ----------

display(
    spark.sql(f"""
    SELECT route_short_name, mode,
           sum(n_arrivals) AS arrivals,
           round(sum(avg_delay_s * n_arrivals) / sum(n_arrivals)) AS avg_delay_s
    FROM {GOLD}.route_delay_hourly
    WHERE is_weekday AND hour_local = 17
    GROUP BY route_short_name, mode
    HAVING sum(n_arrivals) >= 50
    ORDER BY avg_delay_s DESC
    LIMIT 10
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Delay growth along a trip
# MAGIC Does delay build up toward the end of a trip? This matters for the model in M4.

# COMMAND ----------

display(
    spark.sql(f"""
    SELECT mode, least(floor(stop_sequence / 5) * 5, 60) AS stop_sequence_bucket,
           count(*) AS arrivals, round(avg(delay_s)) AS avg_delay_s
    FROM {GOLD}.stop_arrivals
    WHERE is_observed
    GROUP BY mode, least(floor(stop_sequence / 5) * 5, 60)
    ORDER BY mode, stop_sequence_bucket
    """)
)
