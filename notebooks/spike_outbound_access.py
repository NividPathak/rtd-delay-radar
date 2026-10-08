# Databricks notebook source
# MAGIC %md
# MAGIC # Spike 2: can serverless compute reach RTD?
# MAGIC Tries one GET per feed and prints status, size, and the parsed header timestamp
# MAGIC when available. The result decides whether polling could ever run inside Databricks.

# COMMAND ----------

import os
import sys
import urllib.request

sys.path.append(os.path.abspath(".."))
from src.common.config import FEEDS, HTTP_TIMEOUT_SECONDS  # noqa: E402

# COMMAND ----------

results = {}
for name, url in FEEDS.items():
    try:
        with urllib.request.urlopen(url, timeout=HTTP_TIMEOUT_SECONDS) as response:
            body = response.read()
        results[name] = f"OK status={response.status} bytes={len(body)}"
    except Exception as error:
        results[name] = f"FAILED {type(error).__name__}: {error}"

for name, outcome in results.items():
    print(f"{name}: {outcome}")

# COMMAND ----------

dbutils.notebook.exit(str(results))
