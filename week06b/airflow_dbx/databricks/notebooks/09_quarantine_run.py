# Databricks notebook source
from pyspark.sql import functions as F

try:
    dbutils.widgets.text("run_date", "2026-02-21")
    run_date = dbutils.widgets.get("run_date")
except Exception:
    run_date = "2026-02-21"

spark.sql("CREATE DATABASE IF NOT EXISTS data5035_week07")

quarantine = spark.createDataFrame(
    [(run_date, "Publish branch not selected or DQ gate failed", "QUARANTINED")],
    ["run_date", "reason", "status"],
).withColumn("recorded_ts", F.current_timestamp())

quarantine.write.format("delta").mode("append").saveAsTable("data5035_week07.quarantine_trial_run")
print("Appended quarantine record to data5035_week07.quarantine_trial_run")
