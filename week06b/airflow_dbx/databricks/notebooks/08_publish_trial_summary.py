# Databricks notebook source
from pyspark.sql import functions as F

try:
    dbutils.widgets.text("run_date", "2026-02-21")
    run_date = dbutils.widgets.get("run_date")
except Exception:
    run_date = "2026-02-21"

spark.sql("CREATE DATABASE IF NOT EXISTS data5035_week07")

metrics = spark.table("data5035_week07.trial_metrics")

status = (
    metrics.withColumn("pipeline_status", F.lit("PUBLISHED"))
    .withColumn("published_ts", F.current_timestamp())
    .withColumn("run_date", F.lit(run_date))
)

manifest = spark.createDataFrame(
    [(run_date, "trial_daily_status", "PUBLISHED")],
    ["run_date", "target_table", "status"],
).withColumn("created_ts", F.current_timestamp())

status.write.format("delta").mode("overwrite").saveAsTable("data5035_week07.trial_daily_status")
manifest.write.format("delta").mode("append").saveAsTable("data5035_week07.trial_publish_manifest")

print(f"Wrote {status.count()} rows to data5035_week07.trial_daily_status")
print("Appended manifest row to data5035_week07.trial_publish_manifest")
