# Databricks notebook source
from pyspark.sql import functions as F

try:
    dbutils.widgets.text("run_date", "2026-02-21")
    run_date = dbutils.widgets.get("run_date")
except Exception:
    run_date = "2026-02-21"

spark.sql("CREATE DATABASE IF NOT EXISTS data5035_week07")

eligible = spark.table("data5035_week07.eligible_patients")
risk = spark.table("data5035_week07.patient_risk_scores")

metrics = (
    eligible.join(risk.select("patient_id", "site_id", "risk_score"), ["patient_id", "site_id"], "left")
    .groupBy("site_id")
    .agg(
        F.countDistinct("patient_id").alias("eligible_count"),
        F.round(F.avg("risk_score"), 2).alias("avg_risk_score"),
    )
    .withColumn("run_date", F.lit(run_date))
)

metrics.write.format("delta").mode("overwrite").saveAsTable("data5035_week07.trial_metrics")
print(f"Wrote {metrics.count()} rows to data5035_week07.trial_metrics")
