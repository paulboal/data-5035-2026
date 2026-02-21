# Databricks notebook source
from pyspark.sql import functions as F

try:
    dbutils.widgets.text("run_date", "2026-02-21")
    run_date = dbutils.widgets.get("run_date")
except Exception:
    run_date = "2026-02-21"

spark.sql("CREATE DATABASE IF NOT EXISTS data5035_week07")

labs = spark.table("data5035_week07.silver_lab_results")

scored = (
    labs.withColumn(
        "risk_score_raw",
        F.col("biomarker_a") * F.lit(20.0) + F.col("biomarker_b") * F.lit(8.0),
    )
    .withColumn("risk_score", F.round(F.least(F.lit(100.0), F.greatest(F.lit(0.0), F.col("risk_score_raw"))), 2))
    .drop("risk_score_raw")
    .withColumn("run_date", F.lit(run_date))
)

scored.write.format("delta").mode("overwrite").saveAsTable("data5035_week07.patient_risk_scores")
print(f"Wrote {scored.count()} rows to data5035_week07.patient_risk_scores")
