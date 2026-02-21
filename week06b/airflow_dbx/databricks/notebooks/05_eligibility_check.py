# Databricks notebook source
from pyspark.sql import functions as F

try:
    dbutils.widgets.text("run_date", "2026-02-21")
    run_date = dbutils.widgets.get("run_date")
except Exception:
    run_date = "2026-02-21"

spark.sql("CREATE DATABASE IF NOT EXISTS data5035_week07")

screenings = spark.table("data5035_week07.silver_patient_screenings")

eligible = (
    screenings.filter((F.col("age").between(18, 75)) & (F.col("consent_flag") == F.lit("Y")))
    .withColumn("eligibility_status", F.lit("eligible"))
    .withColumn("run_date", F.lit(run_date))
)

eligible.write.format("delta").mode("overwrite").saveAsTable("data5035_week07.eligible_patients")
print(f"Wrote {eligible.count()} rows to data5035_week07.eligible_patients")
