# Databricks notebook source
from pyspark.sql import functions as F

try:
    dbutils.widgets.text("run_date", "2026-02-21")
    run_date = dbutils.widgets.get("run_date")
except Exception:
    run_date = "2026-02-21"

spark.sql("CREATE DATABASE IF NOT EXISTS data5035_week07")

bronze_screenings = spark.table("data5035_week07.bronze_patient_screenings")
bronze_labs = spark.table("data5035_week07.bronze_lab_results")

silver_screenings = (
    bronze_screenings.withColumn("age", F.col("age").cast("int"))
    .withColumn("screening_date", F.to_date(F.col("screening_date")))
    .withColumn("sex", F.coalesce(F.col("sex"), F.lit("U")))
    .withColumn("consent_flag", F.upper(F.coalesce(F.col("consent_flag"), F.lit("N"))))
    .dropDuplicates(["patient_id", "site_id", "screening_date"])
    .withColumn("run_date", F.lit(run_date))
)

silver_labs = (
    bronze_labs.withColumn("lab_date", F.to_date(F.col("lab_date")))
    .withColumn("biomarker_a", F.coalesce(F.col("biomarker_a"), F.lit(0.0)))
    .withColumn("biomarker_b", F.coalesce(F.col("biomarker_b"), F.lit(0.0)))
    .dropDuplicates(["patient_id", "site_id", "lab_date"])
    .withColumn("run_date", F.lit(run_date))
)

silver_screenings.write.format("delta").mode("overwrite").saveAsTable(
    "data5035_week07.silver_patient_screenings"
)
silver_labs.write.format("delta").mode("overwrite").saveAsTable("data5035_week07.silver_lab_results")

print(f"Wrote {silver_screenings.count()} rows to silver_patient_screenings")
print(f"Wrote {silver_labs.count()} rows to silver_lab_results")
