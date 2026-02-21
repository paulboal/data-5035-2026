# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import types as T

try:
    dbutils.widgets.text("run_date", "2026-02-21")
    run_date = dbutils.widgets.get("run_date")
except Exception:
    run_date = "2026-02-21"

spark.sql("CREATE DATABASE IF NOT EXISTS data5035_week07")

source_path = "dbfs:/FileStore/data-5035/week07/patient_screenings.csv"
schema = T.StructType(
    [
        T.StructField("patient_id", T.StringType(), True),
        T.StructField("site_id", T.StringType(), True),
        T.StructField("screening_date", T.StringType(), True),
        T.StructField("age", T.IntegerType(), True),
        T.StructField("sex", T.StringType(), True),
        T.StructField("consent_flag", T.StringType(), True),
    ]
)

try:
    df = spark.read.option("header", True).schema(schema).csv(source_path)
except Exception:
    df = spark.createDataFrame(
        [
            ("P1001", "S001", run_date, 41, "F", "Y"),
            ("P1002", "S001", run_date, 55, "M", "Y"),
            ("P1003", "S002", run_date, 27, "F", "N"),
            ("P1004", "S003", run_date, 67, "M", "Y"),
        ],
        schema=schema,
    )

out_df = df.withColumn("run_date", F.lit(run_date))
out_df.write.format("delta").mode("overwrite").saveAsTable("data5035_week07.bronze_patient_screenings")
print(f"Wrote {out_df.count()} rows to data5035_week07.bronze_patient_screenings")
