# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import types as T

try:
    dbutils.widgets.text("run_date", "2026-02-21")
    run_date = dbutils.widgets.get("run_date")
except Exception:
    run_date = "2026-02-21"

spark.sql("CREATE DATABASE IF NOT EXISTS data5035_week07")

source_path = "dbfs:/FileStore/data-5035/week07/lab_results.csv"
schema = T.StructType(
    [
        T.StructField("patient_id", T.StringType(), True),
        T.StructField("site_id", T.StringType(), True),
        T.StructField("lab_date", T.StringType(), True),
        T.StructField("biomarker_a", T.DoubleType(), True),
        T.StructField("biomarker_b", T.DoubleType(), True),
    ]
)

try:
    df = spark.read.option("header", True).schema(schema).csv(source_path)
except Exception:
    df = spark.createDataFrame(
        [
            ("P1001", "S001", run_date, 1.2, 7.1),
            ("P1002", "S001", run_date, 2.3, 5.4),
            ("P1003", "S002", run_date, 1.0, 9.2),
            ("P1004", "S003", run_date, 3.1, 6.0),
        ],
        schema=schema,
    )

out_df = df.withColumn("run_date", F.lit(run_date))
out_df.write.format("delta").mode("overwrite").saveAsTable("data5035_week07.bronze_lab_results")
print(f"Wrote {out_df.count()} rows to data5035_week07.bronze_lab_results")
