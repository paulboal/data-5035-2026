# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import types as T

try:
    dbutils.widgets.text("run_date", "2026-02-21")
    run_date = dbutils.widgets.get("run_date")
except Exception:
    run_date = "2026-02-21"

spark.sql("CREATE DATABASE IF NOT EXISTS data5035_week07")

source_path = "dbfs:/FileStore/data-5035/week07/site_reference.csv"
schema = T.StructType(
    [
        T.StructField("site_id", T.StringType(), True),
        T.StructField("site_name", T.StringType(), True),
        T.StructField("city", T.StringType(), True),
        T.StructField("state", T.StringType(), True),
    ]
)

try:
    df = spark.read.option("header", True).schema(schema).csv(source_path)
except Exception:
    df = spark.createDataFrame(
        [
            ("S001", "Northside Clinical", "St. Louis", "MO"),
            ("S002", "Lakeside Research", "Chicago", "IL"),
            ("S003", "Pine Valley Health", "Austin", "TX"),
        ],
        schema=schema,
    )

out_df = df.withColumn("run_date", F.lit(run_date))
out_df.write.format("delta").mode("overwrite").saveAsTable("data5035_week07.dim_sites")
print(f"Wrote {out_df.count()} rows to data5035_week07.dim_sites")
