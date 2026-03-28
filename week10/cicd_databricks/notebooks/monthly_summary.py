# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # FACT_MONTHLY_SUMMARY
# MAGIC
# MAGIC Creates a view that aggregates encounter and unique patient counts by month
# MAGIC from the `patient` and `encounter` tables.

# COMMAND ----------

dbutils.widgets.text("schema", "default", "Target Schema")
schema = dbutils.widgets.get("schema")
print(f"Deploying to schema: {schema}")

# COMMAND ----------

spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE VIEW fact_monthly_summary AS
    SELECT
      DATE_TRUNC('MONTH', e.encounter_date) AS encounter_month,
      COUNT(e.encounter_id) AS encounter_count,
      COUNT(DISTINCT p.patient_id) AS unique_patient_count
    FROM
      encounter e INNER JOIN
      patient p ON e.patient_sk = p.patient_sk
    GROUP BY
      encounter_month
    ORDER BY
      encounter_month
""")

print("View fact_monthly_summary created successfully.")

# COMMAND ----------

# Quick sanity check -- display the view contents
display(spark.sql("SELECT * FROM fact_monthly_summary"))
