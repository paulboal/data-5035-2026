# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Test Suite: FACT_MONTHLY_SUMMARY
# MAGIC
# MAGIC Each test cell queries `fact_monthly_summary` and raises an `AssertionError`
# MAGIC on failure. Databricks job runs exit non-zero on uncaught exceptions, so the
# MAGIC CI/CD pipeline detects failures automatically.

# COMMAND ----------

dbutils.widgets.text("schema", "default", "Target Schema")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE SCHEMA {schema}")
print(f"Running tests against schema: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: View must exist and return at least one row

# COMMAND ----------

row_count = spark.sql("SELECT COUNT(*) AS cnt FROM fact_monthly_summary").first()["cnt"]
assert row_count > 0, f"FAIL: View is empty or missing (row_count={row_count})"
print(f"PASS: View has {row_count} rows.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: No NULL encounter months

# COMMAND ----------

nulls = spark.sql("""
    SELECT *
    FROM fact_monthly_summary
    WHERE encounter_month IS NULL
""")
null_count = nulls.count()
assert null_count == 0, f"FAIL: Found {null_count} rows with NULL encounter_month"
print("PASS: No NULL encounter months.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3: All counts must be positive

# COMMAND ----------

bad_counts = spark.sql("""
    SELECT *
    FROM fact_monthly_summary
    WHERE encounter_count <= 0
       OR unique_patient_count <= 0
""")
bad_count = bad_counts.count()
assert bad_count == 0, f"FAIL: Found {bad_count} rows with non-positive counts"
print("PASS: All counts are positive.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4: Unique patients must not exceed encounter count

# COMMAND ----------

violations = spark.sql("""
    SELECT *
    FROM fact_monthly_summary
    WHERE unique_patient_count > encounter_count
""")
violation_count = violations.count()
assert violation_count == 0, f"FAIL: Found {violation_count} rows where unique_patient_count > encounter_count"
print("PASS: Unique patient counts are consistent.")

# COMMAND ----------

print("All tests passed.")
