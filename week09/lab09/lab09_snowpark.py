from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, when, is_null

session = Session.builder.config("connection_name", "DATA-5035-2026").create()
session.use_schema("DATA5035.SPRING26")

patient = session.table("patient")
encounter = session.table("encounter")
diagnosis = session.table("diagnosis")

# INNER JOIN: Only patients that have encounters
print("=== INNER JOIN ===")
encounter.join(
    patient, encounter["patient_sk"] == patient["patient_sk"]
).join(
    diagnosis, encounter["diagnosis_id"] == diagnosis["diagnosis_id"]
).select(
    patient["first_name"],
    patient["last_name"],
    patient["address"],
    patient["city"],
    patient["state"],
    encounter["encounter_date"],
    encounter["encounter_type"],
    diagnosis["diagnosis_code"],
    diagnosis["description"]
).show()

# LEFT JOIN: All patients, even those without encounters
print("=== LEFT JOIN ===")
patient.join(
    encounter, patient["patient_sk"] == encounter["patient_sk"], "left"
).join(
    diagnosis, encounter["diagnosis_id"] == diagnosis["diagnosis_id"], "left"
).select(
    patient["first_name"],
    patient["last_name"],
    patient["address"],
    patient["city"],
    patient["state"],
    patient["is_current"],
    encounter["encounter_date"],
    encounter["encounter_type"],
    diagnosis["diagnosis_code"],
    diagnosis["description"]
).show()

# RIGHT JOIN: All encounters, even those without matching patients
print("=== RIGHT JOIN ===")
patient.join(
    encounter, patient["patient_sk"] == encounter["patient_sk"], "right"
).join(
    diagnosis, encounter["diagnosis_id"] == diagnosis["diagnosis_id"]
).select(
    encounter["encounter_id"],
    encounter["encounter_date"],
    encounter["encounter_type"],
    patient["first_name"],
    patient["last_name"],
    patient["address"],
    patient["city"],
    patient["state"],
    diagnosis["diagnosis_code"],
    diagnosis["description"]
).show()

# FULL OUTER JOIN: Data quality check for orphan patients and encounters
print("=== FULL OUTER JOIN (Data Quality) ===")
patient.join(
    encounter, patient["patient_sk"] == encounter["patient_sk"], "full"
).select(
    when(is_null(patient["patient_sk"]), "Encounter without patient")
    .when(is_null(encounter["encounter_id"]), "Patient without encounter")
    .alias("issue_type"),
    patient["patient_sk"],
    patient["patient_id"],
    patient["first_name"],
    patient["last_name"],
    encounter["encounter_id"],
    encounter["encounter_date"],
    encounter["patient_sk"].alias("encounter_patient_sk")
).filter(
    is_null(patient["patient_sk"]) | is_null(encounter["encounter_id"])
).show()

session.close()
