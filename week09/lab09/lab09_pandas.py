import pandas as pd
from pathlib import Path

data_dir = Path(__file__).parent

patient = pd.read_csv(data_dir / "patient.csv")
encounter = pd.read_csv(data_dir / "encounter.csv")
diagnosis = pd.read_csv(data_dir / "diagnosis.csv")

# INNER JOIN: Only patients that have encounters
print("=== INNER JOIN ===")
inner = (
    encounter
    .merge(patient, on="PATIENT_SK", how="inner")
    .merge(diagnosis, on="DIAGNOSIS_ID", how="inner")
    [["FIRST_NAME", "LAST_NAME", "ADDRESS", "CITY", "STATE",
      "ENCOUNTER_DATE", "ENCOUNTER_TYPE", "DIAGNOSIS_CODE", "DESCRIPTION"]]
)
print(inner.to_string(index=False))
print()

# LEFT JOIN: All patients, even those without encounters
print("=== LEFT JOIN ===")
left = (
    patient
    .merge(encounter, on="PATIENT_SK", how="left")
    .merge(diagnosis, on="DIAGNOSIS_ID", how="left")
    [["FIRST_NAME", "LAST_NAME", "ADDRESS", "CITY", "STATE", "IS_CURRENT",
      "ENCOUNTER_DATE", "ENCOUNTER_TYPE", "DIAGNOSIS_CODE", "DESCRIPTION"]]
)
print(left.to_string(index=False))
print()

# RIGHT JOIN: All encounters, even those without matching patients
print("=== RIGHT JOIN ===")
right = (
    patient
    .merge(encounter, on="PATIENT_SK", how="right")
    .merge(diagnosis, on="DIAGNOSIS_ID", how="inner")
    [["ENCOUNTER_ID", "ENCOUNTER_DATE", "ENCOUNTER_TYPE",
      "FIRST_NAME", "LAST_NAME", "ADDRESS", "CITY", "STATE",
      "DIAGNOSIS_CODE", "DESCRIPTION"]]
)
print(right.to_string(index=False))
print()

# FULL OUTER JOIN: Data quality check for orphan patients and encounters
print("=== FULL OUTER JOIN (Data Quality) ===")
full = patient.merge(encounter, on="PATIENT_SK", how="outer")
full["ISSUE_TYPE"] = None
full.loc[full["PATIENT_ID"].isna(), "ISSUE_TYPE"] = "Encounter without patient"
full.loc[full["ENCOUNTER_ID"].isna(), "ISSUE_TYPE"] = "Patient without encounter"
quality = (
    full[full["ISSUE_TYPE"].notna()]
    [["ISSUE_TYPE", "PATIENT_SK", "PATIENT_ID", "FIRST_NAME", "LAST_NAME",
      "ENCOUNTER_ID", "ENCOUNTER_DATE"]]
)
print(quality.to_string(index=False))
print()

# Current patient info joined to encounters and diagnoses
print("=== CURRENT PATIENT INFO WITH ENCOUNTERS ===")
current_patient = patient[patient["IS_CURRENT"] == True][
    ["PATIENT_ID", "FIRST_NAME", "LAST_NAME", "ADDRESS", "CITY", "STATE", "ZIP_CODE"]
]
current = (
    encounter
    .merge(patient[["PATIENT_SK", "PATIENT_ID"]], on="PATIENT_SK", how="inner")
    .merge(current_patient, on="PATIENT_ID", how="inner")
    .merge(diagnosis, on="DIAGNOSIS_ID", how="inner")
    [["FIRST_NAME", "LAST_NAME", "ADDRESS", "CITY", "STATE", "ZIP_CODE",
      "ENCOUNTER_ID", "ENCOUNTER_DATE", "ENCOUNTER_TYPE",
      "DIAGNOSIS_CODE", "DESCRIPTION"]]
)
print(current.to_string(index=False))
