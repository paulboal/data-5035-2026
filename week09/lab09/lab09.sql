USE SCHEMA data5035.spring26;

-- INNER JOIN: Only patients that have encounters
SELECT
  p.first_name,
  p.last_name,
  p.address,
  p.city,
  p.state,
  e.encounter_date,
  e.encounter_type,
  d.diagnosis_code,
  d.description
FROM
  encounter e INNER JOIN
  patient p ON e.patient_sk = p.patient_sk INNER JOIN
  diagnosis d ON e.diagnosis_id = d.diagnosis_id;

-- LEFT JOIN: All patients, even those without encounters
SELECT
  p.first_name,
  p.last_name,
  p.address,
  p.city,
  p.state,
  p.is_current,
  e.encounter_date,
  e.encounter_type,
  d.diagnosis_code,
  d.description
FROM
  patient p LEFT OUTER JOIN
  encounter e ON p.patient_sk = e.patient_sk LEFT OUTER JOIN
  diagnosis d ON e.diagnosis_id = d.diagnosis_id;

-- RIGHT JOIN: All encounters, even those without matching patients
SELECT
  e.encounter_id,
  e.encounter_date,
  e.encounter_type,
  p.first_name,
  p.last_name,
  p.address,
  p.city,
  p.state,
  d.diagnosis_code,
  d.description
FROM
  patient p RIGHT OUTER JOIN
  encounter e ON p.patient_sk = e.patient_sk INNER JOIN
  diagnosis d ON e.diagnosis_id = d.diagnosis_id;

-- FULL OUTER JOIN: Data quality check for orphan patients and encounters
SELECT
  CASE
    WHEN p.patient_sk IS NULL THEN 'Encounter without patient'
    WHEN e.encounter_id IS NULL THEN 'Patient without encounter'
  END AS issue_type,
  p.patient_sk,
  p.patient_id,
  p.first_name,
  p.last_name,
  e.encounter_id,
  e.encounter_date,
  e.patient_sk AS encounter_patient_sk
FROM
  patient p FULL OUTER JOIN
  encounter e ON p.patient_sk = e.patient_sk
WHERE
  p.patient_sk IS NULL
  OR e.encounter_id IS NULL;

-- Current patient info joined to encounters and diagnoses
-- Joins encounter to its patient version, then to the current
-- SCD2 record via patient_id (business key) for current demographics
SELECT
  pc.first_name,
  pc.last_name,
  pc.address,
  pc.city,
  pc.state,
  pc.zip_code,
  e.encounter_id,
  e.encounter_date,
  e.encounter_type,
  d.diagnosis_code,
  d.description
FROM
  encounter e INNER JOIN
  patient p ON e.patient_sk = p.patient_sk INNER JOIN
  patient pc ON p.patient_id = pc.patient_id AND pc.is_current = TRUE INNER JOIN
  diagnosis d ON e.diagnosis_id = d.diagnosis_id;

-- Alternative SCD2 join using date ranges instead of surrogate key
-- Matches each encounter to the patient version that was active on that date
SELECT
  p.first_name,
  p.last_name,
  p.address,
  p.city,
  p.state,
  p.zip_code,
  e.encounter_id,
  e.encounter_date,
  e.encounter_type,
  d.diagnosis_code,
  d.description
FROM
  encounter_alt e INNER JOIN
  patient_alt p ON e.patient_id = p.patient_id
    AND e.encounter_date BETWEEN p.effective_start_date AND p.effective_end_date INNER JOIN
  diagnosis d ON e.diagnosis_id = d.diagnosis_id;

-- ASOF join: Matches each encounter to the most recent patient version
-- that was effective on or before the encounter date
-- ** Always returns only a single matched row **
SELECT
  p.first_name,
  p.last_name,
  p.address,
  p.city,
  p.state,
  p.zip_code,
  e.encounter_id,
  e.encounter_date,
  e.encounter_type,
  d.diagnosis_code,
  d.description
FROM
  encounter_alt e
  ASOF JOIN patient_alt p
    MATCH_CONDITION (e.encounter_date >= p.effective_start_date)
    ON e.patient_id = p.patient_id INNER JOIN
  diagnosis d ON e.diagnosis_id = d.diagnosis_id;
