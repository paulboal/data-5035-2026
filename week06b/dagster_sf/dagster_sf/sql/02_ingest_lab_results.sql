CREATE DATABASE IF NOT EXISTS {{ database }};
CREATE SCHEMA IF NOT EXISTS {{ database }}.{{ schema }};

CREATE OR REPLACE TABLE {{ database }}.{{ schema }}.bronze_lab_results (
  patient_id STRING,
  site_id STRING,
  lab_date DATE,
  biomarker_a FLOAT,
  biomarker_b FLOAT,
  run_date DATE
);

DELETE FROM {{ database }}.{{ schema }}.bronze_lab_results WHERE run_date = '{{ run_date }}';

INSERT INTO {{ database }}.{{ schema }}.bronze_lab_results
SELECT *
FROM (
  SELECT column1::STRING AS patient_id,
         column2::STRING AS site_id,
         column3::DATE AS lab_date,
         column4::FLOAT AS biomarker_a,
         column5::FLOAT AS biomarker_b,
         '{{ run_date }}'::DATE AS run_date
  FROM VALUES
    ('P1001','S001','{{ run_date }}',1.2,7.1),
    ('P1002','S001','{{ run_date }}',2.3,5.4),
    ('P1003','S002','{{ run_date }}',1.0,9.2),
    ('P1004','S003','{{ run_date }}',3.1,6.0),
    ('P1005','S003','{{ run_date }}',2.8,7.5),
    ('P1006','S004','{{ run_date }}',1.6,4.2),
    ('P1007','S004','{{ run_date }}',3.9,8.8),
    ('P1008','S002','{{ run_date }}',2.1,6.7),
    ('P1009','S005','{{ run_date }}',1.4,5.9),
    ('P1010','S005','{{ run_date }}',2.0,6.1)
);
