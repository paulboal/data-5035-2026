CREATE DATABASE IF NOT EXISTS {{ database }};
CREATE SCHEMA IF NOT EXISTS {{ database }}.{{ schema }};

CREATE OR REPLACE TABLE {{ database }}.{{ schema }}.bronze_patient_screenings (
  patient_id STRING,
  site_id STRING,
  screening_date DATE,
  age NUMBER,
  sex STRING,
  consent_flag STRING,
  run_date DATE
);

DELETE FROM {{ database }}.{{ schema }}.bronze_patient_screenings WHERE run_date = '{{ run_date }}';

INSERT INTO {{ database }}.{{ schema }}.bronze_patient_screenings
SELECT *
FROM (
  SELECT column1::STRING AS patient_id,
         column2::STRING AS site_id,
         column3::DATE AS screening_date,
         column4::NUMBER AS age,
         column5::STRING AS sex,
         column6::STRING AS consent_flag,
         '{{ run_date }}'::DATE AS run_date
  FROM VALUES
    ('P1001','S001','{{ run_date }}',41,'F','Y'),
    ('P1002','S001','{{ run_date }}',55,'M','Y'),
    ('P1003','S002','{{ run_date }}',27,'F','N'),
    ('P1004','S003','{{ run_date }}',67,'M','Y'),
    ('P1005','S003','{{ run_date }}',39,'F','Y'),
    ('P1006','S004','{{ run_date }}',19,'M','Y'),
    ('P1007','S004','{{ run_date }}',77,'F','Y'),
    ('P1008','S002','{{ run_date }}',62,'M','Y'),
    ('P1009','S005','{{ run_date }}',33,'F','Y'),
    ('P1010','S005','{{ run_date }}',48,'M','N')
);
