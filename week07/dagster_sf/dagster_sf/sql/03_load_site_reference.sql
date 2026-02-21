CREATE DATABASE IF NOT EXISTS {{ database }};
CREATE SCHEMA IF NOT EXISTS {{ database }}.{{ schema }};

CREATE OR REPLACE TABLE {{ database }}.{{ schema }}.dim_sites (
  site_id STRING,
  site_name STRING,
  city STRING,
  state STRING,
  run_date DATE
);

DELETE FROM {{ database }}.{{ schema }}.dim_sites WHERE run_date = '{{ run_date }}';

INSERT INTO {{ database }}.{{ schema }}.dim_sites
SELECT *
FROM (
  SELECT column1::STRING AS site_id,
         column2::STRING AS site_name,
         column3::STRING AS city,
         column4::STRING AS state,
         '{{ run_date }}'::DATE AS run_date
  FROM VALUES
    ('S001','Northside Clinical','St. Louis','MO'),
    ('S002','Lakeside Research','Chicago','IL'),
    ('S003','Pine Valley Health','Austin','TX'),
    ('S004','Riverbend Medical','Columbus','OH'),
    ('S005','Sunrise Trial Center','Phoenix','AZ')
);
