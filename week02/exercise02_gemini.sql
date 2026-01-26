/**
Take a look at this file.  Write me 20 different "data quality checks" in a single SQL statement. At least 2 for each column.  Assume this data is stored in a Snowflake table called DATA5035.SPRING26.DONATIONS.  The data quality checks should follow this format CASE WHEN <condition> THEN 1 ELSE 0 END AS dq_<check name> where the check detects if the data in that column/row does not look "good".

Gemini was kind of lazy about the first check. They're all just NULL checks.
**/


WITH checks AS (
  SELECT 
    *,
    -- DONATION_ID checks
    CASE WHEN DONATION_ID IS NULL THEN 1 ELSE 0 END AS dq_donation_id_null,
    CASE WHEN DONATION_ID <= 0 THEN 1 ELSE 0 END AS dq_donation_id_non_positive,

    -- NAME checks
    CASE WHEN NAME IS NULL OR TRIM(NAME) = '' THEN 1 ELSE 0 END AS dq_name_missing,
    CASE WHEN NAME REGEXP '.*[0-9].*' THEN 1 ELSE 0 END AS dq_name_has_numbers,

    -- AGE checks
    CASE WHEN AGE IS NULL THEN 1 ELSE 0 END AS dq_age_null,
    CASE WHEN AGE < 0 OR AGE > 120 THEN 1 ELSE 0 END AS dq_age_invalid_range,

    -- DATE_OF_BIRTH checks
    CASE WHEN DATE_OF_BIRTH IS NULL THEN 1 ELSE 0 END AS dq_dob_null,
    CASE WHEN TRY_TO_DATE(DATE_OF_BIRTH::STRING, 'MM/DD/YY') IS NULL 
          AND TRY_TO_DATE(DATE_OF_BIRTH::STRING, 'MM/DD/YYYY') IS NULL THEN 1 ELSE 0 END AS dq_dob_unparseable,

    -- STREET_ADDRESS checks
    CASE WHEN STREET_ADDRESS IS NULL OR TRIM(STREET_ADDRESS) = '' THEN 1 ELSE 0 END AS dq_street_null,
    CASE WHEN LENGTH(TRIM(STREET_ADDRESS)) < 5 THEN 1 ELSE 0 END AS dq_street_too_short,

    -- CITY checks
    CASE WHEN CITY IS NULL OR TRIM(CITY) = '' THEN 1 ELSE 0 END AS dq_city_null,
    CASE WHEN CITY REGEXP '.*[0-9].*' THEN 1 ELSE 0 END AS dq_city_has_numbers,

    -- STATE checks
    CASE WHEN STATE IS NULL OR TRIM(STATE) = '' THEN 1 ELSE 0 END AS dq_state_null,
    CASE WHEN LENGTH(TRIM(STATE)) != 2 THEN 1 ELSE 0 END AS dq_state_not_two_chars,

    -- ZIP checks
    CASE WHEN ZIP IS NULL THEN 1 ELSE 0 END AS dq_zip_null,
    CASE WHEN LENGTH(CAST(ZIP AS STRING)) NOT IN (5, 9) THEN 1 ELSE 0 END AS dq_zip_invalid_length,

    -- PHONE checks
    CASE WHEN PHONE IS NULL OR TRIM(PHONE) = '' THEN 1 ELSE 0 END AS dq_phone_null,
    CASE WHEN LENGTH(REGEXP_REPLACE(PHONE, '[^0-9]', '')) < 10 THEN 1 ELSE 0 END AS dq_phone_insufficient_digits,

    -- CATEGORY checks
    CASE WHEN CATEGORY IS NULL OR TRIM(CATEGORY) IN ('', 'N/A', 'Unknown', ' ') THEN 1 ELSE 0 END AS dq_category_missing_or_placeholder,
    CASE WHEN LENGTH(CATEGORY) > 50 THEN 1 ELSE 0 END AS dq_category_too_long,

    -- ORGANIZATION checks
    CASE WHEN ORGANIZATION IS NULL OR TRIM(ORGANIZATION) = '' THEN 1 ELSE 0 END AS dq_org_null,
    CASE WHEN LENGTH(TRIM(ORGANIZATION)) < 3 THEN 1 ELSE 0 END AS dq_org_too_short,

    -- AMOUNT checks
    CASE WHEN AMOUNT IS NULL THEN 1 ELSE 0 END AS dq_amount_null
FROM DATA5035.SPRING26.DONATIONS)
SELECT
    COUNT(1) as total_rows,
    SUM(dq_donation_id_null) as donation_id_null_count,
    SUM(dq_donation_id_non_positive) as donation_id_non_positive_count,
    SUM(dq_name_missing) as name_missing_count,
    SUM(dq_name_has_numbers) as name_has_numbers_count,
    SUM(dq_age_null) as age_null_count,
    SUM(dq_age_invalid_range) as age_invalid_range_count,
    SUM(dq_dob_null) as dob_null_count,
    SUM(dq_dob_unparseable) as dob_unparseable_count,
    SUM(dq_street_null) as street_null_count,
    SUM(dq_street_too_short) as street_too_short_count,
    SUM(dq_city_null) as city_null_count,
    SUM(dq_city_has_numbers) as city_has_numbers_count,
    SUM(dq_state_null) as state_null_count,
    SUM(dq_state_not_two_chars) as state_not_two_chars_count,
    SUM(dq_zip_null) as zip_null_count,
    SUM(dq_zip_invalid_length) as zip_invalid_length_count,
    SUM(dq_phone_null) as phone_null_count,
    SUM(dq_phone_insufficient_digits) as phone_insufficient_digits_count,
    SUM(dq_category_missing_or_placeholder) as category_missing_count,
    SUM(dq_category_too_long) as category_too_long_count,
    SUM(dq_org_null) as org_null_count,
    SUM(dq_org_too_short) as org_too_short_count,
    SUM(dq_amount_null) as amount_null_count
FROM checks;


/** GRADING BY CHATGPT
Review the data quality checks below.  Please rate each on a scale of 1 (not very useful) to 5 (very insightful) in terms of how much value does that check have for a human analyst in their understanding of the data and the data's usefulness in a business context.  Another way to think about this evaluation is "would this data quality check save a human analyst a lot of time when they need to use this data for the first time?"

NUMBER OF CHECKS: 23
AVERAGE SCORE: 3.57

column_name	check_name	score
DONATION_ID	Null (DONATION_ID IS NULL)	5
DONATION_ID	Non-positive (DONATION_ID <= 0)	4
NAME	Missing/blank (NAME IS NULL OR TRIM(NAME)=’’)	5
NAME	Contains numbers (NAME REGEXP ‘.[0-9].’)	3
AGE	Null (AGE IS NULL)	3
AGE	Invalid range (<0 or >120)	4
DATE_OF_BIRTH	Null (DATE_OF_BIRTH IS NULL)	3
DATE_OF_BIRTH	Unparseable date format	4
STREET_ADDRESS	Missing/blank	3
STREET_ADDRESS	Too short (<5 chars)	2
CITY	Missing/blank	3
CITY	Contains numbers	2
STATE	Missing/blank	4
STATE	Not 2 chars	4
ZIP	Null	3
ZIP	Invalid length (not 5 or 9)	4
PHONE	Missing/blank	3
PHONE	<10 digits after stripping non-numbers	4
CATEGORY	Missing / placeholder (’’ / N/A / Unknown / etc.)	5
CATEGORY	Too long (>50)	2
ORGANIZATION	Missing/blank	4
ORGANIZATION	Too short (<3 chars)	2
AMOUNT	Null	5
**/