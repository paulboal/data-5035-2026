/**
Take a look at DATA5035.SPRING26.DONATIONS..  Write me 20 different "data quality checks" in a single SQL statement. At least 2 for each column. The data quality checks should follow this format CASE WHEN <condition> THEN 1 ELSE 0 END AS dq_<check name> where the check detects if the data in that column/row does not look "good".

Snowflake's built in AI SQL writer did similarly well.  It had a REGEX error, but clicking "fix" fixed it.
**/


WITH data_quality AS (
  SELECT
  CASE WHEN phone IS NULL OR LENGTH(TRIM(phone)) = 0 THEN 1 ELSE 0 END AS dq_missing_phone,
  CASE WHEN phone NOT REGEXP '^[0-9()+ -]*$' THEN 1 ELSE 0 END AS dq_invalid_phone_format,
  CASE WHEN donation_id IS NULL THEN 1 ELSE 0 END AS dq_missing_donation_id,
  CASE WHEN donation_id <= 0 THEN 1 ELSE 0 END AS dq_invalid_donation_id,
  CASE WHEN date_of_birth IS NULL THEN 1 ELSE 0 END AS dq_missing_dob,
  CASE WHEN date_of_birth > CURRENT_DATE() THEN 1 ELSE 0 END AS dq_future_dob,
  CASE WHEN name IS NULL OR LENGTH(TRIM(name)) = 0 THEN 1 ELSE 0 END AS dq_missing_name,
  CASE WHEN name NOT REGEXP '^[A-Za-z. ]*$' THEN 1 ELSE 0 END AS dq_invalid_name_chars,
  CASE WHEN age IS NULL THEN 1 ELSE 0 END AS dq_missing_age,
  CASE WHEN age < 0 OR age > 120 THEN 1 ELSE 0 END AS dq_invalid_age_range,
  CASE WHEN zip IS NULL THEN 1 ELSE 0 END AS dq_missing_zip,
  CASE WHEN LENGTH(CAST(zip AS VARCHAR)) != 5 THEN 1 ELSE 0 END AS dq_invalid_zip_length,
  CASE WHEN category IS NULL OR LENGTH(TRIM(category)) = 0 THEN 1 ELSE 0 END AS dq_missing_category,
  CASE WHEN category NOT IN ('Business', 'Individual', 'Organization', 'Non-Profit') THEN 1 ELSE 0 END AS dq_invalid_category,
  CASE WHEN street_address IS NULL OR LENGTH(TRIM(street_address)) = 0 THEN 1 ELSE 0 END AS dq_missing_address,
  CASE WHEN LENGTH(street_address) < 5 THEN 1 ELSE 0 END AS dq_short_address,
  CASE WHEN amount IS NULL THEN 1 ELSE 0 END AS dq_missing_amount,
  CASE WHEN amount <= 0 THEN 1 ELSE 0 END AS dq_negative_amount,
  CASE WHEN state IS NULL OR LENGTH(TRIM(state)) = 0 THEN 1 ELSE 0 END AS dq_missing_state,
  CASE WHEN LENGTH(state) != 2 THEN 1 ELSE 0 END AS dq_invalid_state_format
FROM DATA5035.SPRING26.DONATIONS)
SELECT 
  COUNT(1) as total_rows,
  SUM(dq_missing_phone) as total_missing_phone,
  SUM(dq_invalid_phone_format) as total_invalid_phone_format,
  SUM(dq_missing_donation_id) as total_missing_donation_id,
  SUM(dq_invalid_donation_id) as total_invalid_donation_id,
  SUM(dq_missing_dob) as total_missing_dob,
  SUM(dq_future_dob) as total_future_dob,
  SUM(dq_missing_name) as total_missing_name,
  SUM(dq_invalid_name_chars) as total_invalid_name_chars,
  SUM(dq_missing_age) as total_missing_age,
  SUM(dq_invalid_age_range) as total_invalid_age_range,
  SUM(dq_missing_zip) as total_missing_zip,
  SUM(dq_invalid_zip_length) as total_invalid_zip_length,
  SUM(dq_missing_category) as total_missing_category,
  SUM(dq_invalid_category) as total_invalid_category,
  SUM(dq_missing_address) as total_missing_address,
  SUM(dq_short_address) as total_short_address,
  SUM(dq_missing_amount) as total_missing_amount,
  SUM(dq_negative_amount) as total_negative_amount,
  SUM(dq_missing_state) as total_missing_state,
  SUM(dq_invalid_state_format) as total_invalid_state_format
FROM data_quality;




/**
Review the data quality checks below.  Please rate each on a scale of 1 (not very useful) to 5 (very insightful) in terms of how much value does that check have for a human analyst in their understanding of the data and the data's usefulness in a business context.  Another way to think about this evaluation is "would this data quality check save a human analyst a lot of time when they need to use this data for the first time?"

NUMBER OF CHECKS: 20
AVERAGE SCORE: 3.55

column_name	check_name	score
phone	Missing phone (NULL/blank)	3
phone	Invalid phone format (non phone chars)	2
donation_id	Missing donation_id (NULL)	5
donation_id	Invalid donation_id (<= 0)	4
date_of_birth	Missing DOB (NULL)	3
date_of_birth	Future DOB (> current_date)	4
name	Missing name (NULL/blank)	4
name	Invalid name characters (non alpha/space/period)	2
age	Missing age (NULL)	3
age	Invalid age range (<0 or >120)	4
zip	Missing ZIP (NULL)	3
zip	Invalid ZIP length (!= 5)	3
category	Missing category (NULL/blank)	4
category	Invalid category (not in allowed list)	4
street_address	Missing address (NULL/blank)	3
street_address	Short address (< 5 chars)	2
amount	Missing amount (NULL)	5
amount	Negative/zero amount (<= 0)	5
state	Missing state (NULL/blank)	3
state	Invalid state format (length != 2)	3
**/