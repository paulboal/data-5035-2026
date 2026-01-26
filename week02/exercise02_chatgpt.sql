/**
Take a look at this file.  Write me 20 different "data quality checks" in a single SQL statement. At least 2 for each column.  Assume this data is stored in a Snowflake table called DATA5035.SPRING26.DONATIONS.  The data quality checks should follow this format CASE WHEN <condition> THEN 1 ELSE 0 END AS dq_<check name> where the check detects if the data in that column/row does not look "good".

ChatGPT did most of this very well.  I moved the long select of dq checks into a CTE so I could then add an aggregate at the end.
**/

WITH base AS (
  SELECT
      *
    , TRY_TO_DATE(DATE_OF_BIRTH)                           AS DOB_DT
    , REGEXP_REPLACE(COALESCE(PHONE,''), '[^0-9]', '')     AS PHONE_DIGITS
    , LPAD(TO_VARCHAR(ZIP), 5, '0')                        AS ZIP5
  FROM DATA5035.SPRING26.DONATIONS
),
checks AS (
    SELECT
      *
    
      /* DONATION_ID */
      , CASE WHEN DONATION_ID IS NULL OR DONATION_ID <= 0 THEN 1 ELSE 0 END AS dq_donation_id_missing_or_nonpositive
      , CASE WHEN DONATION_ID > 1000000000 THEN 1 ELSE 0 END AS dq_donation_id_unreasonably_large
    
      /* NAME */
      , CASE WHEN NAME IS NULL OR TRIM(NAME) = '' THEN 1 ELSE 0 END AS dq_name_missing_or_blank
      , CASE WHEN REGEXP_LIKE(NAME, '.*[0-9].*') THEN 1 ELSE 0 END AS dq_name_contains_digits
    
      /* AGE */
      , CASE WHEN AGE IS NULL OR AGE < 0 OR AGE > 120 THEN 1 ELSE 0 END AS dq_age_out_of_range
      , CASE
          WHEN DOB_DT IS NOT NULL
               AND ABS(DATEDIFF('year', DOB_DT, CURRENT_DATE) - AGE) > 1
          THEN 1 ELSE 0
        END AS dq_age_mismatch_with_dob
    
      /* DATE_OF_BIRTH */
      , CASE WHEN DATE_OF_BIRTH IS NULL OR TRIM(DATE_OF_BIRTH) = '' OR DOB_DT IS NULL THEN 1 ELSE 0 END AS dq_dob_missing_or_unparseable
      , CASE WHEN DOB_DT IS NOT NULL AND DOB_DT > CURRENT_DATE THEN 1 ELSE 0 END AS dq_dob_in_future
    
      /* STREET_ADDRESS */
      , CASE WHEN STREET_ADDRESS IS NULL OR TRIM(STREET_ADDRESS) = '' THEN 1 ELSE 0 END AS dq_street_address_missing_or_blank
      , CASE WHEN STREET_ADDRESS IS NOT NULL AND NOT REGEXP_LIKE(TRIM(STREET_ADDRESS), '^[0-9]+') THEN 1 ELSE 0 END AS dq_street_address_missing_leading_number
    
      /* CITY */
      , CASE WHEN CITY IS NULL OR TRIM(CITY) = '' THEN 1 ELSE 0 END AS dq_city_missing_or_blank
      , CASE WHEN CITY IS NOT NULL AND REGEXP_LIKE(CITY, '.*[0-9].*') THEN 1 ELSE 0 END AS dq_city_contains_digits
    
      /* STATE */
      , CASE WHEN STATE IS NULL OR TRIM(STATE) = '' THEN 1 ELSE 0 END AS dq_state_missing_or_blank
      , CASE
          WHEN STATE IS NULL THEN 1
          WHEN UPPER(TRIM(STATE)) NOT IN (
            'AL','AK','AZ','AR','CA','CO','CT','DE','DC','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME',
            'MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI',
            'SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY'
          )
          THEN 1 ELSE 0
        END AS dq_state_not_valid_usps_code
    
      /* ZIP */
      , CASE WHEN ZIP IS NULL THEN 1 ELSE 0 END AS dq_zip_missing
      , CASE WHEN ZIP IS NOT NULL AND LENGTH(ZIP5) <> 5 THEN 1 ELSE 0 END AS dq_zip_not_5_digits
    
      /* PHONE */
      , CASE WHEN PHONE IS NULL OR TRIM(PHONE) = '' THEN 1 ELSE 0 END AS dq_phone_missing_or_blank
      , CASE
          WHEN PHONE IS NULL THEN 1
          WHEN LENGTH(PHONE_DIGITS) NOT IN (10,11) THEN 1
          WHEN LENGTH(PHONE_DIGITS) = 11 AND LEFT(PHONE_DIGITS,1) <> '1' THEN 1
          ELSE 0
        END AS dq_phone_not_valid_10_or_11_digits
    
      /* CATEGORY */
      , CASE WHEN CATEGORY IS NULL OR TRIM(CATEGORY) = '' THEN 1 ELSE 0 END AS dq_category_missing_or_blank
      , CASE
          WHEN CATEGORY IS NULL THEN 1
          WHEN TRIM(CATEGORY) IN ('Unknown') THEN 1
          WHEN TRIM(CATEGORY) NOT IN ('Sports','Miscellaneous','Health','Animal Welfare','Education','Arts','Community Service','Religious')
          THEN 1 ELSE 0
        END AS dq_category_not_in_expected_set
    
      /* ORGANIZATION */
      , CASE WHEN ORGANIZATION IS NULL OR TRIM(ORGANIZATION) = '' THEN 1 ELSE 0 END AS dq_organization_missing_or_blank
      , CASE WHEN ORGANIZATION IS NOT NULL AND LENGTH(TRIM(ORGANIZATION)) < 3 THEN 1 ELSE 0 END AS dq_organization_too_short
    
      /* AMOUNT */
      , CASE WHEN AMOUNT IS NULL THEN 1 ELSE 0 END AS dq_amount_missing
      , CASE WHEN AMOUNT IS NOT NULL AND (AMOUNT <= 0 OR AMOUNT > 100000) THEN 1 ELSE 0 END AS dq_amount_nonpositive_or_unreasonably_large
    
    FROM base
)
SELECT
    COUNT(1) as total_rows,
    SUM(dq_donation_id_missing_or_nonpositive) as dq_donation_id_missing_or_nonpositive,
    SUM(dq_donation_id_unreasonably_large) as dq_donation_id_unreasonably_large,
    SUM(dq_name_missing_or_blank) as dq_name_missing_or_blank,
    SUM(dq_name_contains_digits) as dq_name_contains_digits,
    SUM(dq_age_out_of_range) as dq_age_out_of_range,
    SUM(dq_age_mismatch_with_dob) as dq_age_mismatch_with_dob,
    SUM(dq_dob_missing_or_unparseable) as dq_dob_missing_or_unparseable,
    SUM(dq_dob_in_future) as dq_dob_in_future,
    SUM(dq_street_address_missing_or_blank) as dq_street_address_missing_or_blank,
    SUM(dq_street_address_missing_leading_number) as dq_street_address_missing_leading_number,
    SUM(dq_city_missing_or_blank) as dq_city_missing_or_blank,
    SUM(dq_city_contains_digits) as dq_city_contains_digits,
    SUM(dq_state_missing_or_blank) as dq_state_missing_or_blank,
    SUM(dq_state_not_valid_usps_code) as dq_state_not_valid_usps_code,
    SUM(dq_zip_missing) as dq_zip_missing,
    SUM(dq_zip_not_5_digits) as dq_zip_not_5_digits,
    SUM(dq_phone_missing_or_blank) as dq_phone_missing_or_blank,
    SUM(dq_phone_not_valid_10_or_11_digits) as dq_phone_not_valid_10_or_11_digits,
    SUM(dq_category_missing_or_blank) as dq_category_missing_or_blank,
    SUM(dq_category_not_in_expected_set) as dq_category_not_in_expected_set,
    SUM(dq_organization_missing_or_blank) as dq_organization_missing_or_blank,
    SUM(dq_organization_too_short) as dq_organization_too_short,
    SUM(dq_amount_missing) as dq_amount_missing,
    SUM(dq_amount_nonpositive_or_unreasonably_large) as dq_amount_nonpositive_or_unreasonably_large
FROM checks;


/** GRADING BY CHATGPT
Review the data quality checks below.  Please rate each on a scale of 1 (not very useful) to 5 (very insightful) in terms of how much value does that check have for a human analyst in their understanding of the data and the data's usefulness in a business context.  Another way to think about this evaluation is "would this data quality check save a human analyst a lot of time when they need to use this data for the first time?"

NUMBER OF CHECKS: 24
AVERAGE SCORE: 3.75

column_name	check_name	score
DONATION_ID	dq_donation_id_missing_or_nonpositive	5
DONATION_ID	dq_donation_id_unreasonably_large	2
NAME	dq_name_missing_or_blank	4
NAME	dq_name_contains_digits	3
AGE	dq_age_out_of_range	4
AGE	dq_age_mismatch_with_dob	5
DATE_OF_BIRTH	dq_dob_missing_or_unparseable	4
DATE_OF_BIRTH	dq_dob_in_future	3
STREET_ADDRESS	dq_street_address_missing_or_blank	4
STREET_ADDRESS	dq_street_address_missing_leading_number	2
CITY	dq_city_missing_or_blank	3
CITY	dq_city_contains_digits	2
STATE	dq_state_missing_or_blank	4
STATE	dq_state_not_valid_usps_code	5
ZIP	dq_zip_missing	4
ZIP	dq_zip_not_5_digits	4
PHONE	dq_phone_missing_or_blank	3
PHONE	dq_phone_not_valid_10_or_11_digits	4
CATEGORY	dq_category_missing_or_blank	4
CATEGORY	dq_category_not_in_expected_set	5
ORGANIZATION	dq_organization_missing_or_blank	4
ORGANIZATION	dq_organization_too_short	2
AMOUNT	dq_amount_missing	5
AMOUNT	dq_amount_nonpositive_or_unreasonably_large	5
**/