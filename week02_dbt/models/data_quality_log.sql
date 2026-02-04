{{ config(materialized='table') }}

WITH source_data AS (
    SELECT 
        *,
        COUNT(*) OVER (PARTITION BY DONATION_ID) AS donation_id_count
    FROM DATA5035.SPRING26.DONATIONS
),

quality_checks AS (
    SELECT
        DONATION_ID,
        NAME,
        AGE,
        DATE_OF_BIRTH,
        STREET_ADDRESS,
        CITY,
        STATE,
        ZIP,
        PHONE,
        CATEGORY,
        ORGANIZATION,
        AMOUNT,
        OBJECT_CONSTRUCT(
            'donation_id_null', {{ check_null('DONATION_ID') }},
            'name_null', {{ check_null('NAME') }},
            'age_null', {{ check_null('AGE') }},
            'date_of_birth_null', {{ check_null('DATE_OF_BIRTH') }},
            'amount_null', {{ check_null('AMOUNT') }},
            'phone_null', {{ check_null('PHONE') }},
            'state_null', {{ check_null('STATE') }},
            'zip_null', {{ check_null('ZIP') }},
            'donation_id_unique', {{ check_unique('DONATION_ID', 'donation_id_count') }},
            'amount_positive', {{ check_positive('AMOUNT') }},
            'age_valid', {{ check_valid_age('AGE') }},
            'date_of_birth_valid', {{ check_valid_date('DATE_OF_BIRTH') }},
            'zip_valid', {{ check_valid_zip('ZIP') }},
            'phone_valid', {{ check_valid_phone('PHONE') }},
            'state_valid', {{ check_valid_state('STATE') }},
            'name_not_empty', {{ check_not_empty('NAME') }},
            'city_not_empty', {{ check_not_empty('CITY') }},
            'street_address_not_empty', {{ check_not_empty('STREET_ADDRESS') }}
        ) AS all_checks
    FROM source_data
),

flattened_checks AS (
    SELECT
        DONATION_ID,
        NAME,
        AGE,
        DATE_OF_BIRTH,
        STREET_ADDRESS,
        CITY,
        STATE,
        ZIP,
        PHONE,
        CATEGORY,
        ORGANIZATION,
        AMOUNT,
        f.key AS check_name,
        f.value:column_name::STRING AS flagged_column,
        f.value:check_type::STRING AS issue_type,
        f.value:original_value::STRING AS flagged_value,
        f.value:check_fail::INT AS check_fail
    FROM quality_checks,
    LATERAL FLATTEN(input => all_checks) f
)

SELECT
    DONATION_ID,
    check_name,
    flagged_column,
    issue_type,
    flagged_value,
    NAME,
    AGE,
    DATE_OF_BIRTH,
    STREET_ADDRESS,
    CITY,
    STATE,
    ZIP,
    PHONE,
    CATEGORY,
    ORGANIZATION,
    AMOUNT,
    CURRENT_TIMESTAMP() AS logged_at
FROM flattened_checks
WHERE check_fail = 1
ORDER BY DONATION_ID, check_name
