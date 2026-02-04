{{ config(materialized='table') }}

WITH flattened_checks AS (
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
        f.value:check_fail::INT AS check_fail,
        logged_at
    FROM {{ ref('data_quality_checks') }},
    LATERAL FLATTEN(input => quality_checks) f
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
    logged_at
FROM flattened_checks
WHERE check_fail = 1
ORDER BY DONATION_ID, check_name
