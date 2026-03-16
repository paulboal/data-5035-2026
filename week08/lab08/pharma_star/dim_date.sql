-- =============================================================================
-- PHARMA_STAR.DIM_DATE
-- Purpose: Date dimension for the star schema. Covers all dates referenced
--          by batch order, start, end, and shipping dates (April 2024).
--          Surrogate key uses YYYYMMDD integer format.
--          Dynamic table derived from PHARMA_3NF.BATCH.
-- =============================================================================

USE SCHEMA DATA5035.PHARMA_STAR;

CREATE OR REPLACE DYNAMIC TABLE DIM_DATE
    TARGET_LAG = '1 hour'
    WAREHOUSE = SNOWFLAKE_LEARNING_WH
AS
SELECT
    TO_NUMBER(TO_CHAR(d.full_date, 'YYYYMMDD')) AS date_id,
    d.full_date,
    DECODE(DAYNAME(d.full_date),
        'Mon', 'Monday', 'Tue', 'Tuesday', 'Wed', 'Wednesday',
        'Thu', 'Thursday', 'Fri', 'Friday', 'Sat', 'Saturday',
        'Sun', 'Sunday') AS day_of_week,
    MONTH(d.full_date) AS month_num,
    DECODE(MONTHNAME(d.full_date),
        'Jan', 'January', 'Feb', 'February', 'Mar', 'March',
        'Apr', 'April', 'May', 'May', 'Jun', 'June',
        'Jul', 'July', 'Aug', 'August', 'Sep', 'September',
        'Oct', 'October', 'Nov', 'November', 'Dec', 'December') AS month_name,
    QUARTER(d.full_date) AS quarter_num,
    YEAR(d.full_date) AS year_num
FROM (
    SELECT order_date AS full_date FROM DATA5035.PHARMA_3NF.BATCH
    UNION
    SELECT start_timestamp::DATE FROM DATA5035.PHARMA_3NF.BATCH
    UNION
    SELECT end_timestamp::DATE FROM DATA5035.PHARMA_3NF.BATCH
    UNION
    SELECT shipping_complete_date FROM DATA5035.PHARMA_3NF.BATCH
) d;
