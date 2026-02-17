/*******************************************************************************
 * UNIT TESTS FOR BUDGET MONITORING STORED PROCEDURES
 * Schema: DATA5035.INSTRUCTOR1
 * 
 * These tests validate the three stored procedures:
 *   1. CALC_DEPT_LABOR - Calculate prorated department labor costs
 *   2. CALC_DEPT_TO_BUDGET - Compare actual vs budget
 *   3. NOTIFY_DEPT - Generate notifications for over-budget departments
 ******************************************************************************/

-- Set context
USE DATABASE DATA5035;
USE SCHEMA INSTRUCTOR1;

/*******************************************************************************
 * TEST 1: CALC_DEPT_LABOR
 * 
 * Tests:
 *   - Full year salary for employees hired before the target year
 *   - Prorated salary for employees hired mid-year
 *   - Zero salary for employees hired after the target year
 ******************************************************************************/

-- 1a. Create test data tables
CREATE OR REPLACE TEMPORARY TABLE TEST_EMPLOYEES (
    name VARCHAR(100),
    position VARCHAR(50),
    department VARCHAR(50),
    hire_date DATE
);

CREATE OR REPLACE TEMPORARY TABLE TEST_POSITIONS (
    position VARCHAR(50),
    annual_salary NUMBER(12,2)
);

-- 1b. Insert test data with known values
-- Employee A: Hired before 2025, should get full salary ($100,000)
-- Employee B: Hired July 1, 2025, should get ~half salary (184/365 * $100,000 = $50,410.96)
-- Employee C: Hired in 2026, should get $0 for 2025
INSERT INTO TEST_EMPLOYEES VALUES
    ('Employee A', 'Engineer', 'Engineering', '2020-01-15'),
    ('Employee B', 'Engineer', 'Engineering', '2025-07-01'),
    ('Employee C', 'Analyst', 'Analytics', '2026-03-01');

INSERT INTO TEST_POSITIONS VALUES
    ('Engineer', 100000.00),
    ('Analyst', 80000.00);

-- 1c. Execute the procedure
CALL CALC_DEPT_LABOR(
    'DATA5035.INSTRUCTOR1.TEST_EMPLOYEES',
    'DATA5035.INSTRUCTOR1.TEST_POSITIONS',
    2025,
    'DATA5035.INSTRUCTOR1.TEST_LABOR_OUTPUT'
);

-- 1d. Validate results
SELECT '=== TEST 1: CALC_DEPT_LABOR Results ===' AS test_section;

-- Expected: Engineering should have ~$150,410.96 (100000 + 50410.96)
-- Expected: Analytics should have $0 (hired in 2026)
SELECT 
    department,
    total_salary,
    employee_count,
    CASE 
        WHEN department = 'Engineering' AND total_salary BETWEEN 150400 AND 150420 THEN 'PASS'
        WHEN department = 'Analytics' AND total_salary = 0 THEN 'PASS'
        ELSE 'FAIL - Check proration logic'
    END AS test_result
FROM TEST_LABOR_OUTPUT
ORDER BY department;

-- 1e. Detailed proration check for mid-year hire
-- July 1, 2025 to Dec 31, 2025 = 184 days
-- 184/365 * 100000 = 50,410.96
SELECT 
    'Mid-year proration check' AS test_name,
    CASE 
        WHEN ABS(total_salary - 150410.96) < 1 THEN 'PASS'
        ELSE 'FAIL - Expected ~150410.96, got ' || total_salary::VARCHAR
    END AS test_result
FROM TEST_LABOR_OUTPUT
WHERE department = 'Engineering';


/*******************************************************************************
 * TEST 2: CALC_DEPT_TO_BUDGET
 * 
 * Tests:
 *   - OVER status when actual > budget
 *   - UNDER status when actual < budget
 *   - MATCHED status when actual = budget
 ******************************************************************************/

-- 2a. Create test data
CREATE OR REPLACE TEMPORARY TABLE TEST_ACTUAL (
    department VARCHAR(50),
    total_salary NUMBER(12,2),
    employee_count INTEGER,
    calculation_year INTEGER,
    calculated_at TIMESTAMP_NTZ
);

CREATE OR REPLACE TEMPORARY TABLE TEST_BUDGET (
    department VARCHAR(50),
    annual_budget NUMBER(12,2)
);

-- 2b. Insert test data with known outcomes
INSERT INTO TEST_ACTUAL VALUES
    ('Dept_Over', 150000.00, 2, 2025, CURRENT_TIMESTAMP()),
    ('Dept_Under', 80000.00, 1, 2025, CURRENT_TIMESTAMP()),
    ('Dept_Match', 100000.00, 1, 2025, CURRENT_TIMESTAMP());

INSERT INTO TEST_BUDGET VALUES
    ('Dept_Over', 100000.00),   -- actual 150k > budget 100k = OVER
    ('Dept_Under', 120000.00),  -- actual 80k < budget 120k = UNDER
    ('Dept_Match', 100000.00);  -- actual 100k = budget 100k = MATCHED

-- 2c. Execute the procedure
CALL CALC_DEPT_TO_BUDGET(
    'DATA5035.INSTRUCTOR1.TEST_ACTUAL',
    'DATA5035.INSTRUCTOR1.TEST_BUDGET',
    'DATA5035.INSTRUCTOR1.TEST_BUDGET_OUTPUT'
);

-- 2d. Validate results
SELECT '=== TEST 2: CALC_DEPT_TO_BUDGET Results ===' AS test_section;

SELECT 
    department,
    actual_salary,
    budgeted_salary,
    variance,
    status,
    CASE 
        WHEN department = 'Dept_Over' AND status = 'OVER' THEN 'PASS'
        WHEN department = 'Dept_Under' AND status = 'UNDER' THEN 'PASS'
        WHEN department = 'Dept_Match' AND status = 'MATCHED' THEN 'PASS'
        ELSE 'FAIL - Incorrect status'
    END AS test_result
FROM TEST_BUDGET_OUTPUT
ORDER BY department;

-- 2e. Variance calculation check
SELECT 
    'Variance calculation check' AS test_name,
    CASE 
        WHEN (SELECT variance FROM TEST_BUDGET_OUTPUT WHERE department = 'Dept_Over') = -50000 THEN 'PASS'
        ELSE 'FAIL - Expected variance of -50000 for Dept_Over'
    END AS test_result;


/*******************************************************************************
 * TEST 3: NOTIFY_DEPT
 * 
 * Tests:
 *   - Only OVER budget departments generate notifications
 *   - Email address parameter is correctly applied
 *   - Subject line and body are properly formatted
 ******************************************************************************/

-- 3a. Create test data (reuse structure from TEST 2 output)
CREATE OR REPLACE TEMPORARY TABLE TEST_REPORT (
    department VARCHAR(50),
    actual_salary NUMBER(12,2),
    budgeted_salary NUMBER(12,2),
    variance NUMBER(12,2),
    status VARCHAR(10),
    calculated_at TIMESTAMP_NTZ
);

INSERT INTO TEST_REPORT VALUES
    ('Over_Dept_1', 150000.00, 100000.00, -50000.00, 'OVER', CURRENT_TIMESTAMP()),
    ('Over_Dept_2', 200000.00, 180000.00, -20000.00, 'OVER', CURRENT_TIMESTAMP()),
    ('Under_Dept', 80000.00, 120000.00, 40000.00, 'UNDER', CURRENT_TIMESTAMP()),
    ('Match_Dept', 100000.00, 100000.00, 0.00, 'MATCHED', CURRENT_TIMESTAMP());

-- 3b. Execute the procedure
CALL NOTIFY_DEPT(
    'DATA5035.INSTRUCTOR1.TEST_REPORT',
    'test@example.com',
    'DATA5035.INSTRUCTOR1.TEST_NOTIFY_OUTPUT'
);

-- 3c. Validate results
SELECT '=== TEST 3: NOTIFY_DEPT Results ===' AS test_section;

-- Should only have 2 rows (the OVER departments)
SELECT 
    'Row count check' AS test_name,
    CASE 
        WHEN (SELECT COUNT(*) FROM TEST_NOTIFY_OUTPUT) = 2 THEN 'PASS'
        ELSE 'FAIL - Expected 2 rows, got ' || (SELECT COUNT(*) FROM TEST_NOTIFY_OUTPUT)::VARCHAR
    END AS test_result;

-- Email address should match parameter
SELECT 
    'Email parameter check' AS test_name,
    CASE 
        WHEN (SELECT COUNT(DISTINCT email_address) FROM TEST_NOTIFY_OUTPUT WHERE email_address = 'test@example.com') = 1 THEN 'PASS'
        ELSE 'FAIL - Email address not correctly applied'
    END AS test_result;

-- Check notification content
SELECT 
    department,
    email_address,
    subject_line,
    LEFT(email_body, 100) || '...' AS email_body_preview,
    CASE 
        WHEN email_address = 'test@example.com' 
            AND subject_line LIKE '%BUDGET ALERT%' 
            AND email_body LIKE '%exceeded%' THEN 'PASS'
        ELSE 'FAIL - Check notification formatting'
    END AS test_result
FROM TEST_NOTIFY_OUTPUT
ORDER BY department;


/*******************************************************************************
 * TEST 4: INTEGRATION TEST - Full Pipeline
 * 
 * Tests the complete workflow using the actual source tables
 ******************************************************************************/

SELECT '=== TEST 4: Integration Test ===' AS test_section;

-- 4a. Run the full pipeline with actual data
CALL CALC_DEPT_LABOR(
    'DATA5035.SPRING26.EMPLOYEES',
    'DATA5035.SPRING26.POSITIONS',
    2025,
    'DATA5035.INSTRUCTOR1.DEPT_LABOR_ACTUAL'
);

CALL CALC_DEPT_TO_BUDGET(
    'DATA5035.INSTRUCTOR1.DEPT_LABOR_ACTUAL',
    'DATA5035.SPRING26.DEPARTMENTS',
    'DATA5035.INSTRUCTOR1.DEPT_BUDGET_STATUS'
);

CALL NOTIFY_DEPT(
    'DATA5035.INSTRUCTOR1.DEPT_BUDGET_STATUS',
    'pboal@wustl.edu',
    'DATA5035.INSTRUCTOR1.DEPT_NOTIFICATIONS'
);

-- 4b. Validate pipeline results
SELECT 
    'Labor calculation' AS pipeline_step,
    COUNT(*) AS row_count,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS test_result
FROM DEPT_LABOR_ACTUAL
UNION ALL
SELECT 
    'Budget comparison' AS pipeline_step,
    COUNT(*) AS row_count,
    CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS test_result
FROM DEPT_BUDGET_STATUS
UNION ALL
SELECT 
    'Notifications' AS pipeline_step,
    COUNT(*) AS row_count,
    CASE WHEN COUNT(*) >= 0 THEN 'PASS' ELSE 'FAIL' END AS test_result  -- 0 is OK if no over-budget
FROM DEPT_NOTIFICATIONS;

-- 4c. Show final results
SELECT '--- Labor Actual ---' AS section;
SELECT * FROM DEPT_LABOR_ACTUAL ORDER BY department;

SELECT '--- Budget Status ---' AS section;
SELECT * FROM DEPT_BUDGET_STATUS ORDER BY department;

SELECT '--- Notifications (Over Budget Only) ---' AS section;
SELECT department, email_address, subject_line FROM DEPT_NOTIFICATIONS ORDER BY department;


/*******************************************************************************
 * CLEANUP - Remove temporary test tables
 ******************************************************************************/
DROP TABLE IF EXISTS TEST_EMPLOYEES;
DROP TABLE IF EXISTS TEST_POSITIONS;
DROP TABLE IF EXISTS TEST_LABOR_OUTPUT;
DROP TABLE IF EXISTS TEST_ACTUAL;
DROP TABLE IF EXISTS TEST_BUDGET;
DROP TABLE IF EXISTS TEST_BUDGET_OUTPUT;
DROP TABLE IF EXISTS TEST_REPORT;
DROP TABLE IF EXISTS TEST_NOTIFY_OUTPUT;

SELECT 'All unit tests completed!' AS status;
