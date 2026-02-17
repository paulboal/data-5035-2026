/*******************************************************************************
 * Stored Procedure: NOTIFY_DEPT
 * Schema: DATA5035.INSTRUCTOR1
 * 
 * Generate email notifications for departments that are OVER budget.
 * 
 * Parameters:
 *   - report_table: Fully qualified table name for budget comparison report (from CALC_DEPT_TO_BUDGET)
 *   - email_address: Email address to use for notifications
 *   - output_table: Fully qualified table name for notification records
 * 
 * Output Columns:
 *   - department, actual_salary, budgeted_salary, variance, status (from input)
 *   - email_address (from parameter)
 *   - subject_line (generated)
 *   - email_body (generated)
 *   - created_at (timestamp)
 ******************************************************************************/

CREATE OR REPLACE PROCEDURE DATA5035.INSTRUCTOR1.NOTIFY_DEPT(
    REPORT_TABLE VARCHAR,
    EMAIL_ADDRESS VARCHAR,
    OUTPUT_TABLE VARCHAR
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, lit, concat, concat_ws, current_timestamp, abs as sp_abs
from snowflake.snowpark.types import StringType

def run(session, report_table: str, email_address: str, output_table: str) -> str:
    """
    Generate email notifications for departments that are OVER budget.
    
    Outputs a dataframe with:
    - All input columns (department, actual, budget, status)
    - Email address (from parameter)
    - Subject line
    - Email body explaining the situation
    """
    
    # Read input dataframe and filter to OVER budget only
    report_df = session.table(report_table).filter(col("STATUS") == "OVER")
    
    # Check if there are any over-budget departments
    over_count = report_df.count()
    if over_count == 0:
        # Create empty result table with correct schema
        session.sql(f"""
            CREATE OR REPLACE TABLE {output_table} (
                department VARCHAR(50),
                actual_salary NUMBER(12,2),
                budgeted_salary NUMBER(12,2),
                variance NUMBER(12,2),
                status VARCHAR(10),
                email_address VARCHAR(255),
                subject_line VARCHAR(500),
                email_body VARCHAR(10000),
                created_at TIMESTAMP_NTZ
            )
        """).collect()
        return "NOTIFY_DEPT completed: No departments over budget - no notifications generated"
    
    # Build notification columns
    result_df = report_df.with_column(
        "EMAIL_ADDRESS", lit(email_address)
    ).with_column(
        "SUBJECT_LINE",
        concat(lit("BUDGET ALERT: "), col("DEPARTMENT"), lit(" Department Over Budget"))
    ).with_column(
        "EMAIL_BODY",
        concat(
            lit("Budget Alert for "), col("DEPARTMENT"), lit(" Department\\n\\n"),
            lit("This is an automated notification that the "), col("DEPARTMENT"),
            lit(" department has exceeded its budget.\\n\\n"),
            lit("Budget Details:\\n"),
            lit("  - Budgeted Amount: $"), col("BUDGETED_SALARY").cast(StringType()), lit("\\n"),
            lit("  - Actual Spend: $"), col("ACTUAL_SALARY").cast(StringType()), lit("\\n"),
            lit("  - Over Budget By: $"), sp_abs(col("VARIANCE")).cast(StringType()), lit("\\n\\n"),
            lit("Please review and take appropriate action.\\n\\n"),
            lit("This is an automated message from the Budget Monitoring System.")
        )
    ).with_column(
        "CREATED_AT", current_timestamp()
    )
    
    # Write to output table
    result_df.write.mode("overwrite").save_as_table(output_table)
    
    return f"NOTIFY_DEPT completed: {over_count} notification(s) generated for over-budget departments"
';
