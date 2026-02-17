/*******************************************************************************
 * Stored Procedure: CALC_DEPT_TO_BUDGET
 * Schema: DATA5035.INSTRUCTOR1
 * 
 * Compare actual department labor costs against budget.
 * 
 * Parameters:
 *   - actual_table: Fully qualified table name for actual labor costs (from CALC_DEPT_LABOR)
 *   - budget_table: Fully qualified table name for department budgets
 *   - output_table: Fully qualified table name for comparison results
 * 
 * Output Status Values:
 *   - OVER: actual > budget
 *   - UNDER: actual < budget
 *   - MATCHED: actual == budget
 ******************************************************************************/

CREATE OR REPLACE PROCEDURE DATA5035.INSTRUCTOR1.CALC_DEPT_TO_BUDGET(
    ACTUAL_TABLE VARCHAR,
    BUDGET_TABLE VARCHAR,
    OUTPUT_TABLE VARCHAR
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, when, lit, current_timestamp
from snowflake.snowpark.types import DecimalType

def run(session, actual_table: str, budget_table: str, output_table: str) -> str:
    """
    Compare actual department labor costs against budget.
    
    Outputs a status indicator:
    - OVER: actual > budget
    - UNDER: actual < budget  
    - MATCHED: actual == budget
    """
    
    # Read input dataframes
    actual_df = session.table(actual_table)
    budget_df = session.table(budget_table)
    
    # Join actual with budget on department
    joined_df = actual_df.join(
        budget_df,
        actual_df["DEPARTMENT"] == budget_df["DEPARTMENT"],
        "inner"
    ).select(
        actual_df["DEPARTMENT"],
        actual_df["TOTAL_SALARY"].alias("ACTUAL_SALARY"),
        budget_df["ANNUAL_BUDGET"].alias("BUDGETED_SALARY")
    )
    
    # Calculate variance and status
    result_df = joined_df.with_column(
        "VARIANCE",
        (col("BUDGETED_SALARY") - col("ACTUAL_SALARY")).cast(DecimalType(12, 2))
    ).with_column(
        "STATUS",
        when(col("ACTUAL_SALARY") > col("BUDGETED_SALARY"), lit("OVER"))
        .when(col("ACTUAL_SALARY") < col("BUDGETED_SALARY"), lit("UNDER"))
        .otherwise(lit("MATCHED"))
    ).with_column(
        "CALCULATED_AT", current_timestamp()
    )
    
    # Write to output table
    result_df.write.mode("overwrite").save_as_table(output_table)
    
    # Count by status for return message
    status_counts = session.table(output_table).group_by("STATUS").count().collect()
    status_summary = ", ".join([f"{row[''STATUS'']}: {row[''COUNT'']}" for row in status_counts])
    
    return f"CALC_DEPT_TO_BUDGET completed: {status_summary}"
';
