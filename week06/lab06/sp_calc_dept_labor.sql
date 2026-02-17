/*******************************************************************************
 * Stored Procedure: CALC_DEPT_LABOR
 * Schema: DATA5035.INSTRUCTOR1
 * 
 * Calculate department labor costs with day-based proration for mid-year hires.
 * 
 * Parameters:
 *   - employees_table: Fully qualified table name for employee data
 *   - positions_table: Fully qualified table name for position/salary data
 *   - year: The calculation year for proration
 *   - output_table: Fully qualified table name for results
 * 
 * Proration Logic:
 *   - Employees hired before the specified year get full annual salary
 *   - Employees hired during the year get (days_remaining/days_in_year) * salary
 *   - Employees hired after the year contribute $0
 ******************************************************************************/

CREATE OR REPLACE PROCEDURE DATA5035.INSTRUCTOR1.CALC_DEPT_LABOR(
    EMPLOYEES_TABLE VARCHAR,
    POSITIONS_TABLE VARCHAR,
    YEAR NUMBER(38,0),
    OUTPUT_TABLE VARCHAR
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS OWNER
AS '
from snowflake.snowpark.functions import col, sum as sp_sum, count, lit, when, datediff, to_date, current_timestamp
from snowflake.snowpark.types import DecimalType
from datetime import date

def run(session, employees_table: str, positions_table: str, year: int, output_table: str) -> str:
    """
    Calculate department labor costs with day-based proration for mid-year hires.
    
    - Employees hired before the specified year get full annual salary
    - Employees hired during the specified year get prorated salary based on days worked
    - Employees hired after the specified year contribute $0
    """
    
    # Read input dataframes
    employees_df = session.table(employees_table)
    positions_df = session.table(positions_table)
    
    # Define year boundaries
    year_start = f"{year}-01-01"
    year_end = f"{year}-12-31"
    days_in_year = 366 if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)) else 365
    
    # Join employees with positions
    joined_df = employees_df.join(
        positions_df,
        employees_df["POSITION"] == positions_df["POSITION"],
        "inner"
    ).select(
        employees_df["NAME"],
        employees_df["DEPARTMENT"],
        employees_df["HIRE_DATE"],
        positions_df["ANNUAL_SALARY"]
    )
    
    # Calculate prorated salary based on hire date
    # - If hired before year start: full salary (days_in_year / days_in_year)
    # - If hired during year: (days from hire to year end + 1) / days_in_year
    # - If hired after year end: 0
    prorated_df = joined_df.with_column(
        "DAYS_WORKED",
        when(
            col("HIRE_DATE") <= lit(year_start),
            lit(days_in_year)
        ).when(
            col("HIRE_DATE") <= lit(year_end),
            datediff("day", col("HIRE_DATE"), lit(year_end)) + 1
        ).otherwise(lit(0))
    ).with_column(
        "PRORATED_SALARY",
        (col("ANNUAL_SALARY") * col("DAYS_WORKED") / lit(days_in_year)).cast(DecimalType(12, 2))
    )
    
    # Summarize by department
    result_df = prorated_df.group_by("DEPARTMENT").agg(
        sp_sum("PRORATED_SALARY").alias("TOTAL_SALARY"),
        count("*").alias("EMPLOYEE_COUNT")
    ).with_column(
        "CALCULATION_YEAR", lit(year)
    ).with_column(
        "CALCULATED_AT", current_timestamp()
    )
    
    # Write to output table
    result_df.write.mode("overwrite").save_as_table(output_table)
    
    row_count = session.table(output_table).count()
    return f"CALC_DEPT_LABOR completed: {row_count} departments processed for year {year}"
';
