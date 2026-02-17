/*******************************************************************************
 * Task: TASK_CALC_DEPT_TO_BUDGET
 * Schema: DATA5035.INSTRUCTOR1
 * 
 * Second task in the budget monitoring task graph.
 * Runs after TASK_CALC_DEPT_LABOR completes.
 * 
 * Calls CALC_DEPT_TO_BUDGET to compare actual labor costs against
 * department budgets and determine OVER/UNDER/MATCHED status.
 * 
 * Task Graph:
 *   TASK_CALC_DEPT_LABOR
 *           |
 *           v
 *   TASK_CALC_DEPT_TO_BUDGET (this task)
 *           |
 *           v
 *   TASK_NOTIFY_DEPT
 ******************************************************************************/

CREATE OR REPLACE TASK DATA5035.INSTRUCTOR1.TASK_CALC_DEPT_TO_BUDGET
    WAREHOUSE = SNOWFLAKE_LEARNING_WH
    AFTER DATA5035.INSTRUCTOR1.TASK_CALC_DEPT_LABOR
AS
    CALL DATA5035.INSTRUCTOR1.CALC_DEPT_TO_BUDGET(
        'DATA5035.INSTRUCTOR1.DEPT_LABOR_ACTUAL',
        'DATA5035.SPRING26.DEPARTMENTS',
        'DATA5035.INSTRUCTOR1.DEPT_BUDGET_STATUS'
    );
