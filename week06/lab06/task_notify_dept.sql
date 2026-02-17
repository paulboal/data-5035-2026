/*******************************************************************************
 * Task: TASK_NOTIFY_DEPT
 * Schema: DATA5035.INSTRUCTOR1
 * 
 * Final task in the budget monitoring task graph.
 * Runs after TASK_CALC_DEPT_TO_BUDGET completes.
 * 
 * Calls NOTIFY_DEPT to generate email notifications for any departments
 * that are over budget.
 * 
 * Task Graph:
 *   TASK_CALC_DEPT_LABOR
 *           |
 *           v
 *   TASK_CALC_DEPT_TO_BUDGET
 *           |
 *           v
 *   TASK_NOTIFY_DEPT (this task)
 ******************************************************************************/

CREATE OR REPLACE TASK DATA5035.INSTRUCTOR1.TASK_NOTIFY_DEPT
    WAREHOUSE = SNOWFLAKE_LEARNING_WH
    AFTER DATA5035.INSTRUCTOR1.TASK_CALC_DEPT_TO_BUDGET
AS
    CALL DATA5035.INSTRUCTOR1.NOTIFY_DEPT(
        'DATA5035.INSTRUCTOR1.DEPT_BUDGET_STATUS',
        'pboal@wustl.edu',
        'DATA5035.INSTRUCTOR1.DEPT_NOTIFICATIONS'
    );
