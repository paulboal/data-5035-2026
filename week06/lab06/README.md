# Department Budget Monitoring Pipeline

## Original Prompt

> Create a budget monitoring scenario with sample data that includes:
> - **EMPLOYEES** table with name, position, department, and hire_date
> - **POSITIONS** table with position and annual_salary  
> - **DEPARTMENTS** table with department and annual_budget
>
> Design the sample data so that 1 of 3 departments is over budget.
>
> Create a series of Snowpark Python stored procedures in a task graph that will:
> 1. Summarize total salary per department (with day-based proration for mid-year hires)
> 2. Compare actual department labor costs against budget
> 3. Generate a mock email notification if any department is over budget
>
> Requirements:
> - Use DataFrame-based design patterns (read from tables, transform, write to tables)
> - Proration should be by day (employees hired before the year get full salary, mid-year hires get prorated, future hires get $0)
> - Email address should be a parameter to the notification procedure
> - Create SQL unit tests to validate the procedures

---

## Project Description

This project implements an automated department budget monitoring pipeline in Snowflake using Snowpark Python stored procedures and a scheduled task graph.

### Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         SOURCE DATA (SPRING26)                          │
├─────────────────────┬─────────────────────┬─────────────────────────────┤
│     EMPLOYEES       │     POSITIONS       │       DEPARTMENTS           │
│  - name             │  - position         │  - department               │
│  - position         │  - annual_salary    │  - annual_budget            │
│  - department       │                     │                             │
│  - hire_date        │                     │                             │
└──────────┬──────────┴──────────┬──────────┴──────────────┬──────────────┘
           │                     │                         │
           v                     v                         │
┌────────────────────────────────────────────┐             │
│         CALC_DEPT_LABOR                    │             │
│  - Joins employees with positions          │             │
│  - Calculates day-based proration          │             │
│  - Aggregates by department                │             │
└──────────────────────┬─────────────────────┘             │
                       │                                   │
                       v                                   v
              ┌────────────────┐                ┌──────────────────┐
              │DEPT_LABOR_ACTUAL│                │   (budget data)  │
              └────────┬───────┘                └────────┬─────────┘
                       │                                 │
                       └────────────┬────────────────────┘
                                    v
                    ┌───────────────────────────────┐
                    │     CALC_DEPT_TO_BUDGET       │
                    │  - Joins actual vs budget     │
                    │  - Calculates variance        │
                    │  - Assigns status:            │
                    │    OVER / UNDER / MATCHED     │
                    └───────────────┬───────────────┘
                                    │
                                    v
                          ┌─────────────────┐
                          │DEPT_BUDGET_STATUS│
                          └────────┬────────┘
                                   │
                                   v
                    ┌───────────────────────────────┐
                    │        NOTIFY_DEPT            │
                    │  - Filters to OVER only       │
                    │  - Generates email content    │
                    │  - Logs to notifications tbl  │
                    └───────────────┬───────────────┘
                                    │
                                    v
                         ┌───────────────────┐
                         │DEPT_NOTIFICATIONS │
                         └───────────────────┘
```

### Proration Logic

The `CALC_DEPT_LABOR` procedure calculates salaries with day-based proration:

| Hire Date Scenario | Calculation |
|--------------------|-------------|
| Before target year | Full annual salary |
| During target year | `(days_remaining / days_in_year) × annual_salary` |
| After target year | $0 |

**Example for 2025:**
- Hired 2020-01-15 → $100,000 (full year)
- Hired 2025-07-01 → $50,410.96 (184/365 × $100,000)
- Hired 2026-03-01 → $0

### Task Graph

The pipeline runs as a Snowflake task graph scheduled for 6:00 AM Central Time:

```
TASK_CALC_DEPT_LABOR (root - scheduled)
         │
         ▼
TASK_CALC_DEPT_TO_BUDGET (after labor)
         │
         ▼
TASK_NOTIFY_DEPT (after budget)
```

### Files

| File | Description |
|------|-------------|
| **Stored Procedures** | |
| `sp_calc_dept_labor.sql` | Calculates prorated department labor costs |
| `sp_calc_dept_to_budget.sql` | Compares actual labor vs budget |
| `sp_notify_dept.sql` | Generates notifications for over-budget departments |
| **Tasks** | |
| `task_calc_dept_labor.sql` | Root task with CRON schedule |
| `task_calc_dept_to_budget.sql` | Predecessor: TASK_CALC_DEPT_LABOR |
| `task_notify_dept.sql` | Predecessor: TASK_CALC_DEPT_TO_BUDGET |
| **Tests** | |
| `unit_tests_stored_procedures.sql` | Comprehensive unit tests for all procedures |

### Output Tables

| Table | Description |
|-------|-------------|
| `DEPT_LABOR_ACTUAL` | Department totals with employee count and calculation year |
| `DEPT_BUDGET_STATUS` | Actual vs budget comparison with OVER/UNDER/MATCHED status |
| `DEPT_NOTIFICATIONS` | Email notification records for over-budget departments |

### Usage

**Deploy the stored procedures:**
```sql
-- Run each SP script in order
@sp_calc_dept_labor.sql
@sp_calc_dept_to_budget.sql
@sp_notify_dept.sql
```

**Deploy and enable the task graph:**
```sql
-- Create tasks (order matters for predecessors)
@task_calc_dept_labor.sql
@task_calc_dept_to_budget.sql
@task_notify_dept.sql

-- Enable tasks (reverse order - children first)
ALTER TASK DATA5035.INSTRUCTOR1.TASK_NOTIFY_DEPT RESUME;
ALTER TASK DATA5035.INSTRUCTOR1.TASK_CALC_DEPT_TO_BUDGET RESUME;
ALTER TASK DATA5035.INSTRUCTOR1.TASK_CALC_DEPT_LABOR RESUME;
```

**Manual execution:**
```sql
EXECUTE TASK DATA5035.INSTRUCTOR1.TASK_CALC_DEPT_LABOR;
```

**Run unit tests:**
```sql
-- Execute the full test suite
@unit_tests_stored_procedures.sql
```

### Schema

- **Source data:** `DATA5035.SPRING26`
- **Procedures, tasks, output tables:** `DATA5035.INSTRUCTOR1`
- **Warehouse:** `SNOWFLAKE_LEARNING_WH`
