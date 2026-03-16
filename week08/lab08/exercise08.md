# Exercise 08 — Data Serving Models for Manufacturing Cost Analysis

**DATA 5035 | 20 Points**

---

## Scenario

You are a data engineer at the same pharmaceutical manufacturer from this week's lab. The quality team has their serving layer built — normalized tables, a star schema for batch performance, flattened ML features, operational alerts, and vector-enabled notes. Now the finance team wants the same treatment for **production cost data**.

Every batch the company produces generates costs across four categories:

- **Direct materials** — the raw ingredients consumed during manufacturing. Tablet batches (Cardiolex) use three materials: active pharmaceutical ingredient, microcrystalline cellulose, and magnesium stearate. Sterile drops (OptiClear) use two: API and sterile saline. Ointment batches (DermaSmooth) use three: API, white petrolatum, and a preservative blend. Material prices fluctuate by supplier and lot, and the Columbus BioCenter's sterile materials tend to cost more per unit than solid-dose ingredients at St. Louis.

- **Direct labor** — operator hours logged against each batch. Each facility staffs differently: St. Louis Plant A runs two shifts of 4 operators on its tablet compression line; Columbus BioCenter requires 6 operators for its aseptic fill-finish process (gowning, environmental monitoring, and fill operations); Raleigh South uses 3 operators for ointment mixing and filling. Labor rates vary by shift (day shift at $45/hr, night shift at $52/hr) and by facility (Columbus pays a 10% sterile premium). Batch B-10454 required 14 additional overtime hours for rework after its two QC failures.

- **Manufacturing overhead** — allocated costs for equipment depreciation, utilities, cleanroom certification, and environmental monitoring. Overhead is allocated per batch based on the number of hours the production line was occupied. The sterile fill line at Columbus has the highest hourly overhead rate ($320/hr) because of its ISO 5 cleanroom requirements. St. Louis runs at $180/hr and Raleigh at $210/hr. B-10454's 13.5-hour completion time (versus the 12-hour standard for sterile batches) drove excess overhead, and the 9-day QA hold added cold-storage holding costs.

- **Quality control testing** — the cost of each QC test performed. Routine tests (assay, dissolution, viscosity) cost $150–$300 each. Sterility tests cost $800 because they require a 14-day incubation period. When a batch fails a test, the investigation and retest add $1,200–$2,500 per failure. B-10454 incurred two failure investigations (fill volume and assay), adding roughly $4,000 in unplanned QC costs on top of its routine testing.

The CFO wants to understand cost per batch, cost per unit produced, and cost variance from standard — broken down by facility, product line, and cost category. She also wants to know which batches exceeded their standard cost by more than 15% and why. The April 2024 data covers the same 12 batches (B-10450 through B-10461) you worked with in the lab.

---

## Instructions

Using the scenario above, complete the following three deliverables. You are designing the **serving layer only** — assume the source data already exists in a normalized transactional system. You do not need to build ETL processes or load any data.

### Deliverable 1: Bus Matrix (4 points)

Create a Kimball-style bus matrix for the manufacturing cost domain. Your bus matrix should:

- List the business processes (rows) that generate cost data
- List the dimensions (columns) that analysts would use to slice and filter
- Mark which dimensions apply to which business processes
- Include at least **5 dimensions** and at least **2 business processes**

Submit as a table (markdown, spreadsheet, or image).

### Deliverable 2: Star Schema (8 points)

Design a dimensional star schema for manufacturing cost analysis. Provide Snowflake DDL (`CREATE TABLE` statements) for:

- **1 fact table** at a clearly stated grain
- **DIM_DATE** with a surrogate key
- **At least 3 additional dimension tables** (not counting DIM_DATE)

Your fact table must include:

- Foreign keys to all dimension tables
- At least 3 quantitative measures (e.g., cost amounts, quantities, rates)
- A grain statement as a SQL comment at the top of the file

Your dimensions must include:

- Descriptive attributes beyond just the ID and name (at least 3 columns per dimension)
- At least one dimension that denormalizes two source entities into a single lookup table

You do not need to write INSERT statements or load data.

### Deliverable 3: Second Serving Model (8 points)

Choose **one** of the following serving patterns and design it for the cost domain:

| Option | What to Submit |
|--------|---------------|
| **Flattened ML table** | A single wide `CREATE TABLE` with all dimension attributes denormalized onto each row. Include a comment explaining what prediction or analysis this table would support. |
| **Reverse ETL alert table** | A `CREATE TABLE` for cost alerts with a VARIANT column for JSON payloads, plus 3 example JSON documents (as comments or separate text) showing what an alert payload would contain for different cost overrun scenarios. |
| **Vector-enabled notes table** | A `CREATE TABLE` for free-text cost investigation notes with a VECTOR column for embeddings. Include 5 example note texts (as comments) that a cost analyst might write, covering different cost issues. |

You do not need to write INSERT statements, UPDATE statements, or any ETL logic.

---

## Submission

Submit your deliverables as either:

- A single `.sql` file with all DDL and the bus matrix as a comment block, **or**
- A `.sql` file for the DDL plus a separate file (markdown, image, or spreadsheet) for the bus matrix

---

## Rubric

| Deliverable | Criteria | Points |
|---|---|---|
| **Bus Matrix** | Identifies relevant business processes and meaningful dimensions (at least 5 dimensions, at least 2 processes) with correct applicability markings | 4 |
| **Star Schema** | Fact table has a clearly stated grain, foreign keys to all dimensions, and at least 3 quantitative measures. DIM_DATE has a surrogate key. At least 3 non-date dimensions with 3+ descriptive columns each. At least one dimension denormalizes multiple source entities. DDL is valid Snowflake SQL. | 8 |
| **Second Model** | Appropriate structure for the chosen serving pattern. Design reflects the cost domain (not generic). Includes required supporting artifacts (JSON examples, note texts, or ML comment). | 8 |
| **Total** | | **20** |
