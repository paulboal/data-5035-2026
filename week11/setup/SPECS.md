# Week 10 Lab: Data Normalization and SCD Type 2


You are helping me generate realistic synthetic data for a data engineering lab focused on normalization and slowly changing dimensions (SCD Type 2).

## SCENARIO

A drone drives across a field collecting soil radiation measurements. It has:

- 1 GPS unit
- 3 radiation detectors (Type A, B, C)

Each measurement includes both sensor readings AND metadata about the calibration state of each device at the time of measurement.

## INPUT TABLE STRUCTURE

Generate data with the following columns:

- Collection Timestamp
- GPS Lat
- GPS Lng
- GPS Unit Number
- GPS Unit Calibration Timestamp
- GPS Unit Calibration Precision
- Gamma Level (microsieverts per hour)
- Gamma Detector Unit Number
- Gamma Detector Calibration Timestamp
- Gamma Detector Calibration Precision
- Cesium 137 Level (pCi/g)
- Cesium Detector Unit Number
- Cesium Detector Calibration Timestamp
- Cesium Detector Calibration Precision
- Thorium 232 Level (pCi/g)
- Thorium Detector Unit Number
- Thorium Detector Calibration Timestamp
- Thorium Detector Calibration Precision

## DATA GENERATION REQUIREMENTS

### 1. Row Count

- Generate enough rows to cover the full expanse of time in the described time behavior

### 2. Time Behavior

- Measurements occur in 6 hour batches on 5 sequential days
- On each day, measurements occur sequentially approximately every 30 seconds
- All timestamps should be realistic and increasing
- Calibration timestamps must ALWAYS be before the measurement timestamp

### 3. GPS Movement

- Simulate a drone moving across a rectangular field that is 0.1 miles x 0.3 miles
- Lat/Lng should gradually change as if a drone were driving in a back-and-forth motion to cover the entire rectangle one row at a time, moving down to the next row, and so on (not random jumps)
- Lat/Lng may leave some gaps or deviate slightly from the back-and-forth pattern as if the drone is driving around obstacles
- Add slight noise to simulate real GPS drift

### 4. Device Behavior

#### GPS Unit

- Use 1–2 GPS unit numbers across the dataset
- Include at least 2 calibration events
- Calibration precision improves after recalibration (e.g., 1.5 → 0.8 meters)
- Calibration timestamps change in blocks of rows

#### Radiation Detector Units

- Each detector type has its own unit number
- Occasionally swap out a detector mid-run (simulate replacement)
- Each detector has independent calibration cycles
- Precision improves after each recalibration
- Precision should be within a few decimal points on either side of 0.1 for Gama, 1.5 for Cesium 137, and 1.5 for Thorium 232

### 5. Radiation Measurements

Generate realistic soil contamination values:

- Gamma radiation: baseline (0.05 - 0.20 microsievers per hour), variability up to 0.5, one region that starts at 0.8 and declines over time)
- Cesium 137: baseline (0-1 pCi/g), variability up to 5, several regions that spike to 7, but rarely
- Thorium 232: Baseline (0.5-2 pCi/g), variability up to 5, one region that is consistently above 5

Behavior:

- Add small random noise

### 6. Data Patterns for Teaching SCD

VERY IMPORTANT:

- Calibration timestamps and precision should remain constant for a block of rows, then change
- This allows students to detect "dimension changes"
- Ensure overlapping usage periods are clean (no ambiguity)

### 7. Output Requirements

Generate a Snowflake SQL script that includes:

1. CREATE OR REPLACE TABLE statement
2. Proper column data types:

   - TIMESTAMP for timestamps
   - FLOAT for coordinates and measurements
   - STRING or VARCHAR for unit IDs
   - FLOAT for precision values
3. INSERT INTO statement with all rows

### 8. Formatting

- Use clean, readable SQL
- One row per line in the VALUES clause
- Use realistic IDs like:
  - GPS-001, GPS-002
  - RAD-A-01, RAD-B-02, etc.

## GOAL OF THE LAB

This dataset will be used by students to:

- Normalize the data into fact and dimension tables
- Build SCD Type 2 tables for:
  - GPS Unit
  - Radiation Detector Unit
- Create a Measurement fact table

So structure the data to clearly support those transformations.

## OUTPUT ONLY

Return ONLY the SQL script. Do not include explanations.
