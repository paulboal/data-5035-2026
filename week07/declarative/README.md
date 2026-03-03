# AI-Scale Data Center Site Selection

A declarative SQL solution using Snowflake geospatial functions to identify optimal parcels for a new AI-scale data center.

## Overview

The view `DATA5035.SPRING26.TOP_DATACENTER_PARCELS` evaluates land parcels against infrastructure proximity, environmental constraints, and economic factors to produce a ranked list of the top 10 candidate sites.

## Source Tables

| Table | Columns | Purpose |
|-------|---------|---------|
| `PARCELS` | parcel_id, geom (GEOGRAPHY), land_cost | Land parcels under consideration |
| `POWER_PLANTS` | plant_id, geom (GEOGRAPHY), capacity_mw | Power generation facilities |
| `FLOODPLAINS` | geom (GEOGRAPHY) | Flood-prone areas to avoid |
| `WETLANDS` | geom (GEOGRAPHY) | Protected wetland areas |
| `FIBER_LINES` | geom (GEOGRAPHY) | Fiber optic backbone infrastructure |
| `PUBLIC_SENTIMENT` | county_id, sentiment_score | Community sentiment by county |

## Selection Criteria

### Inclusion Requirements
- **Power proximity**: Within 10 miles of a power plant with capacity >= 500 MW
- **Fiber access**: Within 2 miles of fiber backbone infrastructure

### Exclusion Requirements
- **Flood risk**: Must not intersect any floodplain
- **Wetland buffer**: Must be at least 5 miles from wetlands

## Composite Scoring

Parcels passing all geospatial filters are scored using four normalized components (0-1 scale):

| Component | Weight | Logic |
|-----------|--------|-------|
| Power proximity | 40% | Closer to power plant = higher score |
| Inverse land cost | 25% | Lower cost = higher score |
| Public sentiment | 20% | Higher sentiment = higher score |
| Fiber proximity | 15% | Closer to fiber = higher score |

**Formula:**
```
composite_score = (0.40 * power_proximity) + 
                  (0.25 * inverse_land_cost) + 
                  (0.20 * sentiment) + 
                  (0.15 * fiber_proximity)
```

## Geospatial Functions Used

| Function | Purpose |
|----------|---------|
| `ST_DWITHIN(geom1, geom2, distance)` | Tests if geometries are within specified distance (meters) |
| `ST_INTERSECTS(geom1, geom2)` | Tests if geometries share any portion of space |
| `ST_DISTANCE(geom1, geom2)` | Returns distance between geometries in meters |

### Distance Conversions
- 10 miles = 16,093.4 meters
- 5 miles = 8,046.7 meters
- 2 miles = 3,218.7 meters

## Query Architecture

The view uses Common Table Expressions (CTEs) to build the solution declaratively:

```
power_distances      -- Min distance to qualifying power plants
fiber_distances      -- Min distance to fiber lines
flood_parcels        -- Parcels intersecting floodplains (excluded)
wetland_parcels      -- Parcels near wetlands (excluded)
candidate_parcels    -- Parcels meeting all geospatial requirements
normalization_stats  -- Min/max values for score normalization
sentiment_stats      -- Min/max sentiment for normalization
scored_parcels       -- Normalized component scores
```

## Output Columns

| Column | Description |
|--------|-------------|
| `parcel_id` | Unique parcel identifier |
| `geom` | Parcel geometry (GEOGRAPHY) |
| `land_cost` | Land acquisition cost |
| `power_distance_m` | Distance to nearest qualifying power plant (meters) |
| `fiber_distance_m` | Distance to nearest fiber line (meters) |
| `power_proximity_score` | Normalized power proximity (0-1) |
| `inverse_land_cost_score` | Normalized inverse cost (0-1) |
| `fiber_proximity_score` | Normalized fiber proximity (0-1) |
| `sentiment_score_normalized` | Normalized sentiment (0-1) |
| `composite_score` | Weighted composite score (0-1) |

## Usage

```sql
-- View top 10 candidates with all scores
SELECT * FROM DATA5035.SPRING26.TOP_DATACENTER_PARCELS;

-- Summary view of top candidates
SELECT 
    parcel_id,
    land_cost,
    ROUND(power_distance_m / 1609.34, 2) AS power_dist_miles,
    ROUND(fiber_distance_m / 1609.34, 2) AS fiber_dist_miles,
    ROUND(composite_score, 4) AS score
FROM DATA5035.SPRING26.TOP_DATACENTER_PARCELS;
```

## Limitations

- **Sentiment integration**: The PARCELS table lacks a `county_id` column, so sentiment scoring uses a neutral 0.5 value. To enable actual sentiment scoring, add county information to parcels or perform a spatial join with county boundaries.

- **Result limit**: The view returns at most 10 parcels. Fewer results indicate limited parcels meeting all criteria.

## Design Principles

- **Declarative SQL only**: No procedural constructs; lets the Snowflake optimizer determine execution strategy
- **Min-max normalization**: Scores are relative to the candidate pool, ensuring full 0-1 range utilization
- **Null-safe operations**: Uses `NULLIF` to prevent division by zero when all values are identical
