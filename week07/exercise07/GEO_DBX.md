# Databricks Geospatial Functions Quick Guide

## Overview

Databricks supports geospatial analysis through:
- **Mosaic** - Databricks' native geospatial library (recommended)
- **H3** - Built-in hexagonal spatial indexing
- **Apache Sedona** - Alternative Spark geospatial library

## Setup

### Enable Mosaic
```python
%pip install databricks-mosaic

from mosaic import enable_mosaic
enable_mosaic(spark)
```

### Enable H3 (Built-in)
```sql
-- H3 functions available by default in Databricks Runtime 11.3+
SELECT h3_pointash3(lat, lon, resolution)
```

## Data Types

| Type | Description |
|------|-------------|
| `STRING` | WKT (Well-Known Text) format |
| `BINARY` | WKB (Well-Known Binary) format |
| `STRUCT` | GeoJSON as struct |

Mosaic uses WKT/WKB strings; convert as needed.

## Creating Geometries (Mosaic)

```sql
-- Point (longitude, latitude)
SELECT st_point(lon, lat)

-- Make point from WKT
SELECT st_geomfromwkt('POINT(-90.1994 38.6270)')

-- Line from WKT
SELECT st_geomfromwkt('LINESTRING(-90.19 38.62, -94.57 39.09)')

-- Polygon from WKT
SELECT st_geomfromwkt('POLYGON((-90.2 38.6, -90.1 38.6, -90.1 38.7, -90.2 38.7, -90.2 38.6))')

-- From GeoJSON
SELECT st_geomfromgeojson('{"type":"Point","coordinates":[-90.2,38.6]}')
```

## Measurement Functions

```sql
-- Distance between geometries (meters on sphere)
SELECT st_distance(geom1, geom2)

-- Area of polygon (square meters)
SELECT st_area(polygon)

-- Length of line (meters)
SELECT st_length(line)

-- Perimeter of polygon
SELECT st_perimeter(polygon)

-- Haversine distance (direct lat/lon)
SELECT st_haversine(lat1, lon1, lat2, lon2)
```

## Spatial Relationships

```sql
-- Do geometries intersect?
SELECT st_intersects(geom1, geom2)

-- Is geom1 within geom2?
SELECT st_within(geom1, geom2)

-- Does geom1 contain geom2?
SELECT st_contains(geom1, geom2)

-- Do geometries touch?
SELECT st_touches(geom1, geom2)

-- Are geometries equal?
SELECT st_equals(geom1, geom2)

-- Do geometries overlap?
SELECT st_overlaps(geom1, geom2)
```

## Accessors

```sql
-- Get X coordinate (longitude)
SELECT st_x(point)

-- Get Y coordinate (latitude)
SELECT st_y(point)

-- Get centroid
SELECT st_centroid(geom)

-- Get bounding box
SELECT st_envelope(geom)

-- Get geometry type
SELECT st_geometrytype(geom)

-- Check if valid
SELECT st_isvalid(geom)
```

## Transformations

```sql
-- Buffer (distance in geometry units)
SELECT st_buffer(geom, distance)

-- Intersection
SELECT st_intersection(geom1, geom2)

-- Union
SELECT st_union(geom1, geom2)

-- Difference
SELECT st_difference(geom1, geom2)

-- Simplify
SELECT st_simplify(geom, tolerance)

-- Convex hull
SELECT st_convexhull(geom)
```

## Conversion Functions

```sql
-- To WKT string
SELECT st_astext(geom)

-- To GeoJSON
SELECT st_asgeojson(geom)

-- To WKB binary
SELECT st_asbinary(geom)

-- From WKT
SELECT st_geomfromwkt(wkt_string)

-- From WKB
SELECT st_geomfromwkb(wkb_binary)
```

## H3 Hexagonal Indexing (Built-in)

H3 divides Earth into hexagonal cells at various resolutions (0-15).

```sql
-- Convert point to H3 index
SELECT h3_pointash3(lat, lon, resolution)
-- Resolution 9 ≈ 100m, Resolution 7 ≈ 1km

-- Get H3 cell boundary as polygon
SELECT h3_boundaryaswkt(h3_index)

-- Check if H3 cells are neighbors
SELECT h3_isneighbor(h3_index1, h3_index2)

-- Get parent cell (lower resolution)
SELECT h3_toparent(h3_index, parent_resolution)

-- Get child cells (higher resolution)
SELECT h3_tochildren(h3_index, child_resolution)

-- Get k-ring neighbors
SELECT h3_kring(h3_index, k)

-- Distance between H3 cells (in cells)
SELECT h3_distance(h3_index1, h3_index2)

-- Polyfill: cover polygon with H3 cells
SELECT h3_polyfillash3(polygon_wkt, resolution)
```

## Common Patterns

### Find nearest neighbor
```sql
SELECT a.id,
       MIN(st_distance(a.geom, b.geom)) AS min_dist,
       FIRST(b.id) AS nearest_id
FROM table_a a
CROSS JOIN table_b b
GROUP BY a.id
ORDER BY min_dist
```

### Points within distance
```sql
SELECT *
FROM locations
WHERE st_distance(
    geom, 
    st_point(-90.2, 38.6)
) <= 5000  -- 5km
```

### Spatial join with H3 (performant)
```sql
-- Index both tables by H3, then join
WITH indexed_a AS (
    SELECT *, h3_pointash3(lat, lon, 9) AS h3_idx
    FROM table_a
),
indexed_b AS (
    SELECT *, h3_pointash3(lat, lon, 9) AS h3_idx
    FROM table_b
)
SELECT a.*, b.*
FROM indexed_a a
JOIN indexed_b b ON a.h3_idx = b.h3_idx
```

### Buffer analysis
```sql
SELECT s.id,
       COUNT(p.id) AS points_in_buffer
FROM segments s
LEFT JOIN points p 
    ON st_intersects(st_buffer(s.geom, 1609.34), p.geom)
GROUP BY s.id
```

### Mosaic grid tessellation (large polygon optimization)
```python
from mosaic import grid_tessellateexplode

# Break large polygons into indexed chips
df_tessellated = df.select(
    "id",
    grid_tessellateexplode("geom", resolution=9)
)
```

## PySpark API

```python
from mosaic import *
from pyspark.sql import functions as F

# Enable Mosaic
enable_mosaic(spark)

# Create geometry column
df = df.withColumn("geom", st_point("lon", "lat"))

# Calculate distance
df = df.withColumn(
    "dist_to_target",
    st_distance(
        F.col("geom"),
        st_point(F.lit(-90.2), F.lit(38.6))
    )
)

# Spatial filter
df_filtered = df.filter(
    st_intersects(
        F.col("geom"),
        st_geomfromwkt(F.lit("POLYGON((...))"))
    )
)

# Add H3 index
df = df.withColumn(
    "h3_index",
    F.expr("h3_pointash3(lat, lon, 9)")
)
```

## Apache Sedona Alternative

```python
# Install
%pip install apache-sedona

from sedona.spark import SedonaContext

sedona = SedonaContext.create(spark)

# Register functions
sedona.sql("SELECT ST_Point(-90.2, 38.6)")
```

Sedona uses `ST_` prefix (uppercase) vs Mosaic's `st_` (lowercase).

## H3 Resolution Reference

| Resolution | Avg Edge (km) | Avg Area (km²) |
|------------|---------------|----------------|
| 0 | 1,107 | 4,250,547 |
| 4 | 22.6 | 1,770 |
| 7 | 1.22 | 5.16 |
| 9 | 0.17 | 0.105 |
| 12 | 0.006 | 0.0003 |
| 15 | 0.0005 | 0.000001 |

## Performance Tips

1. **Use H3 indexing** for spatial joins—much faster than geometry comparisons.

2. **Tessellate large polygons** with Mosaic's `grid_tessellate` for distributed processing.

3. **Partition by H3** when storing geospatial data:
   ```sql
   CREATE TABLE geo_data
   PARTITIONED BY (h3_res7)
   AS SELECT *, h3_toparent(h3_idx, 7) AS h3_res7 FROM source
   ```

4. **Broadcast small tables** in spatial joins:
   ```python
   df_large.join(F.broadcast(df_small), st_intersects(...))
   ```

5. **Filter before join** using bounding boxes:
   ```sql
   WHERE st_intersects(st_envelope(a.geom), st_envelope(b.geom))
     AND st_intersects(a.geom, b.geom)
   ```

6. **Cache intermediate results** when reusing geometry calculations.

## Snowflake vs Databricks Comparison

| Operation | Snowflake | Databricks (Mosaic) |
|-----------|-----------|---------------------|
| Make point | `ST_MAKEPOINT(lon, lat)` | `st_point(lon, lat)` |
| Distance | `ST_DISTANCE(a, b)` | `st_distance(a, b)` |
| Intersects | `ST_INTERSECTS(a, b)` | `st_intersects(a, b)` |
| From WKT | `TO_GEOGRAPHY(wkt)` | `st_geomfromwkt(wkt)` |
| To WKT | `ST_ASTEXT(geom)` | `st_astext(geom)` |
| Centroid | `ST_CENTROID(geom)` | `st_centroid(geom)` |
| Buffer | `ST_BUFFER(geom, m)` | `st_buffer(geom, m)` |
| Hex index | N/A | `h3_pointash3(lat, lon, res)` |
