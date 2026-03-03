# Snowflake Geospatial Functions Quick Guide

## Data Types

| Type          | Description                                           |
| ------------- | ----------------------------------------------------- |
| `GEOGRAPHY` | Spherical Earth model (lat/lon), distances in meters  |
| `GEOMETRY`  | Planar/Cartesian model, distances in coordinate units |

Use `GEOGRAPHY` for real-world locations (recommended for most cases).

## Creating Geometries / Geographies

```sql
-- Point (longitude, latitude)
ST_MAKEPOINT(-90.1994, 38.6270)

-- Line from two points
ST_MAKELINE(
    ST_MAKEPOINT(-90.19, 38.62),
    ST_MAKEPOINT(-94.57, 39.09)
)

-- Polygon from WKT string
TO_GEOGRAPHY('POLYGON((-90.2 38.6, -90.1 38.6, -90.1 38.7, -90.2 38.7, -90.2 38.6))')

-- Polygon from coordinates
ST_MAKEPOLYGON(ST_MAKELINE(ARRAY_CONSTRUCT(
    ST_MAKEPOINT(-90.2, 38.6),
    ST_MAKEPOINT(-90.1, 38.6),
    ST_MAKEPOINT(-90.1, 38.7),
    ST_MAKEPOINT(-90.2, 38.6)  -- must close
)))
```

## Measurement Functions

```sql
-- Distance between geometries (meters for GEOGRAPHY)
ST_DISTANCE(geom1, geom2)

-- Area of polygon (square meters)
ST_AREA(polygon)

-- Length of line (meters)
ST_LENGTH(line)

-- Perimeter of polygon
ST_PERIMETER(polygon)
```

## Spatial Relationships

```sql
-- Do geometries overlap?
ST_INTERSECTS(geom1, geom2)  -- returns BOOLEAN

-- Is geom1 completely inside geom2?
ST_WITHIN(geom1, geom2)

-- Does geom1 completely contain geom2?
ST_CONTAINS(geom1, geom2)

-- Do boundaries touch?
ST_TOUCHES(geom1, geom2)

-- Are geometries equal?
ST_EQUALS(geom1, geom2)
```

## Accessors

```sql
-- Get X coordinate (longitude)
ST_X(point)

-- Get Y coordinate (latitude)
ST_Y(point)

-- Get centroid of geometry
ST_CENTROID(geom)

-- Get bounding envelope
ST_ENVELOPE(geom)

-- Get start/end points of line
ST_STARTPOINT(line)
ST_ENDPOINT(line)
```

## Transformations

```sql
-- Buffer around geometry (distance in meters for GEOGRAPHY)
ST_BUFFER(geom, distance)

-- Intersection of two geometries
ST_INTERSECTION(geom1, geom2)

-- Union of geometries
ST_UNION(geom1, geom2)

-- Difference (geom1 minus geom2)
ST_DIFFERENCE(geom1, geom2)

-- Simplify geometry (reduce points)
ST_SIMPLIFY(geom, tolerance)
```

## Conversion Functions

```sql
-- Convert to GEOGRAPHY
TO_GEOGRAPHY(wkt_or_geojson)
ST_GEOGRAPHYFROMWKT('POINT(-90.2 38.6)')
ST_GEOGRAPHYFROMGEOJSON('{"type":"Point","coordinates":[-90.2,38.6]}')

-- Convert to text formats
ST_ASTEXT(geom)      -- WKT
ST_ASGEOJSON(geom)   -- GeoJSON
ST_ASWKB(geom)       -- Well-Known Binary
```

## Aggregation

```sql
-- Collect multiple geometries into one
ST_COLLECT(geom)

-- Union all geometries in group
ST_UNION_AGG(geom)
```

## Common Patterns

### Find nearest neighbor

```sql
SELECT a.id, 
       MIN(ST_DISTANCE(a.geom, b.geom)) AS min_dist,
       MIN_BY(b.id, ST_DISTANCE(a.geom, b.geom)) AS nearest_id
FROM table_a a
CROSS JOIN table_b b
GROUP BY a.id
```

### Points within distance

```sql
SELECT *
FROM locations
WHERE ST_DISTANCE(geom, ST_MAKEPOINT(-90.2, 38.6)) <= 5000  -- 5km
```

### Intersection check

```sql
SELECT s.segment_id,
       MAX(CASE WHEN ST_INTERSECTS(s.geom, c.geom) THEN 1 ELSE 0 END) AS has_intersection
FROM segments s
LEFT JOIN constraints c ON ST_DISTANCE(s.geom, c.geom) <= 10000
GROUP BY s.segment_id
```

### Buffer analysis

```sql
-- Count points within 1 mile of each segment
SELECT s.id, COUNT(p.id) AS points_within_1mi
FROM segments s
LEFT JOIN points p ON ST_DISTANCE(ST_CENTROID(s.geom), p.geom) <= 1609.34
GROUP BY s.id
```

## Unit Reference

| Distance    | Meters   |
| ----------- | -------- |
| 1 mile      | 1,609.34 |
| 1 kilometer | 1,000    |
| 1 foot      | 0.3048   |

## Tips

1. **GEOGRAPHY vs GEOMETRY**: Use `GEOGRAPHY` for real-world coordinates; it handles Earth's curvature automatically.
2. **Coordinate order**: Snowflake uses (longitude, latitude) order—X before Y.
3. **Performance**: For large datasets, filter with `ST_DISTANCE` before expensive operations like `ST_INTERSECTS`.
4. **Closing polygons**: The first and last points must be identical.
5. **NULL handling**: Geospatial functions return NULL if any input is NULL.
6. **Index support**: Snowflake auto-optimizes geospatial queries; no manual indexing needed.
