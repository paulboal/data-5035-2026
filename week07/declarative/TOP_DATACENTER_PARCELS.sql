"create or replace view TOP_DATACENTER_PARCELS(
	PARCEL_ID,
	GEOM,
	LAND_COST,
	POWER_DISTANCE_M,
	FIBER_DISTANCE_M,
	POWER_PROXIMITY_SCORE,
	INVERSE_LAND_COST_SCORE,
	FIBER_PROXIMITY_SCORE,
	SENTIMENT_SCORE_NORMALIZED,
	COMPOSITE_SCORE
) as
WITH 
-- Calculate minimum distance to qualifying power plants (>= 500 MW, within 10 miles)
power_distances AS (
    SELECT 
        p.parcel_id,
        MIN(ST_DISTANCE(p.geom, pp.geom)) AS power_distance_m
    FROM DATA5035.SPRING26.PARCELS p
    JOIN DATA5035.SPRING26.POWER_PLANTS pp
        ON ST_DWITHIN(p.geom, pp.geom, 16093.4)  -- 10 miles in meters
        AND pp.capacity_mw >= 500
    GROUP BY p.parcel_id
),

-- Calculate minimum distance to fiber lines (within 2 miles)
fiber_distances AS (
    SELECT 
        p.parcel_id,
        MIN(ST_DISTANCE(p.geom, fl.geom)) AS fiber_distance_m
    FROM DATA5035.SPRING26.PARCELS p
    JOIN DATA5035.SPRING26.FIBER_LINES fl
        ON ST_DWITHIN(p.geom, fl.geom, 3218.7)  -- 2 miles in meters
    GROUP BY p.parcel_id
),

-- Identify parcels that intersect floodplains (to exclude)
flood_parcels AS (
    SELECT DISTINCT p.parcel_id
    FROM DATA5035.SPRING26.PARCELS p
    JOIN DATA5035.SPRING26.FLOODPLAINS f
        ON ST_INTERSECTS(p.geom, f.geom)
),

-- Identify parcels within 5 miles of wetlands (to exclude)
wetland_parcels AS (
    SELECT DISTINCT p.parcel_id
    FROM DATA5035.SPRING26.PARCELS p
    JOIN DATA5035.SPRING26.WETLANDS w
        ON ST_DWITHIN(p.geom, w.geom, 8046.7)  -- 5 miles in meters
),

-- Candidate parcels meeting all geospatial requirements
candidate_parcels AS (
    SELECT 
        p.parcel_id,
        p.geom,
        p.land_cost,
        pd.power_distance_m,
        fd.fiber_distance_m
    FROM DATA5035.SPRING26.PARCELS p
    INNER JOIN power_distances pd ON p.parcel_id = pd.parcel_id
    INNER JOIN fiber_distances fd ON p.parcel_id = fd.parcel_id
    WHERE p.parcel_id NOT IN (SELECT parcel_id FROM flood_parcels)
      AND p.parcel_id NOT IN (SELECT parcel_id FROM wetland_parcels)
),

-- Calculate min/max values for normalization
normalization_stats AS (
    SELECT
        MIN(power_distance_m) AS min_power_dist,
        MAX(power_distance_m) AS max_power_dist,
        MIN(land_cost) AS min_land_cost,
        MAX(land_cost) AS max_land_cost,
        MIN(fiber_distance_m) AS min_fiber_dist,
        MAX(fiber_distance_m) AS max_fiber_dist
    FROM candidate_parcels
),

sentiment_stats AS (
    SELECT
        MIN(sentiment_score) AS min_sentiment,
        MAX(sentiment_score) AS max_sentiment
    FROM DATA5035.SPRING26.PUBLIC_SENTIMENT
),

-- Calculate normalized scores
scored_parcels AS (
    SELECT 
        cp.parcel_id,
        cp.geom,
        cp.land_cost,
        cp.power_distance_m,
        cp.fiber_distance_m,
        
        -- Power proximity: closer is better (1 = closest, 0 = farthest)
        CASE 
            WHEN ns.max_power_dist = ns.min_power_dist THEN 1.0
            ELSE 1.0 - ((cp.power_distance_m - ns.min_power_dist) / 
                        NULLIF(ns.max_power_dist - ns.min_power_dist, 0))
        END AS power_proximity_score,
        
        -- Inverse land cost: lower cost is better (1 = cheapest, 0 = most expensive)
        CASE 
            WHEN ns.max_land_cost = ns.min_land_cost THEN 1.0
            ELSE 1.0 - ((cp.land_cost - ns.min_land_cost) / 
                        NULLIF(ns.max_land_cost - ns.min_land_cost, 0))
        END AS inverse_land_cost_score,
        
        -- Fiber proximity: closer is better (1 = closest, 0 = farthest)
        CASE 
            WHEN ns.max_fiber_dist = ns.min_fiber_dist THEN 1.0
            ELSE 1.0 - ((cp.fiber_distance_m - ns.min_fiber_dist) / 
                        NULLIF(ns.max_fiber_dist - ns.min_fiber_dist, 0))
        END AS fiber_proximity_score,
        
        -- Sentiment normalization boundaries
        ss.min_sentiment,
        ss.max_sentiment
        
    FROM candidate_parcels cp
    CROSS JOIN normalization_stats ns
    CROSS JOIN sentiment_stats ss
)

-- Final selection with composite score
-- Note: Sentiment join would require a county_id on parcels; using average sentiment as fallback
SELECT 
    sp.parcel_id,
    sp.geom,
    sp.land_cost,
    sp.power_distance_m,
    sp.fiber_distance_m,
    sp.power_proximity_score,
    sp.inverse_land_cost_score,
    sp.fiber_proximity_score,
    
    -- Use average sentiment normalized (0.5) as placeholder since PARCELS lacks county_id
    0.5 AS sentiment_score_normalized,
    
    -- Composite score: 40% power + 25% land cost + 20% sentiment + 15% fiber
    (0.40 * sp.power_proximity_score) +
    (0.25 * sp.inverse_land_cost_score) +
    (0.20 * 0.5) +
    (0.15 * sp.fiber_proximity_score) AS composite_score
    
FROM scored_parcels sp
ORDER BY composite_score DESC
LIMIT 10;"
