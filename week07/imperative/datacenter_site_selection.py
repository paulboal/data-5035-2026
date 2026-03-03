"""
Snowpark Python Script: Geospatial Site Selection for AI Data Center Parcels
St. Louis, MO Region

This script performs site selection analysis using geospatial operations
to identify optimal locations for AI data center development.

Written in imperative Snowpark style with explicit intermediate DataFrames.
"""

from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, min as sf_min, max as sf_max
from snowflake.snowpark.functions import call_builtin, when, row_number
from snowflake.snowpark import Window

# Constants for distance calculations (in meters)
MILES_TO_METERS = 1609.34
MAX_POWER_DISTANCE = 10 * MILES_TO_METERS  # 10 miles from power plants
MIN_WETLAND_DISTANCE = 5 * MILES_TO_METERS  # 5 miles from wetlands
MAX_FIBER_DISTANCE = 2 * MILES_TO_METERS    # 2 miles from fiber lines
MIN_POWER_CAPACITY = 500  # Minimum power plant capacity in MW

# Scoring weights
WEIGHT_POWER_PROXIMITY = 0.40    # 40% - closer to power is better
WEIGHT_LAND_COST = 0.25          # 25% - lower cost is better (inverted)
WEIGHT_SENTIMENT = 0.20          # 20% - higher sentiment is better
WEIGHT_FIBER_PROXIMITY = 0.15   # 15% - closer to fiber is better


def create_session() -> Session:
    """Create Snowpark session using default connection parameters."""
    return Session.builder.getOrCreate()


def load_base_tables(session: Session) -> tuple:
    """
    Load all base tables as DataFrames.
    Returns tuple of (parcels, power_plants, floodplains, wetlands, fiber_lines, sentiment)
    
    Note: Parcels, floodplains, and wetlands are polygon geometries.
    We add a CENTROID column to parcels for distance calculations.
    """
    # Load parcels table - polygon geometries representing land parcels
    # Add centroid for distance calculations (ST_DISTANCE works with points)
    parcels_df = session.table("DATA5035.SPRING26.PARCELS").with_column(
        "CENTROID", call_builtin("ST_CENTROID", col("GEOM"))
    )
    
    # Load power plants table - point geometries for power facilities
    power_plants_df = session.table("DATA5035.SPRING26.POWER_PLANTS")
    
    # Load floodplains table - polygon geometries for flood zones
    floodplains_df = session.table("DATA5035.SPRING26.FLOODPLAINS")
    
    # Load wetlands table - polygon geometries for conservation areas
    wetlands_df = session.table("DATA5035.SPRING26.WETLANDS")
    
    # Load fiber lines table - linestring geometries for fiber routes
    fiber_lines_df = session.table("DATA5035.SPRING26.FIBER_LINES")
    
    # Load public sentiment table - community support matters for permitting
    sentiment_df = session.table("DATA5035.SPRING26.PUBLIC_SENTIMENT")
    
    return parcels_df, power_plants_df, floodplains_df, wetlands_df, fiber_lines_df, sentiment_df


def filter_power_proximity(parcels_df, power_plants_df) -> "DataFrame":
    """
    STEP 1: Filter parcels within 10 miles of power plants with capacity >= 500 MW.
    
    Uses ST_DISTANCE to calculate geodesic distance between parcel centroids
    and power plant points. Only keeps parcels within 10 miles of at least
    one qualifying power plant (capacity >= 500 MW).
    """
    # Filter power plants to only those with sufficient capacity (>= 500 MW)
    # This ensures the data center can source adequate power
    large_power_plants_df = power_plants_df.filter(
        col("CAPACITY_MW") >= MIN_POWER_CAPACITY
    )
    
    # Cross join parcels with large power plants to compute all distances
    # Use CENTROID for distance calculation since parcels are polygons
    parcels_with_power_dist_df = parcels_df.cross_join(
        large_power_plants_df.select(
            col("PLANT_ID"),
            col("GEOM").alias("PLANT_GEOM"),
            col("CAPACITY_MW")
        )
    ).with_column(
        "POWER_DISTANCE",
        call_builtin("ST_DISTANCE", col("CENTROID"), col("PLANT_GEOM"))
    )
    
    # Filter to only parcels within 10 miles of a qualifying power plant
    parcels_near_power_df = parcels_with_power_dist_df.filter(
        col("POWER_DISTANCE") <= MAX_POWER_DISTANCE
    )
    
    # For each parcel, keep only the nearest power plant distance
    # This gives us the minimum distance to power infrastructure
    window_spec = Window.partition_by("PARCEL_ID").order_by(col("POWER_DISTANCE"))
    
    parcels_nearest_power_df = parcels_near_power_df.with_column(
        "RN", row_number().over(window_spec)
    ).filter(col("RN") == 1).drop("RN", "PLANT_ID", "PLANT_GEOM", "CAPACITY_MW")
    
    return parcels_nearest_power_df


def exclude_floodplains(parcels_df, floodplains_df) -> "DataFrame":
    """
    STEP 2: Exclude parcels intersecting 100-year floodplains.
    
    Uses ST_INTERSECTS to check if parcel polygon overlaps any floodplain polygon.
    Data centers require flood-safe locations to protect expensive equipment
    and ensure operational continuity during extreme weather events.
    """
    # Check if each parcel polygon intersects ANY floodplain polygon
    # ST_INTERSECTS returns TRUE if geometries share any portion of space
    parcels_with_flood_check_df = parcels_df.join(
        floodplains_df.select(col("GEOM").alias("FLOOD_GEOM")),
        call_builtin("ST_INTERSECTS", col("GEOM"), col("FLOOD_GEOM")),
        "left"
    )
    
    # Keep only parcels that do NOT intersect any floodplain (FLOOD_GEOM is NULL)
    parcels_outside_floodplains_df = parcels_with_flood_check_df.filter(
        col("FLOOD_GEOM").is_null()
    ).drop("FLOOD_GEOM")
    
    return parcels_outside_floodplains_df


def exclude_wetland_buffer(parcels_df, wetlands_df) -> "DataFrame":
    """
    STEP 3: Exclude parcels within 5 miles of wetlands.
    
    Uses ST_DISTANCE to calculate distance from each parcel centroid to each 
    wetland centroid. Maintains required environmental buffer zones around 
    protected wetland areas. This helps with environmental permitting.
    
    Note: Uses window function instead of GROUP BY because GEOGRAPHY columns
    cannot be used as GROUP BY keys in Snowflake.
    """
    # Cross join to compute distance from each parcel centroid to each wetland centroid
    parcels_wetland_dist_df = parcels_df.cross_join(
        wetlands_df.select(call_builtin("ST_CENTROID", col("GEOM")).alias("WETLAND_CENTROID"))
    ).with_column(
        "WETLAND_DISTANCE",
        call_builtin("ST_DISTANCE", col("CENTROID"), col("WETLAND_CENTROID"))
    )
    
    # Use window function to find minimum wetland distance per parcel
    # (GEOGRAPHY columns cannot be GROUP BY keys, so we use ROW_NUMBER instead)
    window_spec = Window.partition_by("PARCEL_ID").order_by(col("WETLAND_DISTANCE"))
    min_wetland_df = parcels_wetland_dist_df.with_column(
        "RN", row_number().over(window_spec)
    ).filter(col("RN") == 1).drop("RN", "WETLAND_CENTROID")
    
    # Keep only parcels that are at least 5 miles from all wetlands
    parcels_outside_wetland_buffer_df = min_wetland_df.filter(
        col("WETLAND_DISTANCE") >= MIN_WETLAND_DISTANCE
    ).drop("WETLAND_DISTANCE")
    
    return parcels_outside_wetland_buffer_df


def filter_fiber_proximity(parcels_df, fiber_lines_df) -> "DataFrame":
    """
    STEP 4: Keep parcels within 2 miles of fiber backbone lines.
    
    Uses ST_DISTANCE to calculate distance from each parcel centroid to each
    fiber line (LINESTRING geometry). Data centers require high-bandwidth
    connectivity to major internet exchange points via fiber infrastructure.
    
    Note: Uses window function instead of GROUP BY because GEOGRAPHY columns
    cannot be used as GROUP BY keys in Snowflake.
    """
    # Cross join to compute distance from each parcel centroid to each fiber line
    # ST_DISTANCE works with LINESTRING geometries, returning shortest distance
    parcels_fiber_dist_df = parcels_df.cross_join(
        fiber_lines_df.select(col("GEOM").alias("FIBER_GEOM"))
    ).with_column(
        "FIBER_DISTANCE",
        call_builtin("ST_DISTANCE", col("CENTROID"), col("FIBER_GEOM"))
    )
    
    # Use window function to find minimum fiber distance per parcel
    window_spec = Window.partition_by("PARCEL_ID").order_by(col("FIBER_DISTANCE"))
    min_fiber_df = parcels_fiber_dist_df.with_column(
        "RN", row_number().over(window_spec)
    ).filter(col("RN") == 1).drop("RN", "FIBER_GEOM")
    
    # Keep only parcels within 2 miles of fiber infrastructure
    parcels_near_fiber_df = min_fiber_df.filter(
        col("FIBER_DISTANCE") <= MAX_FIBER_DISTANCE
    )
    
    return parcels_near_fiber_df


def assign_county_sentiment(parcels_df, sentiment_df) -> "DataFrame":
    """
    STEP 5 & 6: Assign county based on location and join sentiment data.
    
    For simplicity, we assign county based on longitude bands in the St. Louis region.
    In production, this would use ST_CONTAINS with actual county boundary polygons.
    Joins the sentiment score which reflects community favorability toward data centers.
    """
    # Extract longitude from parcel centroid for county assignment
    # ST_X extracts the X coordinate (longitude) from a point geometry
    parcels_with_coords_df = parcels_df.with_column(
        "LONGITUDE",
        call_builtin("ST_X", col("CENTROID"))
    )
    
    # Assign county_id based on longitude bands (simplified county boundaries)
    # In production, would use ST_CONTAINS with actual county polygons
    parcels_with_county_df = parcels_with_coords_df.with_column(
        "COUNTY_ID",
        when(col("LONGITUDE") < -90.7, lit(3))       # Franklin County (west)
        .when(col("LONGITUDE") < -90.4, lit(2))      # St. Charles County
        .when(col("LONGITUDE") < -90.2, lit(1))      # St. Louis County
        .when(col("LONGITUDE") < -90.0, lit(8))      # Madison County (IL)
        .otherwise(lit(9))                            # St. Clair County (IL)
    ).drop("LONGITUDE")
    
    # Join with sentiment data to get community favorability scores
    parcels_with_sentiment_df = parcels_with_county_df.join(
        sentiment_df.select("COUNTY_ID", "SENTIMENT_SCORE", "COUNTY_NAME"),
        on="COUNTY_ID",
        how="left"
    )
    
    return parcels_with_sentiment_df


def compute_weighted_score(parcels_df) -> "DataFrame":
    """
    STEP 7: Compute weighted site selection score.
    
    Score components (all normalized to 0-1 scale):
    - 40% Power proximity (closer is better, so invert distance)
    - 25% Land cost (cheaper is better, so invert cost)
    - 20% Public sentiment (higher is better)
    - 15% Fiber proximity (closer is better, so invert distance)
    
    Final score ranges from 0 to 1, with higher scores being better sites.
    """
    # First, we need to normalize all metrics to 0-1 scale
    # Calculate min/max values for normalization
    stats_df = parcels_df.select(
        sf_min("POWER_DISTANCE").alias("MIN_POWER_DIST"),
        sf_max("POWER_DISTANCE").alias("MAX_POWER_DIST"),
        sf_min("LAND_COST").alias("MIN_LAND_COST"),
        sf_max("LAND_COST").alias("MAX_LAND_COST"),
        sf_min("SENTIMENT_SCORE").alias("MIN_SENTIMENT"),
        sf_max("SENTIMENT_SCORE").alias("MAX_SENTIMENT"),
        sf_min("FIBER_DISTANCE").alias("MIN_FIBER_DIST"),
        sf_max("FIBER_DISTANCE").alias("MAX_FIBER_DIST")
    )
    
    # Cross join to add stats to each parcel row for normalization calculations
    parcels_with_stats_df = parcels_df.cross_join(stats_df)
    
    # Calculate normalized scores (0-1 scale)
    # Power proximity: invert so closer = higher score
    # Formula: 1 - (distance - min) / (max - min)
    parcels_with_scores_df = parcels_with_stats_df.with_column(
        "POWER_SCORE",
        lit(1) - (col("POWER_DISTANCE") - col("MIN_POWER_DIST")) / 
                 (col("MAX_POWER_DIST") - col("MIN_POWER_DIST") + lit(1))
    ).with_column(
        # Land cost: invert so cheaper = higher score
        "COST_SCORE",
        lit(1) - (col("LAND_COST") - col("MIN_LAND_COST")) / 
                 (col("MAX_LAND_COST") - col("MIN_LAND_COST") + lit(1))
    ).with_column(
        # Sentiment: higher is better (no inversion needed)
        "SENTIMENT_NORM",
        (col("SENTIMENT_SCORE") - col("MIN_SENTIMENT")) / 
        (col("MAX_SENTIMENT") - col("MIN_SENTIMENT") + lit(1))
    ).with_column(
        # Fiber proximity: invert so closer = higher score
        "FIBER_SCORE",
        lit(1) - (col("FIBER_DISTANCE") - col("MIN_FIBER_DIST")) / 
                 (col("MAX_FIBER_DIST") - col("MIN_FIBER_DIST") + lit(1))
    )
    
    # Calculate final weighted score
    parcels_final_score_df = parcels_with_scores_df.with_column(
        "WEIGHTED_SCORE",
        (col("POWER_SCORE") * lit(WEIGHT_POWER_PROXIMITY)) +
        (col("COST_SCORE") * lit(WEIGHT_LAND_COST)) +
        (col("SENTIMENT_NORM") * lit(WEIGHT_SENTIMENT)) +
        (col("FIBER_SCORE") * lit(WEIGHT_FIBER_PROXIMITY))
    )
    
    # Select final columns, dropping intermediate calculation columns
    final_parcels_df = parcels_final_score_df.select(
        "PARCEL_ID",
        "GEOM",
        "LAND_COST",
        "COUNTY_NAME",
        "SENTIMENT_SCORE",
        "POWER_DISTANCE",
        "FIBER_DISTANCE",
        "POWER_SCORE",
        "COST_SCORE",
        "SENTIMENT_NORM",
        "FIBER_SCORE",
        "WEIGHTED_SCORE"
    )
    
    return final_parcels_df


def rank_and_select_top_parcels(parcels_df, top_n: int = 10) -> "DataFrame":
    """
    STEP 8: Rank parcels by weighted score and return top N.
    
    Orders parcels by WEIGHTED_SCORE descending (highest scores are best sites)
    and returns the top candidates for data center development.
    """
    # Order by weighted score descending and limit to top N
    window_spec = Window.order_by(col("WEIGHTED_SCORE").desc())
    
    ranked_parcels_df = parcels_df.with_column(
        "RANK", row_number().over(window_spec)
    ).filter(col("RANK") <= top_n)
    
    # Convert distances from meters to miles for readability
    final_df = ranked_parcels_df.with_column(
        "POWER_DISTANCE_MILES",
        col("POWER_DISTANCE") / lit(MILES_TO_METERS)
    ).with_column(
        "FIBER_DISTANCE_MILES",
        col("FIBER_DISTANCE") / lit(MILES_TO_METERS)
    ).select(
        "RANK",
        "PARCEL_ID",
        "WEIGHTED_SCORE",
        "COUNTY_NAME",
        "LAND_COST",
        "SENTIMENT_SCORE",
        "POWER_DISTANCE_MILES",
        "FIBER_DISTANCE_MILES",
        "POWER_SCORE",
        "COST_SCORE",
        "FIBER_SCORE",
        "GEOM"
    )
    
    return final_df


def main():
    """
    Main execution function that orchestrates the geospatial site selection pipeline.
    """
    print("=" * 70)
    print("AI DATA CENTER SITE SELECTION - St. Louis Region")
    print("=" * 70)
    
    # Initialize Snowpark session
    print("\n[1/8] Initializing Snowpark session...")
    session = create_session()
    
    # Load all base tables
    print("[2/8] Loading base tables...")
    parcels, power_plants, floodplains, wetlands, fiber_lines, sentiment = load_base_tables(session)
    print(f"      - Parcels loaded: {parcels.count()} records")
    print(f"      - Power plants loaded: {power_plants.count()} records")
    print(f"      - Floodplain zones loaded: {floodplains.count()} records")
    print(f"      - Wetland areas loaded: {wetlands.count()} records")
    print(f"      - Fiber lines loaded: {fiber_lines.count()} records")
    print(f"      - County sentiment records: {sentiment.count()} records")
    
    # Step 1: Filter by power proximity
    print("\n[3/8] Filtering parcels within 10 miles of power plants (>= 500 MW)...")
    parcels_near_power = filter_power_proximity(parcels, power_plants)
    print(f"      - Parcels remaining: {parcels_near_power.count()}")
    
    # Step 2: Exclude floodplains
    print("\n[4/8] Excluding parcels in 100-year floodplains...")
    parcels_flood_safe = exclude_floodplains(parcels_near_power, floodplains)
    print(f"      - Parcels remaining: {parcels_flood_safe.count()}")
    
    # Step 3: Exclude wetland buffer zones
    print("\n[5/8] Excluding parcels within 5 miles of wetlands...")
    parcels_wetland_safe = exclude_wetland_buffer(parcels_flood_safe, wetlands)
    print(f"      - Parcels remaining: {parcels_wetland_safe.count()}")
    
    # Step 4: Filter by fiber proximity
    print("\n[6/8] Filtering parcels within 2 miles of fiber backbone...")
    parcels_with_fiber = filter_fiber_proximity(parcels_wetland_safe, fiber_lines)
    print(f"      - Parcels remaining: {parcels_with_fiber.count()}")
    
    # Step 5 & 6: Assign county and join sentiment
    print("\n[7/8] Assigning counties and joining sentiment data...")
    parcels_with_sentiment = assign_county_sentiment(parcels_with_fiber, sentiment)
    
    # Step 7: Compute weighted scores
    print("\n[8/8] Computing weighted site selection scores...")
    print(f"      - Power proximity weight: {WEIGHT_POWER_PROXIMITY*100}%")
    print(f"      - Land cost weight: {WEIGHT_LAND_COST*100}%")
    print(f"      - Sentiment weight: {WEIGHT_SENTIMENT*100}%")
    print(f"      - Fiber proximity weight: {WEIGHT_FIBER_PROXIMITY*100}%")
    parcels_scored = compute_weighted_score(parcels_with_sentiment)
    
    # Step 8: Rank and select top 10
    print("\n" + "=" * 70)
    print("TOP 10 RECOMMENDED DATA CENTER SITES")
    print("=" * 70)
    top_parcels = rank_and_select_top_parcels(parcels_scored, top_n=10)
    
    # Display results
    top_parcels.show()
    
    # Save results to table for Streamlit app access
    print("\nSaving results to SITE_SELECTION_RESULTS table...")
    top_parcels.write.mode("overwrite").save_as_table("DATA5035.SPRING26.SITE_SELECTION_RESULTS")
    print("Results saved successfully.")
    
    # Return the result DataFrame for further use
    return top_parcels


if __name__ == "__main__":
    main()
