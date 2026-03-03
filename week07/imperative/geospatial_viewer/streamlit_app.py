"""
Geospatial Data Viewer - Streamlit in Snowflake App
Visualizes geospatial data from the AI Data Center Site Selection project.
"""

import streamlit as st
import pandas as pd
import pydeck as pdk
import json

st.set_page_config(
    page_title="Geospatial Data Viewer",
    page_icon="🗺️",
    layout="wide"
)

st.title("Geospatial Data Viewer")
st.markdown("Visualize geospatial data from the AI Data Center Site Selection project.")

# Connect to Snowflake
conn = st.connection("snowflake")

# Define available tables with their geometry type and display settings
TABLES = {
    "PARCELS": {
        "description": "Candidate land parcels for data center development (polygons)",
        "geom_type": "polygon",
        "fill_color": [65, 182, 196, 140],  # Teal
        "line_color": [40, 120, 140, 200]
    },
    "POWER_PLANTS": {
        "description": "Power generation facilities in the St. Louis region",
        "geom_type": "point",
        "fill_color": [255, 127, 14, 200],  # Orange
        "radius": 1500
    },
    "FLOODPLAINS": {
        "description": "100-year floodplain zones along major rivers",
        "geom_type": "polygon",
        "fill_color": [31, 119, 180, 100],  # Blue
        "line_color": [20, 80, 150, 200]
    },
    "WETLANDS": {
        "description": "Protected wetland conservation areas",
        "geom_type": "polygon",
        "fill_color": [44, 160, 44, 140],  # Green
        "line_color": [30, 120, 30, 200]
    },
    "FIBER_LINES": {
        "description": "Major fiber backbone routes along highway corridors",
        "geom_type": "line",
        "line_color": [148, 103, 189, 220]  # Purple
    },
    "TOP_PARCELS": {
        "description": "Top ranked parcels from site selection analysis",
        "geom_type": "polygon",
        "fill_color": [214, 39, 40, 140],  # Red
        "line_color": [180, 30, 30, 200]
    }
}

# Sidebar for table selection
st.sidebar.header("Select Data Layer")
selected_table = st.sidebar.selectbox(
    "Choose a table to visualize:",
    options=list(TABLES.keys()),
    format_func=lambda x: x.replace("_", " ").title()
)

# Display table description
st.sidebar.markdown(f"**Description:** {TABLES[selected_table]['description']}")


@st.cache_data(ttl=300)
def load_geospatial_data(table_name: str) -> pd.DataFrame:
    """Load geospatial data from Snowflake with GeoJSON for rendering."""
    
    if table_name == "TOP_PARCELS":
        # Query pre-computed site selection results table
        df = conn.query("""
            SELECT 
                RANK,
                PARCEL_ID,
                ROUND(WEIGHTED_SCORE, 4) as SCORE,
                COUNTY_NAME,
                ROUND(LAND_COST, 0) as LAND_COST,
                ROUND(POWER_DISTANCE_MILES, 2) as POWER_DIST_MI,
                ROUND(FIBER_DISTANCE_MILES, 2) as FIBER_DIST_MI,
                ST_ASGEOJSON(GEOM) as GEOJSON,
                ST_X(ST_CENTROID(GEOM)) as LONGITUDE,
                ST_Y(ST_CENTROID(GEOM)) as LATITUDE
            FROM DATA5035.SPRING26.SITE_SELECTION_RESULTS
            ORDER BY RANK
        """)
    elif table_name == "PARCELS":
        df = conn.query("""
            SELECT 
                PARCEL_ID,
                ROUND(LAND_COST, 0) as LAND_COST,
                ROUND(ACRES, 1) as ACRES,
                ST_ASGEOJSON(GEOM) as GEOJSON,
                ST_X(ST_CENTROID(GEOM)) as LONGITUDE,
                ST_Y(ST_CENTROID(GEOM)) as LATITUDE
            FROM DATA5035.SPRING26.PARCELS
        """)
    elif table_name == "POWER_PLANTS":
        df = conn.query("""
            SELECT 
                PLANT_ID,
                CAPACITY_MW,
                ST_X(GEOM) as LONGITUDE,
                ST_Y(GEOM) as LATITUDE
            FROM DATA5035.SPRING26.POWER_PLANTS
        """)
    elif table_name == "WETLANDS":
        df = conn.query("""
            SELECT 
                WETLAND_ID,
                WETLAND_NAME,
                ST_ASGEOJSON(GEOM) as GEOJSON,
                ST_X(ST_CENTROID(GEOM)) as LONGITUDE,
                ST_Y(ST_CENTROID(GEOM)) as LATITUDE
            FROM DATA5035.SPRING26.WETLANDS
        """)
    elif table_name == "FLOODPLAINS":
        df = conn.query("""
            SELECT 
                FLOODPLAIN_ID,
                RIVER_NAME,
                ST_ASGEOJSON(GEOM) as GEOJSON,
                ST_X(ST_CENTROID(GEOM)) as LONGITUDE,
                ST_Y(ST_CENTROID(GEOM)) as LATITUDE
            FROM DATA5035.SPRING26.FLOODPLAINS
        """)
    elif table_name == "FIBER_LINES":
        df = conn.query("""
            SELECT 
                FIBER_ID,
                ST_ASGEOJSON(GEOM) as GEOJSON
            FROM DATA5035.SPRING26.FIBER_LINES
        """)
    else:
        df = conn.query(f"""
            SELECT *,
                ST_ASGEOJSON(GEOM) as GEOJSON,
                ST_X(ST_CENTROID(GEOM)) as LONGITUDE,
                ST_Y(ST_CENTROID(GEOM)) as LATITUDE
            FROM DATA5035.SPRING26.{table_name}
        """)
    
    return df


def parse_geojson_to_coords(geojson_str: str) -> list:
    """Parse GeoJSON string to coordinate list for pydeck."""
    geojson = json.loads(geojson_str)
    geom_type = geojson.get("type", "")
    
    if geom_type == "Polygon":
        # Return the exterior ring coordinates
        return geojson["coordinates"][0]
    elif geom_type == "LineString":
        return geojson["coordinates"]
    elif geom_type == "Point":
        return geojson["coordinates"]
    else:
        return []


# Load the data
with st.spinner(f"Loading {selected_table} data..."):
    df = load_geospatial_data(selected_table)

# Display data statistics
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Records", len(df))
with col2:
    if "LONGITUDE" in df.columns:
        st.metric("Avg Longitude", f"{df['LONGITUDE'].mean():.4f}")
with col3:
    if "LATITUDE" in df.columns:
        st.metric("Avg Latitude", f"{df['LATITUDE'].mean():.4f}")

# Create the map
st.subheader(f"Map: {selected_table.replace('_', ' ').title()}")

# Set initial view state centered on St. Louis
view_state = pdk.ViewState(
    latitude=38.6,
    longitude=-90.4,
    zoom=9,
    pitch=0
)

# Create appropriate layer based on geometry type
table_config = TABLES[selected_table]
geom_type = table_config["geom_type"]

if geom_type == "line":
    # Create path data from GeoJSON
    path_data = []
    for _, row in df.iterrows():
        coords = parse_geojson_to_coords(row["GEOJSON"])
        path_data.append({"path": coords, "id": row["FIBER_ID"]})
    
    layer = pdk.Layer(
        "PathLayer",
        data=path_data,
        get_path="path",
        get_color=table_config["line_color"],
        width_scale=20,
        width_min_pixels=4,
        pickable=True
    )
    
elif geom_type == "polygon":
    # Create polygon data from GeoJSON
    polygon_data = []
    for _, row in df.iterrows():
        coords = parse_geojson_to_coords(row["GEOJSON"])
        record = {"polygon": coords}
        # Add relevant attributes for tooltip
        for col in df.columns:
            if col not in ["GEOJSON", "LONGITUDE", "LATITUDE"]:
                record[col] = row[col]
        polygon_data.append(record)
    
    layer = pdk.Layer(
        "PolygonLayer",
        data=polygon_data,
        get_polygon="polygon",
        get_fill_color=table_config["fill_color"],
        get_line_color=table_config["line_color"],
        line_width_min_pixels=1,
        pickable=True,
        filled=True,
        extruded=False,
        wireframe=True
    )
    
else:
    # Point data - use ScatterplotLayer
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position=["LONGITUDE", "LATITUDE"],
        get_fill_color=table_config["fill_color"],
        get_radius=table_config["radius"],
        pickable=True,
        opacity=0.8,
        stroked=True,
        get_line_color=[255, 255, 255],
        line_width_min_pixels=1
    )

# Build tooltip based on table
if selected_table == "PARCELS":
    tooltip = {"text": "Parcel {PARCEL_ID}\nCost: ${LAND_COST}\nAcres: {ACRES}"}
elif selected_table == "POWER_PLANTS":
    tooltip = {"text": "Plant {PLANT_ID}\nCapacity: {CAPACITY_MW} MW"}
elif selected_table == "FLOODPLAINS":
    tooltip = {"text": "{RIVER_NAME}"}
elif selected_table == "WETLANDS":
    tooltip = {"text": "{WETLAND_NAME}"}
elif selected_table == "FIBER_LINES":
    tooltip = {"text": "Fiber Route {id}"}
elif selected_table == "TOP_PARCELS":
    tooltip = {"text": "Rank {RANK}: Parcel {PARCEL_ID}\nScore: {SCORE}\nCounty: {COUNTY_NAME}"}
else:
    tooltip = {"text": "Feature"}

# Create and display the map
deck = pdk.Deck(
    layers=[layer],
    initial_view_state=view_state,
    tooltip=tooltip,
    map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"
)

st.pydeck_chart(deck)

# Display data table (exclude GEOJSON column for readability)
st.subheader("Data Table")
display_cols = [col for col in df.columns if col != "GEOJSON"]
st.dataframe(df[display_cols], use_container_width=True)

# Add info about the project
with st.sidebar.expander("About This App"):
    st.markdown("""
    This app visualizes geospatial data from the **AI Data Center Site Selection** 
    project for the St. Louis, MO region.
    
    **Geometry Types:**
    - **Parcels**: 200 land parcel polygons
    - **Power Plants**: 7 point locations
    - **Floodplains**: 3 river corridor polygons
    - **Wetlands**: 6 conservation area polygons
    - **Fiber Lines**: 4 linestring routes
    - **Top Parcels**: Ranked results (points)
    """)
