import streamlit as st
from pyspark.sql import SparkSession
import pandas as pd
import plotly.express as px
import datetime
import os
import configparser

# Page Configuration
st.set_page_config(
    page_title="Cold-Chain IoT Streaming Lakehouse Dashboard",
    page_icon="❄️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Dark Mode/Custom Styling (Aesthetics)
st.markdown("""
<style>
    .main {
        background-color: #0f1116;
        color: #f0f2f6;
    }
    .stCard {
        background-color: #1b202c;
        border-radius: 8px;
        padding: 15px;
        border: 1px solid #2d3748;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    h1, h2, h3 {
        color: #63b3ed !important;
        font-family: 'Outfit', sans-serif;
    }
    .metric-value {
        font-size: 24px;
        font-weight: bold;
        color: #ffffff;
    }
    .metric-label {
        font-size: 14px;
        color: #a0aec0;
    }
</style>
""", unsafe_allow_html=True)

# Cache SparkSession connection using LakehouseSparkEnv
@st.cache_resource
def get_lakehouse_env():
    import sys
    sys.path.append("/app/apps/core")
    from lakehouse_spark_env import LakehouseSparkEnv
    # Load env (no Hive support needed/enabled by default)
    return LakehouseSparkEnv("LakehouseDashboard", log_level="ERROR")

# Try to initialize Spark
try:
    lakehouse = get_lakehouse_env()
    spark = lakehouse.spark
    spark_connected = True
    
    # Register temp views dynamically if the paths exist
    def register_views():
        for name, path in [("bronze", lakehouse.path_bronze), ("silver", lakehouse.path_silver), ("gold", lakehouse.path_gold)]:
            try:
                spark.read.format("delta").load(path).createOrReplaceTempView(name)
            except Exception as ex:
                print(f"Initial view '{name}' skipped: {ex}", flush=True)

    register_views()
except Exception as e:
    spark_connected = False
    spark_error = e

# Sidebar Controls
st.sidebar.title("❄️ Cold-Chain Lakehouse")
st.sidebar.markdown("This dashboard queries the **Gold** and **Silver** tables in the Delta Lake on MinIO.")

# Guarantee auto-refresh starts turned OFF
if "auto_refresh_enabled" not in st.session_state:
    st.session_state.auto_refresh_enabled = False

auto_refresh = st.sidebar.checkbox("Auto Refresh Data", value=st.session_state.auto_refresh_enabled, key="auto_refresh_enabled")
refresh_rate = st.sidebar.slider("Refresh Interval (seconds)", min_value=3, max_value=60, value=10, disabled=not auto_refresh)
st.sidebar.caption("💡 Page is static by default to conserve memory & CPU. Click Refresh to fetch latest records.")

if st.sidebar.button("🔄 Refresh Data Now", type="primary", use_container_width=True):
    st.cache_data.clear()
    try:
        register_views()
    except Exception:
        pass
    st.rerun()

time_now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
st.sidebar.caption(f"Last fetched: `{time_now}`")

if not spark_connected:
    st.error(f"Failed to connect to Spark. Error: {spark_error}")
    st.stop()

# Cached data loaders
@st.cache_data(ttl=60)
def fetch_stream_stats(_spark, path_bronze, path_silver, path_gold):
    stats = {}
    try:
        b_df = _spark.sql(f"SELECT count(*) as total, max(timestamp) as latest_ts, max(offset) as latest_offset FROM delta.`{path_bronze}`").toPandas()
        stats["bronze_count"] = int(b_df["total"].iloc[0]) if not b_df.empty and pd.notna(b_df["total"].iloc[0]) else 0
        stats["bronze_latest"] = str(b_df["latest_ts"].iloc[0]) if not b_df.empty and pd.notna(b_df["latest_ts"].iloc[0]) else "N/A"
        stats["bronze_offset"] = int(b_df["latest_offset"].iloc[0]) if not b_df.empty and pd.notna(b_df["latest_offset"].iloc[0]) else 0
    except Exception:
        stats["bronze_count"] = 0
        stats["bronze_latest"] = "N/A"
        stats["bronze_offset"] = 0

    try:
        s_df = _spark.sql(f"SELECT count(*) as total, max(Timestamp) as latest_ts FROM delta.`{path_silver}`").toPandas()
        stats["silver_count"] = int(s_df["total"].iloc[0]) if not s_df.empty and pd.notna(s_df["total"].iloc[0]) else 0
        stats["silver_latest"] = str(s_df["latest_ts"].iloc[0]) if not s_df.empty and pd.notna(s_df["latest_ts"].iloc[0]) else "N/A"
    except Exception:
        stats["silver_count"] = 0
        stats["silver_latest"] = "N/A"

    try:
        g_df = _spark.sql(f"SELECT count(*) as total, max(Window_End) as latest_ts FROM delta.`{path_gold}`").toPandas()
        stats["gold_count"] = int(g_df["total"].iloc[0]) if not g_df.empty and pd.notna(g_df["total"].iloc[0]) else 0
        stats["gold_latest"] = str(g_df["latest_ts"].iloc[0]) if not g_df.empty and pd.notna(g_df["latest_ts"].iloc[0]) else "N/A"
    except Exception:
        stats["gold_count"] = 0
        stats["gold_latest"] = "N/A"

    return stats

@st.cache_data(ttl=60)
def fetch_bronze_df(_spark, path):
    try:
        return _spark.sql(f"SELECT timestamp, partition, offset, substring(CAST(value AS STRING), 1, 150) as payload FROM delta.`{path}` ORDER BY timestamp DESC LIMIT 20").toPandas()
    except Exception as ex:
        print(f"Bronze table fetch error: {ex}", flush=True)
        return pd.DataFrame()

@st.cache_data(ttl=60)
def fetch_silver_df(_spark, path):
    try:
        return _spark.sql(f"SELECT * FROM delta.`{path}` ORDER BY Timestamp DESC LIMIT 500").toPandas()
    except Exception as ex:
        print(f"Silver table fetch error: {ex}", flush=True)
        return pd.DataFrame()

@st.cache_data(ttl=60)
def fetch_gold_df(_spark, path):
    try:
        return _spark.sql(f"SELECT * FROM delta.`{path}` ORDER BY Window_End DESC LIMIT 100").toPandas()
    except Exception as ex:
        print(f"Gold table fetch error: {ex}", flush=True)
        return pd.DataFrame()

@st.cache_data(ttl=60)
def fetch_delta_history(_spark, path):
    try:
        return _spark.sql(f"DESCRIBE HISTORY delta.`{path}` LIMIT 10").select("version", "timestamp", "operation").toPandas()
    except Exception as ex:
        return pd.DataFrame()

# Main UI Header
header_col1, header_col2 = st.columns([4, 1])
with header_col1:
    st.title("Cold-Chain IoT Streaming Lakehouse Dashboard")
with header_col2:
    st.write("")
    if st.button("🔄 Refresh Data", key="header_refresh", type="primary"):
        st.cache_data.clear()
        try:
            register_views()
        except Exception:
            pass
        st.rerun()

# Fetch latest status using direct delta path queries
stats = fetch_stream_stats(spark, lakehouse.path_bronze, lakehouse.path_silver, lakehouse.path_gold)
silver_df = fetch_silver_df(spark, lakehouse.path_silver)
gold_df = fetch_gold_df(spark, lakehouse.path_gold)

# Sidebar Ingestion Heartbeat & Logs Feed
st.sidebar.markdown("---")
st.sidebar.subheader("📡 Ingestion Heartbeat")

st.sidebar.markdown(
    f"**🟢 Bronze (Kafka Raw)**\n\n"
    f"Events: **{stats['bronze_count']:,}** | Offset: `{stats['bronze_offset']}`\n\n"
    f"Latest: `{stats['bronze_latest']}`"
)

st.sidebar.markdown(
    f"**🔵 Silver (Cleaned IoT)**\n\n"
    f"Records: **{stats['silver_count']:,}**\n\n"
    f"Latest: `{stats['silver_latest']}`"
)

st.sidebar.markdown(
    f"**🟡 Gold (Aggregated Windows)**\n\n"
    f"Windows: **{stats['gold_count']:,}**\n\n"
    f"Latest: `{stats['gold_latest']}`"
)

st.sidebar.markdown("---")
st.sidebar.subheader("📋 Layer Logs Feed")

with st.sidebar.expander("🟢 Bronze Kafka Logs", expanded=False):
    bronze_records = fetch_bronze_df(spark, lakehouse.path_bronze)
    if not bronze_records.empty:
        for _, b_row in bronze_records.head(5).iterrows():
            st.code(f"[{b_row['timestamp']}] Offset {b_row['offset']}\n{b_row['payload'][:110]}...", language="json")
    else:
        st.info("No Bronze logs available yet.")

with st.sidebar.expander("🔵 Silver Cleaned Logs", expanded=False):
    if not silver_df.empty:
        for _, s_row in silver_df.head(5).iterrows():
            st.caption(f"**{s_row['Vehicle_ID']}** @ `{s_row['Timestamp']}`\n* Cargo: `{s_row['Cargo_Temp']}°C` | Door: `{s_row['Door_Status']}` | Δ: `{s_row['Temp_Delta']}°C`")
    else:
        st.info("No Silver logs available yet.")

with st.sidebar.expander("🟡 Gold Window Logs", expanded=False):
    if not gold_df.empty:
        for _, g_row in gold_df.head(5).iterrows():
            st.caption(f"**{g_row['Vehicle_ID']}** [{str(g_row['Window_Start'])[-8:]} - {str(g_row['Window_End'])[-8:]}]\n* Avg Cargo: `{g_row['Avg_Cargo_Temp']}°C` | Anomaly: `{g_row['Anomaly_Flag']}`")
    else:
        st.info("No Gold logs available yet.")

with st.sidebar.expander("📜 Delta Commit History", expanded=False):
    layer_sel = st.selectbox("Select Layer", ["Bronze", "Silver", "Gold"], key="hist_layer_sel")
    layer_path = lakehouse.path_bronze if layer_sel == "Bronze" else (lakehouse.path_silver if layer_sel == "Silver" else lakehouse.path_gold)
    hist_df = fetch_delta_history(spark, layer_path)
    if not hist_df.empty:
        st.dataframe(hist_df, hide_index=True, use_container_width=True)
    else:
        st.info("No commit history.")

with st.sidebar.expander("🚛 Fleet Manager (Add Trucks)", expanded=True):
    cfg_file = "/app/config/pipeline.conf"
    current_fleet_size = 5
    if os.path.exists(cfg_file):
        try:
            cp = configparser.ConfigParser()
            cp.read(cfg_file)
            if cp.has_section("fleet") and cp.has_option("fleet", "num_trucks"):
                current_fleet_size = int(cp.get("fleet", "num_trucks"))
        except Exception:
            pass

    st.markdown(f"**Active Fleet Target:** `{current_fleet_size} Trucks`")
    f_col1, f_col2 = st.columns(2)
    with f_col1:
        if st.button("➕ Add 1 Truck", key="btn_add_1", use_container_width=True):
            new_size = current_fleet_size + 1
            try:
                cp = configparser.ConfigParser()
                if os.path.exists(cfg_file):
                    cp.read(cfg_file)
                if not cp.has_section("fleet"):
                    cp.add_section("fleet")
                cp.set("fleet", "num_trucks", str(new_size))
                with open(cfg_file, "w") as f:
                    cp.write(f)
                st.success(f"Added truck! Fleet is now {new_size}.")
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")

    with f_col2:
        if st.button("➕ Add 3 Trucks", key="btn_add_3", use_container_width=True):
            new_size = current_fleet_size + 3
            try:
                cp = configparser.ConfigParser()
                if os.path.exists(cfg_file):
                    cp.read(cfg_file)
                if not cp.has_section("fleet"):
                    cp.add_section("fleet")
                cp.set("fleet", "num_trucks", str(new_size))
                with open(cfg_file, "w") as f:
                    cp.write(f)
                st.success(f"Added 3 trucks! Fleet is now {new_size}.")
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")

    st.caption("Simulator reads pipeline.conf dynamically and injects new vehicles onto designated routes.")

st.markdown("---")

# 1. KPIs Section
st.subheader("Real-Time Fleet Status")
col1, col2, col3, col4, col5 = st.columns(5)

# Render metrics and map if data is available
if not silver_df.empty:
    total_events = len(silver_df)
    latest_per_vehicle = silver_df.sort_values("Timestamp").groupby("Vehicle_ID").last().reset_index()
    
    avg_cargo_temp = latest_per_vehicle["Cargo_Temp"].mean()
    anomalies_active = latest_per_vehicle[
        (latest_per_vehicle["Cargo_Temp"] > 8.0) | 
        (latest_per_vehicle["Door_Status"] == "OPEN") | 
        (latest_per_vehicle["Vibration"] > 3.0)
    ]
    
    num_anomalies = len(anomalies_active)
    
    # Active condensation risks (T_cargo - T_dew < 2.0C)
    condensation_active = latest_per_vehicle[latest_per_vehicle["Condensation_Risk"] == True]
    num_condensation_risks = len(condensation_active)
    
    # Trigger popup alerts for active anomalies
    for idx, alert in anomalies_active.iterrows():
        st.toast(
            f"🚨 **Anomaly Alert**: {alert['Vehicle_ID']} "
            f"(Temp: {alert['Cargo_Temp']}°C, Door: {alert['Door_Status']}, Vibration: {alert['Vibration']}G)",
            icon="⚠️"
        )
        
    # Trigger popup alerts for condensation risks
    for idx, alert in condensation_active.iterrows():
        st.toast(
            f"💧 **Condensation Warning**: {alert['Vehicle_ID']} "
            f"(Temp: {alert['Cargo_Temp']}°C, Dew Point: {alert['Dew_Point']}°C, Humidity: {alert['Humidity']}%)",
            icon="💧"
        )
    
    with col1:
        st.markdown(f"""
        <div class="stCard">
            <div class="metric-label">Ingested Events (Sample)</div>
            <div class="metric-value">{total_events}</div>
        </div>
        """, unsafe_allow_html=True)
    with col2:
        st.markdown(f"""
        <div class="stCard">
            <div class="metric-label">Fleet Avg Cargo Temp</div>
            <div class="metric-value" style="color: {'#e53e3e' if avg_cargo_temp > 6.0 else '#48bb78'}">{avg_cargo_temp:.2f} °C</div>
        </div>
        """, unsafe_allow_html=True)
    with col3:
        st.markdown(f"""
        <div class="stCard">
            <div class="metric-label">Active Fleet Anomalies</div>
            <div class="metric-value" style="color: {'#e53e3e' if num_anomalies > 0 else '#48bb78'}">{num_anomalies}</div>
        </div>
        """, unsafe_allow_html=True)
    with col4:
        st.markdown(f"""
        <div class="stCard">
            <div class="metric-label">Active Condensation Risks</div>
            <div class="metric-value" style="color: {'#3182ce' if num_condensation_risks > 0 else '#48bb78'}">{num_condensation_risks}</div>
        </div>
        """, unsafe_allow_html=True)
    with col5:
        open_doors = len(latest_per_vehicle[latest_per_vehicle["Door_Status"] == "OPEN"])
        st.markdown(f"""
        <div class="stCard">
            <div class="metric-label">Open Cargo Doors</div>
            <div class="metric-value" style="color: {'#dd6b20' if open_doors > 0 else '#48bb78'}">{open_doors}</div>
        </div>
        """, unsafe_allow_html=True)

    # 1.5. Live Fleet Tracking Map
    st.markdown("---")
    st.subheader("📍 Live Fleet Tracking Map")

    # Distinct color palette for unique vehicle tracking
    VEHICLE_PALETTE = [
        {"name": "Cyan", "rgba": [0, 210, 255, 230], "hex": "#00d2ff"},
        {"name": "Coral", "rgba": [255, 107, 107, 230], "hex": "#ff6b6b"},
        {"name": "Emerald", "rgba": [46, 213, 115, 230], "hex": "#2ed573"},
        {"name": "Purple", "rgba": [165, 94, 234, 230], "hex": "#a55eea"},
        {"name": "Gold", "rgba": [254, 211, 48, 230], "hex": "#fed330"},
        {"name": "Hot Pink", "rgba": [255, 75, 145, 230], "hex": "#ff4b91"},
        {"name": "Teal", "rgba": [29, 209, 161, 230], "hex": "#1dd1a1"},
        {"name": "Royal Blue", "rgba": [75, 123, 236, 230], "hex": "#4b7bec"},
        {"name": "Tangerine", "rgba": [250, 130, 49, 230], "hex": "#fa8231"},
        {"name": "Lime", "rgba": [38, 222, 129, 230], "hex": "#26de81"},
    ]

    def get_truck_color(v_id):
        try:
            num = int(v_id.split("-")[-1]) - 1
        except Exception:
            num = hash(v_id)
        return VEHICLE_PALETTE[num % len(VEHICLE_PALETTE)]["rgba"]

    def get_truck_hex(v_id):
        try:
            num = int(v_id.split("-")[-1]) - 1
        except Exception:
            num = hash(v_id)
        return VEHICLE_PALETTE[num % len(VEHICLE_PALETTE)]["hex"]

    # Active vehicle filter
    available_trucks = sorted(silver_df["Vehicle_ID"].dropna().unique().tolist())
    map_filter_col1, map_filter_col2 = st.columns([3, 1])
    with map_filter_col1:
        selected_trucks = st.multiselect(
            "🚛 Vehicles to Display on Map:",
            options=available_trucks,
            default=available_trucks[:6] if len(available_trucks) > 6 else available_trucks
        )
    with map_filter_col2:
        trail_length = st.slider("Route Trail Length", min_value=5, max_value=50, value=15)

    # Render color badges for selected trucks
    if selected_trucks:
        legend_badges = "".join([
            f'<span style="background-color: {get_truck_hex(v)}; color: #000; font-weight: bold; '
            f'padding: 3px 8px; border-radius: 4px; margin-right: 8px; font-size: 13px;">{v}</span>'
            for v in selected_trucks
        ])
        st.markdown(f"**Vehicle Colors:** {legend_badges}", unsafe_allow_html=True)

    # Grab coordinates per vehicle
    map_df = silver_df.sort_values(["Vehicle_ID", "Timestamp"], ascending=[True, False])
    if selected_trucks:
        map_df = map_df[map_df["Vehicle_ID"].isin(selected_trucks)]
    map_df = map_df.groupby("Vehicle_ID").head(trail_length).reset_index(drop=True)
    map_df = map_df[["Latitude", "Longitude", "Vehicle_ID", "Cargo_Temp", "Door_Status", "Vibration", "Condensation_Risk", "Timestamp"]].dropna()

    if not map_df.empty:
        map_df = map_df.rename(columns={"Latitude": "lat", "Longitude": "lon"})
        map_df["Timestamp"] = map_df["Timestamp"].astype(str)
        map_df["color"] = map_df["Vehicle_ID"].apply(get_truck_color)
        
        import pydeck as pdk
        
        point_layer = pdk.Layer(
            "ScatterplotLayer",
            data=map_df,
            get_position=["lon", "lat"],
            get_color="color",
            get_radius=200,
            radius_min_pixels=3,
            radius_max_pixels=7,
            pickable=True,
            auto_highlight=True,
        )
        
        view_state = pdk.ViewState(
            latitude=float(map_df["lat"].mean()),
            longitude=float(map_df["lon"].mean()),
            zoom=9.2,
            pitch=0,
        )
        
        st.pydeck_chart(
            pdk.Deck(
                layers=[point_layer],
                initial_view_state=view_state,
                tooltip={
                    "html": "<b>Truck: {Vehicle_ID}</b><br/>"
                            "<b>Time:</b> {Timestamp}<br/>"
                            "<b>Cargo Temp:</b> {Cargo_Temp}°C<br/>"
                            "<b>Door:</b> {Door_Status}<br/>"
                            "<b>Vibration:</b> {Vibration}G",
                    "style": {"backgroundColor": "#1a202c", "color": "white", "fontSize": "12px"}
                },
                map_style=None
            ),
            use_container_width=True
        )
    else:
        st.info("No coordinates available for the selected vehicle(s).")

else:
    st.info("No data received in Silver table yet. Please make sure the telemetry simulator and Spark streaming container are running.")

st.markdown("---")

# 2. Charts Section
if not silver_df.empty:
    chart_col1, chart_col2 = st.columns(2)
    
    with chart_col1:
        st.subheader("Cargo Temp vs Ambient Temp")
        # Line chart showing Cargo Temp for each vehicle
        fig_temp = px.line(
            silver_df, 
            x="Timestamp", 
            y="Cargo_Temp", 
            color="Vehicle_ID", 
            title="Cargo Temperature Timeline (Silver)",
            template="plotly_dark"
        )
        st.plotly_chart(fig_temp, use_container_width=True)
        
    with chart_col2:
        st.subheader("Vibration G-Force")
        # Vibration chart
        fig_vib = px.line(
            silver_df,
            x="Timestamp",
            y="Vibration",
            color="Vehicle_ID",
            title="Vibration Timeline (Silver)",
            template="plotly_dark"
        )
        st.plotly_chart(fig_vib, use_container_width=True)

st.markdown("---")

# 3. Tables & SQL Console
tab1, tab2, tab3 = st.tabs(["Gold Aggregates", "Silver Cleaned Events", "Ad-Hoc SQL Console"])

with tab1:
    st.subheader("Gold Table: 1-Minute Windowed Aggregates")
    if not gold_df.empty:
        st.dataframe(gold_df, use_container_width=True)
    else:
        st.info("Waiting for finalized aggregation windows from the Gold table (takes ~1 minute and 10 seconds of stream ingestion)...")

with tab2:
    st.subheader("Silver Table: Cleaned and Enriched Events")
    if not silver_df.empty:
        st.dataframe(silver_df.head(50), use_container_width=True)
    else:
        st.info("Waiting for Silver table records...")

with tab3:
    st.subheader("Execute Ad-Hoc SQL Queries")
    
    # Create two columns: left for Schema Explorer, right for SQL Console
    schema_col, console_col = st.columns([1, 2])
    
    with schema_col:
        st.markdown("### 📁 Schema Explorer")
        st.caption("Expand tables to view column schemas:")
        
        for name, path in [("bronze", lakehouse.path_bronze), ("silver", lakehouse.path_silver), ("gold", lakehouse.path_gold)]:
            # Check if temp view is registered
            if spark.catalog.tableExists(name):
                with st.expander(f"📊 {name.upper()}"):
                    st.markdown(f"**Path:** `{path}`")
                    try:
                        fields = [(f.name, f.dataType.simpleString()) for f in spark.table(name).schema.fields]
                        schema_df = pd.DataFrame(fields, columns=["Column", "Type"])
                        st.dataframe(schema_df, use_container_width=True, hide_index=True)
                    except Exception as ex:
                        st.error(f"Failed to load schema: {ex}")
            else:
                st.markdown(f"❌ **{name.upper()}**: *Not created yet*")
                st.caption(f"Path: `{path}`")
                
    with console_col:
        st.markdown("### 💻 SQL Console")
        
        # Templates dictionary mapping description to query
        templates = {
            "Custom Query (Blank)": "",
            "Total Events & Max Temp per Vehicle (Silver)": "SELECT Vehicle_ID, count(*) as total_events, max(Cargo_Temp) as max_cargo_temp FROM silver GROUP BY Vehicle_ID",
            "Active Fleet Anomalies & Condensation Risks (Silver)": "SELECT Timestamp, Vehicle_ID, Cargo_Temp, Dew_Point, Humidity, Condensation_Risk FROM silver WHERE Cargo_Temp > 8.0 OR Door_Status = 'OPEN' OR Vibration > 3.0 OR Condensation_Risk = true ORDER BY Timestamp DESC LIMIT 50",
            "Latest Cleaned Events (Silver)": "SELECT Timestamp, Vehicle_ID, Ambient_Temp, Cargo_Temp, Dew_Point, Condensation_Risk FROM silver ORDER BY Timestamp DESC LIMIT 10",
            "Gold Windowed Aggregates (Gold)": "SELECT Window_Start, Window_End, Vehicle_ID, Avg_Cargo_Temp, Avg_Dew_Point, Condensation_Risk_Count FROM gold ORDER BY Window_End DESC LIMIT 20",
            "Raw Kafka Payloads (Bronze)": "SELECT timestamp, partition, offset, substring(CAST(value AS STRING), 1, 120) as payload_preview FROM bronze ORDER BY timestamp DESC LIMIT 10"
        }
        
        selected_template = st.selectbox("Choose a query template:", list(templates.keys()))
        
        if "query_editor" not in st.session_state:
            st.session_state.query_editor = templates[selected_template]
            st.session_state.prev_template = selected_template

        if st.session_state.prev_template != selected_template:
            st.session_state.query_editor = templates[selected_template]
            st.session_state.prev_template = selected_template

        user_query = st.text_area("SQL Editor", value=st.session_state.query_editor, height=150)
        
        # Capture current typed value in session state so it survives typing updates
        st.session_state.query_editor = user_query
        
        if st.button("Execute Query", type="primary"):
            if not user_query.strip():
                st.warning("Please enter a SQL query first.")
            else:
                try:
                    with st.spinner("Running SQL Query..."):
                        import time as t_mod
                        t_start = t_mod.time()
                        # Refresh views dynamically to invalidate cached metadata files
                        try:
                            register_views()
                        except Exception:
                            pass
                        query_res = spark.sql(user_query).toPandas()
                        t_elapsed = t_mod.time() - t_start
                        
                    st.success(f"Query executed successfully in {t_elapsed:.3f} seconds!")
                    if query_res.empty:
                        st.info("Query returned 0 results.")
                    else:
                        st.caption(f"Showing {len(query_res)} rows")
                        st.dataframe(query_res, use_container_width=True)
                except Exception as e:
                    st.error(f"SQL execution error: {e}")

# Auto Refresh loop delay
if auto_refresh:
    import time
    time.sleep(refresh_rate)
    st.rerun()
