import streamlit as st
from pyspark.sql import SparkSession
import pandas as pd
import plotly.express as px
import datetime

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
    for name, path in [("bronze", lakehouse.path_bronze), ("silver", lakehouse.path_silver), ("gold", lakehouse.path_gold)]:
        try:
            spark.read.format("delta").load(path).createOrReplaceTempView(name)
        except Exception:
            pass
except Exception as e:
    spark_connected = False
    spark_error = e

# Sidebar Controls
st.sidebar.title("❄️ Cold-Chain Lakehouse")
st.sidebar.markdown("This dashboard queries the **Gold** and **Silver** tables in the Delta Lake on MinIO.")

refresh_rate = st.sidebar.slider("Refresh Rate (seconds)", min_value=2, max_value=30, value=5)
st.sidebar.caption("💡 Dynamic temporary views (`bronze`, `silver`, `gold`) are automatically registered for easy SQL querying.")
auto_refresh = st.sidebar.checkbox("Auto Refresh Data", value=True)

if not spark_connected:
    st.error(f"Failed to connect to Spark. Error: {spark_error}")
    st.stop()

# Auto refresh logic
if auto_refresh:
    time_delta = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    st.sidebar.caption(f"Last updated: {time_delta}")
    st.sidebar.button("Force Refresh")

# Helper to refresh temp views and invalidate cached file metadata
def refresh_views():
    for name, path in [("bronze", lakehouse.path_bronze), ("silver", lakehouse.path_silver), ("gold", lakehouse.path_gold)]:
        # Attempt to register if missing, otherwise refresh it
        if not spark.catalog.tableExists(name):
            try:
                spark.read.format("delta").load(path).createOrReplaceTempView(name)
            except Exception:
                pass
        else:
            try:
                spark.catalog.refreshTable(name)
            except Exception:
                pass

# Run initial view refresh
refresh_views()

# Main UI Header
st.title("Cold-Chain IoT Streaming Lakehouse Dashboard")
st.markdown("---")

# 1. KPIs Section
st.subheader("Real-Time Fleet Status")
col1, col2, col3, col4 = st.columns(4)

# Fetch latest status from Silver Table view
try:
    silver_df = spark.sql("SELECT * FROM silver ORDER BY Timestamp DESC LIMIT 100").toPandas()
except Exception as e:
    silver_df = pd.DataFrame()
    st.warning(f"Waiting for Silver Delta table data... ({e})")

# Fetch status from Gold Table view
try:
    gold_df = spark.sql("SELECT * FROM gold ORDER BY Window_End DESC LIMIT 100").toPandas()
except Exception as e:
    gold_df = pd.DataFrame()
    st.warning(f"Waiting for Gold Delta table data... ({e})")

# Render metrics if data is available
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
        open_doors = len(latest_per_vehicle[latest_per_vehicle["Door_Status"] == "OPEN"])
        st.markdown(f"""
        <div class="stCard">
            <div class="metric-label">Open Cargo Doors</div>
            <div class="metric-value" style="color: {'#dd6b20' if open_doors > 0 else '#48bb78'}">{open_doors}</div>
        </div>
        """, unsafe_allow_html=True)
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
            "Active Fleet Anomalies (Silver)": "SELECT * FROM silver WHERE Cargo_Temp > 8.0 OR Door_Status = 'OPEN' OR Vibration > 3.0 ORDER BY Timestamp DESC LIMIT 50",
            "Latest Cleaned Events (Silver)": "SELECT Timestamp, Vehicle_ID, Ambient_Temp, Cargo_Temp, Door_Status FROM silver ORDER BY Timestamp DESC LIMIT 10",
            "Gold Windowed Aggregates (Gold)": "SELECT Window_Start, Window_End, Vehicle_ID, Avg_Cargo_Temp, Anomaly_Flag FROM gold ORDER BY Window_End DESC LIMIT 20",
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
                        refresh_views()
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
