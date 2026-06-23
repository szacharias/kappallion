import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "core")))
from lakehouse_spark_env import LakehouseSparkEnv

print("Initializing Lakehouse Bootstrapper...", flush=True)

# Initialize environment context
lakehouse = LakehouseSparkEnv("LakehouseBootstrapper", log_level="WARN")
spark = lakehouse.spark

# ------------------------------------------------------------------
# Medallion Table Scaffolding (DDL)
# ------------------------------------------------------------------

print("Creating database and Medallion tables if they do not exist...", flush=True)
spark.sql("CREATE DATABASE IF NOT EXISTS db")

# 1. Bronze: Raw Kafka logs
print(f"Creating Bronze table: {lakehouse.table_bronze} at {lakehouse.path_bronze}...", flush=True)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {lakehouse.table_bronze} (
    key BINARY,
    value BINARY,
    topic STRING,
    partition INT,
    offset BIGINT,
    timestamp TIMESTAMP,
    timestampType INT
) USING delta
LOCATION '{lakehouse.path_bronze}'
""")

# 2. Silver: Cleansed, typed, deduplicated, enriched
print(f"Creating Silver table: {lakehouse.table_silver} at {lakehouse.path_silver}...", flush=True)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {lakehouse.table_silver} (
    Timestamp TIMESTAMP,
    Vehicle_ID STRING,
    Ambient_Temp DOUBLE,
    Cargo_Temp DOUBLE,
    Humidity DOUBLE,
    Vibration DOUBLE,
    Door_Status STRING,
    Temp_Delta DOUBLE
) USING delta
PARTITIONED BY (Vehicle_ID)
LOCATION '{lakehouse.path_silver}'
""")

# 3. Gold: Stateful event-time aggregates
print(f"Creating Gold table: {lakehouse.table_gold} at {lakehouse.path_gold}...", flush=True)
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {lakehouse.table_gold} (
    Window_Start TIMESTAMP,
    Window_End TIMESTAMP,
    Vehicle_ID STRING,
    Avg_Cargo_Temp DOUBLE,
    Avg_Ambient_Temp DOUBLE,
    Max_Vibration DOUBLE,
    Door_Open_Count LONG,
    Anomaly_Flag BOOLEAN
) USING delta
LOCATION '{lakehouse.path_gold}'
""")

print("Lakehouse Bootstrapping Completed Successfully!", flush=True)
spark.stop()
