import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "core")))
from lakehouse_spark_env import LakehouseSparkEnv

# Initialize environment context
lakehouse = LakehouseSparkEnv("AdHocQueryEngine", log_level="ERROR")
spark = lakehouse.spark

print(f"\n=== Bronze Table Sample (Path: {lakehouse.path_bronze} - First 5 records) ===", flush=True)
try:
    spark.sql(f"SELECT CAST(value AS STRING) as payload, timestamp, partition, offset FROM delta.`{lakehouse.path_bronze}` ORDER BY timestamp DESC LIMIT 5").show(truncate=False)
except Exception as e:
    print(f"Error querying Bronze table: {e}", flush=True)

print(f"\n=== Silver Table Sample (Path: {lakehouse.path_silver} - First 5 records) ===", flush=True)
try:
    spark.sql(f"SELECT Timestamp, Vehicle_ID, Ambient_Temp, Cargo_Temp, Temp_Delta, Door_Status FROM delta.`{lakehouse.path_silver}` ORDER BY Timestamp DESC LIMIT 5").show(truncate=False)
except Exception as e:
    print(f"Error querying Silver table: {e}", flush=True)

print(f"\n=== Gold Table (Path: {lakehouse.path_gold} - Finalized Windows) ===", flush=True)
try:
    spark.sql(f"SELECT Window_Start, Window_End, Vehicle_ID, Avg_Cargo_Temp, Max_Vibration, Door_Open_Count, Anomaly_Flag FROM delta.`{lakehouse.path_gold}` ORDER BY Window_End DESC LIMIT 10").show(truncate=False)
except Exception as e:
    print(f"Error querying Gold table: {e}", flush=True)

spark.stop()
