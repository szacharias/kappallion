import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "core")))
from lakehouse_spark_env import LakehouseSparkEnv

# Initialize environment context
lakehouse = LakehouseSparkEnv("DeltaTableMaintenance", log_level="WARN")
spark = lakehouse.spark

print("Starting Delta Table Maintenance Batch Job...", flush=True)

# Allow vacuuming files immediately for the PoC (disables the default 7-day retention safety check)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

tables = [lakehouse.table_bronze, lakehouse.table_silver, lakehouse.table_gold]

for table in tables:
    print(f"\n--- Maintenance for table: {table} ---", flush=True)
    path = lakehouse.path_bronze if "bronze" in table else (lakehouse.path_silver if "silver" in table else lakehouse.path_gold)
    
    # 1. Optimization / Compaction: Merge small parquet files and optionally Z-Order
    try:
        print(f"Optimizing / compacting files for {table} (Path: {path})...", flush=True)
        # If it's the silver table, Z-Order by Timestamp for fast time-range queries
        if "silver" in table:
            opt_res = spark.sql(f"OPTIMIZE '{path}' ZORDER BY (Timestamp)")
        else:
            opt_res = spark.sql(f"OPTIMIZE '{path}'")
        opt_res.show(truncate=False)
    except Exception as e:
        print(f"Failed to optimize {table}: {e}", flush=True)
        
    # 2. Vacuuming: Clean up old snapshots/orphaned files
    try:
        print(f"Vacuuming historical logs for {table} (Path: {path})...", flush=True)
        vacuum_res = spark.sql(f"VACUUM '{path}' RETAIN 1 HOURS")
        vacuum_res.show(truncate=False)
    except Exception as e:
        print(f"Failed to vacuum {table}: {e}", flush=True)

print("\nDelta Table Maintenance Completed Successfully!", flush=True)
spark.stop()
