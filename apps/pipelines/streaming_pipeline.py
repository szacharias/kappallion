import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "core")))
from lakehouse_spark_env import LakehouseSparkEnv
from bronze_flow import start_bronze_stream
from silver_flow import start_silver_stream
from gold_flow import start_gold_stream

# Initialize environment context
lakehouse = LakehouseSparkEnv("ColdChainLakehousePipeline", log_level="WARN")
spark = lakehouse.spark

# Start all streaming flows concurrently
bronze_query = start_bronze_stream(spark, lakehouse)
silver_query = start_silver_stream(spark, lakehouse)
gold_query = start_gold_stream(spark, lakehouse)

# flush = true pushes print immediatly through buffers 
print("All streaming queries started. Monitoring...", flush=True)

# Await termination of any of the streams
spark.streams.awaitAnyTermination()
