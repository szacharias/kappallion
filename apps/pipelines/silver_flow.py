import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def start_silver_stream(spark, lakehouse):
    print("Starting Silver Stream...", flush=True)
    
    # Read streaming source from Bronze Delta path
    bronze_stream_df = spark.readStream \
        .format("delta") \
        .load(lakehouse.path_bronze)

    # Define IoT payload schema matching the simulator structure
    telemetry_schema = StructType([
        StructField("eventId", StringType(), True),
        StructField("vehicleId", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("telemetry", StructType([
            StructField("ambientTempC", DoubleType(), True),
            StructField("cargoContainerTempC", DoubleType(), True),
            StructField("relativeHumidityPct", DoubleType(), True),
            StructField("vibrationG", DoubleType(), True),
            StructField("doorStatus", StringType(), True)
        ]), True)
    ])

    # Parse binary JSON value from Bronze and map to Silver schema
    parsed_silver_df = bronze_stream_df \
        .selectExpr("CAST(value AS STRING) as json_str") \
        .select(f.from_json("json_str", telemetry_schema).alias("data")) \
        .select(
            f.to_timestamp("data.timestamp", "yyyy-MM-dd HH:mm:ss").alias("Timestamp"),
            f.col("data.vehicleId").alias("Vehicle_ID"),
            f.col("data.telemetry.ambientTempC").alias("Ambient_Temp"),
            f.col("data.telemetry.cargoContainerTempC").alias("Cargo_Temp"),
            f.col("data.telemetry.relativeHumidityPct").alias("Humidity"),
            f.col("data.telemetry.vibrationG").alias("Vibration"),
            f.col("data.telemetry.doorStatus").alias("Door_Status")
        ) \
        .filter(f.col("Vehicle_ID").isNotNull() & f.col("Timestamp").isNotNull()) \
        .withWatermark("Timestamp", "10 seconds") \
        .dropDuplicates(["Vehicle_ID", "Timestamp"]) \
        .withColumn("Temp_Delta", f.round(f.col("Cargo_Temp") - f.col("Ambient_Temp"), 2))

    # Write cleaned and enriched events to Silver table path
    return parsed_silver_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .option("checkpointLocation", lakehouse.pipeline_cfg.get("spark", "checkpoint_silver")) \
        .start(lakehouse.path_silver)
