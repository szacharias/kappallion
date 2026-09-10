import time
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

print("Initializing SparkSession with Delta Lake...", flush=True)

# Build SparkSession with Delta Lake and S3/MinIO configurations
spark = SparkSession.builder \
    .appName("ColdChainDeltaLakehousePipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("SparkSession successfully created!", flush=True)

# ------------------------------------------------------------------
# 1. Database & Table Scaffolding (DDL using USING delta)
# ------------------------------------------------------------------

print("Creating database and Medallion tables if they do not exist...", flush=True)
spark.sql("CREATE DATABASE IF NOT EXISTS db")

# Bronze: Raw Kafka logs mapped to MinIO path
spark.sql("""
CREATE TABLE IF NOT EXISTS db.bronze_telemetry (
    key BINARY,
    value BINARY,
    topic STRING,
    partition INT,
    offset BIGINT,
    timestamp TIMESTAMP,
    timestampType INT
) USING delta LOCATION 's3a://warehouse/bronze_telemetry'
""")

# Silver: Cleansed, typed, deduplicated, enriched mapped to MinIO path
spark.sql("""
CREATE TABLE IF NOT EXISTS db.silver_telemetry (
    Timestamp TIMESTAMP,
    Vehicle_ID STRING,
    Ambient_Temp DOUBLE,
    Cargo_Temp DOUBLE,
    Humidity DOUBLE,
    Vibration DOUBLE,
    Door_Status STRING,
    Temp_Delta DOUBLE
) USING delta LOCATION 's3a://warehouse/silver_telemetry'
""")

# Gold: Stateful event-time aggregates mapped to MinIO path
spark.sql("""
CREATE TABLE IF NOT EXISTS db.gold_telemetry_agg (
    Window_Start TIMESTAMP,
    Window_End TIMESTAMP,
    Vehicle_ID STRING,
    Avg_Cargo_Temp DOUBLE,
    Avg_Ambient_Temp DOUBLE,
    Max_Vibration DOUBLE,
    Door_Open_Count LONG,
    Anomaly_Flag BOOLEAN
) USING delta LOCATION 's3a://warehouse/gold_telemetry_agg'
""")

print("Database and Delta tables initialized.", flush=True)

# ------------------------------------------------------------------
# 2. Bronze Stream (Kafka -> Bronze Delta Table)
# ------------------------------------------------------------------
print("Starting Bronze Stream...", flush=True)

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "cold-chain-telemetry") \
    .option("startingOffsets", "latest") \
    .load()

# Write raw streaming events to Bronze table using Delta format
bronze_query = kafka_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .option("checkpointLocation", "/tmp/spark-checkpoints/bronze") \
    .toTable("db.bronze_telemetry")

# ------------------------------------------------------------------
# 3. Silver Stream (Bronze Delta -> Silver Delta Table)
# ------------------------------------------------------------------
print("Starting Silver Stream...", flush=True)

# Read streaming source from Bronze Delta table
bronze_stream_df = spark.readStream \
    .format("delta") \
    .load("s3a://warehouse/bronze_telemetry")

# Define IoT payload schema
telemetry_schema = StructType([
    StructField("Timestamp", TimestampType(), True),
    StructField("Vehicle_ID", StringType(), True),
    StructField("Ambient_Temp", DoubleType(), True),
    StructField("Cargo_Temp", DoubleType(), True),
    StructField("Humidity", DoubleType(), True),
    StructField("Vibration", DoubleType(), True),
    StructField("Door_Status", StringType(), True)
])

# Parse binary JSON value from Bronze
parsed_silver_df = bronze_stream_df \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(f.from_json("json_str", telemetry_schema).alias("data")) \
    .select("data.*") \
    .filter(f.col("Vehicle_ID").isNotNull() & f.col("Timestamp").isNotNull()) \
    .withWatermark("Timestamp", "10 seconds") \
    .dropDuplicates(["Vehicle_ID", "Timestamp"]) \
    .withColumn("Temp_Delta", f.round(f.col("Cargo_Temp") - f.col("Ambient_Temp"), 2))

# Write cleaned and enriched events to Silver Delta table
silver_query = parsed_silver_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .option("checkpointLocation", "/tmp/spark-checkpoints/silver") \
    .toTable("db.silver_telemetry")

# ------------------------------------------------------------------
# 4. Gold Stream (Silver Delta -> Gold Delta Table)
# ------------------------------------------------------------------
print("Starting Gold Stream...", flush=True)

# Read streaming source from Silver Delta table
silver_stream_df = spark.readStream \
    .format("delta") \
    .load("s3a://warehouse/silver_telemetry")

# Apply 1-minute window, sliding every 10 seconds
gold_agg_df = silver_stream_df \
    .withWatermark("Timestamp", "10 seconds") \
    .groupBy(
        f.window(f.col("Timestamp"), "1 minute", "10 seconds"),
        f.col("Vehicle_ID")
    ) \
    .agg(
        f.round(f.avg("Cargo_Temp"), 2).alias("Avg_Cargo_Temp"),
        f.round(f.avg("Ambient_Temp"), 2).alias("Avg_Ambient_Temp"),
        f.round(f.max("Vibration"), 2).alias("Max_Vibration"),
        f.sum(f.when(f.col("Door_Status") == "OPEN", 1).otherwise(0)).alias("Door_Open_Count")
    ) \
    .withColumn("Window_Start", f.col("window.start")) \
    .withColumn("Window_End", f.col("window.end")) \
    .withColumn(
        "Anomaly_Flag",
        (f.col("Avg_Cargo_Temp") > 8.0) | (f.col("Door_Open_Count") > 0) | (f.col("Max_Vibration") > 3.0)
    ) \
    .select("Window_Start", "Window_End", "Vehicle_ID", "Avg_Cargo_Temp", "Avg_Ambient_Temp", "Max_Vibration", "Door_Open_Count", "Anomaly_Flag")

# Write aggregates to Gold Delta table (append Mode writes only finalized windows)
gold_query = gold_agg_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .option("checkpointLocation", "/tmp/spark-checkpoints/gold") \
    .toTable("db.gold_telemetry_agg")

print("All streaming queries started. Monitoring Delta Lakehouse...", flush=True)

# Await termination of any of the streams
spark.streams.awaitAnyTermination()
