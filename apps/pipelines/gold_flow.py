import pyspark.sql.functions as f

def start_gold_stream(spark, lakehouse):
    print("Starting Gold Stream...", flush=True)
    
    # Read streaming source from Silver Delta path
    silver_stream_df = spark.readStream \
        .format("delta") \
        .load(lakehouse.path_silver)

    # Apply 1-minute window, sliding every 10 seconds (optimized for PoC speed)
    # Watermark of 10 seconds allows fast finalized-window output
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

    return gold_agg_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .option("checkpointLocation", lakehouse.pipeline_cfg.get("spark", "checkpoint_gold")) \
        .start(lakehouse.path_gold)
