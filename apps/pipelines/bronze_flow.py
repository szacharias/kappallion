def start_bronze_stream(spark, lakehouse):
    print("Starting Bronze Stream...", flush=True)
    
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", lakehouse.pipeline_cfg.get("kafka", "bootstrap_servers")) \
        .option("subscribe", lakehouse.pipeline_cfg.get("kafka", "topic")) \
        .option("startingOffsets", "latest") \
        .load()

    # Write raw streaming events to Bronze table path
    return kafka_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .trigger(processingTime="5 seconds") \
        .option("checkpointLocation", lakehouse.pipeline_cfg.get("spark", "checkpoint_bronze")) \
        .start(lakehouse.path_bronze)
