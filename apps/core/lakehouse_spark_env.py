import os
import configparser
from pyspark.sql import SparkSession

class LakehouseSparkEnv:
    def __init__(self, app_name: str, log_level: str = "WARN"):
        # 1. Resolve configuration paths with environment variable overrides
        fallback_config = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "config"))
        config_dir = os.environ.get("CONFIG_DIR", fallback_config)
        minio_config_path = os.environ.get("MINIO_CONFIG_PATH", os.path.join(config_dir, "minio_path.conf"))
        pipeline_config_path = os.environ.get("PIPELINE_CONFIG_PATH", os.path.join(config_dir, "pipeline.conf"))
        
        # 2. Parse INI configurations
        self.minio_cfg = configparser.ConfigParser()
        self.minio_cfg.read(minio_config_path)
        
        self.pipeline_cfg = configparser.ConfigParser()
        self.pipeline_cfg.read(pipeline_config_path)
        
        # 3. Initialize the SparkSession (auto-loads settings from spark-defaults.conf)
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .getOrCreate()
            
        self.spark.sparkContext.setLogLevel(log_level)
        
        # 4. Map and expose metadata fields for easy access in scripts
        self.table_bronze = self.pipeline_cfg.get("tables", "bronze")
        self.table_silver = self.pipeline_cfg.get("tables", "silver")
        self.table_gold = self.pipeline_cfg.get("tables", "gold")
        
        self.path_bronze = self.minio_cfg.get("minio", "bronze_path")
        self.path_silver = self.minio_cfg.get("minio", "silver_path")
        self.path_gold = self.minio_cfg.get("minio", "gold_path")
