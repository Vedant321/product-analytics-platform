# Databricks notebook source
# DBTITLE 1,Platform Configuration
"""
Product Analytics Platform - Central Configuration

This module defines all configuration settings for the platform including:
- Unity Catalog namespaces (catalog, schema, volume)
- Table definitions (Medallion Architecture with Star Schema)
  * Bronze: Raw events (batch CSV + streaming Kafka)
  * Silver: Star schema (fact_events + dimension tables with SCD Type 2)
  * Gold: Pre-aggregated analytics tables
- Storage locations and checkpoint paths
- Streaming configuration (Kafka settings, trigger intervals)
- Environment settings (optimization, retention)
"""

import logging

# Configure logging
logger = logging.getLogger(__name__)


from dataclasses import dataclass
from typing import Dict
import os


@dataclass
class CatalogConfig:
    """Unity Catalog namespace configuration"""
    catalog: str = "product_analytics"
    schema: str = "ecommerce"
    volume: str = "raw_data"
    
    @property
    def full_schema(self) -> str:
        """Returns fully qualified schema name"""
        return f"{self.catalog}.{self.schema}"
    
    @property
    def volume_path(self) -> str:
        """Returns volume path for raw data storage"""
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"


@dataclass
class TableNames:
    """Table naming configuration for all layers - STAR SCHEMA ARCHITECTURE"""
    
    # Bronze Layer - Raw Events (from CSV batch load or Kafka streaming)
    bronze_events: str = "bronze_events"
    
    # Silver Layer - Star Schema (Fact + Dimension Tables)
    # Fact Table (110M+ rows)
    fact_events: str = "fact_events"
    
    # Dimension Tables (Silver layer with SCD Type 2)
    silver_dim_date: str = "silver_dim_date"
    silver_dim_products: str = "silver_dim_products"
    silver_dim_categories: str = "silver_dim_categories"
    silver_dim_users: str = "silver_dim_users"
    
    # Gold Layer - Pre-aggregated Analytics Tables
    gold_user_metrics: str = "gold_user_metrics"
    gold_product_performance: str = "gold_product_performance"
    gold_category_performance: str = "gold_category_performance"
    gold_funnel_metrics: str = "gold_funnel_metrics"
    gold_daily_metrics: str = "gold_daily_metrics"
    gold_session_metrics: str = "gold_session_metrics"
    gold_brand_performance: str = "gold_brand_performance"
    gold_cohort_analysis: str = "gold_cohort_analysis"
    
    def get_full_name(self, table_name: str, catalog_config: CatalogConfig) -> str:
        """Returns fully qualified table name"""
        return f"{catalog_config.catalog}.{catalog_config.schema}.{table_name}"


@dataclass
class StorageConfig:
    """Storage paths and checkpoint locations"""
    
    # Base checkpoint directory
    checkpoint_base: str = "/tmp/checkpoints/product_analytics"
    
    @property
    def bronze_checkpoint(self) -> str:
        return f"{self.checkpoint_base}/bronze_events"
    
    @property
    def silver_checkpoint(self) -> str:
        return f"{self.checkpoint_base}/silver_events"
    
    @property
    def gold_checkpoint(self) -> str:
        return f"{self.checkpoint_base}/gold_aggregations"


@dataclass
class StreamingConfig:
    """Streaming and Kafka configuration"""
    
    # Kafka settings (to be configured for actual deployment)
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "product_events"
    
    # Streaming trigger intervals
    bronze_trigger_interval: str = "10 seconds"
    silver_trigger_interval: str = "30 seconds"
    gold_trigger_interval: str = "1 minute"
    
    # Replay settings
    replay_speed_multiplier: int = 100  # 100x faster than real-time
    replay_batch_size: int = 10000


@dataclass
class EnvironmentConfig:
    """Environment-specific settings"""
    
    environment: str = os.getenv("ENVIRONMENT", "development")
    
    # Data retention (in days)
    bronze_retention_days: int = 30
    silver_retention_days: int = 90
    gold_retention_days: int = 365
    
    # Optimization settings
    enable_auto_optimize: bool = True
    enable_auto_compaction: bool = True
    z_order_columns: Dict[str, list] = None
    
    def __post_init__(self):
        if self.z_order_columns is None:
            self.z_order_columns = {
                "bronze_events": ["event_time", "event_type"],
                "fact_events": ["event_time", "user_sk", "product_sk"],
                "gold_user_metrics": ["user_sk"],
                "gold_product_performance": ["product_sk"],
                "gold_daily_metrics": ["metric_date"]
            }


class PlatformConfig:
    """Central configuration manager for the entire platform"""
    
    def __init__(self):
        self.catalog = CatalogConfig()
        self.tables = TableNames()
        self.storage = StorageConfig()
        self.streaming = StreamingConfig()
        self.environment = EnvironmentConfig()
    
    def get_table(self, table_name: str) -> str:
        """Get fully qualified table name"""
        return self.tables.get_full_name(table_name, self.catalog)
    
    def display_config(self):
        """Display current configuration"""
        print("=" * 80)
        logger.info("PRODUCT ANALYTICS PLATFORM - CONFIGURATION")
        print("=" * 80)
        logger.info(f"\nEnvironment: {self.environment.environment}")
        logger.info(f"\nCatalog: {self.catalog.catalog}")
        logger.info(f"Schema: {self.catalog.schema}")
        logger.info(f"Volume Path: {self.catalog.volume_path}")
        logger.info("\nArchitecture: Medallion (Bronze-Silver-Gold) with Star Schema")
        logger.info("  Bronze: Raw events (batch + streaming)")
        logger.info("  Silver: Star schema (fact + dimensions with SCD Type 2)")
        logger.info("  Gold: Pre-aggregated analytics (8 tables)")
        logger.info("\nCheckpoint Locations:")
        logger.info(f"  Bronze: {self.storage.bronze_checkpoint}")
        logger.info(f"  Silver: {self.storage.silver_checkpoint}")
        logger.info(f"  Gold: {self.storage.gold_checkpoint}")
        print("\n" + "=" * 80)


# Initialize global configuration
config = PlatformConfig()

# Display configuration
config.display_config()

# COMMAND ----------

# DBTITLE 1,Create Unity Catalog Structure
# MAGIC %sql
# MAGIC -- Create catalog if it doesn't exist
# MAGIC CREATE CATALOG IF NOT EXISTS product_analytics;
# MAGIC
# MAGIC -- Use the catalog
# MAGIC USE CATALOG product_analytics;
# MAGIC
# MAGIC -- Create schema
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce
# MAGIC   COMMENT 'eCommerce product analytics data';
# MAGIC
# MAGIC -- Create volume for raw data storage
# MAGIC CREATE VOLUME IF NOT EXISTS product_analytics.ecommerce.raw_data
# MAGIC   COMMENT 'Raw data files from Kaggle dataset';

# COMMAND ----------

# DBTITLE 1,Example Usage
"""
Example: How to use this configuration in other notebooks
"""

# Access configuration values
logger.info("\n📋 EXAMPLE USAGE:")
print("=" * 60)

# Get fully qualified table names
logger.info("\n1. Get fully qualified table names:")
logger.info(f"   Bronze Events: {config.get_table('bronze_events')}")
logger.info(f"   Fact Events: {config.get_table('fact_events')}")
logger.info(f"   Dim Products: {config.get_table('silver_dim_products')}")
logger.info(f"   Gold User Metrics: {config.get_table('gold_user_metrics')}")

# Access volume path
logger.info("\n2. Volume path for raw data:")
logger.info(f"   {config.catalog.volume_path}")

# Access checkpoint locations
logger.info("\n3. Checkpoint locations:")
logger.info(f"   Bronze: {config.storage.bronze_checkpoint}")
logger.info(f"   Silver: {config.storage.silver_checkpoint}")
logger.info(f"   Gold: {config.storage.gold_checkpoint}")

# Streaming settings
logger.info("\n4. Streaming configuration:")
logger.info(f"   Kafka Topic: {config.streaming.kafka_topic}")
logger.info(f"   Bronze Trigger: {config.streaming.bronze_trigger_interval}")
logger.info(f"   Replay Speed: {config.streaming.replay_speed_multiplier}x")

print("\n" + "=" * 60)
logger.info("\n Configuration loaded successfully!")
logger.info("\n To use in other notebooks:")
logger.info("   %run ./config/platform_config")
logger.info("   Then access via: config.get_table('bronze_events')")