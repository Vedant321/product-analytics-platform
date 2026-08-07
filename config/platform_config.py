# Databricks notebook source
# DBTITLE 1,Platform Configuration
"""
Product Analytics Platform - Central Configuration

This module defines all configuration settings for the platform including:
- Unity Catalog namespaces
- Table definitions (Bronze, Silver, Gold)
- Storage locations
- Checkpoint paths
- Environment settings
"""

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
    """Table naming configuration for all layers"""
    
    # Bronze Layer - Raw Events
    bronze_events: str = "bronze_events"
    
    # Silver Layer - Cleaned & Validated
    silver_events_enriched: str = "silver_events_enriched"
    silver_sessions_aggregated: str = "silver_sessions_aggregated"
    silver_users: str = "silver_users"
    
    # Gold Layer - Analytics Ready
    gold_user_metrics: str = "gold_user_metrics"
    gold_product_metrics: str = "gold_product_metrics"
    gold_category_metrics: str = "gold_category_metrics"
    gold_funnel_metrics: str = "gold_funnel_metrics"
    gold_daily_revenue: str = "gold_daily_revenue"
    
    # Dimension Tables
    dim_products: str = "dim_products"
    dim_categories: str = "dim_categories"
    dim_users: str = "dim_users"
    
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
                "silver_events": ["event_date", "user_id"],
                "gold_user_metrics": ["metric_date", "user_id"]
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
        print("PRODUCT ANALYTICS PLATFORM - CONFIGURATION")
        print("=" * 80)
        print(f"\nEnvironment: {self.environment.environment}")
        print(f"\nCatalog: {self.catalog.catalog}")
        print(f"Schema: {self.catalog.schema}")
        print(f"Volume Path: {self.catalog.volume_path}")
        print(f"\nBronze Checkpoint: {self.storage.bronze_checkpoint}")
        print(f"Silver Checkpoint: {self.storage.silver_checkpoint}")
        print(f"Gold Checkpoint: {self.storage.gold_checkpoint}")
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
print("\n📋 EXAMPLE USAGE:")
print("=" * 60)

# Get fully qualified table names
print("\n1. Get fully qualified table names:")
print(f"   Bronze Events: {config.get_table('bronze_events')}")
print(f"   Silver Events: {config.get_table('silver_events')}")
print(f"   Gold User Metrics: {config.get_table('gold_user_metrics')}")

# Access volume path
print(f"\n2. Volume path for raw data:")
print(f"   {config.catalog.volume_path}")

# Access checkpoint locations
print(f"\n3. Checkpoint locations:")
print(f"   Bronze: {config.storage.bronze_checkpoint}")
print(f"   Silver: {config.storage.silver_checkpoint}")
print(f"   Gold: {config.storage.gold_checkpoint}")

# Streaming settings
print(f"\n4. Streaming configuration:")
print(f"   Kafka Topic: {config.streaming.kafka_topic}")
print(f"   Bronze Trigger: {config.streaming.bronze_trigger_interval}")
print(f"   Replay Speed: {config.streaming.replay_speed_multiplier}x")

print("\n" + "=" * 60)
print("\n✅ Configuration loaded successfully!")
print("\n💡 To use in other notebooks:")
print("   %run ./config/platform_config")
print("   Then access via: config.get_table('bronze_events')")