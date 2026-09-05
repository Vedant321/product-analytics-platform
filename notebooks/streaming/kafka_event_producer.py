# Databricks notebook source
# DBTITLE 1,Kafka Event Producer - Lambda Architecture
# MAGIC %md
# MAGIC # Event Producer - Streaming Simulation
# MAGIC
# MAGIC **Purpose:** Simulates real-time e-commerce events using historical data
# MAGIC
# MAGIC ## What This Does
# MAGIC
# MAGIC 1. **Reads historical events** from `fact_events` table
# MAGIC 2. **Simulates real-time** by sending events with current timestamp
# MAGIC 3. **Writes to staging table** `streaming_events_input`
# MAGIC 4. **Consumer reads** from staging → writes to unified `bronze_events`
# MAGIC
# MAGIC ## Lambda Architecture
# MAGIC
# MAGIC ```
# MAGIC fact_events (historical)
# MAGIC      ↓
# MAGIC Producer adds current timestamp
# MAGIC      ↓
# MAGIC streaming_events_input (staging)
# MAGIC      ↓
# MAGIC Consumer reads stream
# MAGIC      ↓
# MAGIC bronze_events (unified: source='streaming')
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Configuration
import logging

# Configure logging
logger = logging.getLogger(__name__)

from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
from datetime import datetime

# Staging Table (simulates Kafka topic)
STAGING_TABLE = 'product_analytics.ecommerce.streaming_events_input'

# Simulation Configuration
EVENTS_PER_BATCH = 100
BATCH_INTERVAL_SECONDS = 10
TOTAL_BATCHES = 6

logger.info("Configuration loaded")
logger.info(f"Staging table: {STAGING_TABLE}")
logger.info(f"Events per batch: {EVENTS_PER_BATCH}")

# COMMAND ----------

# DBTITLE 1,Create Staging Table
# Create staging table (simulates Kafka topic)
# Schema: RAW natural keys only (matches bronze_events!)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {STAGING_TABLE} (
        event_time TIMESTAMP NOT NULL,
        event_type STRING NOT NULL,
        product_id INT NOT NULL,
        category_id BIGINT NOT NULL,
        category_code STRING,
        brand STRING,
        price DOUBLE,
        user_id INT NOT NULL,
        user_session STRING,
        batch_id INT,
        produced_at TIMESTAMP NOT NULL
    )
    USING DELTA
    COMMENT 'Staging table for streaming simulation (simulates Kafka topic)'
""")

logger.info(f"Staging table ready: {STAGING_TABLE}")
logger.info("Schema: RAW natural keys (product_id, user_id, category_id)")

# COMMAND ----------

# DBTITLE 1,Load Historical Events
# Load recent historical events from fact_events for simulation
# Note: fact_events has surrogate keys, we need to join back to get natural keys
logger.info("Loading historical events from fact_events...")

events_df = spark.sql("""
    SELECT 
        f.event_time,
        f.event_type,
        p.product_id,
        c.category_id,
        p.category_code,
        p.brand,
        f.price,
        u.user_id,
        f.user_session
    FROM product_analytics.ecommerce.fact_events f
    JOIN product_analytics.ecommerce.dim_products p ON f.product_sk = p.product_sk
    JOIN product_analytics.ecommerce.dim_users u ON f.user_sk = u.user_sk
    JOIN product_analytics.ecommerce.dim_categories c ON f.category_sk = c.category_sk
    WHERE f.date_sk >= 20191115  -- Last 2 weeks
    ORDER BY RAND()
    LIMIT 10000
""")

logger.info(f"Loaded {events_df.count():,} events for simulation")
events_df.cache()

# COMMAND ----------

# DBTITLE 1,Send Events to Staging Table
logger.info("\nStarting event production...")
logger.info(f"Sending {TOTAL_BATCHES} batches of {EVENTS_PER_BATCH} events each")

for batch_num in range(1, TOTAL_BATCHES + 1):
    logger.info(f"\nBatch {batch_num}/{TOTAL_BATCHES}")
    
    # Sample events for this batch
    batch_df = events_df.sample(fraction=EVENTS_PER_BATCH/events_df.count()) \
        .limit(EVENTS_PER_BATCH) \
        .withColumn('event_time', current_timestamp()) \
        .withColumn('batch_id', lit(batch_num)) \
        .withColumn('produced_at', current_timestamp())
    
    # Write to staging table (append)
    batch_df.write \
        .format('delta') \
        .mode('append') \
        .saveAsTable(STAGING_TABLE)
    
    logger.info(f"Sent {EVENTS_PER_BATCH} events to {STAGING_TABLE}")
    
    if batch_num < TOTAL_BATCHES:
        logger.info(f"Waiting {BATCH_INTERVAL_SECONDS} seconds before next batch...")
        time.sleep(BATCH_INTERVAL_SECONDS)

logger.info("\nProduction complete!")
logger.info(f"Total events sent: {EVENTS_PER_BATCH * TOTAL_BATCHES}")

# COMMAND ----------

# DBTITLE 1,Verify Staging Table
# MAGIC %sql
# MAGIC -- Verify staging table has data
# MAGIC SELECT 
# MAGIC     batch_id,
# MAGIC     COUNT(*) as event_count,
# MAGIC     COUNT(DISTINCT user_id) as unique_users,
# MAGIC     MIN(produced_at) as first_event,
# MAGIC     MAX(produced_at) as last_event
# MAGIC FROM product_analytics.ecommerce.streaming_events_input
# MAGIC GROUP BY batch_id
# MAGIC ORDER BY batch_id;

# COMMAND ----------

# DBTITLE 1,Sample Events
# MAGIC %sql
# MAGIC -- Show sample events from staging
# MAGIC SELECT 
# MAGIC     event_type,
# MAGIC     user_id,
# MAGIC     product_id,
# MAGIC     price,
# MAGIC     batch_id,
# MAGIC     produced_at
# MAGIC FROM product_analytics.ecommerce.streaming_events_input
# MAGIC ORDER BY produced_at DESC
# MAGIC LIMIT 10;