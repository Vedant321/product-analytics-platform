# Databricks notebook source
# DBTITLE 1,Event Consumer - Streaming Simulation
# MAGIC %md
# MAGIC # Event Consumer - Lambda Architecture
# MAGIC
# MAGIC **Purpose:** Reads events from streaming source and writes to unified Bronze layer
# MAGIC
# MAGIC ## Lambda Architecture
# MAGIC
# MAGIC ```
# MAGIC Streaming Source              Spark Streaming              Unified Bronze
# MAGIC  (Delta/Kafka)     →      Parse + Add Metadata    →      bronze_events
# MAGIC                               (Micro-batch)            (source='streaming')
# MAGIC ```
# MAGIC
# MAGIC Both batch and streaming write to the SAME bronze_events table!
# MAGIC - Batch records: source = 'batch'
# MAGIC - Streaming records: source = 'streaming'

# COMMAND ----------

# DBTITLE 1,Configuration
import logging

# Configure logging
logger = logging.getLogger(__name__)

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Streaming Configuration (Lambda Architecture)
INPUT_TABLE = 'product_analytics.ecommerce.streaming_events_input'
BRONZE_TABLE = 'product_analytics.ecommerce.bronze_events'  # Unified bronze table!
CHECKPOINT_LOCATION = '/tmp/checkpoints/streaming_events'
TRIGGER_INTERVAL = '10 seconds'

logger.info(f"Input: {INPUT_TABLE}")
logger.info(f"Output: {BRONZE_TABLE} (unified bronze layer)")
logger.info(f"Checkpoint: {CHECKPOINT_LOCATION}")

# COMMAND ----------

# DBTITLE 1,Read Delta Stream
# Read from Delta table as a streaming source
input_stream = spark.readStream \
    .format('delta') \
    .table(INPUT_TABLE)

logger.info("Delta stream reader configured")
logger.info(f"Reading from: {INPUT_TABLE}")
logger.info("\nStream Schema:")
input_stream.printSchema()

# COMMAND ----------

# DBTITLE 1,Add Metadata for Lambda Architecture
# Add metadata to distinguish streaming records from batch
# Schema must match bronze_events (natural keys only - NO surrogate keys!)
parsed_stream = input_stream \
    .withColumn('ingestion_timestamp', current_timestamp()) \
    .withColumn('source', lit('streaming'))  # Lambda Architecture marker

logger.info("Stream enriched with metadata")
logger.info("Schema matches bronze_events (raw natural keys)")

# COMMAND ----------

# DBTITLE 1,Bronze Table Configuration
# MAGIC %md
# MAGIC ## Lambda Architecture: Unified Bronze Layer
# MAGIC
# MAGIC Both batch and streaming write to the same `bronze_events` table.
# MAGIC - **Batch source:** Kaggle CSV files (source = 'batch')
# MAGIC - **Streaming source:** Kafka/Delta stream (source = 'streaming')
# MAGIC
# MAGIC The table already exists from batch ingestion. No need to create.
# MAGIC
# MAGIC **Key Principle:** Bronze = RAW data only (natural keys: product_id, user_id, category_id)
# MAGIC - NO surrogate keys (user_sk, product_sk)
# MAGIC - NO enrichments
# MAGIC - Enrichment happens in Silver layer

# COMMAND ----------

# DBTITLE 1,Start Streaming to Unified Bronze
# Write stream to unified Bronze Delta table
query = parsed_stream.writeStream \
    .format('delta') \
    .outputMode('append') \
    .option('checkpointLocation', CHECKPOINT_LOCATION) \
    .trigger(processingTime=TRIGGER_INTERVAL) \
    .table(BRONZE_TABLE)

logger.info("Streaming query started!")
logger.info(f"Writing to: {BRONZE_TABLE} (unified bronze layer)")
logger.info(f"Checkpoint: {CHECKPOINT_LOCATION}")
logger.info(f"Trigger interval: {TRIGGER_INTERVAL}")
logger.info(f"\nQuery ID: {query.id}")
logger.info(f"Status: {query.status}")

# COMMAND ----------

# DBTITLE 1,Monitor Streaming Query
# Check streaming query status
logger.info("\nStreaming Query Metrics:\n")

# Wait a bit for first batch
import time
time.sleep(15)

# Get latest progress
if query.lastProgress:
    progress = query.lastProgress
    logger.info(f"Batch ID: {progress['batchId']}")
    logger.info(f"Input rows: {progress['numInputRows']}")
    logger.info(f"Processing rate: {progress.get('processedRowsPerSecond', 0):.1f} rows/sec")
    logger.info(f"Batch duration: {progress.get('durationMs', {}).get('triggerExecution', 0) / 1000:.2f} seconds")
else:
    logger.info("Waiting for first batch...")

logger.info("\nTo stop the stream: query.stop()")
logger.info("To view Spark UI: Check 'Structured Streaming' tab")

# COMMAND ----------

# DBTITLE 1,Query Bronze Table (Both Batch + Streaming)
# MAGIC %sql
# MAGIC -- Query the unified bronze table
# MAGIC -- Shows both batch and streaming records
# MAGIC SELECT 
# MAGIC     source,
# MAGIC     COUNT(*) as total_events,
# MAGIC     COUNT(DISTINCT user_id) as unique_users,
# MAGIC     MIN(event_time) as first_event,
# MAGIC     MAX(event_time) as last_event
# MAGIC FROM product_analytics.ecommerce.bronze_events
# MAGIC GROUP BY source
# MAGIC ORDER BY source;

# COMMAND ----------

# DBTITLE 1,Sample Recent Streaming Events
# MAGIC %sql
# MAGIC -- Show recent streaming events only
# MAGIC SELECT 
# MAGIC     event_type,
# MAGIC     user_id,
# MAGIC     product_id,
# MAGIC     event_time,
# MAGIC     price,
# MAGIC     source,
# MAGIC     ingestion_timestamp
# MAGIC FROM product_analytics.ecommerce.bronze_events
# MAGIC WHERE source = 'streaming'
# MAGIC ORDER BY ingestion_timestamp DESC
# MAGIC LIMIT 20;