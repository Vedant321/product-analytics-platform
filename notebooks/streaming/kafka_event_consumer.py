# Databricks notebook source
# DBTITLE 1,Event Consumer - Streaming Simulation
# MAGIC %md
# MAGIC # Event Consumer - Streaming Simulation
# MAGIC
# MAGIC **Purpose:** Reads events from Kafka in real-time and writes to Delta Lake
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC ```
# MAGIC Kafka Topic                Spark Streaming              Delta Lake
# MAGIC    (JSON)        →   Parse + Enrich    →      Bronze Table
# MAGIC                           (Micro-batch)           (Append-only)
# MAGIC ```
# MAGIC
# MAGIC ## Technical Concepts
# MAGIC
# MAGIC ### 1. **Spark Structured Streaming**
# MAGIC - **Micro-batch processing:** Processes data in small batches (e.g., every 10 seconds)
# MAGIC - **Exactly-once semantics:** Using checkpoints + Delta transactions
# MAGIC - **Late data handling:** Watermarking for out-of-order events
# MAGIC
# MAGIC ### 2. **Kafka Integration**
# MAGIC - **Binary format:** Kafka stores messages as bytes
# MAGIC - **Deserialization:** Convert bytes → string → JSON → struct
# MAGIC - **Metadata:** Kafka provides topic, partition, offset, timestamp
# MAGIC
# MAGIC ### 3. **Checkpointing**
# MAGIC - **Purpose:** Fault tolerance (tracks processed offsets)
# MAGIC - **Location:** DBFS path (persisted across failures)
# MAGIC - **Recovery:** On restart, resumes from last checkpoint
# MAGIC
# MAGIC ### 4. **Delta Lake**
# MAGIC - **ACID writes:** Atomic, consistent, isolated, durable
# MAGIC - **Schema enforcement:** Validates incoming data
# MAGIC - **Time travel:** Can query historical versions

# COMMAND ----------

# DBTITLE 1,Configuration
import logging

# Configure logging
logger = logging.getLogger(__name__)


from pyspark.sql.functions import *
from pyspark.sql.types import *

# Streaming Configuration
INPUT_TABLE = 'product_analytics.ecommerce.streaming_events_input'
BRONZE_TABLE = 'product_analytics.ecommerce.bronze_streaming_events'
CHECKPOINT_LOCATION = '/tmp/checkpoints/streaming_events'
TRIGGER_INTERVAL = '10 seconds'

logger.info("📥 Input: {INPUT_TABLE}")
logger.info("📤 Output: {BRONZE_TABLE}")
logger.info("💾 Checkpoint: {CHECKPOINT_LOCATION}")

# COMMAND ----------

# DBTITLE 1,Read Delta Stream
# Read from Delta table as a streaming source
# Key concept: Delta tables CAN be read as streams!
# - Spark monitors Delta log for new commits
# - Only reads newly appended data (not full scan)
# - Checkpoint tracks which version processed

input_stream = spark.readStream \
    .format('delta') \
    .table(INPUT_TABLE)

logger.info(" Delta stream reader configured")
logger.info("   Reading from: {INPUT_TABLE}")
logger.info("\nStream Schema:")
input_stream.printSchema()

# COMMAND ----------

# DBTITLE 1,Add Metadata
# Add processing metadata
# In production: validate schema, handle nulls, log bad records
parsed_stream = input_stream \
    .withColumn('processed_time', current_timestamp()) \
    .withColumn('source', lit('streaming_simulation'))

logger.info(" Stream enriched with metadata")
# format('kafka'): Uses Kafka as source
# option('subscribe', topic): Which topic to read from
# option('startingOffsets', 'latest'): Start from newest messages (use 'earliest' for historical)
# option('maxOffsetsPerTrigger'): Limit records per micro-batch

raw_stream = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP_SERVERS) \
    .option('subscribe', KAFKA_TOPIC) \
    .option('startingOffsets', 'latest') \
    .option('maxOffsetsPerTrigger', MAX_OFFSETS_PER_TRIGGER) \
    .load()

logger.info(" Kafka stream initialized")
logger.info("\nKafka Stream Schema:")
raw_stream.printSchema()

# Kafka provides these columns:
# - key: message key (binary)
# - value: message payload (binary) <- we need to parse this
# - topic: topic name
# - partition: partition number
# - offset: message offset
# - timestamp: Kafka ingestion timestamp
# - timestampType: 0=CreateTime, 1=LogAppendTime

# COMMAND ----------

# DBTITLE 1,Parse JSON and Extract Fields
# Step 1: Convert 'value' column from binary to string
# Step 2: Parse JSON string using our schema
# Step 3: Flatten the nested struct to get individual columns
# Step 4: Convert event_time string to timestamp
# Step 5: Add Kafka metadata columns

parsed_stream = raw_stream \
    .selectExpr('CAST(value AS STRING) as json_string') \
    .select(from_json(col('json_string'), event_schema).alias('data')) \
    .select('data.*') \
    .withColumn('event_time', to_timestamp(col('event_time'))) \
    .withColumn('ingestion_time', current_timestamp())

logger.info(" Event parsing configured")
logger.info("\nParsed Event Schema:")
parsed_stream.printSchema()

# COMMAND ----------

# DBTITLE 1,Create Bronze Table
# MAGIC %sql
# MAGIC -- Create bronze table for raw streaming events
# MAGIC CREATE TABLE IF NOT EXISTS product_analytics.ecommerce.bronze_streaming_events (
# MAGIC     event_id STRING NOT NULL,
# MAGIC     event_type STRING NOT NULL,
# MAGIC     user_id INT NOT NULL,
# MAGIC     product_id INT NOT NULL,
# MAGIC     category_id INT NOT NULL,
# MAGIC     event_time TIMESTAMP NOT NULL,
# MAGIC     price DOUBLE,
# MAGIC     quantity INT,
# MAGIC     revenue DOUBLE,
# MAGIC     batch_id INT,
# MAGIC     produced_at TIMESTAMP,
# MAGIC     processed_time TIMESTAMP NOT NULL,
# MAGIC     source STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Real-time events from Kafka (bronze layer)';
# MAGIC
# MAGIC -- Enable optimizations
# MAGIC ALTER TABLE product_analytics.ecommerce.bronze_streaming_events
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Start Streaming
# Write stream to Bronze Delta table
# Key concepts:
# - format('delta'): ACID transactions
# - outputMode('append'): Bronze is immutable
# - checkpointLocation: Exactly-once semantics
# - Delta transaction log prevents duplicates!
# format('delta'): Write to Delta format
# outputMode('append'): Only add new rows (no updates/deletes)
# option('checkpointLocation'): For fault tolerance and exactly-once
# trigger(processingTime): Micro-batch interval
# start(): Starts the streaming query (non-blocking)

query = parsed_stream.writeStream \
    .format('delta') \
    .outputMode('append') \
    .option('checkpointLocation', CHECKPOINT_LOCATION) \
    .trigger(processingTime=TRIGGER_INTERVAL) \
    .table(BRONZE_TABLE)

logger.info(" Streaming query started!")
logger.info("📂 Writing to: {BRONZE_TABLE}")
logger.info("💾 Checkpoint: {CHECKPOINT_LOCATION}")
logger.info("⏱  Trigger interval: {TRIGGER_INTERVAL}")
logger.info(" Max offsets per trigger: {MAX_OFFSETS_PER_TRIGGER}")
logger.info("\n Query ID: {query.id}")
logger.info("🟢 Status: {query.status}")

# COMMAND ----------

# DBTITLE 1,Monitor Streaming Query
# Check streaming query status
logger.info("\n Streaming Query Metrics:\n")

# Wait a bit for first batch
import time
time.sleep(15)

# Get latest progress
if query.lastProgress:
    progress = query.lastProgress
    logger.info("Batch ID: {progress['batchId']}")
    logger.info("Input rows: {progress['numInputRows']}")
    logger.info("Processing rate: {progress.get('processedRowsPerSecond', 0):.1f} rows/sec")
    logger.info("Batch duration: {progress.get('durationMs', {}).get('triggerExecution', 0) / 1000:.2f} seconds")
else:
    logger.info("Waiting for first batch...")

logger.info("\n To stop the stream: query.stop()")
logger.info(" To view Spark UI: Check 'Structured Streaming' tab")

# COMMAND ----------

# DBTITLE 1,Query Bronze Table
# MAGIC %sql
# MAGIC -- Query the bronze streaming table
# MAGIC SELECT 
# MAGIC     COUNT(*) as total_events,
# MAGIC     COUNT(DISTINCT user_id) as unique_users,
# MAGIC     MIN(event_time) as first_event,
# MAGIC     MAX(event_time) as last_event,
# MAGIC     event_type,
# MAGIC     COUNT(*) as count
# MAGIC FROM product_analytics.ecommerce.bronze_streaming_events
# MAGIC GROUP BY event_type
# MAGIC ORDER BY count DESC;

# COMMAND ----------

# DBTITLE 1,Sample Recent Events
# MAGIC %sql
# MAGIC -- Show recent events
# MAGIC SELECT 
# MAGIC     event_id,
# MAGIC     event_type,
# MAGIC     user_id,
# MAGIC     product_id,
# MAGIC     event_time,
# MAGIC     price,
# MAGIC     revenue,
# MAGIC     ingestion_time
# MAGIC FROM product_analytics.ecommerce.bronze_streaming_events
# MAGIC ORDER BY ingestion_time DESC
# MAGIC LIMIT 20;

# COMMAND ----------

