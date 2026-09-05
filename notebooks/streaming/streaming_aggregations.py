# Databricks notebook source
# DBTITLE 1,Streaming Aggregations - Real-Time Metrics
# MAGIC %md
# MAGIC # Streaming Aggregations
# MAGIC
# MAGIC **Purpose:** Real-time windowed metrics from Kafka stream
# MAGIC
# MAGIC ## Window Types
# MAGIC
# MAGIC ### 1. **Tumbling Window**
# MAGIC - Fixed, non-overlapping time intervals
# MAGIC - Example: Count events every 5 minutes
# MAGIC - Use case: Real-time dashboards
# MAGIC
# MAGIC ### 2. **Sliding Window**
# MAGIC - Overlapping time intervals
# MAGIC - Example: 10-minute window, slides every 5 minutes
# MAGIC - Use case: Moving averages
# MAGIC
# MAGIC ### 3. **Session Window**
# MAGIC - Dynamic windows based on inactivity gap
# MAGIC - Example: User session (30-min timeout)
# MAGIC - Use case: User engagement analysis
# MAGIC
# MAGIC ## Watermarking
# MAGIC
# MAGIC **Problem:** Events can arrive late (network delays, clock skew)
# MAGIC
# MAGIC **Solution:** Watermark defines how long to wait for late data
# MAGIC
# MAGIC ```python
# MAGIC .withWatermark("event_time", "10 minutes")
# MAGIC ```
# MAGIC
# MAGIC This means: "Wait up to 10 minutes for late events, then finalize the window"
# MAGIC
# MAGIC ## State Management
# MAGIC
# MAGIC - **State:** In-memory data for aggregations (counts, sums, etc.)
# MAGIC - **Storage:** RocksDB (disk-backed for fault tolerance)
# MAGIC - **Eviction:** Watermark triggers state cleanup

# COMMAND ----------

# DBTITLE 1,Imports and Configuration
import logging

# Configure logging
logger = logging.getLogger(__name__)


from pyspark.sql.functions import *
from pyspark.sql.types import *

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'ecommerce-events'

# Streaming Configuration
TRIGGER_INTERVAL = '10 seconds'
WATERMARK_DELAY = '10 minutes'  # How long to wait for late data
WINDOW_DURATION = '5 minutes'   # Tumbling window size

# Output Configuration
OUTPUT_TABLE = 'product_analytics.ecommerce.streaming_metrics'
CHECKPOINT_LOCATION = '/tmp/checkpoints/streaming_metrics'

# COMMAND ----------

# DBTITLE 1,Define Event Schema and Read Stream
# Event schema
event_schema = StructType([
    StructField('event_id', StringType(), False),
    StructField('event_type', StringType(), False),
    StructField('user_id', IntegerType(), False),
    StructField('product_id', IntegerType(), False),
    StructField('category_id', IntegerType(), False),
    StructField('event_time', StringType(), False),
    StructField('price', DoubleType(), True),
    StructField('quantity', IntegerType(), True),
    StructField('revenue', DoubleType(), True)
])

# Read and parse stream
raw_stream = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP_SERVERS) \
    .option('subscribe', KAFKA_TOPIC) \
    .option('startingOffsets', 'latest') \
    .load()

parsed_stream = raw_stream \
    .selectExpr('CAST(value AS STRING) as json_string') \
    .select(from_json(col('json_string'), event_schema).alias('data')) \
    .select('data.*') \
    .withColumn('event_time', to_timestamp(col('event_time')))

logger.info(" Stream initialized")

# COMMAND ----------

# DBTITLE 1,Tumbling Window Aggregation
# Tumbling window: Count events per 5-minute window
# withWatermark: Wait up to 10 minutes for late events
# window: Group by 5-minute tumbling windows
# groupBy: Also group by event_type

tumbling_agg = parsed_stream \
    .withWatermark('event_time', WATERMARK_DELAY) \
    .groupBy(
        window(col('event_time'), WINDOW_DURATION),
        col('event_type')
    ) \
    .agg(
        count('*').alias('event_count'),
        countDistinct('user_id').alias('unique_users'),
        countDistinct('product_id').alias('unique_products'),
        sum('revenue').alias('total_revenue'),
        avg('price').alias('avg_price')
    ) \
    .select(
        col('window.start').alias('window_start'),
        col('window.end').alias('window_end'),
        col('event_type'),
        col('event_count'),
        col('unique_users'),
        col('unique_products'),
        round(col('total_revenue'), 2).alias('total_revenue'),
        round(col('avg_price'), 2).alias('avg_price')
    )

logger.info(" Tumbling window aggregation defined")
logger.info("\nSchema:")
tumbling_agg.printSchema()

# COMMAND ----------

# DBTITLE 1,Write Tumbling Metrics to Console
# Write aggregated metrics to console for debugging
# outputMode('update'): Only output updated/new aggregations
# format('console'): Print to console
# truncate(False): Show full output

console_query = tumbling_agg.writeStream \
    .outputMode('update') \
    .format('console') \
    .option('truncate', False) \
    .trigger(processingTime=TRIGGER_INTERVAL) \
    .start()

logger.info(" Console output stream started")
logger.info(f"Query ID: {console_query.id}")

# COMMAND ----------

# DBTITLE 1,Sliding Window Aggregation
# Sliding window: 10-minute window, slides every 5 minutes
# This creates overlapping windows for moving averages

sliding_agg = parsed_stream \
    .withWatermark('event_time', WATERMARK_DELAY) \
    .groupBy(
        window(col('event_time'), '10 minutes', '5 minutes'),  # window_duration, slide_duration
        col('event_type')
    ) \
    .agg(
        count('*').alias('event_count'),
        avg('revenue').alias('avg_revenue_10min')
    ) \
    .select(
        col('window.start').alias('window_start'),
        col('window.end').alias('window_end'),
        col('event_type'),
        col('event_count'),
        round(col('avg_revenue_10min'), 2).alias('avg_revenue_10min')
    )

logger.info(" Sliding window aggregation defined")

# COMMAND ----------

# DBTITLE 1,Real-Time Conversion Funnel
# Calculate real-time conversion funnel per window
# Pivot event types into separate columns

funnel_agg = parsed_stream \
    .withWatermark('event_time', WATERMARK_DELAY) \
    .groupBy(window(col('event_time'), WINDOW_DURATION)) \
    .agg(
        sum(when(col('event_type') == 'view', 1).otherwise(0)).alias('views'),
        sum(when(col('event_type') == 'cart', 1).otherwise(0)).alias('carts'),
        sum(when(col('event_type') == 'purchase', 1).otherwise(0)).alias('purchases'),
        countDistinct('user_id').alias('active_users'),
        sum('revenue').alias('revenue')
    ) \
    .select(
        col('window.start').alias('window_start'),
        col('window.end').alias('window_end'),
        col('views'),
        col('carts'),
        col('purchases'),
        col('active_users'),
        round(col('revenue'), 2).alias('revenue'),
        round((col('carts') / col('views')) * 100, 2).alias('view_to_cart_rate'),
        round((col('purchases') / col('carts')) * 100, 2).alias('cart_to_purchase_rate')
    )

logger.info(" Funnel aggregation defined")

# COMMAND ----------

# DBTITLE 1,Write Funnel Metrics to Delta
# Write funnel metrics to Delta table
# outputMode('complete'): Output all aggregations (even unchanged)

funnel_query = funnel_agg.writeStream \
    .format('delta') \
    .outputMode('complete') \
    .option('checkpointLocation', f'{CHECKPOINT_LOCATION}_funnel') \
    .trigger(processingTime=TRIGGER_INTERVAL) \
    .table('product_analytics.ecommerce.streaming_funnel_metrics')

logger.info(" Funnel metrics stream started")
logger.info(f"Query ID: {funnel_query.id}")

# COMMAND ----------

# DBTITLE 1,Monitor Streams
# Monitor active streaming queries
import time
time.sleep(15)  # Wait for first batch

logger.info("\n Active Streaming Queries:\n")

for query in spark.streams.active:
    logger.info(f"Query: {query.name if query.name else query.id}")
    logger.info(f"Status: {query.status['message']}")
    
    if query.lastProgress:
        progress = query.lastProgress
        logger.info(f"Batch ID: {progress['batchId']}")
        logger.info(f"Input rows: {progress['numInputRows']}")
        logger.info(f"Processing rate: {progress.get('processedRowsPerSecond', 0):.1f} rows/sec")
    print("-" * 50)

logger.info("\n To stop all streams: spark.streams.active[i].stop()")

# COMMAND ----------

# DBTITLE 1,Query Streaming Metrics
# MAGIC %sql
# MAGIC -- Query the streaming funnel metrics
# MAGIC SELECT 
# MAGIC     window_start,
# MAGIC     window_end,
# MAGIC     views,
# MAGIC     carts,
# MAGIC     purchases,
# MAGIC     active_users,
# MAGIC     revenue,
# MAGIC     view_to_cart_rate,
# MAGIC     cart_to_purchase_rate
# MAGIC FROM product_analytics.ecommerce.streaming_funnel_metrics
# MAGIC ORDER BY window_start DESC
# MAGIC LIMIT 20;

# COMMAND ----------

