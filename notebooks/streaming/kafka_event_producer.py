# Databricks notebook source
# DBTITLE 1,Kafka Event Producer - Real-Time Simulation
# MAGIC %md
# MAGIC # Event Producer - Streaming Simulation
# MAGIC
# MAGIC **Purpose:** Simulates real-time e-commerce events using Delta tables
# MAGIC
# MAGIC ## What This Does
# MAGIC
# MAGIC 1. **Reads historical events** from `fact_events` table
# MAGIC 2. **Simulates real-time** by sending events with current timestamp
# MAGIC 3. **Writes to Delta table** `product_analytics.ecommerce.streaming_events_input`
# MAGIC 4. **Configurable rate** (events/second)
# MAGIC
# MAGIC ## Why Delta Instead of Kafka?
# MAGIC
# MAGIC **Problem:** Databricks runs in cloud, can't reach `localhost:9092` Kafka
# MAGIC
# MAGIC **Solution:** Use Delta table as streaming source:
# MAGIC - Producer writes to Delta table (this notebook)
# MAGIC - Consumer reads Delta table as stream (kafka_event_consumer)
# MAGIC - Same learning experience, no external infrastructure!
# MAGIC
# MAGIC ## Technical Details
# MAGIC
# MAGIC - **Storage:** Delta Lake (append-only)
# MAGIC - **Schema:** Same as Kafka messages (JSON-like)
# MAGIC - **Partitioning:** By date (optimizes reads)
# MAGIC - **Rate Control:** Sleep between batches

# COMMAND ----------

# DBTITLE 1,Install Kafka Client
# No external dependencies needed!
# We're using native Spark + Delta

# COMMAND ----------

# DBTITLE 1,Imports and Configuration
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
from datetime import datetime
import uuid

# Delta Table Configuration
INPUT_TABLE = 'product_analytics.ecommerce.streaming_events_input'

# Simulation Configuration
EVENTS_PER_BATCH = 100  # Number of events per batch
BATCH_INTERVAL_SECONDS = 10  # Time between batches
TOTAL_BATCHES = 6  # Total batches to send (6 batches = 1 minute of data)

# COMMAND ----------

# DBTITLE 1,Initialize Kafka Producer
# Create input table if not exists
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {INPUT_TABLE} (
        event_id STRING,
        event_type STRING,
        user_id INT,
        product_id INT,
        category_id INT,
        event_time TIMESTAMP,
        price DOUBLE,
        quantity INT,
        revenue DOUBLE,
        batch_id INT,
        produced_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Simulated real-time events for streaming pipeline testing'
""")

print(f"✅ Input table ready: {INPUT_TABLE}")
print(f"📊 This simulates Kafka topic behavior using Delta")

# COMMAND ----------

# DBTITLE 1,Load Historical Events for Simulation
# Load historical events for simulation
events_df = spark.sql("""
    SELECT 
        event_id,
        event_type,
        user_sk as user_id,
        product_sk as product_id,
        category_sk as category_id,
        price,
        quantity,
        revenue
    FROM product_analytics.ecommerce.fact_events
    WHERE date_sk >= 20191115  -- Last 2 weeks of data
    ORDER BY RAND()  -- Randomize order
    LIMIT 1000
""")

total_events = events_df.count()
print(f"📊 Loaded {total_events} historical events for simulation")
print(f"📦 Will send {EVENTS_PER_BATCH} events per batch")
print(f"⏱️  {BATCH_INTERVAL_SECONDS} seconds between batches")
print(f"🎯 Total batches: {TOTAL_BATCHES}")
events_df.show(5)

# COMMAND ----------

# DBTITLE 1,Event Publishing Function
# Convert to Pandas for batch processing
events_pandas = events_df.toPandas()

print(f"✅ Converted {len(events_pandas)} events to Pandas for batching")

# COMMAND ----------

# DBTITLE 1,Start Real-Time Simulation
print(f"\n🚀 Starting event simulation...")
print(f"📤 Sending {EVENTS_PER_BATCH} events per batch")
print(f"⏱️  {BATCH_INTERVAL_SECONDS} seconds between batches\n")

total_sent = 0
start_time = time.time()

for batch_num in range(1, TOTAL_BATCHES + 1):
    # Select events for this batch
    start_idx = (batch_num - 1) * EVENTS_PER_BATCH
    end_idx = start_idx + EVENTS_PER_BATCH
    batch_events = events_pandas.iloc[start_idx:end_idx].copy()
    
    if len(batch_events) == 0:
        print("⚠️  No more events to send")
        break
    
    # Add current timestamp (simulates real-time)
    batch_events['event_time'] = datetime.now()
    batch_events['batch_id'] = batch_num
    batch_events['produced_at'] = datetime.now()
    
    # Generate new event IDs
    batch_events['event_id'] = [str(uuid.uuid4()) for _ in range(len(batch_events))]
    
    # Convert to Spark DataFrame
    batch_df = spark.createDataFrame(batch_events)
    
    # Append to Delta table (simulates Kafka publish)
    batch_df.write \
        .format('delta') \
        .mode('append') \
        .saveAsTable(INPUT_TABLE)
    
    total_sent += len(batch_events)
    elapsed = time.time() - start_time
    rate = total_sent / elapsed if elapsed > 0 else 0
    
    print(f"📤 Batch {batch_num}/{TOTAL_BATCHES} | Sent {len(batch_events)} events | Total: {total_sent} | Rate: {rate:.1f} events/sec")
    
    # Wait before next batch (except last)
    if batch_num < TOTAL_BATCHES:
        print(f"   ⏳ Waiting {BATCH_INTERVAL_SECONDS} seconds...")
        time.sleep(BATCH_INTERVAL_SECONDS)

elapsed = time.time() - start_time

print(f"\n✅ Simulation Complete!")
print(f"📊 Summary:")
print(f"   Total events: {total_sent}")
print(f"   Duration: {elapsed:.1f} seconds")
print(f"   Average rate: {total_sent / elapsed:.1f} events/sec")
print(f"   Batches sent: {batch_num}")

# COMMAND ----------

