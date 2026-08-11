# Databricks notebook source
# DBTITLE 1,Kafka Event Producer - Real-Time Simulation
# MAGIC %md
# MAGIC # Kafka Event Producer
# MAGIC
# MAGIC **Purpose:** Simulates real-time e-commerce events and publishes to Kafka
# MAGIC
# MAGIC ## What This Does
# MAGIC
# MAGIC 1. **Reads historical events** from `fact_events` table
# MAGIC 2. **Simulates real-time** by sending events with current timestamp
# MAGIC 3. **Publishes to Kafka** topic `ecommerce-events`
# MAGIC 4. **Configurable rate** (events/second)
# MAGIC
# MAGIC ## Technical Details
# MAGIC
# MAGIC - **Kafka Library:** `kafka-python`
# MAGIC - **Serialization:** JSON
# MAGIC - **Partitioning:** Hash by `user_id` (maintains order per user)
# MAGIC - **Compression:** Snappy (balance speed/size)

# COMMAND ----------

# DBTITLE 1,Install Kafka Client
# MAGIC %pip install kafka-python

# COMMAND ----------

# DBTITLE 1,Imports and Configuration
from kafka import KafkaProducer
import json
import time
from datetime import datetime
import uuid
from pyspark.sql.functions import *

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'  # Update with your Kafka broker
KAFKA_TOPIC = 'ecommerce-events'

# Simulation Configuration
EVENTS_PER_SECOND = 100  # Adjust based on desired throughput
BATCH_SIZE = 1000  # Number of events to send in each batch

# COMMAND ----------

# DBTITLE 1,Initialize Kafka Producer
# Create Kafka Producer
# key_serializer: converts user_id to bytes for partitioning
# value_serializer: converts event dict to JSON bytes
# compression_type: 'snappy' for efficient compression
# acks: 'all' ensures message is written to all replicas (durability)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    key_serializer=lambda k: str(k).encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    compression_type='snappy',
    acks='all',  # Wait for all replicas to acknowledge
    retries=3     # Retry failed sends
)

print(f"✅ Kafka Producer connected to {KAFKA_BOOTSTRAP_SERVERS}")
print(f"📤 Publishing to topic: {KAFKA_TOPIC}")

# COMMAND ----------

# DBTITLE 1,Load Historical Events for Simulation
# Load a sample of historical events from fact_events
# We'll replay these with current timestamps to simulate real-time
events_df = spark.sql("""
    SELECT 
        event_id,
        event_type,
        user_sk,
        product_sk,
        category_sk,
        price,
        quantity,
        revenue
    FROM product_analytics.ecommerce.fact_events
    ORDER BY event_time
    LIMIT 10000
""")

print(f"📊 Loaded {events_df.count()} historical events for simulation")
events_df.show(5)

# COMMAND ----------

# DBTITLE 1,Event Publishing Function
def publish_event(event_row):
    """
    Publishes a single event to Kafka
    
    Technical Details:
    - Key: user_sk (ensures all events for same user go to same partition)
    - Value: JSON event payload
    - Timestamp: Current time (simulates real-time)
    """
    event_payload = {
        'event_id': str(uuid.uuid4()),  # New UUID for each event
        'event_type': event_row.event_type,
        'user_id': event_row.user_sk,
        'product_id': event_row.product_sk,
        'category_id': event_row.category_sk,
        'event_time': datetime.utcnow().isoformat() + 'Z',  # Current timestamp
        'price': float(event_row.price) if event_row.price else 0.0,
        'quantity': int(event_row.quantity) if event_row.quantity else 1,
        'revenue': float(event_row.revenue) if event_row.revenue else 0.0
    }
    
    # Send to Kafka
    # key: user_id for partitioning
    # value: event payload
    future = producer.send(
        topic=KAFKA_TOPIC,
        key=event_row.user_sk,
        value=event_payload
    )
    
    return future

# COMMAND ----------

# DBTITLE 1,Start Real-Time Simulation
# Convert Spark DataFrame to list for iteration
events_list = events_df.collect()

print(f"🚀 Starting real-time event simulation...")
print(f"⚡ Publishing {EVENTS_PER_SECOND} events/second")
print(f"📦 Batch size: {BATCH_SIZE}")
print("\nPress Ctrl+C to stop\n")

total_sent = 0
start_time = time.time()

try:
    for i, event in enumerate(events_list):
        # Publish event
        future = publish_event(event)
        total_sent += 1
        
        # Print progress every 100 events
        if total_sent % 100 == 0:
            elapsed = time.time() - start_time
            rate = total_sent / elapsed if elapsed > 0 else 0
            print(f"📤 Sent {total_sent} events | Rate: {rate:.1f} events/sec")
        
        # Rate limiting: sleep to maintain desired events/second
        time.sleep(1.0 / EVENTS_PER_SECOND)
        
        # Flush every BATCH_SIZE events
        if total_sent % BATCH_SIZE == 0:
            producer.flush()  # Ensure all messages are sent
            print(f"✅ Flushed batch {total_sent // BATCH_SIZE}")
            
except KeyboardInterrupt:
    print("\n⏸️  Stopping simulation...")
finally:
    # Flush any remaining messages
    producer.flush()
    producer.close()
    
    elapsed = time.time() - start_time
    rate = total_sent / elapsed if elapsed > 0 else 0
    
    print(f"\n📊 Summary:")
    print(f"   Total events sent: {total_sent}")
    print(f"   Duration: {elapsed:.1f} seconds")
    print(f"   Average rate: {rate:.1f} events/sec")
    print(f"✅ Producer closed")

# COMMAND ----------

