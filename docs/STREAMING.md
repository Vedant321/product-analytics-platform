# Real-Time Streaming Architecture

## Overview

This streaming pipeline processes e-commerce events in real-time using **Apache Kafka** and **Spark Structured Streaming**.

## Architecture Components

```
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│  Event Source   │─────▶│  Kafka Topic    │─────▶│ Spark Streaming │─────▶│  Delta Tables   │
│  (Producer)     │      │  (Message Bus)  │      │   (Consumer)    │      │  (Bronze/Silver)│
└─────────────────┘      └─────────────────┘      └─────────────────┘      └─────────────────┘
```

### 1. **Kafka Producer** (`kafka_event_producer`)

**Purpose:** Simulates real-time e-commerce events and publishes them to Kafka

**Technical Details:**
- **Library:** `kafka-python` (pure Python Kafka client)
- **Serialization:** JSON (easily debuggable, schema-flexible)
- **Topic:** `ecommerce-events`
- **Partitioning Strategy:** Hash by `user_id` (ensures all events for a user go to same partition, maintaining order)
- **Event Schema:**
  ```json
  {
    "event_id": "uuid",
    "event_type": "view|cart|remove_from_cart|purchase",
    "user_id": "uuid",
    "product_id": "integer",
    "category_id": "integer",
    "event_time": "2024-01-01T12:00:00Z",
    "price": "float",
    "quantity": "integer"
  }
  ```

**Why Kafka?**
- **Decoupling:** Producers and consumers are independent
- **Durability:** Messages persist even if consumer is down (configurable retention)
- **Scalability:** Can handle millions of events per second
- **Ordering:** Guarantees order within a partition
- **Replay:** Can reprocess historical data by resetting consumer offset

### 2. **Kafka Topic**

**Technical Details:**
- **Replication Factor:** 3 (for production durability)
- **Partitions:** 6 (allows parallel processing)
- **Retention:** 7 days (configurable based on needs)
- **Compression:** `snappy` (balance between compression ratio and CPU)

**Key Concepts:**
- **Partition:** Physical subdivision of a topic (like shards)
- **Offset:** Sequential ID for each message in a partition
- **Consumer Group:** Multiple consumers reading from same topic, each partition assigned to one consumer
- **Commit:** Marking messages as processed (automatic or manual)

### 3. **Spark Structured Streaming** (`kafka_event_consumer`)

**Purpose:** Reads from Kafka, processes events, writes to Delta Lake

**Technical Details:**
- **Read Pattern:** `spark.readStream.format("kafka")`
- **Trigger Mode:** `processingTime='10 seconds'` (micro-batch processing)
- **Checkpoint Location:** DBFS path for exactly-once semantics
- **Output Mode:** `append` (for bronze), `update` (for aggregations)
- **Watermarking:** Handles late-arriving events (configurable delay threshold)

**Processing Flow:**
```python
# 1. Read from Kafka (binary)
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ecommerce-events") \
    .option("startingOffsets", "latest") \
    .load()

# 2. Parse JSON (from binary to structured)
parsed_stream = raw_stream \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# 3. Enrich with dimensions (lookups)
enriched_stream = parsed_stream \
    .join(dim_products, "product_id") \
    .join(dim_categories, "category_id")

# 4. Write to Delta (transactional, ACID)
enriched_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .start(bronze_table_path)
```

**Key Concepts:**
- **Micro-batch:** Processes data in small batches (not true streaming, but feels real-time)
- **Watermark:** Defines how long to wait for late data (`withWatermark("event_time", "1 hour")`)
- **Checkpoint:** Stores progress (offset, state) for fault tolerance
- **State Store:** In-memory + RocksDB for stateful operations (windowing, aggregations)

### 4. **Streaming Aggregations** (`streaming_aggregations`)

**Purpose:** Real-time metrics (5-min windows, sliding windows, session windows)

**Technical Details:**
- **Window Types:**
  - **Tumbling Window:** Fixed, non-overlapping (e.g., every 5 minutes)
  - **Sliding Window:** Overlapping (e.g., 10-min window, sliding every 5 min)
  - **Session Window:** Dynamic, based on inactivity gap

**Example Aggregation:**
```python
# Tumbling window: count events per 5-minute window
windowed_counts = parsed_stream \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("event_type")
    ) \
    .agg(
        count("*").alias("event_count"),
        countDistinct("user_id").alias("unique_users")
    )
```

### 5. **Delta Lake Integration**

**Why Delta for Streaming?**
- **ACID Transactions:** No partial writes, even in streaming
- **Schema Evolution:** Can add columns without breaking pipeline
- **Time Travel:** Can query historical versions
- **Merge/Upsert:** Can handle late updates (e.g., refunds, corrections)
- **Unified Batch + Streaming:** Same table for both paradigms

**Bronze → Silver → Gold Pattern:**
- **Bronze:** Raw Kafka events (append-only, immutable)
- **Silver:** Cleaned, enriched, deduplicated (can use MERGE for upserts)
- **Gold:** Aggregated metrics (windowed, sessionized)

## Technical Deep-Dive: Exactly-Once Semantics

**Problem:** How do we ensure each event is processed exactly once?

**Solution Stack:**
1. **Kafka:** Idempotent producer (deduplicates retries)
2. **Spark:** Checkpointing (tracks processed offsets)
3. **Delta:** Transactional writes (atomic commit)

**Flow:**
```
1. Spark reads batch N (offsets 1000-1999)
2. Processes data
3. Writes to Delta
4. Updates checkpoint (commits offsets)
   → If failure before step 4: replays batch N (Delta handles duplicates)
   → If failure after step 4: moves to batch N+1
```

## Performance Tuning

### Kafka
- **`batch.size`:** Larger = better throughput, more latency (default 16KB)
- **`linger.ms`:** Wait time before sending batch (default 0)
- **`compression.type`:** `snappy` for balance, `lz4` for speed

### Spark
- **`maxOffsetsPerTrigger`:** Limit records per micro-batch (prevents overload)
- **`shuffle.partitions`:** Match Kafka partitions for optimal parallelism
- **`trigger.processingTime`:** Trade-off between latency and efficiency

### Delta
- **Auto-optimize:** Compact small files automatically
- **Z-ordering:** Co-locate frequently queried columns

## Monitoring

**Key Metrics:**
- **Kafka:** Lag (# of uncommitted messages), throughput (msgs/sec)
- **Spark:** Processing time vs batch interval (should be < interval)
- **Delta:** File count (avoid small file problem)

**Tools:**
- Kafka Manager / Confluent Control Center
- Spark UI (Structured Streaming tab)
- Databricks Monitoring

## Getting Started

1. **Setup Kafka:** `kafka_event_producer` (install `kafka-python`)
2. **Start Consumer:** `kafka_event_consumer` (Spark Structured Streaming)
3. **Run Aggregations:** `streaming_aggregations` (windowed metrics)
4. **Monitor:** Check Spark UI, query Delta tables

## Files

- `kafka_event_producer.py` - Simulates and publishes events to Kafka
- `kafka_event_consumer.py` - Consumes from Kafka, writes to bronze Delta
- `streaming_aggregations.py` - Real-time windowed aggregations