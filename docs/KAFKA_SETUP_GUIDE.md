# Kafka Streaming Pipeline - Setup & Testing Guide

## Prerequisites

* Docker installed on your local machine
* Databricks workspace (you have this)
* Git repo cloned locally (product-analytics-platform)

---

## Step 1: Start Local Kafka with Docker

### Create docker-compose.yml

In your local `product-analytics-platform` directory, create:

```yaml
# docker-compose.yml
version: '3'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
```

### Start Kafka

```bash
# From product-analytics-platform directory
docker-compose up -d

# Verify containers are running
docker ps

# You should see:
# - zookeeper (port 2181)
# - kafka (port 9092)
```

### Create Kafka Topic

```bash
# Create the ecommerce-events topic
docker exec -it $(docker ps -q -f name=kafka) kafka-topics \
  --create \
  --topic ecommerce-events \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

# Verify topic created
docker exec -it $(docker ps -q -f name=kafka) kafka-topics \
  --list \
  --bootstrap-server localhost:9092
```

**What just happened:**
- ✅ Zookeeper started (Kafka's coordination service)
- ✅ Kafka broker started (message bus)
- ✅ Topic created with 3 partitions (allows 3 parallel consumers)

---

## Step 2: Update Databricks Notebooks

### Problem: Databricks Can't Reach localhost

Your Databricks cluster runs in the cloud (AWS), so `localhost:9092` won't work.

### Solution: Use Databricks Kafka Simulator

Instead of external Kafka, we'll use **in-memory simulation** for testing:

1. Producer writes to a **temp Delta table** (instead of Kafka)
2. Consumer reads from that Delta table (simulates Kafka stream)

### Update kafka_event_producer.py

**Change from:** Publishing to external Kafka  
**Change to:** Writing to Delta table `product_analytics.ecommerce.kafka_simulation`

This simulates Kafka without needing external infrastructure.

---

## Step 3: Test Plan

### Phase 1: Producer Test (5 minutes)

1. Run `kafka_event_producer.py`
2. Verify data written to simulation table
3. Check row count and sample data

### Phase 2: Consumer Test (10 minutes)

1. Run `kafka_event_consumer.py` (reads from simulation table)
2. Verify data lands in Bronze Delta table
3. Check for duplicates, nulls, schema correctness

### Phase 3: Aggregations Test (10 minutes)

1. Run `streaming_aggregations.py`
2. Verify windowed metrics computed correctly
3. Query results, validate conversion rates look reasonable

---

## Alternative: Use Databricks Event Hubs (Production Setup)

For production, you'd use managed Kafka:

### AWS MSK (Managed Streaming for Kafka)

```bash
# Create MSK cluster (via AWS Console)
# Takes ~15 minutes to provision
# Cost: ~$2.50/day for dev cluster

# Update notebooks with MSK endpoint:
KAFKA_BOOTSTRAP_SERVERS = 'b-1.mskcluster.xxx.kafka.us-east-1.amazonaws.com:9092'
```

### Azure Event Hubs

```python
# Event Hubs is Kafka-compatible
KAFKA_BOOTSTRAP_SERVERS = 'my-eventhub.servicebus.windows.net:9093'
KAFKA_SASL_MECHANISM = 'PLAIN'
KAFKA_SECURITY_PROTOCOL = 'SASL_SSL'
```

---

## Monitoring Commands

### Check Kafka Lag (How far behind is consumer?)

```bash
docker exec -it $(docker ps -q -f name=kafka) kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group streaming-consumer \
  --describe
```

### View Messages in Topic

```bash
docker exec -it $(docker ps -q -f name=kafka) kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic ecommerce-events \
  --from-beginning \
  --max-messages 10
```

---

## Troubleshooting

### Kafka Won't Start

```bash
# Check logs
docker logs $(docker ps -q -f name=kafka)

# Common issue: Port 9092 already in use
lsof -i :9092
kill -9 <PID>
```

### Consumer Not Reading Messages

1. Check topic exists: `docker exec ... kafka-topics --list`
2. Check producer actually sent messages: `kafka-console-consumer --from-beginning`
3. Check consumer offset: `kafka-consumer-groups --describe`

### Databricks Connection Error

Remember: Databricks runs in cloud, can't reach your `localhost`.

**Solution:** Use the in-memory simulation approach (Delta table instead of Kafka).

---

## Next Steps

Once you've verified the pipeline works with simulation:

1. **Deploy to AWS MSK** (managed Kafka)
2. **Update KAFKA_BOOTSTRAP_SERVERS** in notebooks
3. **Run producer** from local machine (publishes to MSK)
4. **Run consumer** in Databricks (reads from MSK)
5. **Monitor** lag, throughput, errors

---

## Cost Estimate

| Component | Cost | Notes |
|-----------|------|-------|
| Local Kafka (Docker) | $0 | Free for development |
| AWS MSK (dev) | ~$2.50/day | t3.small broker |
| AWS MSK (prod) | ~$15/day | m5.large broker + HA |
| Databricks Serverless | Pay-per-use | ~$0.50/hr when running |

**Recommendation:** Start with local Docker (free), move to MSK when ready for production.