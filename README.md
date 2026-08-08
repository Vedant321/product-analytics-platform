# Project Specification: Real-Time Product Intelligence & Experimentation Platform

## 📊 Current Implementation Status (As of Aug 2026)

### ✅ **Phase 1: Foundation & Data Ingestion - COMPLETE**

**What's Built:**
- ✅ Kaggle dataset ingestion pipeline (`kaggle_data_ingestion.py`)
- ✅ Unity Catalog setup (`product_analytics.ecommerce` schema)
- ✅ Platform configuration framework (`config/platform_config.py`)
- ✅ Raw data ingestion from eCommerce behavior dataset (109.9M events)

**Tables:**
- `raw.ecommerce.kaggle_events` (109.9M rows) - Raw CSV data from Kaggle

---

### ✅ **Phase 2: Bronze Layer - COMPLETE**

**What's Built:**
- ✅ Bronze events table with append-only architecture
- ✅ Raw event storage in Delta Lake format
- ✅ Data partitioning by event_date
- ✅ Delta Lake features: OPTIMIZE, Time Travel, Auto-Optimize

**Tables:**
- `product_analytics.ecommerce.bronze_events` (109.9M rows)

**Schema:**
```
event_time        TIMESTAMP
event_type        STRING (view, cart, purchase, remove_from_cart)
product_id        INTEGER
category_id       BIGINT
category_code     STRING (electronics.smartphone, etc.)
brand             STRING
price             DOUBLE
user_id           INTEGER
user_session      STRING (UUID)
event_date        DATE (partition column)
ingestion_time    TIMESTAMP
```

**Delta Features Enabled:**
- zstd compression
- Deletion vectors
- Auto-Optimize (optimizeWrite + autoCompact)
- Version history tracking

---

### ✅ **Phase 3: Silver Layer - COMPLETE**

**What's Built:**
- ✅ **Star Schema Design** (4 dimensions + 1 fact table)
- ✅ **SCD Type 2** for slowly changing dimensions (products, users)
- ✅ **Surrogate Keys** for all dimensions
- ✅ **Incremental Update Framework** for SCD Type 2 processing
- ✅ **Complete Delta Lake/Lakehouse Features**

**Architecture: Kimball Star Schema**

```
                    ┌─────────────────┐
                    │  silver_dim_    │
                    │     date        │
                    │  (426 rows)     │
                    └────────┬────────┘
                             │
                             │ date_key
        ┌────────────────────┼────────────────────┐
        │                    │                    │
┌───────▼─────────┐  ┌──────▼──────────┐  ┌──────▼──────────┐
│ silver_dim_     │  │   fact_events   │  │  silver_dim_    │
│  categories     │  │                 │  │    products     │
│  (130 rows)     │  │  (109.9M rows)  │  │ (206.9K rows)   │
└─────────────────┘  │                 │  │  SCD Type 2     │
    category_sk ─────┤   Star Schema   ├───── product_sk    │
                     │                 │  └─────────────────┘
                     │                 │
                     │                 │
                     └────────┬────────┘
                              │
                         user_sk
                              │
                     ┌────────▼─────────┐
                     │  silver_dim_     │
                     │     users        │
                     │  (5.3M rows)     │
                     │   SCD Type 2     │
                     └──────────────────┘
```

**Dimension Tables:**

**1. `silver_dim_date` (426 rows)**
```
date_key             INTEGER (surrogate key, e.g., 20191001)
full_date            DATE
year                 INTEGER
quarter              INTEGER
month                INTEGER
month_name           STRING
week_of_year         INTEGER
day_of_month         INTEGER
day_of_week          INTEGER (1=Monday)
day_name             STRING
is_weekend           BOOLEAN
is_month_start       BOOLEAN
is_month_end         BOOLEAN
is_quarter_start     BOOLEAN
is_quarter_end       BOOLEAN
```

**2. `silver_dim_categories` (130 rows)**
```
category_sk          INTEGER (surrogate key)
category_l1          STRING (level 1: electronics, accessories, etc.)
category_l2          STRING (level 2: smartphone, audio, etc.)
category_l3          STRING (level 3: optional third level)
category_full_path   STRING (dot-separated path)
category_depth       INTEGER (1, 2, or 3)
created_at           TIMESTAMP
```

**3. `silver_dim_products` (206,876 rows - SCD Type 2)**
```
product_sk           INTEGER (surrogate key, unique per version)
product_id           INTEGER (natural key, repeats for versions)
brand                STRING
price                DOUBLE (tracked attribute)
category_sk          INTEGER (FK to dim_categories)
category_code        STRING
effective_from       DATE (version valid from this date)
effective_to         DATE (version valid until this date, 9999-12-31 for current)
is_current_version   BOOLEAN (TRUE for active, FALSE for historical)
version_number       INTEGER (1, 2, 3, ... for each product_id)
created_at           TIMESTAMP
updated_at           TIMESTAMP
```

**4. `silver_dim_users` (5,316,649 rows - SCD Type 2)**
```
user_sk              INTEGER (surrogate key, unique per version)
user_id              INTEGER (natural key, repeats for versions)
user_segment         STRING (casual, engaged, power_user)
total_events         LONG
event_types_count    LONG
first_seen_date      DATE
last_seen_date       DATE
active_days          LONG
avg_events_per_day   DOUBLE
purchase_count       LONG
view_count           LONG
cart_count           LONG
effective_from       DATE
effective_to         DATE
is_current_version   BOOLEAN
version_number       INTEGER
created_at           TIMESTAMP
updated_at           TIMESTAMP
```

**User Segmentation Logic:**
- **power_user**: ≥100 events OR ≥10 purchases OR avg_events_per_day ≥10
- **engaged**: ≥20 events OR ≥2 purchases OR avg_events_per_day ≥2
- **casual**: All others

**Fact Table:**

**5. `fact_events` (109,950,743 rows)**
```
event_time           TIMESTAMP
event_date           DATE
event_type           STRING
date_sk              INTEGER (FK to dim_date.date_key)
user_sk              INTEGER (FK to dim_users.user_sk)
product_sk           INTEGER (FK to dim_products.product_sk)
category_sk          INTEGER (FK to dim_categories.category_sk)
price_at_event       DOUBLE (point-in-time price from product dimension)
quantity             INTEGER (1 for purchases, 0 otherwise)
revenue              DOUBLE (price * quantity for purchases)
event_count          INTEGER (always 1)
user_session         STRING
created_at           TIMESTAMP
```

**SCD Type 2 Implementation:**
- Products and users use SCD Type 2 to track changes over time
- Price changes and user behavior evolution are preserved
- Point-in-time accuracy: fact table joins to the correct product version at event_time
- Incremental update framework ready (`silver_dimensions_incremental.py`)

**Delta Lake Features (ALL tables):**
- ✅ OPTIMIZE executed (file compaction)
- ✅ Z-ORDER applied to fact table (event_date, product_sk)
- ✅ Auto-Optimize enabled (optimizeWrite + autoCompact)
- ✅ Time Travel enabled (version history tracked)
- ✅ zstd compression
- ✅ Deletion vectors enabled
- ✅ Table comments/descriptions
- ✅ MERGE support for SCD Type 2 updates

**Queries Ready:**
- Monthly revenue by brand (star schema join)
- User segment analysis (behavioral segmentation)
- Category performance by day of week
- Time Travel queries (query historical versions)
- SCD Type 2 history tracking (product price changes over time)

---

### 🚧 **Phase 4: Gold Layer - PLANNED**

**Next Steps:**
- Aggregate tables for common analytics queries
- Pre-calculated metrics (DAU, WAU, MAU, conversion rates)
- Funnel analysis tables
- Cohort analysis tables
- Product performance dashboards

---

### 📁 **Project Structure**

```
product-analytics-platform/
│
├── config/
│   ├── platform_config.py          ✅ Platform configuration
│   └── .gitkeep
│
├── notebooks/
│   ├── ingestion/
│   │   ├── kaggle_data_ingestion.py   ✅ Kaggle dataset ingestion
│   │   └── .gitkeep
│   │
│   ├── bronze/
│   │   ├── bronze_events_ingestion.py ✅ Bronze layer creation
│   │   └── .gitkeep
│   │
│   ├── silver/
│   │   ├── silver_dimensions_build.py        ✅ Initial dimension build
│   │   ├── silver_dimensions_incremental.py  ✅ SCD Type 2 incremental updates
│   │   └── .gitkeep
│   │
│   ├── gold/
│   │   └── .gitkeep                          🚧 Planned
│   │
│   └── analysis/
│       └── .gitkeep                          🚧 Planned
│
├── pipelines/
│   └── .gitkeep                              🚧 Planned (dbt models)
│
├── docs/
│   ├── SILVER_LAYER_ARCHITECTURE.md     ✅ Silver layer design decisions
│   └── DIMENSIONS_COMPLETE_GUIDE.md     ✅ Complete dimension guide
│
└── README.md                            ✅ This file
```

---

### 🎯 **Key Design Decisions**

**1. Star Schema vs. Snowflake Schema**
- **Chosen: Star Schema**
- **Rationale:**
  - Fewer joins (better query performance)
  - Simpler for BI tools and analysts
  - Denormalized dimensions (categories stored flat)
  - Optimized for OLAP workloads
  - Delta Lake compression handles redundancy efficiently

**2. SCD Type 2 for Products & Users**
- **Rationale:**
  - Track price changes over time (products)
  - Track behavioral evolution (users)
  - Point-in-time accuracy for historical analysis
  - Fact table always joins to correct version at event_time

**3. Surrogate Keys Throughout**
- **Rationale:**
  - Natural keys can change (product_id)
  - SCD Type 2 requires unique keys per version
  - Surrogate keys enable efficient joins
  - Integer keys are faster than composite keys

**4. Delta Lake as Foundation**
- **Rationale:**
  - ACID transactions
  - Time Travel for auditing
  - OPTIMIZE for query performance
  - Auto-Optimize for automatic maintenance
  - MERGE for SCD Type 2 updates

---

### 📊 **Sample Analytics Queries**

All queries are tested and working. See corrected column names:

```sql
-- Monthly Revenue by Brand
SELECT 
    d.year,
    d.month_name,
    p.brand,
    SUM(f.revenue) as total_revenue,
    COUNT(*) as purchases
FROM product_analytics.ecommerce.fact_events f
JOIN product_analytics.ecommerce.silver_dim_date d 
    ON f.date_sk = d.date_key          -- Note: date_key, not date_sk
JOIN product_analytics.ecommerce.silver_dim_products p 
    ON f.product_sk = p.product_sk
WHERE f.event_type = 'purchase' AND d.year = 2019
GROUP BY d.year, d.month_name, d.month, p.brand
ORDER BY d.month, total_revenue DESC;

-- User Segment Performance
SELECT 
    u.user_segment,
    COUNT(DISTINCT f.user_sk) as unique_users,
    SUM(CASE WHEN f.event_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
    ROUND(SUM(f.revenue), 2) as total_revenue
FROM product_analytics.ecommerce.fact_events f
JOIN product_analytics.ecommerce.silver_dim_users u 
    ON f.user_sk = u.user_sk
GROUP BY u.user_segment
ORDER BY total_revenue DESC;

-- Category Performance by Day of Week
SELECT 
    d.day_name,
    c.category_l1,
    COUNT(*) as events,
    SUM(CASE WHEN f.event_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
    ROUND(SUM(f.revenue), 2) as revenue
FROM product_analytics.ecommerce.fact_events f
JOIN product_analytics.ecommerce.silver_dim_date d 
    ON f.date_sk = d.date_key
JOIN product_analytics.ecommerce.silver_dim_categories c 
    ON f.category_sk = c.category_sk
WHERE f.event_type = 'purchase'
GROUP BY d.day_name, c.category_l1
ORDER BY revenue DESC;
```

---

## Objective

Build a production-inspired, end-to-end data platform that simulates how modern technology companies (Amazon, Uber, Airbnb, Walmart, Meta, Netflix, etc.) process user interaction events in real time to power analytics, experimentation, dashboards, and machine learning.

The project is **NOT** intended to be another ETL pipeline or another recommendation system.

Instead, the goal is to answer the following question:

> **"How do large technology companies transform billions of raw user events into trustworthy business metrics and experimentation datasets that Product Managers, Data Scientists, Analysts, and Machine Learning Engineers use every day?"**

This project should demonstrate both Data Engineering and Product Analytics/Data Science capabilities.

---

# Dataset

Use the **eCommerce Behavior Data from Multi-Category Store (REES46)** dataset.

The dataset contains historical user behavior events such as:

* page views
* product views
* add to cart
* remove from cart
* purchases

Typical fields include

* event_time
* event_type
* product_id
* category_id
* category_code
* brand
* price
* user_id
* user_session

Although the dataset is static, it will **NOT** be processed as a batch CSV.

Instead, it will simulate a live production event stream.

---

# High-Level Vision

The historical dataset represents production logs collected over several months.

A custom Event Replay Service will stream those events into Kafka at configurable rates, making Spark Structured Streaming process them exactly as if they were arriving from a live website.

The architecture should resemble a modern product analytics platform rather than a simple data pipeline.

```
Historical Dataset
        │
        ▼
 Event Replay Service
        │
        ▼
      Kafka
        │
        ▼
Spark Structured Streaming
        │
        ▼
 Bronze Delta Tables
        │
        ▼
 Silver Delta Tables
        │
        ▼
 Gold Analytics Tables
        │
   ┌────┴────────────┐
   ▼                 ▼
 Dashboards     Experimentation
                      │
                      ▼
             Product Data Science
```

---

# Guiding Principles

The project should prioritize:

* production realism
* modular architecture
* scalability
* fault tolerance
* data quality
* business usefulness

Avoid building unnecessary complexity merely to include technologies.

Every component should exist because it solves a realistic engineering or analytics problem.

---

# Business Story

Imagine this platform powers an online marketplace similar to Amazon.

Every second, users perform actions:

* browse products
* click products
* search
* add items to cart
* remove items
* purchase products

Leadership wants answers to questions such as:

### Product

Which homepage layout converts better?

Which recommendation algorithm performs better?

Which product categories are growing?

Which products have high interest but poor conversion?

---

### Growth

Where are customers abandoning the funnel?

Which customer cohorts retain the longest?

Which acquisition channels bring valuable users?

---

### Marketing

Which campaigns generate purchases?

What is click-through rate?

What is return on ad spend?

---

### Engineering

Are events arriving late?

Is Kafka healthy?

Are there duplicate events?

Are schemas changing unexpectedly?

---

### Data Science

Can we predict purchases?

Can we predict churn?

Can we identify valuable customers?

Can we evaluate experiments statistically?

The entire project exists to answer these questions.

---

# Major Components

---

## Phase 1 — Event Replay System

This replaces live website traffic.

Read historical events sequentially.

Publish each event into Kafka.

Requirements:

* configurable event rate
* configurable replay speed
* configurable time scaling
* multiple producers
* reproducible replay

Support different modes:

Normal Traffic

Peak Hours

Black Friday

Random Bursts

Slow Traffic

Night Traffic

This makes downstream systems behave realistically.

---

## Phase 2 — Kafka

Kafka represents the event backbone.

Suggested topics:

product_events

dead_letter_events

pipeline_metrics

Optional future topics:

recommendation_events

search_events

experiment_events

---

## Phase 3 — Spark Structured Streaming

Consume Kafka continuously.

Responsibilities include:

JSON parsing

Schema validation

Deduplication

Late event handling

Watermarking

Windowed aggregations

Checkpointing

Fault tolerance

Idempotent processing

Output should be Delta tables.

---

## Phase 4 — Bronze Layer

Purpose:

Store raw immutable events.

Characteristics:

Append only

No transformations

Original schema

Audit friendly

Columns may include:

event_time

ingestion_time

event_type

raw_json

partition_date

---

## Phase 5 — Silver Layer

Purpose:

Create trusted clean events.

Operations:

Remove duplicates

Handle malformed records

Normalize timestamps

Validate prices

Filter invalid users

Sessionization

Enrich product information

Create derived columns

Example derived fields:

session_duration

hour_of_day

day_of_week

is_purchase

is_cart

is_view

---

## Phase 6 — Gold Layer

Business-ready analytics tables.

Examples:

### User Metrics

daily active users

weekly active users

monthly active users

session counts

average session duration

bounce rate

---

### Product Metrics

views

cart additions

purchases

conversion rate

revenue

average order value

cart abandonment

---

### Category Metrics

top categories

highest revenue

highest conversion

lowest conversion

growth trends

---

### Funnel Metrics

View

↓

Cart

↓

Purchase

Compute:

drop-off

conversion percentages

average completion time

---

### Time Metrics

hourly revenue

daily revenue

weekly revenue

seasonality

traffic spikes

---

# Data Modeling

Design proper dimensional models.

Example:

Dimension tables

dim_user

dim_product

dim_category

dim_date

Fact tables

fact_events

fact_sessions

fact_orders

fact_revenue

fact_product_metrics

Avoid one giant denormalized table.

---

# dbt Layer

dbt should manage transformations beyond the streaming ingestion.

Responsibilities:

business logic

incremental models

testing

documentation

lineage

Suggested tests:

unique keys

not null

accepted values

relationships

freshness

---

# Monitoring

Expose operational metrics.

Pipeline latency

Kafka lag

Streaming throughput

Processing failures

Invalid events

Dead letter counts

Duplicate events

Late arrivals

Visualize these in dashboards.

---

# Dashboards

The dashboard should resemble what Product Managers actually use.

Examples:

Executive Dashboard

Revenue

Orders

DAU

Conversion

Traffic

---

Product Dashboard

CTR

Conversion

Top Products

Category Performance

---

Operations Dashboard

Pipeline latency

Kafka health

Spark throughput

Data freshness

---

Customer Dashboard

Retention

Repeat purchases

Lifetime value

Session duration

---

# Experimentation Layer

This is the differentiator.

The platform should support experiments.

Imagine two homepage versions.

Version A

Version B

Each generates different user behavior.

The platform computes:

CTR

Conversion

Revenue

Retention

Average basket size

Statistical significance

The project should include A/B testing workflows rather than only dashboards.

---

# Product Analytics

Perform analyses such as:

Funnel Analysis

Path Analysis

Cohort Analysis

Retention Curves

Customer Segmentation

Repeat Purchase Analysis

Time-to-Purchase

Cart Abandonment

Product Affinity

Category Performance

These analyses should be powered entirely by the Gold tables.

---

# Machine Learning (Optional Final Layer)

Machine learning should consume the analytics layer rather than raw events.

Possible models:

Purchase Prediction

Customer Churn Prediction

Customer Lifetime Value

Recommendation Ranking

Next Best Action

Anomaly Detection

This demonstrates the correct architecture:

Raw Events

↓

Curated Features

↓

ML

rather than

Raw Events

↓

ML

---

# Production Scenarios to Simulate

To make the project feel realistic, simulate operational issues.

Examples:

Black Friday traffic spikes

Duplicate Kafka messages

Late arriving events

Out-of-order events

Producer failures

Schema evolution

Missing optional fields

Corrupted records

Network delays

Backpressure

The pipeline should demonstrate graceful handling wherever practical.

---

# Technologies

Data Ingestion

Python

Kafka

Streaming

PySpark Structured Streaming

Storage

Delta Lake

Analytics Engineering

dbt

Warehouse

DuckDB (local) or Snowflake (optional extension)

Orchestration

Airflow

Monitoring

Prometheus

Grafana

Visualization

Streamlit

Plotly

(Optional: Apache Superset)

Machine Learning

Scikit-learn

XGBoost

LightGBM

(Optional: MLflow)

Containerization

Docker

Version Control

GitHub Actions for CI/CD

---

# What This Project Demonstrates to Recruiters

## Data Engineering

Real-time ingestion

Streaming pipelines

Distributed processing

Lakehouse architecture

Data modeling

Incremental processing

Data quality

Monitoring

Scalable design

---

## Analytics Engineering

Metric definitions

Dimensional modeling

dbt

Business transformations

Documentation

---

## Product Analytics

KPIs

Funnels

Retention

Cohorts

Path analysis

Experimentation

Dashboard design

---

## Data Science

Feature engineering

Statistical testing

Predictive modeling

Model evaluation

Business interpretation

---

# Final Deliverable

The end result should **not** be described as "a Kafka + Spark project."

It should be presented as:

> **A production-inspired Real-Time Product Intelligence & Experimentation Platform that replays historical user behavior into a streaming architecture, transforms raw events into trusted analytics datasets using a modern Lakehouse architecture, powers executive dashboards and experimentation workflows, and enables downstream product analytics and machine learning.**

Every architectural decision, technology choice, and analysis should reinforce that narrative. The project should tell a single cohesive story: raw user interactions become reliable business intelligence that helps teams make product decisions with confidence.
