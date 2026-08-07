# Silver Layer Architecture - Product Analytics Platform

## 🎯 Purpose

The Silver layer transforms raw Bronze data into **analytics-ready**, **dimensional** datasets following enterprise Lakehouse patterns. This layer implements:

- **Dimensional Modeling** (Star Schema)
- **SCD Type 2** (Slowly Changing Dimensions)
- **Feature Engineering** (RFM, behavioral signals)
- **Data Quality Validation**
- **Incremental Processing**

---

## 🏗️ Architecture Overview

```
BRONZE LAYER (Raw)
    ↓
    ↓ ETL Pipeline (This Layer)
    ↓
SILVER LAYER (Dimensional Star Schema)
├── Fact Tables
│   ├── silver_fact_events (Event-level facts)
│   └── silver_fact_sessions (Session-level facts)
│
├── Dimension Tables (SCD Type 2)
│   ├── silver_dim_products (Product attributes with history)
│   ├── silver_dim_users (User attributes with segments)
│   ├── silver_dim_categories (Category hierarchy)
│   └── silver_dim_date (Calendar dimension)
│
└── Feature Tables (Advanced Analytics)
    ├── silver_user_behavioral_features (RFM, engagement)
    ├── silver_product_performance_features (Metrics, trends)
    └── silver_product_affinity (Co-occurrence, recommendations)
```

---

## 📊 Table Definitions

### 1. Fact Tables

#### `silver_fact_events`
**Purpose:** Event-level facts with surrogate keys to dimensions

**Schema:**
```
event_id                    BIGINT (generated surrogate key)
event_time                  TIMESTAMP
event_date                  DATE (partition key)
event_type                  STRING (view, cart, purchase, remove_from_cart)
user_session                STRING

-- Surrogate Keys (join to dimensions)
user_sk                     BIGINT (FK to dim_users)
product_sk                  BIGINT (FK to dim_products)
category_sk                 BIGINT (FK to dim_categories)
date_key                    INT (FK to dim_date, YYYYMMDD)

-- Measures
price                       DOUBLE

-- Time Features
hour_of_day                 INT
day_of_week                 INT
is_weekend                  BOOLEAN
is_business_hours           BOOLEAN
time_of_day                 STRING (morning, afternoon, evening, night)

-- Session Context
event_number_in_session     INT
is_first_event_in_session   BOOLEAN

-- Audit
ingestion_timestamp         TIMESTAMP
source_file                 STRING
```

**Why Surrogate Keys?**
- Natural keys (user_id, product_id) can change or be reused
- SCD Type 2 needs unique keys per version
- Integer joins are faster than string joins
- Decouples fact tables from dimension changes

---

#### `silver_fact_sessions`
**Purpose:** Session-level aggregated facts

**Schema:**
```
session_id                     STRING (PK)
user_sk                        BIGINT (FK to dim_users)
session_date                   DATE (partition key)

-- Session Timing
session_start_time             TIMESTAMP
session_end_time               TIMESTAMP
session_duration_seconds       INT

-- Event Counts
total_events                   INT
total_views                    INT
total_carts                    INT
total_purchases                INT
total_removals                 INT

-- Engagement Metrics
unique_products_viewed         INT
unique_categories_viewed       INT
avg_time_between_events_sec    DOUBLE

-- Conversion
did_purchase                   BOOLEAN
did_add_to_cart                BOOLEAN
funnel_stage                   STRING (view_only, cart, purchase)
session_revenue                DOUBLE

-- Behavioral Flags
is_bounce_session              BOOLEAN (single event)
is_weekend                     BOOLEAN
session_time_of_day            STRING
```

---

### 2. Dimension Tables (SCD Type 2)

#### `silver_dim_products` (SCD Type 2)
**Purpose:** Product master with price history

**Schema:**
```
product_sk                  BIGINT (PK, auto-increment)
product_id                  BIGINT (natural key)
product_name                STRING (future: join with product catalog)
brand                       STRING
category_l1                 STRING
category_l2                 STRING
category_l3                 STRING
category_full_path          STRING

-- SCD Type 2 Fields
current_price               DOUBLE
effective_from_date         DATE (when this version became active)
effective_to_date           DATE (when this version expired, 9999-12-31 if current)
is_current_version          BOOLEAN
version_number              INT (1, 2, 3, ...)

-- Audit
first_seen_date             DATE (when product first appeared)
last_seen_date              DATE (last event with this product)
created_at                  TIMESTAMP
updated_at                  TIMESTAMP
```

**Example: Price Change Tracking**
```
product_sk | product_id | price | effective_from | effective_to | is_current
-----------|------------|-------|----------------|--------------|------------
1001       | 12345      | 999   | 2019-10-01     | 2019-11-15   | False
1002       | 12345      | 799   | 2019-11-16     | 9999-12-31   | True
```

**Why SCD Type 2?**
- Track how product attributes change over time
- Answer questions like: "What was the price when user X viewed it?"
- Essential for accurate revenue analysis
- Common in e-commerce (price changes, promotions)

---

#### `silver_dim_users` (SCD Type 2)
**Purpose:** User master with behavioral segments

**Schema:**
```
user_sk                        BIGINT (PK, auto-increment)
user_id                        BIGINT (natural key)

-- Lifetime Metrics
first_event_date               DATE
last_event_date                DATE
total_lifetime_sessions        INT
total_lifetime_events          INT
total_lifetime_revenue         DOUBLE
total_lifetime_purchases       INT

-- Behavioral Segmentation
customer_segment               STRING (new, active, loyal, at_risk, churned)
favorite_category_l1           STRING
favorite_brand                 STRING
avg_session_duration           DOUBLE
avg_events_per_session         DOUBLE
conversion_rate                DOUBLE (purchases / sessions)

-- RFM Scores (calculated from features)
recency_score                  INT (1-5)
frequency_score                INT (1-5)
monetary_score                 INT (1-5)
rfm_combined_score             STRING (555, 554, etc.)

-- SCD Type 2 Fields
effective_from_date            DATE
effective_to_date              DATE
is_current_version             BOOLEAN
version_number                 INT

-- Audit
created_at                     TIMESTAMP
updated_at                     TIMESTAMP
```

**Segmentation Logic:**
- **New:** < 7 days since first event, < 3 sessions
- **Active:** Recent activity (< 30 days), regular sessions
- **Loyal:** High frequency, high revenue, long tenure
- **At Risk:** No activity in 30-60 days
- **Churned:** No activity in > 60 days

**Why Track User Segments Over Time?**
- Users move between segments (active → at_risk → churned)
- Marketing teams need to know "when did this user churn?"
- Enables cohort analysis and retention studies

---

#### `silver_dim_categories`
**Purpose:** Category hierarchy (static, no SCD needed)

**Schema:**
```
category_sk                 BIGINT (PK, auto-increment)
category_l1                 STRING
category_l2                 STRING
category_l3                 STRING
category_full_path          STRING (dot-separated)
category_depth              INT (1, 2, or 3)
parent_category_sk          BIGINT (FK to self, for hierarchy)
created_at                  TIMESTAMP
```

**Example:**
```
category_sk | l1          | l2         | l3    | full_path                    | depth | parent_sk
------------|-----------|------------|-------|------------------------------|-------|----------
1           | electronics| NULL       | NULL  | electronics                  | 1     | NULL
2           | electronics| smartphone | NULL  | electronics.smartphone       | 2     | 1
3           | electronics| smartphone | apple | electronics.smartphone.apple | 3     | 2
```

---

#### `silver_dim_date`
**Purpose:** Pre-built calendar dimension for time-based analysis

**Schema:**
```
date_key                    INT (PK, YYYYMMDD format, e.g. 20191101)
full_date                   DATE
year                        INT
quarter                     INT (1-4)
month                       INT (1-12)
month_name                  STRING (January, February, ...)
week_of_year                INT (1-52)
day_of_month                INT (1-31)
day_of_week                 INT (1-7)
day_name                    STRING (Monday, Tuesday, ...)
is_weekend                  BOOLEAN
is_month_start              BOOLEAN
is_month_end                BOOLEAN
is_quarter_start            BOOLEAN
is_quarter_end              BOOLEAN
fiscal_year                 INT (if fiscal calendar differs)
fiscal_quarter              INT
fiscal_month                INT
```

**Why a Date Dimension?**
- Faster joins (integer vs date operations)
- Consistent date attributes across all queries
- Support fiscal calendars
- Enable date-based filtering without calculations

---

### 3. Feature Tables

#### `silver_user_behavioral_features`
**Purpose:** Per-user features for ML and segmentation

**Schema:**
```
user_id                        BIGINT (PK)
feature_date                   DATE (PK, daily snapshot)

-- RFM Components
recency_days                   INT (days since last event)
frequency_events_30d           INT (events in last 30 days)
frequency_sessions_30d         INT (sessions in last 30 days)
monetary_revenue_30d           DOUBLE (revenue in last 30 days)

-- Rolling Window Metrics
events_last_7d                 INT
events_last_14d                INT
events_last_30d                INT
sessions_last_7d               INT
sessions_last_30d              INT
revenue_last_7d                DOUBLE
revenue_last_30d               DOUBLE

-- Engagement
avg_session_duration_30d       DOUBLE
avg_events_per_session_30d     DOUBLE
avg_time_between_sessions_days DOUBLE

-- Conversion
purchases_last_30d             INT
conversion_rate_30d            DOUBLE (purchases / sessions)
avg_order_value_30d            DOUBLE

-- Behavioral Patterns
favorite_shopping_hour         INT (most common hour)
preferred_day_of_week          STRING
is_weekend_shopper             BOOLEAN (majority weekend activity)

-- Trend Signals
trend_events_7d_vs_prior_7d    DOUBLE (ratio, > 1 = increasing)
trend_revenue_30d_vs_prior_30d DOUBLE

-- Calculated at
calculated_at                  TIMESTAMP
```

**Use Cases:**
- Customer segmentation
- Churn prediction models
- Personalization engines
- Targeted marketing

---

#### `silver_product_performance_features`
**Purpose:** Per-product metrics for ranking and trends

**Schema:**
```
product_id                     BIGINT (PK)
feature_date                   DATE (PK, daily snapshot)

-- Lifetime Metrics
total_views_lifetime           INT
total_cart_adds_lifetime       INT
total_purchases_lifetime       INT
total_revenue_lifetime         DOUBLE

-- Rolling Windows
views_last_7d                  INT
views_last_30d                 INT
purchases_last_7d              INT
purchases_last_30d             INT
revenue_last_7d                DOUBLE
revenue_last_30d               DOUBLE

-- Conversion Rates
view_to_cart_rate_lifetime     DOUBLE
cart_to_purchase_rate_lifetime DOUBLE
view_to_purchase_rate_lifetime DOUBLE

-- Pricing
avg_price_sold_at              DOUBLE
min_price_seen                 DOUBLE
max_price_seen                 DOUBLE
last_known_price               DOUBLE

-- Freshness
days_since_last_view           INT
days_since_last_purchase       INT
is_active_product              BOOLEAN (viewed in last 30 days)

-- Trend Signals
is_trending                    BOOLEAN (view velocity increasing)
trend_views_7d_vs_prior_7d     DOUBLE
trend_purchases_30d_vs_prior   DOUBLE

-- Calculated at
calculated_at                  TIMESTAMP
```

**Use Cases:**
- Product ranking/sorting
- Inventory optimization
- Recommendation engines
- Marketing campaign targeting

---

#### `silver_product_affinity`
**Purpose:** Product co-occurrence for recommendations

**Schema:**
```
product_id_1                   BIGINT (PK)
product_id_2                   BIGINT (PK)
affinity_type                  STRING (PK: view_together, purchase_together)

-- Co-occurrence Counts
cooccurrence_count             INT (how many times seen together)
sessions_with_both             INT (distinct sessions)

-- Association Metrics
support                        DOUBLE (% of sessions with both)
confidence_1_to_2              DOUBLE (P(product_2 | product_1))
confidence_2_to_1              DOUBLE (P(product_1 | product_2))
lift                           DOUBLE (how much more likely together vs. independent)

-- Calculated at
calculated_at                  TIMESTAMP
```

**Market Basket Analysis:**
```
Product 1       | Product 2      | Type            | Lift | Confidence
----------------|----------------|-----------------|------|------------
iPhone 11       | AirPods        | purchase_together| 3.5  | 0.42
Samsung TV      | HDMI Cable     | view_together   | 5.2  | 0.68
Laptop          | Laptop Bag     | purchase_together| 4.1  | 0.55
```

**Use Cases:**
- "Frequently bought together"
- "Customers who viewed X also viewed Y"
- Cross-sell recommendations
- Bundle pricing optimization

---

## 🔄 Processing Strategy

### Incremental vs. Full Refresh

| Table | Strategy | Reason |
|-------|----------|--------|
| fact_events | Incremental MERGE | Large volume, append new events |
| fact_sessions | Incremental MERGE | Sessionization requires lookback |
| dim_products | SCD Type 2 MERGE | Track price/attribute changes |
| dim_users | SCD Type 2 MERGE | Update segments periodically |
| dim_categories | Full REFRESH | Small, static |
| dim_date | Full REFRESH (once) | Static calendar |
| user_features | Full REFRESH daily | Recalculate rolling windows |
| product_features | Full REFRESH daily | Recalculate metrics |
| product_affinity | Full REFRESH weekly | Expensive computation |

---

## 📝 Implementation Sequence

### Phase 1: Core Dimensional Model (MVP)
1. ✅ Build `dim_date` (static, run once)
2. ✅ Build `dim_categories` (extract from Bronze)
3. ✅ Build `dim_products` (SCD Type 2 initial load)
4. ✅ Build `dim_users` (SCD Type 2 initial load)
5. ✅ Build `fact_events` (with surrogate keys)
6. ✅ Build `fact_sessions` (aggregated from events)

### Phase 2: Feature Engineering
7. ✅ Build `user_behavioral_features` (RFM + rolling windows)
8. ✅ Build `product_performance_features` (metrics + trends)
9. ✅ Build `product_affinity` (market basket analysis)

### Phase 3: Data Quality & Optimization
10. ✅ Add data quality checks
11. ✅ Implement incremental merge logic
12. ✅ Add Z-ORDER optimization
13. ✅ Document lineage

---

## 🎯 Success Metrics

**Technical:**
- ✅ All tables follow star schema
- ✅ SCD Type 2 correctly tracks changes
- ✅ Surrogate keys enable fast joins
- ✅ Incremental processing works correctly
- ✅ Data quality checks pass

**Portfolio:**
- ✅ Demonstrates dimensional modeling expertise
- ✅ Shows understanding of SCD patterns
- ✅ Exhibits feature engineering skills
- ✅ Proves production-grade coding

---

## 📚 Learning Resources

**Concepts to Research:**
- Kimball Star Schema methodology
- SCD Type 2 (Slowly Changing Dimensions)
- Surrogate vs. Natural Keys
- RFM Analysis (Recency, Frequency, Monetary)
- Market Basket Analysis / Association Rules
- Delta Lake MERGE operation
- Z-ORDER optimization in Databricks

**This Architecture Document:**
- Will guide our implementation
- Serves as your portfolio documentation
- Can be shown to interviewers
- Proves you think before you code

---

**Next Steps:** Build the pipeline notebook-by-notebook, starting with dimension tables.
