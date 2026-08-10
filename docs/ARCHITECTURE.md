# Platform Architecture

Visual guide to the Product Analytics Platform architecture and data flow.

---

## 📊 Full Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              BRONZE LAYER                                │
│                         (Raw Data - Immutable)                           │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  📦 bronze_events                                                        │
│  ├─ 109,950,743 rows                                                    │
│  ├─ Raw eCommerce events from Kaggle                                    │
│  ├─ Columns: event_time, event_type, product_id, category_id,          │
│  │            user_id, brand, price, user_session                       │
│  └─ Storage: Delta Lake (append-only, immutable)                        │
│                                                                          │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               │ Transform: Clean, Type, Add Surrogate Keys
                               │ Notebook: silver_dimensions_build.py
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                              SILVER LAYER                                │
│                   (Star Schema - Analytics Ready)                        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│                    ┌─────────────────────────┐                          │
│                    │     dim_date (426)      │                          │
│                    ├─────────────────────────┤                          │
│                    │ PK: date_key           │                          │
│                    │ • full_date            │                          │
│                    │ • year, quarter, month │                          │
│                    │ • day_name, is_weekend │                          │
│                    └───────────┬─────────────┘                          │
│                                │                                         │
│              ┌─────────────────┼─────────────────┐                      │
│              │                 │                 │                      │
│    ┌─────────▼────────┐  ┌────▼──────────┐  ┌──▼──────────────┐       │
│    │  dim_categories  │  │  fact_events  │  │  dim_products   │       │
│    │      (130)       │  │  (109.9M)     │  │   (206,900)     │       │
│    ├──────────────────┤  ├───────────────┤  ├─────────────────┤       │
│    │PK: category_sk   │  │FK: user_sk    │  │PK: product_sk   │       │
│    │• category_code   │  │FK: product_sk │  │• product_id     │       │
│    │• category_l1     │  │FK: date_sk    │  │• brand          │       │
│    │• category_l2     │  │FK: category_sk│  │• price          │       │
│    │• category_l3     │  │• event_type   │  │• effective_from │       │
│    │                  │  │• event_time   │  │• effective_to   │       │
│    │                  │  │• revenue      │  │• is_current     │       │
│    │                  │  │• quantity     │  │• version_number │       │
│    └──────────────────┘  └───────┬───────┘  └─────────────────┘       │
│                                  │                                      │
│                           ┌──────▼─────────┐                           │
│                           │   dim_users    │                           │
│                           │   (5,317,900)  │                           │
│                           ├────────────────┤                           │
│                           │PK: user_sk     │                           │
│                           │• user_id       │                           │
│                           │• user_segment  │                           │
│                           │• total_events  │                           │
│                           │• effective_from│                           │
│                           │• effective_to  │                           │
│                           │• is_current    │                           │
│                           │• version_number│                           │
│                           └────────────────┘                           │
│                                                                         │
│  Key Features:                                                          │
│  ✓ Surrogate keys (product_sk, user_sk, date_sk, category_sk)         │
│  ✓ SCD Type 2 (dim_products, dim_users)                                │
│  ✓ Point-in-time accurate joins                                        │
│  ✓ Delta Lake: OPTIMIZE, Z-ORDER, Time Travel                          │
│                                                                         │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               │ Aggregate: GROUP BY, Pre-calculate Metrics
                               │ Notebooks: gold_*.py
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                              GOLD LAYER                                  │
│                   (Business Metrics - Dashboard Ready)                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ✅ gold_daily_metrics (61 rows)                                        │
│  ├─ Grain: One row per date                                             │
│  ├─ Metrics: DAU, revenue, conversions, AOV                             │
│  ├─ Query Speed: 0.1s (vs 30s+ on silver)                               │
│  └─ Use: Daily KPI dashboards, trend analysis                           │
│                                                                          │
│  🔲 gold_user_metrics (planned: ~5.3M rows)                             │
│  ├─ Grain: One row per user                                             │
│  ├─ Metrics: LTV, RFM scores, total purchases                           │
│  └─ Use: Customer segmentation, retention analysis                      │
│                                                                          │
│  🔲 gold_product_performance (planned: ~207K rows)                      │
│  ├─ Grain: One row per product                                          │
│  ├─ Metrics: Views, conversions, revenue per product                    │
│  └─ Use: Product analytics, inventory decisions                         │
│                                                                          │
│  🔲 gold_category_performance (planned: ~130 rows)                      │
│  🔲 gold_conversion_funnel (planned)                                    │
│  🔲 gold_cohort_retention (planned)                                     │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 🔄 Data Flow: Bronze → Silver → Gold

### **Step 1: Bronze Ingestion**
```
Source (Kaggle CSV) 
    ↓ 
bronze_events (Delta Lake)
    • Append-only
    • No transformations
    • Immutable audit trail
```

### **Step 2: Silver Transformation**
```
bronze_events
    ↓
Star Schema Creation:
    ├─ Extract dimensions (date, users, products, categories)
    ├─ Assign surrogate keys
    ├─ Implement SCD Type 2 (products, users)
    └─ Create fact table with FKs
    ↓
Silver Layer (fact_events + 4 dimensions)
```

### **Step 3: Gold Aggregation**
```
Silver Layer (fact_events + dimensions)
    ↓
SQL Aggregations:
    ├─ GROUP BY date → gold_daily_metrics
    ├─ GROUP BY user → gold_user_metrics
    └─ GROUP BY product → gold_product_performance
    ↓
Gold Layer (business metrics)
```

---

## 🎯 Gold Layer: gold_daily_metrics Explained

### **The Transformation**

```
INPUT: fact_events (109,950,743 rows)
├─ 2019-10-01: 1,800,000 events
├─ 2019-10-02: 1,500,000 events
├─ 2019-10-03: 1,900,000 events
└─ ... (61 days total)

SQL TRANSFORMATION:
SELECT 
    d.full_date,
    COUNT(DISTINCT f.user_sk) as daily_active_users,
    COUNT(*) as total_events,
    SUM(revenue) as total_revenue,
    ... (20+ metrics)
FROM fact_events f
JOIN dim_date d ON f.date_sk = d.date_key
JOIN dim_users u ON f.user_sk = u.user_sk
GROUP BY d.full_date, d.year, d.month, ...

OUTPUT: gold_daily_metrics (61 rows)
├─ 2019-10-01: DAU=487K, Revenue=$10M, Conversion=1.8%
├─ 2019-10-02: DAU=251K, Revenue=$5M, Conversion=1.6%
└─ ... (aggregated metrics per day)
```

### **What's Pre-Calculated**

| Metric Category | Examples |
|----------------|----------|
| **User Metrics** | DAU, DAU by segment (power/engaged/casual) |
| **Event Counts** | Total views, carts, purchases, removes |
| **Revenue Metrics** | Total revenue, AOV, quantity sold |
| **Conversion Rates** | View→cart, cart→purchase, overall |
| **Activity** | Unique products viewed/sold, categories active |

### **Speed Comparison**

```
Question: "What was revenue on 2019-11-30?"

WITHOUT GOLD (Query Silver):
SELECT SUM(revenue) 
FROM fact_events 
WHERE DATE(event_time) = '2019-11-30'
→ Scans 109.9M rows
→ Takes 30+ seconds

WITH GOLD (Query Gold):
SELECT total_revenue 
FROM gold_daily_metrics 
WHERE full_date = '2019-11-30'
→ Reads 1 row
→ Takes 0.1 seconds

Speed-up: 300x faster
```

---

## 🔗 Table Relationships

### **Star Schema (Silver Layer)**

```
               fact_events
                    │
        ┌───────────┼───────────┐
        │           │           │
        │           │           │
   date_sk     product_sk   category_sk
        │           │           │
        ▼           ▼           ▼
   dim_date   dim_products  dim_categories
                    │
              ┌─────┘
              │
           user_sk
              │
              ▼
          dim_users

Join Example:
SELECT 
    d.month_name,
    u.user_segment,
    SUM(f.revenue)
FROM fact_events f
JOIN dim_date d ON f.date_sk = d.date_key
JOIN dim_users u ON f.user_sk = u.user_sk
WHERE f.event_type = 'purchase'
GROUP BY d.month_name, u.user_segment
```

### **Gold Layer Dependencies**

```
gold_daily_metrics
    └─ Reads from:
        ├─ fact_events (event counts, revenue)
        ├─ dim_date (calendar attributes)
        └─ dim_users (user segments)

gold_user_metrics (planned)
    └─ Reads from:
        ├─ fact_events (user activity)
        └─ dim_users (user details)

gold_product_performance (planned)
    └─ Reads from:
        ├─ fact_events (product activity)
        └─ dim_products (product details)
```

---

## 📁 File Structure

```
product-analytics-platform/
│
├── notebooks/
│   ├── ingestion/
│   │   └── kaggle_data_ingestion.py          # Raw data → Bronze
│   │
│   ├── bronze/
│   │   └── bronze_events_ingestion.py        # Validate bronze layer
│   │
│   ├── silver/
│   │   ├── silver_dimensions_build.py        # Bronze → Silver (initial)
│   │   └── silver_dimensions_incremental.py  # SCD Type 2 updates
│   │
│   └── gold/
│       └── gold_daily_metrics.py             # ✅ Silver → Gold (built)
│
├── docs/
│   ├── ARCHITECTURE.md                       # ← You are here
│   ├── SILVER_LAYER_ARCHITECTURE.md
│   └── DIMENSIONS_COMPLETE_GUIDE.md
│
└── README.md
```

---

## 🎨 Design Decisions

### **Why Star Schema (Not Snowflake)?**
- **Fewer joins** = faster queries
- **BI-friendly** = easier for analysts
- **Denormalization** = acceptable with Delta Lake compression

### **Why SCD Type 2?**
- **Track history** of price changes (products)
- **Track evolution** of user behavior (casual → engaged → power)
- **Point-in-time accuracy** in historical analysis

### **Why Surrogate Keys?**
- **Natural keys change** (product IDs can be reused)
- **SCD Type 2 requires** unique keys per version
- **Integer joins** are faster than string joins

### **Why 3 Layers?**

| Layer | Purpose | Grain | Use Case |
|-------|---------|-------|----------|
| **Bronze** | Raw truth | Event-level | Auditing, reprocessing |
| **Silver** | Clean truth | Event-level | Ad-hoc analysis, joins |
| **Gold** | Business truth | Aggregated | Dashboards, reports |

---

## 🔢 Current Data Volumes

| Table | Rows | Size | Update Frequency |
|-------|------|------|------------------|
| **Bronze** ||||
| bronze_events | 109,950,743 | ~15 GB | Append-only |
| **Silver** ||||
| fact_events | 109,950,743 | ~12 GB | Daily refresh |
| dim_date | 426 | <1 MB | Static |
| dim_categories | 130 | <1 MB | Static |
| dim_products | 206,900 | ~50 MB | Incremental (SCD2) |
| dim_users | 5,317,900 | ~800 MB | Incremental (SCD2) |
| **Gold** ||||
| gold_daily_metrics | 61 | <1 MB | Daily refresh |

**Total Platform Storage:** ~28 GB

---

## 🚀 Performance Optimizations

### **Applied to All Tables:**
- ✅ Delta Lake format
- ✅ OPTIMIZE (file compaction)
- ✅ Z-ORDER (data clustering)
- ✅ Auto-Optimize (optimizeWrite + autoCompact)
- ✅ Deletion Vectors (efficient updates)
- ✅ zstd Compression
- ✅ Time Travel enabled

### **Specific Optimizations:**
```
fact_events:      Z-ORDER BY (event_date, product_sk)
gold_daily_metrics: Z-ORDER BY (date_key)
dim_products:     Z-ORDER BY (product_sk, effective_from)
dim_users:        Z-ORDER BY (user_sk, effective_from)
```

---

## 📊 Query Patterns

### **Common Dashboard Queries (Use Gold)**

```sql
-- Daily KPIs
SELECT * FROM gold_daily_metrics 
WHERE full_date >= CURRENT_DATE - INTERVAL 7 DAYS;

-- Monthly trends
SELECT 
    year, month_name,
    SUM(total_revenue) as revenue
FROM gold_daily_metrics
GROUP BY year, month, month_name;
```

### **Ad-Hoc Analysis (Use Silver)**

```sql
-- Product analysis with history
SELECT 
    p.product_id,
    p.brand,
    p.price,
    p.effective_from,
    COUNT(*) as purchases
FROM fact_events f
JOIN dim_products p ON f.product_sk = p.product_sk
WHERE f.event_type = 'purchase'
GROUP BY ALL;
```

---

## 🎯 Gold Layer Roadmap

```
Phase 1: Daily Metrics ✅
└─ gold_daily_metrics (61 rows)

Phase 2: User Analytics
└─ gold_user_metrics (~5.3M rows)

Phase 3: Product Analytics
├─ gold_product_performance (~207K rows)
└─ gold_category_performance (~130 rows)

Phase 4: Advanced Analytics
├─ gold_conversion_funnel
└─ gold_cohort_retention
```

---

## 🔍 How to Navigate

1. **Start with Bronze** if you need raw, unprocessed data
2. **Use Silver** for detailed analysis with joins
3. **Use Gold** for dashboards and pre-aggregated metrics

**Rule of Thumb:**
- Need transaction details? → Silver
- Need business metrics? → Gold
- Need raw data? → Bronze

---

## 📚 Related Documentation

- [Silver Layer Architecture](SILVER_LAYER_ARCHITECTURE.md) - Star schema details
- [Dimensions Guide](DIMENSIONS_COMPLETE_GUIDE.md) - SCD Type 2 implementation
- [README](../README.md) - Project overview

---

**Last Updated:** August 10, 2026  
**Platform Version:** Bronze ✅ | Silver ✅ | Gold Phase 1 ✅
