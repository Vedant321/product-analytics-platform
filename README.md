# Product Analytics Platform

A production-grade data platform demonstrating how technology companies transform raw user events into business metrics and analytics.

Built on Databricks with Delta Lake, implementing medallion architecture (Bronze → Silver → Gold) with proper dimensional modeling and SCD Type 2.

---

## What's Built

### Data Pipeline
- **Bronze Layer**: Raw event storage (109.9M events) with append-only Delta tables
- **Silver Layer**: Star schema with 4 dimensions + 1 fact table, SCD Type 2 for product/user history
- **Source**: eCommerce behavior dataset (REES46) - views, cart events, purchases

### Silver Layer Schema

**Star Schema Architecture:**
```
         dim_date (426 rows)
              │
              ├─────────────┐
              │             │
     dim_categories    fact_events    dim_products
       (130 rows)    (109.9M rows)   (206.9K rows, SCD2)
              │             │
              └─────────────┘
                    │
              dim_users
           (5.3M rows, SCD2)
```

**Fact Table:** `fact_events`
- 109.9M rows (one row per event)
- Grain: individual user event (view, cart, purchase)
- FKs to all 4 dimensions via surrogate keys
- Measures: revenue, quantity, event_count

**Dimensions:**
1. **dim_date** - Date dimension (426 days)
2. **dim_categories** - Product category hierarchy (3 levels)
3. **dim_products** - Product master with price/brand (SCD Type 2)
4. **dim_users** - User profiles with behavioral segments (SCD Type 2)

**SCD Type 2:**
- Products track price/brand changes over time
- Users track behavioral evolution (casual → engaged → power_user)
- Point-in-time accuracy: fact table joins to correct version at event_time
- History preserved: effective_from/effective_to dates, version_number

---

## Key Technical Decisions

**Star Schema over Snowflake**
- Fewer joins, better query performance
- BI-friendly, simpler for analysts
- Delta Lake compression handles denormalization

**SCD Type 2 for Products/Users**
- Track price changes over time
- Track user behavioral evolution
- Point-in-time accurate historical analysis

**Surrogate Keys**
- Natural keys can change
- SCD Type 2 requires unique keys per version
- Integer surrogates enable efficient joins

**Delta Lake Foundation**
- ACID transactions
- Time Travel for auditing
- OPTIMIZE/Z-ORDER for performance
- Auto-Optimize for automatic maintenance
- MERGE support for SCD Type 2 updates

---

## Sample Queries

**Monthly Revenue by Brand:**
```sql
SELECT 
    d.year, d.month_name, p.brand,
    SUM(f.revenue) as total_revenue,
    COUNT(*) as purchases
FROM product_analytics.ecommerce.fact_events f
JOIN product_analytics.ecommerce.silver_dim_date d 
    ON f.date_sk = d.date_key
JOIN product_analytics.ecommerce.silver_dim_products p 
    ON f.product_sk = p.product_sk
WHERE f.event_type = 'purchase' AND d.year = 2019
GROUP BY d.year, d.month_name, d.month, p.brand
ORDER BY d.month, total_revenue DESC;
```

**User Segment Performance:**
```sql
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
```

**Category Performance by Day:**
```sql
SELECT 
    d.day_name, c.category_l1,
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

## Project Structure

```
product-analytics-platform/
│
├── config/
│   └── platform_config.py              # Platform configuration
│
├── notebooks/
│   ├── ingestion/
│   │   └── kaggle_data_ingestion.py    # Kaggle dataset ingestion
│   ├── bronze/
│   │   └── bronze_events_ingestion.py  # Bronze layer creation
│   ├── silver/
│   │   ├── silver_dimensions_build.py        # Initial dimension build
│   │   └── silver_dimensions_incremental.py  # SCD Type 2 updates
│   ├── gold/                           # (planned)
│   └── analysis/                       # (planned)
│
├── pipelines/                          # (planned - dbt models)
├── docs/
│   ├── SILVER_LAYER_ARCHITECTURE.md
│   └── DIMENSIONS_COMPLETE_GUIDE.md
│
└── README.md
```

---

## Delta Lake Features

All tables use Delta Lake with:
- **OPTIMIZE**: File compaction for query performance
- **Z-ORDER**: Data clustering (fact_events on event_date, product_sk)
- **Auto-Optimize**: Automatic optimizeWrite + autoCompact
- **Time Travel**: Version history tracking
- **Compression**: zstd compression
- **Deletion Vectors**: Efficient deletes/updates
- **MERGE**: Transactional SCD Type 2 updates

---

## User Segmentation

Users are classified based on behavior:

- **power_user**: ≥100 events OR ≥10 purchases OR avg ≥10 events/day
- **engaged**: ≥20 events OR ≥2 purchases OR avg ≥2 events/day  
- **casual**: Everyone else

---

## What's Next

**Gold Layer:**
- Pre-aggregated metrics (DAU, WAU, MAU)
- Conversion funnel analysis
- Cohort retention tables
- Product performance dashboards

**Advanced Analytics:**
- Customer lifetime value
- Churn prediction
- Product recommendation
- A/B testing framework

---

## Technologies

- **Platform**: Databricks on AWS
- **Storage**: Delta Lake (Unity Catalog)
- **Compute**: Spark (Serverless)
- **Language**: Python, SQL
- **Architecture**: Medallion (Bronze/Silver/Gold)
- **Modeling**: Kimball star schema, SCD Type 2

---

## Dataset

**Source**: eCommerce Behavior Data from Multi-Category Store (REES46)

**Event Types**:
- `view` - Product page view
- `cart` - Add to cart
- `remove_from_cart` - Remove from cart
- `purchase` - Purchase completed

**Original Fields**:
- event_time, event_type, product_id, category_id, category_code
- brand, price, user_id, user_session

**Period**: 426 days of e-commerce activity  
**Volume**: 109.9M events across 5.3M users and 206K products

---

## Notes

This platform demonstrates production data engineering practices:
- Proper dimensional modeling (Kimball methodology)
- SCD Type 2 for slowly changing dimensions
- Surrogate key management
- Delta Lake best practices
- Star schema for BI/analytics
- Incremental processing frameworks

Built to showcase end-to-end data engineering capabilities, from raw ingestion through dimensional modeling to analytics-ready data marts.
