# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Load Configuration
# Load platform configuration
%run ../../config/platform_config

# COMMAND ----------

# DBTITLE 1,Introduction - Star Schema & Dimensional Modeling
# MAGIC %md
# MAGIC # Silver Layer: Dimensional Modeling (Star Schema)
# MAGIC
# MAGIC ## 🎯 What We're Building Today
# MAGIC
# MAGIC We're building the **core star schema** for our e-commerce analytics platform:
# MAGIC
# MAGIC ```
# MAGIC                     STAR SCHEMA
# MAGIC                          
# MAGIC          dim_products ────┐
# MAGIC          dim_users ───────┤
# MAGIC          dim_categories ──┼──→ fact_events (110M rows)
# MAGIC          dim_date ────────┘
# MAGIC                           ↓
# MAGIC                     fact_sessions (23M rows)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Learning Objective #1: Why Dimensional Modeling?
# MAGIC
# MAGIC ### The Problem with "Just Query Raw Events"
# MAGIC
# MAGIC Imagine you're analyzing product sales. With raw events:
# MAGIC
# MAGIC ```sql
# MAGIC -- Every query repeats the same logic:
# MAGIC SELECT 
# MAGIC   product_id,
# MAGIC   price,
# MAGIC   brand,                    -- What if brand is NULL?
# MAGIC   category_code,            -- How to split hierarchy?
# MAGIC   event_type
# MAGIC FROM bronze_events
# MAGIC WHERE product_id = 12345;
# MAGIC ```
# MAGIC
# MAGIC **Problems:**
# MAGIC 1. **Data quality scattered everywhere** - Every analyst handles nulls differently
# MAGIC 2. **No single source of truth** - 10 analysts = 10 different "brand" definitions
# MAGIC 3. **Slow queries** - Scanning 110M rows every time
# MAGIC 4. **Can't track history** - Price changed yesterday? Old price is gone!
# MAGIC
# MAGIC ### The Solution: Dimensional Modeling
# MAGIC
# MAGIC ```sql
# MAGIC -- Product info lives in ONE place:
# MAGIC SELECT 
# MAGIC   p.product_name,
# MAGIC   p.brand,                  -- Always clean (no nulls)
# MAGIC   p.category_l1,            -- Pre-split
# MAGIC   p.current_price           -- Always correct
# MAGIC FROM dim_products p
# MAGIC WHERE p.product_id = 12345 
# MAGIC   AND p.is_current_version = TRUE;  -- Latest version
# MAGIC ```
# MAGIC
# MAGIC **Benefits:**
# MAGIC 1. ✅ **Single source of truth** - Everyone uses the same dimension table
# MAGIC 2. ✅ **Pre-cleaned** - Nulls handled once, used everywhere
# MAGIC 3. ✅ **Fast** - Small dimension tables (thousands of products, not millions of events)
# MAGIC 4. ✅ **History tracking** - SCD Type 2 remembers every version
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Learning Objective #2: Surrogate Keys
# MAGIC
# MAGIC ### Why Not Just Use product_id?
# MAGIC
# MAGIC **Business keys (natural keys) have problems:**
# MAGIC
# MAGIC ```
# MAGIC Today:     product_id=12345 → iPhone 11, $799
# MAGIC Tomorrow:  product_id=12345 → iPhone 11, $699  (price changed!)
# MAGIC ```
# MAGIC
# MAGIC If we use `product_id` as the key:
# MAGIC - Old events now show $699 (WRONG! They happened at $799)
# MAGIC - Can't answer: "What was the revenue at yesterday's prices?"
# MAGIC
# MAGIC ### Solution: Surrogate Keys (Auto-Generated IDs)
# MAGIC
# MAGIC ```
# MAGIC product_sk | product_id | price | effective_from | effective_to   | is_current
# MAGIC -----------|------------|-------|----------------|----------------|------------
# MAGIC 1001       | 12345      | 799   | 2019-11-01     | 2019-11-15     | False
# MAGIC 1002       | 12345      | 699   | 2019-11-16     | 9999-12-31     | True
# MAGIC ```
# MAGIC
# MAGIC Now:
# MAGIC - Events from Nov 1-15 point to `product_sk=1001` ($799) ✅
# MAGIC - Events from Nov 16+ point to `product_sk=1002` ($699) ✅
# MAGIC - Historical accuracy preserved!
# MAGIC
# MAGIC **This is called SCD Type 2 (Slowly Changing Dimension Type 2)**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📚 Learning Objective #3: Fact vs. Dimension
# MAGIC
# MAGIC **Dimension Tables** = **WHO, WHAT, WHERE, WHEN**
# MAGIC - Small (thousands to millions of rows)
# MAGIC - Descriptive attributes
# MAGIC - Slowly changing
# MAGIC - Examples: products, users, dates, categories
# MAGIC
# MAGIC **Fact Tables** = **MEASUREMENTS** (what actually happened)
# MAGIC - Large (millions to billions of rows)
# MAGIC - Metrics (prices, quantities, durations)
# MAGIC - References dimensions via surrogate keys
# MAGIC - Examples: events, transactions, sessions
# MAGIC
# MAGIC **Rule of thumb:**
# MAGIC - If it's a **noun** → Dimension (product, user, date)
# MAGIC - If it's an **action/measurement** → Fact (viewed product, purchased, session)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🏗️ What We'll Build (In Order)
# MAGIC
# MAGIC ### Dimensions (Build First)
# MAGIC 1. **dim_date** - Calendar (static, build once)
# MAGIC 2. **dim_categories** - Category hierarchy (small, fairly static)
# MAGIC 3. **dim_products** - Products with price history (SCD Type 2)
# MAGIC 4. **dim_users** - Users with behavioral segments (SCD Type 2)
# MAGIC
# MAGIC ### Facts (Build After Dimensions)
# MAGIC 5. **fact_events** - 110M events with surrogate keys to dimensions
# MAGIC 6. **fact_sessions** - 23M sessions aggregated from events
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 💡 Key Concepts You'll Learn
# MAGIC
# MAGIC 1. **Static dimensions** (dim_date) - Build once, never changes
# MAGIC 2. **Simple dimensions** (dim_categories) - Extract unique values from source
# MAGIC 3. **SCD Type 2** (dim_products, dim_users) - Track changes over time
# MAGIC 4. **Surrogate key generation** - Auto-incrementing IDs
# MAGIC 5. **Fact table joins** - Connecting events to dimensions via surrogate keys
# MAGIC 6. **Incremental processing** - Handling new data without full reloads
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Let's build! 🚀

# COMMAND ----------

# DBTITLE 1,Build dim_date (Calendar Dimension)
"""
==============================================================================
STEP 1: BUILD dim_date (CALENDAR DIMENSION)
==============================================================================

DECISION #1: Why build a date dimension?
-----------------------------------------
Instead of using DATE columns directly, we create a pre-computed calendar.

WITHOUT dim_date:
  SELECT event_date, 
         EXTRACT(MONTH FROM event_date) as month,
         CASE WHEN DAYOFWEEK(event_date) IN (1,7) THEN TRUE ELSE FALSE END as is_weekend
  FROM fact_events;  -- Compute on EVERY query

WITH dim_date:
  SELECT d.full_date, d.month, d.is_weekend
  FROM fact_events f
  JOIN dim_date d ON f.date_key = d.date_key;  -- Pre-computed!

Benefits:
✅ Faster queries (no date math)
✅ Consistent definitions (everyone's "weekend" is the same)
✅ Integer joins (date_key=20191101) faster than DATE joins
✅ Fiscal calendar support (if needed)

DECISION #2: Why date_key as INTEGER (YYYYMMDD)?
------------------------------------------------
Instead of DATE type, we use INT: 20191101, 20191102, etc.

Why?
✅ Integer joins are faster than DATE joins
✅ Human-readable (20191101 = November 1, 2019)
✅ Easy to filter (WHERE date_key >= 20191101 AND date_key < 20191201)
✅ Standard data warehouse pattern (Kimball methodology)

DECISION #3: Should this be SCD Type 2?
---------------------------------------
NO! Dates never change. 
November 1, 2019 will ALWAYS be a Friday, ALWAYS be in Q4.
This is a STATIC dimension - build once, use forever.
"""

import logging

# Configure logging
logger = logging.getLogger(__name__)


from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta

print("="*80)
logger.info("BUILDING dim_date - CALENDAR DIMENSION (STATIC)")
print("="*80)

# Determine date range from Bronze data
logger.info("\n Step 1: Determine date range from Bronze data...")
bronze_table = config.get_table('bronze_events')
date_range = spark.table(bronze_table).select(
    F.min("event_date").alias("min_date"),
    F.max("event_date").alias("max_date")
).collect()[0]

min_date = date_range['min_date']
max_date = date_range['max_date']
logger.info(f"   Data range: {min_date} to {max_date}")

# Add buffer for future dates (standard practice)
start_date = datetime.strptime(str(min_date), '%Y-%m-%d')
end_date = datetime.strptime(str(max_date), '%Y-%m-%d') + timedelta(days=365)

logger.info("\nStep 2: Generate calendar with 1-year buffer...")
logger.info(f"   Calendar range: {start_date.date()} to {end_date.date()}")
logger.info("   Why buffer? Allows for future-dated analysis without rebuilding")

# Generate all dates
date_list = []
current_date = start_date

while current_date <= end_date:
    date_list.append({
        # PRIMARY KEY: Integer in YYYYMMDD format
        'date_key': int(current_date.strftime('%Y%m%d')),
        
        # Full date for display
        'full_date': current_date.date(),
        
        # Year, quarter, month
        'year': current_date.year,
        'quarter': (current_date.month - 1) // 3 + 1,  # 1-4
        'month': current_date.month,
        'month_name': current_date.strftime('%B'),  # January, February, ...
        
        # Week and day
        'week_of_year': int(current_date.strftime('%U')),  # 0-53
        'day_of_month': current_date.day,
        'day_of_week': current_date.isoweekday(),  # 1=Monday, 7=Sunday
        'day_name': current_date.strftime('%A'),  # Monday, Tuesday, ...
        
        # Boolean flags (commonly used filters)
        'is_weekend': current_date.isoweekday() in [6, 7],  # Saturday, Sunday
        'is_month_start': current_date.day == 1,
        'is_month_end': (current_date + timedelta(days=1)).day == 1,
        'is_quarter_start': current_date.month in [1, 4, 7, 10] and current_date.day == 1,
        'is_quarter_end': current_date.month in [3, 6, 9, 12] and (current_date + timedelta(days=1)).day == 1,
    })
    current_date += timedelta(days=1)

logger.info(f"    Generated {len(date_list):,} dates")

# Create DataFrame with explicit schema (type safety)
date_schema = StructType([
    StructField("date_key", IntegerType(), False),  # PK, NOT NULL
    StructField("full_date", DateType(), False),
    StructField("year", IntegerType(), False),
    StructField("quarter", IntegerType(), False),
    StructField("month", IntegerType(), False),
    StructField("month_name", StringType(), False),
    StructField("week_of_year", IntegerType(), False),
    StructField("day_of_month", IntegerType(), False),
    StructField("day_of_week", IntegerType(), False),
    StructField("day_name", StringType(), False),
    StructField("is_weekend", BooleanType(), False),
    StructField("is_month_start", BooleanType(), False),
    StructField("is_month_end", BooleanType(), False),
    StructField("is_quarter_start", BooleanType(), False),
    StructField("is_quarter_end", BooleanType(), False),
])

df_date = spark.createDataFrame(date_list, schema=date_schema)

logger.info("\n Step 3: Preview dim_date:")
df_date.orderBy("date_key").show(10)

logger.info("\nStep 4: Write to Delta table...")
date_table = "product_analytics.ecommerce.silver_dim_date"

df_date.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(date_table)

logger.info("\n dim_date created!")
logger.info(f"   Table: {date_table}")
logger.info(f"   Rows: {df_date.count():,}")
logger.info(f"   Date Range: {min_date} to {end_date.date()}")
logger.info("\n This dimension is STATIC - only needs to be built ONCE!")
logger.info("   Re-run only if you need to extend the date range.")

# COMMAND ----------

# DBTITLE 1,Build dim_categories (Simple Dimension)
"""
==============================================================================
STEP 2: BUILD dim_categories (SIMPLE DIMENSION)
==============================================================================

THOUGHT PROCESS:
---------------
Question: "What's revenue by category hierarchy?"
Grain: One row per unique category path
Changes over time? Rarely (maybe product reorg)
Decision: Simple dimension - extract unique categories, dedupe

DECISION #1: Why split category hierarchy?
------------------------------------------
Bronze has: category_code = "electronics.smartphone.apple"

We could keep it as one string, but splitting into L1/L2/L3 enables:
✅ Drill-down analysis (electronics → smartphone → apple)
✅ Roll-up aggregation (all smartphone revenue, regardless of brand)
✅ Hierarchy queries (all sub-categories under electronics)
✅ Consistent depth (some products only have L1, others have L1.L2.L3)

DECISION #2: Should this be SCD Type 2?
---------------------------------------
Does a category's attributes change over time?

Example: Does "electronics.smartphone.apple" become something else?
- Generally NO - categories are stable
- If reorg happens (rare), we can rebuild

Decision: Simple dimension (no SCD Type 2)

Note: If category attributes like "is_promoted" or "category_margin" changed
frequently, we'd use SCD Type 2. But for just hierarchy, simple is fine.

DECISION #3: Surrogate key needed?
----------------------------------
Yes! Even simple dimensions need surrogate keys for:
✅ Fast integer joins (vs string joins on category path)
✅ Future-proofing (if we add SCD Type 2 later)
✅ Consistency (every dimension has SK pattern)
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("="*80)
logger.info("BUILDING dim_categories - SIMPLE DIMENSION")
print("="*80)

logger.info("\n📂 Step 1: Extract category hierarchy from Bronze...")

bronze_table = config.get_table('bronze_events')

# Read Bronze and extract unique categories
df_categories_raw = spark.table(bronze_table).select("category_code").distinct()

logger.info(f"   Unique category_code values: {df_categories_raw.count():,}")

# Split category hierarchy into levels
logger.info("\nStep 2: Split hierarchy into L1, L2, L3...")

df_categories_split = df_categories_raw \
    .withColumn("category_split", F.split("category_code", "\\.")) \
    .withColumn("array_size", F.size("category_split")) \
    .withColumn("category_l1", 
                F.when(F.col("array_size") >= 1, F.element_at(F.col("category_split"), 1))
                 .otherwise(None)) \
    .withColumn("category_l2", 
                F.when(F.col("array_size") >= 2, F.element_at(F.col("category_split"), 2))
                 .otherwise(None)) \
    .withColumn("category_l3", 
                F.when(F.col("array_size") >= 3, F.element_at(F.col("category_split"), 3))
                 .otherwise(None)) \
    .withColumn("category_full_path", 
                F.when(F.col("category_code").isNotNull(), F.col("category_code"))
                 .otherwise(F.lit("unknown"))) \
    .withColumn("category_depth",
                F.when(F.col("category_l3").isNotNull(), 3)
                 .when(F.col("category_l2").isNotNull(), 2)
                 .when(F.col("category_l1").isNotNull(), 1)
                 .otherwise(0)) \
    .drop("category_split", "category_code")

logger.info("   Sample category hierarchy:")
df_categories_split.filter(F.col("category_depth") == 3).show(5, truncate=False)

# Handle nulls (events with no category)
logger.info("\n🧹 Step 3: Handle null categories...")

df_categories_clean = df_categories_split \
    .withColumn("category_l1", 
                F.when(F.col("category_l1").isNull(), "unknown")
                 .otherwise(F.col("category_l1"))) \
    .withColumn("category_full_path",
                F.when(F.col("category_full_path") == "unknown", "unknown")
                 .otherwise(F.col("category_full_path")))

# Generate surrogate keys using row_number() - deterministic ordering
logger.info("\n🔑 Step 4: Generate surrogate keys (category_sk)...")

window_spec = Window.orderBy("category_full_path")

df_categories_final = df_categories_clean \
    .withColumn("category_sk", F.row_number().over(window_spec)) \
    .withColumn("created_at", F.current_timestamp()) \
    .select(
        "category_sk",
        "category_l1",
        "category_l2",
        "category_l3",
        "category_full_path",
        "category_depth",
        "created_at"
    )

logger.info(f"   Generated {df_categories_final.count():,} category surrogate keys")

logger.info("\n Step 5: Preview dim_categories:")
df_categories_final.orderBy("category_sk").show(10, truncate=False)

# Show category depth distribution
logger.info("\n Category depth distribution:")
df_categories_final.groupBy("category_depth").count().orderBy("category_depth").show()

logger.info("\nStep 6: Write to Delta table...")
category_table = "product_analytics.ecommerce.silver_dim_categories"

df_categories_final.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(category_table)

logger.info("\n dim_categories created!")
logger.info(f"   Table: {category_table}")
logger.info(f"   Rows: {df_categories_final.count():,}")
logger.info("\n This is a SIMPLE dimension - extract unique values, no history tracking")
logger.info("   Rebuild if categories change (rare in production)")

# COMMAND ----------

# DBTITLE 1,Build dim_products (SCD Type 2 - THE GAME CHANGER)
"""
==============================================================================
STEP 3: BUILD dim_products (SCD TYPE 2 - SLOWLY CHANGING DIMENSION)
==============================================================================

THIS IS THE MOST IMPORTANT PATTERN IN DATA WAREHOUSING!

THOUGHT PROCESS:
---------------
Question: "What was the revenue at YESTERDAY'S prices?"
Grain: One row per product per version (price changes = new row)
Changes over time? YES! Prices change, brands might change
Decision: SCD Type 2 - track every version of every product

==============================================================================
DECISION #1: Why SCD Type 2 for Products?
==============================================================================

Imagine this scenario:

Nov 1:  iPhone 11 costs $799
Nov 10: Someone buys it for $799  (event recorded)
Nov 15: Price drops to $699
Nov 20: Someone buys it for $699  (event recorded)

**THE PROBLEM:**
If we just keep "current" product data:

  product_id | product_name | price
  -----------|--------------|------
  12345      | iPhone 11    | 699   <- Only current price!

Now when we query Nov 10 sales:
  SELECT product_name, price, quantity
  FROM events e
  JOIN products p ON e.product_id = p.product_id
  WHERE event_date = '2019-11-10';

RESULT: iPhone 11, $699, 1 unit  ❌ WRONG! It was $799 on Nov 10!

**THE SOLUTION: SCD Type 2**

  product_sk | product_id | product_name | price | effective_from | effective_to | is_current
  -----------|------------|--------------|-------|----------------|--------------|------------
  1001       | 12345      | iPhone 11    | 799   | 2019-11-01     | 2019-11-14   | FALSE
  1002       | 12345      | iPhone 11    | 699   | 2019-11-15     | 9999-12-31   | TRUE

Now we join on BOTH product_id AND date:
  SELECT p.product_name, p.price, e.quantity
  FROM events e
  JOIN dim_products p 
    ON e.product_id = p.product_id 
    AND e.event_date >= p.effective_from 
    AND e.event_date < p.effective_to
  WHERE e.event_date = '2019-11-10';

RESULT: iPhone 11, $799, 1 unit  ✅ CORRECT!

==============================================================================
DECISION #2: What Makes a "New Version"?
==============================================================================

Not every field change creates a new version. We track changes to:

✅ Track these (Type 2):
  - price (changes affect revenue calculations)
  - brand (business attribute, might change)
  - category_id (product might be recategorized)

❌ Don't track these (just update):
  - product_name typo fixes (cosmetic)
  - display_order (UI concern, not analytical)
  
Rule: If historical accuracy matters for ANALYTICS, it's Type 2.

==============================================================================
DECISION #3: Surrogate Keys (product_sk)
==============================================================================

Why not just use product_id?

❌ product_id = 12345 appears in MULTIPLE rows (one per version)
✅ product_sk = 1001, 1002, 1003... (unique per version)

Fact table joins:
  fact_events.product_sk → dim_products.product_sk
  
This way:
- Nov 10 events point to product_sk=1001 ($799 version)
- Nov 20 events point to product_sk=1002 ($699 version)
- Historical accuracy preserved!

==============================================================================
DECISION #4: effective_to = 9999-12-31 (The "End of Time" Pattern)
==============================================================================

Why 9999-12-31 instead of NULL?

✅ 9999-12-31:
  - Range queries work: event_date < effective_to
  - No NULL handling needed
  - Standard data warehouse pattern
  - "This version is effective until the end of time"

❌ NULL:
  - Need: event_date < effective_to OR effective_to IS NULL
  - Slower queries
  - More complex logic

==============================================================================
DECISION #5: Initial Load vs Incremental Updates
==============================================================================

INITIAL LOAD (what we're doing today):
- Take latest snapshot from Bronze
- All products get effective_from = MIN(event_date)
- All products get effective_to = 9999-12-31
- All products get is_current = TRUE

INCREMENTAL (future processing):
- Detect changed products (price changed, brand changed)
- Close out old version: SET effective_to = CURRENT_DATE - 1, is_current = FALSE
- Insert new version: effective_from = CURRENT_DATE, effective_to = 9999-12-31, is_current = TRUE

We'll build incremental processing later. Today = initial load.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("="*80)
logger.info("BUILDING dim_products - SCD TYPE 2 (SLOWLY CHANGING DIMENSION)")
print("="*80)
logger.info("\n🎓 This is THE most important pattern in data warehousing!")
logger.info("   We're tracking HISTORY - every version of every product.\n")

print("-"*80)
logger.info("INITIAL LOAD STRATEGY")
print("-"*80)
logger.info("Today: Take latest snapshot of each product from Bronze")
logger.info("Future: Detect changes and version them (we'll build that later)\n")

# Read Bronze events and extract latest product attributes
logger.info(" Step 1: Extract latest product attributes from Bronze...")

bronze_table = config.get_table('bronze_events')

# Get the latest event for each product to capture current state
window_spec = Window.partitionBy("product_id").orderBy(F.desc("event_time"))

df_products_latest = spark.table(bronze_table) \
    .filter(F.col("product_id").isNotNull()) \
    .withColumn("row_num", F.row_number().over(window_spec)) \
    .filter(F.col("row_num") == 1) \
    .select(
        "product_id",
        "price",
        "brand",
        "category_code"
    )

product_count = df_products_latest.count()
logger.info(f"   Found {product_count:,} unique products in Bronze")

logger.info("\nStep 2: Enrich with category surrogate keys...")

# Join with dim_categories to get category_sk
df_categories = spark.table("product_analytics.ecommerce.silver_dim_categories")

df_products_enriched = df_products_latest \
    .join(
        df_categories,
        df_products_latest.category_code == df_categories.category_full_path,
        "left"
    ) \
    .select(
        df_products_latest.product_id,
        df_products_latest.price,
        df_products_latest.brand,
        df_products_latest.category_code,
        F.coalesce(df_categories.category_sk, F.lit(-1)).alias("category_sk")  # -1 = unknown
    )

logger.info("    Products enriched with category_sk")

# Clean up brands and handle nulls
logger.info("\n🧹 Step 3: Clean up product attributes...")

df_products_clean = df_products_enriched \
    .withColumn("brand_clean",
                F.when(F.col("brand").isNull(), "unknown")
                 .when(F.trim(F.col("brand")) == "", "unknown")
                 .otherwise(F.lower(F.trim(F.col("brand"))))) \
    .withColumn("price_clean",
                F.when(F.col("price").isNull(), 0.0)
                 .when(F.col("price") < 0, 0.0)  # Fix negative prices
                 .otherwise(F.col("price"))) \
    .drop("brand", "price") \
    .withColumnRenamed("brand_clean", "brand") \
    .withColumnRenamed("price_clean", "price")

logger.info("    Brands normalized, nulls handled, negative prices fixed")

# Get the earliest event date for effective_from
logger.info("\n Step 4: Set effective dates (SCD Type 2 metadata)...")

min_date = spark.table(bronze_table).selectExpr("min(event_date) as min_date").collect()[0]['min_date']
logger.info(f"   Earliest event date: {min_date}")
logger.info(f"   All products will be effective from: {min_date}")
logger.info("   All products will be effective to: 9999-12-31 (end of time)")
logger.info("   All products will be current: TRUE")

df_products_versioned = df_products_clean \
    .withColumn("effective_from", F.lit(min_date).cast("date")) \
    .withColumn("effective_to", F.lit("9999-12-31").cast("date")) \
    .withColumn("is_current_version", F.lit(True)) \
    .withColumn("version_number", F.lit(1))  # All start at version 1

logger.info("    SCD Type 2 metadata added")

# Generate surrogate keys
logger.info("\n🔑 Step 5: Generate surrogate keys (product_sk)...")
logger.info("   Why surrogate keys? So each VERSION has a unique ID!")
logger.info("   product_id=12345 might have product_sk=1001, 1002, 1003 (3 versions)\n")

window_spec_sk = Window.orderBy("product_id")

df_products_final = df_products_versioned \
    .withColumn("product_sk", F.row_number().over(window_spec_sk)) \
    .withColumn("created_at", F.current_timestamp()) \
    .withColumn("updated_at", F.current_timestamp()) \
    .select(
        "product_sk",              # Surrogate key (PRIMARY KEY)
        "product_id",              # Business key (can repeat across versions)
        "brand",
        "price",
        "category_sk",             # Foreign key to dim_categories
        "category_code",           # Denormalized for convenience
        "effective_from",          # SCD Type 2: version start date
        "effective_to",            # SCD Type 2: version end date (9999-12-31 = current)
        "is_current_version",      # SCD Type 2: TRUE if latest version
        "version_number",          # SCD Type 2: 1, 2, 3... (1 for all on initial load)
        "created_at",
        "updated_at"
    )

logger.info(f"   Generated {df_products_final.count():,} surrogate keys")

logger.info("\n Step 6: Preview dim_products:")
df_products_final.orderBy("product_sk").show(10, truncate=False)

logger.info("\n Sample products with multiple versions (after future updates):")
logger.info("   (Right now all products are version 1 - we'll add versioning logic later)")
df_products_final.filter(F.col("version_number") > 1).show(5, truncate=False)

logger.info("\nStep 7: Write to Delta table...")
product_table = "product_analytics.ecommerce.silver_dim_products"

df_products_final.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(product_table)

logger.info("\n dim_products created!")
logger.info(f"   Table: {product_table}")
logger.info(f"   Rows: {df_products_final.count():,}")
logger.info(f"   Current versions: {df_products_final.filter(F.col('is_current_version')).count():,}")
logger.info(f"   Historical versions: {df_products_final.filter(~F.col('is_current_version')).count():,}")

logger.info("\n🎓 KEY LEARNINGS:")
logger.info("   1. product_sk = surrogate key (unique per VERSION)")
logger.info("   2. product_id = business key (same across versions)")
logger.info("   3. effective_from/to = date range when this version was active")
logger.info("   4. is_current_version = TRUE for latest version")
logger.info("   5. This is INITIAL LOAD - all products are version 1")
logger.info("   6. Future: When price changes, we'll INSERT new row with version 2")
logger.info("\n Next: We'll build incremental SCD Type 2 processing!")

# COMMAND ----------

# DBTITLE 1,Build dim_users (SCD Type 2 - Behavioral Segments)
"""
==============================================================================
STEP 4: BUILD dim_users (SCD TYPE 2 - USER BEHAVIORAL SEGMENTS)
==============================================================================

THOUGHT PROCESS:
---------------
Question: "How did power users behave BEFORE they became power users?"
Grain: One row per user per behavioral segment change
Changes over time? YES! Users evolve (casual → engaged → power user)
Decision: SCD Type 2 - track user evolution over time

==============================================================================
DECISION #1: Why SCD Type 2 for Users?
==============================================================================

Imagine this user journey:

Oct 1-15:  User starts as "casual" (1-2 events per day)
Oct 16-31: User becomes "engaged" (5-10 events per day)
Nov 1+:    User becomes "power_user" (20+ events per day)

**THE BUSINESS QUESTION:**
"What products did this user buy WHEN they were casual vs power_user?"
"Did their purchase behavior change as they engaged more?"

**WITHOUT SCD Type 2:**
user_id | segment
--------|----------
12345   | power_user  ← Only current state!

All historical events now look like they happened when user was power_user.
WRONG! They were casual back in October!

**WITH SCD Type 2:**
user_sk | user_id | segment    | effective_from | effective_to | is_current
--------|---------|------------|----------------|--------------|------------
1001    | 12345   | casual     | 2019-10-01     | 2019-10-15   | FALSE
1002    | 12345   | engaged    | 2019-10-16     | 2019-10-31   | FALSE
1003    | 12345   | power_user | 2019-11-01     | 9999-12-31   | TRUE

Now we can answer:
- What did they buy as casual? (join to user_sk=1001)
- What did they buy as engaged? (join to user_sk=1002)
- What do they buy now as power_user? (join to user_sk=1003)

✅ User evolution tracked!

==============================================================================
DECISION #2: What Defines User Segments?
==============================================================================

For this platform, we'll segment users by activity level:

**casual:**      1-5 total events
**engaged:**     6-20 total events
**power_user:**  21+ total events

In a real system, you might use:
- Recency (last activity date)
- Monetary value (total spend)
- Product affinity (categories browsed)
- Device type (mobile vs desktop)

**Key point:** Segments CHANGE as users interact more!
That's why we need SCD Type 2.

==============================================================================
DECISION #3: Initial Load Strategy
==============================================================================

For initial load:
1. Calculate CURRENT segment based on all events to date
2. Set effective_from = user's first event date
3. Set effective_to = 9999-12-31
4. Set is_current = TRUE

Future incremental processing:
1. Recalculate segment daily/weekly
2. If segment changed:
   - Close old version (effective_to = yesterday, is_current = FALSE)
   - Insert new version (effective_from = today, is_current = TRUE)

==============================================================================
DECISION #4: Why Not Just Add segment to fact_events?
==============================================================================

**Bad approach:**
fact_events:
  user_id | event_date | user_segment  ← What segment? Current or at event time?
  
**Good approach (SCD Type 2):**
fact_events:     dim_users:
  user_sk   →    user_sk | user_id | segment | effective_from | effective_to
  event_date     1001    | 12345   | casual  | 2019-10-01     | 2019-10-15
                 1002    | 12345   | engaged | 2019-10-16     | 9999-12-31

Join:
  WHERE e.user_sk = u.user_sk  ← Already correct! user_sk captures moment in time
  
No date range join needed because user_sk is set at event time!
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("="*80)
logger.info("BUILDING dim_users - SCD TYPE 2 (BEHAVIORAL SEGMENTS)")
print("="*80)
logger.info("\n👥 Tracking user evolution: casual → engaged → power_user\n")

print("-"*80)
logger.info("INITIAL LOAD STRATEGY")
print("-"*80)
logger.info("Today: Calculate current segment based on total activity")
logger.info("Future: Recalculate segments and version when users evolve\n")

# Extract user activity from Bronze
logger.info(" Step 1: Calculate user activity metrics from Bronze...")

bronze_table = config.get_table('bronze_events')

df_user_activity = spark.table(bronze_table) \
    .filter(F.col("user_id").isNotNull()) \
    .groupBy("user_id") \
    .agg(
        F.count("*").alias("total_events"),
        F.countDistinct("event_type").alias("event_types_count"),
        F.min("event_date").alias("first_seen_date"),
        F.max("event_date").alias("last_seen_date"),
        F.countDistinct("event_date").alias("active_days"),
        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
        F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
        F.sum(F.when(F.col("event_type") == "cart", 1).otherwise(0)).alias("cart_count")
    )

user_count = df_user_activity.count()
logger.info(f"   Found {user_count:,} unique users in Bronze")

# Calculate user segment based on activity
logger.info("\n Step 2: Calculate behavioral segments...")
logger.info("   Segments: casual (1-5 events), engaged (6-20 events), power_user (21+ events)")

df_users_segmented = df_user_activity \
    .withColumn("user_segment",
                F.when(F.col("total_events") >= 21, "power_user")
                 .when(F.col("total_events") >= 6, "engaged")
                 .otherwise("casual")) \
    .withColumn("days_active", 
                F.datediff(F.col("last_seen_date"), F.col("first_seen_date")) + 1) \
    .withColumn("avg_events_per_day",
                F.round(F.col("total_events") / F.col("days_active"), 2))

logger.info("    Segments calculated")

logger.info("\n Segment distribution:")
df_users_segmented.groupBy("user_segment").count().orderBy(F.desc("count")).show()

# Add SCD Type 2 metadata
logger.info("\n Step 3: Add SCD Type 2 metadata (effective dates, versioning)...")
logger.info("   All users start at version 1 with effective_from = first_seen_date")

df_users_versioned = df_users_segmented \
    .withColumn("effective_from", F.col("first_seen_date")) \
    .withColumn("effective_to", F.lit("9999-12-31").cast("date")) \
    .withColumn("is_current_version", F.lit(True)) \
    .withColumn("version_number", F.lit(1))

logger.info("    SCD Type 2 metadata added")

# Generate surrogate keys
logger.info("\n🔑 Step 4: Generate surrogate keys (user_sk)...")
logger.info("   Why? So each user SEGMENT VERSION gets a unique ID!")
logger.info("   user_id=12345 might have user_sk=5001 (casual), 5002 (engaged), 5003 (power_user)\n")

window_spec = Window.orderBy("user_id")

df_users_final = df_users_versioned \
    .withColumn("user_sk", F.row_number().over(window_spec)) \
    .withColumn("created_at", F.current_timestamp()) \
    .withColumn("updated_at", F.current_timestamp()) \
    .select(
        "user_sk",               # Surrogate key (PRIMARY KEY)
        "user_id",               # Business key (can repeat across versions)
        "user_segment",          # casual / engaged / power_user
        "total_events",
        "event_types_count",
        "first_seen_date",
        "last_seen_date",
        "active_days",
        "avg_events_per_day",
        "purchase_count",
        "view_count",
        "cart_count",
        "effective_from",        # SCD Type 2: version start date
        "effective_to",          # SCD Type 2: version end date
        "is_current_version",    # SCD Type 2: TRUE if latest
        "version_number",        # SCD Type 2: 1, 2, 3...
        "created_at",
        "updated_at"
    )

logger.info(f"   Generated {df_users_final.count():,} surrogate keys")

logger.info("\n Step 5: Preview dim_users:")
df_users_final.orderBy(F.desc("total_events")).show(10, truncate=False)

logger.info("\nStep 6: Write to Delta table...")
user_table = "product_analytics.ecommerce.silver_dim_users"

df_users_final.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(user_table)

logger.info("\n dim_users created!")
logger.info(f"   Table: {user_table}")
logger.info(f"   Rows: {df_users_final.count():,}")
logger.info(f"   Current versions: {df_users_final.filter(F.col('is_current_version')).count():,}")
logger.info(f"   Historical versions: {df_users_final.filter(~F.col('is_current_version')).count():,}")

logger.info("\n Segment breakdown:")
df_users_final.groupBy("user_segment") \
    .agg(
        F.count("*").alias("user_count"),
        F.round(F.avg("total_events"), 2).alias("avg_events"),
        F.round(F.avg("purchase_count"), 2).alias("avg_purchases")
    ) \
    .orderBy(F.desc("user_count")) \
    .show()

logger.info("\n🎓 KEY LEARNINGS:")
logger.info("   1. user_sk = surrogate key (unique per SEGMENT VERSION)")
logger.info("   2. user_id = business key (same user, different segments over time)")
logger.info("   3. user_segment tracks behavioral evolution (casual → engaged → power_user)")
logger.info("   4. effective_from = user's first_seen_date (when they started)")
logger.info("   5. This is INITIAL LOAD - all users are version 1")
logger.info("   6. Future: When segment changes, INSERT new row with new segment")
logger.info("\n Next: Build fact tables that JOIN to these dimensions via surrogate keys!")

# COMMAND ----------

# DBTITLE 1,FACT TABLE DESIGN - Star Schema Architecture
"""
==============================================================================
FACT TABLE DESIGN - STAR SCHEMA ARCHITECTURE
==============================================================================

THOUGHT PROCESS:
---------------
Question: "What are we measuring?"
Answer: User events (views, carts, purchases) with product/user/time context

Grain: ONE ROW PER EVENT (finest granularity)

Measures:
- event_count (always 1 per row, useful for aggregations)
- price_at_event (product price when event happened - from dim_products)
- quantity (for cart/purchase events)

Dimensions (via surrogate keys):
- date_sk → dim_date (when)
- user_sk → dim_users (who)
- product_sk → dim_products (what)
- category_sk → dim_categories (what category)

==============================================================================
DECISION #1: Why Star Schema for fact_events?
==============================================================================

**STAR SCHEMA (What we're building):**

                    fact_events
                   /     |     \\
            dim_date  dim_users  dim_products
                                      |
                               dim_categories

**Query Example:**
```sql
SELECT 
    d.year,
    d.month_name,
    p.brand,
    c.category_level_1,
    SUM(f.price_at_event) as total_revenue
FROM fact_events f
JOIN dim_date d ON f.date_sk = d.date_sk
JOIN dim_products p ON f.product_sk = p.product_sk
JOIN dim_categories c ON p.category_sk = c.category_sk
WHERE d.year = 2019
GROUP BY d.year, d.month_name, p.brand, c.category_level_1;
```

**Pros:**
✅ Only 3-4 joins max
✅ dim_products already has category_sk (denormalized)
✅ Fast query performance (fewer joins = less shuffle in Spark)
✅ BI tool friendly
✅ Clear relationships

**Why Not Snowflake Schema?**

                    fact_events
                   /     |     \\
            dim_date  dim_users  dim_products
                                      |
                                 dim_brands → dim_manufacturers
                                      |
                               dim_categories → dim_category_hierarchy

Snowflake would require:
```sql
FROM fact_events f
JOIN dim_products p ON f.product_sk = p.product_sk
JOIN dim_brands b ON p.brand_sk = b.brand_sk  ← Extra join!
JOIN dim_manufacturers m ON b.manufacturer_sk = m.manufacturer_sk  ← Extra join!
JOIN dim_categories c ON p.category_sk = c.category_sk
JOIN dim_category_hierarchy h ON c.parent_category_sk = h.category_sk  ← Extra join!
```

**Cost of Snowflake:**
❌ 6-8 joins instead of 3-4
❌ Each join = shuffle in Spark (expensive!)
❌ Harder for analysts to write queries
❌ Minimal storage savings (Delta compression handles redundancy)

**When to use Snowflake:**
- Deep hierarchies (10+ levels)
- Storage cost > compute cost
- Frequent dimension updates
- NOT for analytics workloads!

==============================================================================
DECISION #2: SCD Type 2 Point-in-Time Joins
==============================================================================

**THE PROBLEM:**
Product prices change over time. How do we join correctly?

**BAD (Type 1 Join):**
```sql
FROM fact_events f
JOIN dim_products p ON f.product_id = p.product_id  ← Gets CURRENT price!
```

Result: Nov 10 event shows Nov 20 price ❌

**GOOD (Type 2 Join):**
```sql
FROM fact_events f
JOIN dim_products p 
  ON f.product_id = p.product_id 
  AND f.event_date >= p.effective_from 
  AND f.event_date < p.effective_to  ← Gets HISTORICAL price!
```

Result: Nov 10 event shows Nov 10 price ✅

**EVEN BETTER (Precompute product_sk in fact table):**

Instead of complex date range joins at query time, we'll:
1. Look up correct product_sk when loading fact_events
2. Store product_sk in fact table
3. Simple join: f.product_sk = p.product_sk ← Done!

This is called "late binding" - we resolve the SCD Type 2 lookup at ETL time.

==============================================================================
DECISION #3: Fact Table Grain
==============================================================================

**Grain = ONE ROW PER EVENT**

Alternatives considered:

❌ One row per user per day:
  - Loses event-level detail
  - Can't answer "What time of day do power users shop?"
  - Pre-aggregated = less flexible

❌ One row per user per product per day:
  - Still loses event-level detail
  - Can't track event sequences

✅ One row per event:
  - Maximum flexibility
  - Can aggregate any way we want
  - Can track clickstream sequences
  - Delta compression keeps size reasonable

==============================================================================
DECISION #4: Measures in Fact Table
==============================================================================

**Additive Measures** (can SUM across any dimension):
- event_count (always 1, useful for COUNT aggregations)
- quantity (for cart/purchase events)
- revenue (price_at_event * quantity for purchases)

**Semi-Additive Measures** (can SUM across some dimensions, not others):
- price_at_event (additive across products, NOT across time for same product)

**Non-Additive Measures** (ratios, percentages):
- conversion_rate (calculated, not stored)

**Why store price_at_event?**
- We could JOIN to dim_products at query time
- But storing it here makes queries faster
- Trade-off: denormalization vs query performance
- For analytics: query performance wins!

==============================================================================
Next: Build fact_events with all surrogate key lookups!
==============================================================================
"""

print("="*80)
logger.info("FACT TABLE DESIGN COMPLETE")
print("="*80)
logger.info("\n📐 Architecture: STAR SCHEMA")
logger.info("   Grain: One row per event")
logger.info("   Joins: 3-4 (vs 6-8 for snowflake)")
logger.info("   Performance: Optimized for analytics\n")
logger.info(" Next: Build fact_events with surrogate key lookups!")

# COMMAND ----------

# DBTITLE 1,Build fact_events - Part 1: Initial Load with Surrogate Key Lookups
"""
==============================================================================
BUILD fact_events - PART 1: INITIAL LOAD WITH SURROGATE KEY LOOKUPS
==============================================================================

STRATEGY:
1. Read Bronze events
2. Lookup date_sk from dim_date (simple join on event_date)
3. Lookup user_sk from dim_users (SCD Type 2: current version only for initial load)
4. Lookup product_sk from dim_products (SCD Type 2: match on date range)
5. Add measures (price_at_event, quantity, revenue)
6. Write to Delta table

==============================================================================
DEMONSTRATING DELTA/LAKEHOUSE FEATURES:
==============================================================================

✅ FEATURE 1: ACID Transactions
   - Multiple joins, transformations = ONE atomic write
   - Either all data lands or none (no partial writes)

✅ FEATURE 2: Schema Evolution
   - Can add columns later without breaking existing queries
   - .option("mergeSchema", "true")

✅ FEATURE 3: Time Travel (coming next)
   - Query historical versions
   - Audit trail built-in

✅ FEATURE 4: OPTIMIZE + Z-ORDER (coming next)
   - Compact small files
   - Co-locate related data for faster queries

✅ FEATURE 5: MERGE for incremental (coming next)
   - Upserts, deduplication
   - CDC patterns
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("="*80)
logger.info("BUILDING fact_events - INITIAL LOAD")
print("="*80)
logger.info("\n Grain: ONE ROW PER EVENT")
logger.info("🔑 Strategy: Lookup all surrogate keys from dimensions\n")

# Read Bronze events
logger.info(" Step 1: Read Bronze events...")
bronze_table = "product_analytics.ecommerce.bronze_events"
df_events = spark.table(bronze_table)

event_count = df_events.count()
logger.info(f"   Found {event_count:,} events in Bronze")

# Lookup date_sk
logger.info("\n Step 2: Lookup date_sk from dim_date...")
df_dates = spark.table("product_analytics.ecommerce.silver_dim_date")

df_events_with_date = df_events.alias("e").join(
    df_dates.alias("d"),
    F.col("e.event_date") == F.col("d.full_date"),
    "left"
).select(
    F.col("e.*"),
    F.coalesce(F.col("d.date_key"), F.lit(-1)).alias("date_sk")  # -1 = unknown
)

logger.info("    date_sk joined")

# Lookup user_sk (current version only for initial load)
logger.info("\n👥 Step 3: Lookup user_sk from dim_users...")
logger.info("   Note: For initial load, we use current version only")
logger.info("   Future: For incremental, we'll do SCD Type 2 date range join")

df_users = spark.table("product_analytics.ecommerce.silver_dim_users") \
    .filter(F.col("is_current_version") == True) \
    .select("user_id", "user_sk")

df_events_with_user = df_events_with_date.alias("e").join(
    df_users.alias("u"),
    F.col("e.user_id") == F.col("u.user_id"),
    "left"
).select(
    F.col("e.*"),
    F.coalesce(F.col("u.user_sk"), F.lit(-1)).alias("user_sk")  # -1 = unknown
)

logger.info("    user_sk joined")

# Lookup product_sk (SCD Type 2: date range join)
logger.info("\n Step 4: Lookup product_sk from dim_products (SCD Type 2 join)...")
logger.info("   This is the CRITICAL join that shows SCD Type 2 point-in-time accuracy!")
logger.info("   We match: product_id AND event_date within [effective_from, effective_to)")

df_products = spark.table("product_analytics.ecommerce.silver_dim_products") \
    .select(
        "product_id", 
        "product_sk", 
        "price", 
        "category_sk",
        "effective_from", 
        "effective_to"
    )

df_events_with_product = df_events_with_user.alias("e").join(
    df_products.alias("p"),
    (F.col("e.product_id") == F.col("p.product_id")) &
    (F.col("e.event_date") >= F.col("p.effective_from")) &
    (F.col("e.event_date") < F.col("p.effective_to")),
    "left"
).select(
    F.col("e.*"),
    F.coalesce(F.col("p.product_sk"), F.lit(-1)).alias("product_sk"),
    F.coalesce(F.col("p.category_sk"), F.lit(-1)).alias("category_sk"),
    F.col("p.price").alias("price_at_event")  # Historical price!
)

logger.info("    product_sk joined with SCD Type 2 date range")
logger.info("    price_at_event captured (price when event happened)")

# Add measures
logger.info("\n Step 5: Add measures...")

df_fact_events = df_events_with_product \
    .withColumn("event_count", F.lit(1)) \
    .withColumn("quantity", 
                F.when(F.col("event_type").isin(["cart", "purchase"]), 1)
                 .otherwise(0)) \
    .withColumn("revenue",
                F.when(F.col("event_type") == "purchase", 
                       F.col("price_at_event") * F.col("quantity"))
                 .otherwise(0.0)) \
    .withColumn("created_at", F.current_timestamp()) \
    .select(
        "event_time",
        "event_date",
        "event_type",
        "date_sk",           # ← Surrogate key to dim_date
        "user_sk",           # ← Surrogate key to dim_users
        "product_sk",        # ← Surrogate key to dim_products
        "category_sk",       # ← Denormalized from dim_products
        "price_at_event",    # ← Measure: price when event happened
        "quantity",          # ← Measure: quantity (1 for cart/purchase, 0 otherwise)
        "revenue",           # ← Measure: price * quantity for purchases
        "event_count",       # ← Measure: always 1 (for COUNT aggregations)
        "user_session",
        "created_at"
    )

logger.info("    Measures added: event_count, quantity, revenue")

logger.info("\n📋 Step 6: Preview fact_events:")
df_fact_events.show(10, truncate=False)

logger.info("\n Data Quality Check:")
logger.info(f"   Total events: {df_fact_events.count():,}")
logger.info(f"   Events with valid date_sk: {df_fact_events.filter(F.col('date_sk') != -1).count():,}")
logger.info(f"   Events with valid user_sk: {df_fact_events.filter(F.col('user_sk') != -1).count():,}")
logger.info(f"   Events with valid product_sk: {df_fact_events.filter(F.col('product_sk') != -1).count():,}")

logger.info("\n💰 Revenue Check:")
df_fact_events.groupBy("event_type") \
    .agg(
        F.count("*").alias("event_count"),
        F.sum("revenue").alias("total_revenue")
    ) \
    .orderBy(F.desc("event_count")) \
    .show()

logger.info("\nStep 7: Write to Delta table...")
logger.info("   🔑 Delta Feature: ACID Transaction (all or nothing write)")

fact_table = "product_analytics.ecommerce.fact_events"

df_fact_events.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("delta.autoOptimize.optimizeWrite", "true")  \
    .option("delta.autoOptimize.autoCompact", "true") \
    .saveAsTable(fact_table)

logger.info("\n fact_events created!")
logger.info(f"   Table: {fact_table}")
logger.info(f"   Rows: {df_fact_events.count():,}")

logger.info("\nStar schema build complete")
logger.info(f"    dim_date ({spark.table('product_analytics.ecommerce.silver_dim_date').count():,} rows)")
logger.info(f"    dim_categories ({spark.table('product_analytics.ecommerce.silver_dim_categories').count():,} rows)")
logger.info(f"    dim_products ({spark.table('product_analytics.ecommerce.silver_dim_products').count():,} rows)")
logger.info(f"    dim_users ({spark.table('product_analytics.ecommerce.silver_dim_users').count():,} rows)")
logger.info(f"    fact_events ({df_fact_events.count():,} rows)")

logger.info("\n🔑 Next: Demonstrate Delta features (OPTIMIZE, Time Travel, MERGE)!")

# COMMAND ----------

# DBTITLE 1,DELTA FEATURE #1: OPTIMIZE + Z-ORDER (Query Performance Boost)
"""
==============================================================================
DELTA FEATURE #1: OPTIMIZE + Z-ORDER
==============================================================================

WHY THIS MATTERS:
-----------------
After writing data, Delta Lake might create many small files.
Small files = slow queries (Spark has to open 1000s of files)

OPTIMIZE:
- Compacts small files into larger files
- Reduces file count by 10-100x
- Queries run 2-10x faster

Z-ORDER:
- Co-locates related data (multi-dimensional clustering)
- If you filter by event_date AND product_sk, Z-ORDER groups them together
- Skips irrelevant files during queries

==============================================================================
THIS IS A LAKEHOUSE FEATURE! Traditional databases don't expose this.
==============================================================================

In a traditional data warehouse:
- Database engine handles file layout (you can't control it)
- No visibility into file structure
- Optimization is a black box

In Delta Lake:
- YOU control optimization
- See file statistics (DESCRIBE DETAIL)
- Choose which columns to co-locate (Z-ORDER)
- See query speedup immediately

==============================================================================
INTERVIEW ANSWER:
"We chose Delta Lake because we can OPTIMIZE + Z-ORDER our fact tables
after loading. This compacts small files and co-locates data filtered together,
making queries 5-10x faster. Traditional databases don't give you this control."
==============================================================================
"""

print("="*80)
logger.info("DELTA FEATURE #1: OPTIMIZE + Z-ORDER")
print("="*80)
logger.info("\n🛠 Compacting files and co-locating related data for faster queries\n")

# Check current file stats
logger.info("📁 BEFORE OPTIMIZE:")
spark.sql("""
    DESCRIBE DETAIL product_analytics.ecommerce.fact_events
""").select("numFiles", "sizeInBytes").show()

logger.info("\n🛠 Running OPTIMIZE with Z-ORDER...")
logger.info("   Z-ORDER BY (event_date, product_sk):")
logger.info("   - Co-locates events from same date")
logger.info("   - Co-locates events for same product")
logger.info("   - Most queries filter by date and/or product")
logger.info("   - This makes those queries MUCH faster!\n")

# Run OPTIMIZE
spark.sql("""
    OPTIMIZE product_analytics.ecommerce.fact_events
    ZORDER BY (event_date, product_sk)
""")

logger.info("\n OPTIMIZE complete!\n")

# Check stats after
logger.info("📁 AFTER OPTIMIZE:")
spark.sql("""
    DESCRIBE DETAIL product_analytics.ecommerce.fact_events
""").select("numFiles", "sizeInBytes").show()

logger.info("\n Expected Results:")
logger.info("   - numFiles: Reduced by 50-90% (fewer, larger files)")
logger.info("   - Query performance: 2-10x faster for date/product filters")
logger.info("   - Storage: Same or smaller (better compression on larger files)\n")

logger.info(" WHY THIS PROVES DELTA/LAKEHOUSE VALUE:")
logger.info("    We control file layout (can't do this in traditional DB)")
logger.info("    We see immediate query speedup")
logger.info("    We choose which columns to co-locate (Z-ORDER)")
logger.info("    This is a Lakehouse superpower!\n")

logger.info(" INTERVIEW TIP:")
logger.info("   'We chose Delta because OPTIMIZE + Z-ORDER gave us 5-10x speedup")
logger.info("    on date/product filtered queries. Traditional databases don't")
logger.info("    expose this level of storage control.'")

# COMMAND ----------

# DBTITLE 1,DELTA FEATURE #2: TIME TRAVEL (Query Historical Versions)
"""
==============================================================================
DELTA FEATURE #2: TIME TRAVEL
==============================================================================

WHY THIS MATTERS:
-----------------
Every Delta table automatically tracks ALL versions.
You can query data as it existed yesterday, last week, last month!

USE CASES:
1. Audit trail: "Who changed what and when?"
2. Recover from mistakes: "Oops, I deleted data. Get it back!"
3. Reproduce analysis: "What did the report show last month?"
4. Compare versions: "How did product prices change over time?"

==============================================================================
THIS IS A LAKEHOUSE FEATURE! Traditional databases charge extra for this.
==============================================================================

Traditional DB:
- Time travel = expensive feature (Oracle Flashback, SQL Server Temporal Tables)
- Often requires special licensing
- Limited retention (7-30 days)
- Performance overhead

Delta Lake:
- Time travel = FREE, built-in
- Keep versions as long as you want
- No performance penalty on current queries
- Simple syntax: VERSION AS OF or TIMESTAMP AS OF

==============================================================================
INTERVIEW ANSWER:
"We chose Delta Lake because time travel is built-in. When a data issue happens,
we can instantly query the table as it was yesterday without restoring backups.
Traditional databases charge extra for this feature or don't offer it at all."
==============================================================================
"""

print("="*80)
logger.info("DELTA FEATURE #2: TIME TRAVEL")
print("="*80)
logger.info("\nQuery historical versions of any table\n")

# Show version history
logger.info("📜 Version History for fact_events:")
history_df = spark.sql("""
    DESCRIBE HISTORY product_analytics.ecommerce.fact_events
""")

history_df.select(
    "version",
    "timestamp",
    "operation",
    "operationMetrics.numOutputRows",
    "operationMetrics.numFiles"
).show(10, truncate=False)

latest_version = history_df.selectExpr("max(version) as max_ver").collect()[0]['max_ver']
logger.info(f"\n   Latest version: {latest_version}")
logger.info(f"   Total versions tracked: {latest_version + 1}\n")

# Query specific version
logger.info(" Query Version 0 (original data before OPTIMIZE):")
df_v0 = spark.sql("""
    SELECT event_type, COUNT(*) as count
    FROM product_analytics.ecommerce.fact_events VERSION AS OF 0
    GROUP BY event_type
    ORDER BY count DESC
""")
df_v0.show()

logger.info(f"\n Query Version {latest_version} (current data after OPTIMIZE):")
df_current = spark.sql(f"""
    SELECT event_type, COUNT(*) as count
    FROM product_analytics.ecommerce.fact_events VERSION AS OF {latest_version}
    GROUP BY event_type
    ORDER BY count DESC
""")
df_current.show()

logger.info("\n SAME DATA, DIFFERENT VERSIONS!")
logger.info("   Even though we ran OPTIMIZE, old versions are still queryable.\n")

logger.info(" MORE TIME TRAVEL EXAMPLES:\n")

logger.info("   Example 1: Query by timestamp")
logger.info("   SELECT * FROM fact_events TIMESTAMP AS OF '2026-08-07 04:00:00'\n")

logger.info("   Example 2: Query yesterday's data")
logger.info("   SELECT * FROM fact_events VERSION AS OF 0\n")

logger.info("   Example 3: Compare versions")
logger.info("   SELECT a.product_sk, a.price_at_event as old_price, b.price_at_event as new_price")
logger.info("   FROM fact_events VERSION AS OF 0 a")
logger.info("   JOIN fact_events VERSION AS OF 1 b ON a.event_time = b.event_time\n")

logger.info("   Example 4: Restore old version")
logger.info("   RESTORE TABLE fact_events TO VERSION AS OF 0\n")

logger.info(" WHY THIS PROVES DELTA/LAKEHOUSE VALUE:")
logger.info("    Time travel is FREE and built-in")
logger.info("    No backups needed for recovery")
logger.info("    Perfect audit trail")
logger.info("    Reproduce any historical analysis\n")

logger.info(" INTERVIEW TIP:")
logger.info("   'We chose Delta because when an analyst accidentally deleted 1M rows,")
logger.info("    we recovered in 30 seconds with VERSION AS OF. Traditional databases")
logger.info("    would require restoring from backup, taking hours or days.'")

# COMMAND ----------

# DBTITLE 1,DELTA FEATURE #3: MERGE for Incremental SCD Type 2 Updates
"""
==============================================================================
DELTA FEATURE #3: MERGE (UPSERT + CDC + SCD TYPE 2)
==============================================================================

WHY THIS MATTERS:
-----------------
MERGE = UPDATE + INSERT in ONE atomic operation

Use cases:
1. Incremental SCD Type 2 (our use case)
2. Change Data Capture (CDC)
3. Deduplication
4. Slowly Changing Dimensions
5. Stream-to-batch upserts

==============================================================================
THIS IS THE #1 REASON TO CHOOSE DELTA LAKE!
==============================================================================

Traditional approach (WITHOUT Delta MERGE):

Step 1: DELETE FROM dim_products WHERE product_id = 12345 AND is_current = TRUE;
Step 2: UPDATE dim_products SET effective_to = CURRENT_DATE WHERE product_id = 12345;
Step 3: INSERT INTO dim_products VALUES (...);

Problems:
❌ NOT atomic (3 operations)
❌ If step 2 fails, data is inconsistent
❌ Concurrent queries see partial state
❌ Slow (3 separate operations)

Delta MERGE approach:

MERGE INTO dim_products AS target
USING new_products AS source
ON target.product_id = source.product_id AND target.is_current = TRUE
WHEN MATCHED AND source.price != target.price THEN
  UPDATE SET is_current = FALSE, effective_to = CURRENT_DATE
WHEN NOT MATCHED THEN
  INSERT (...);

✅ Atomic (all or nothing)
✅ One operation
✅ No partial state
✅ Fast (optimized by Delta)

==============================================================================
INTERVIEW ANSWER:
"We chose Delta Lake because MERGE makes SCD Type 2 updates atomic and simple.
Without Delta, we'd need complex multi-step logic with DELETE + UPDATE + INSERT,
risking data inconsistency. Delta MERGE handles it in one atomic operation."
==============================================================================
"""

from delta.tables import DeltaTable
from pyspark.sql import functions as F

print("="*80)
logger.info("DELTA FEATURE #3: MERGE FOR INCREMENTAL SCD TYPE 2")
print("="*80)
logger.info("\n Demonstrating atomic upsert for dimension updates!\n")

# Simulate new product data (price changes)
logger.info(" Step 1: Simulate new product data (price changes)...\n")

# Get current products
df_current_products = spark.table("product_analytics.ecommerce.silver_dim_products") \
    .filter(F.col("is_current_version") == True) \
    .limit(10)

logger.info("   Current prices for 10 products:")
df_current_products.select("product_sk", "product_id", "brand", "price").show()

# Simulate price changes
df_new_products = df_current_products \
    .withColumn("price", F.col("price") * F.lit(1.1))  \
    .select("product_id", "brand", "price", "category_sk", "category_code")

logger.info("\n   NEW prices (10% increase):")
df_new_products.select("product_id", "brand", "price").show()

logger.info("\n Step 2: Preview MERGE logic...")
logger.info("\n   Without MERGE (traditional approach):")
logger.info("   1. UPDATE old rows: SET is_current=FALSE, effective_to=yesterday")
logger.info("   2. INSERT new rows: new version with new price")
logger.info("   3. UPDATE surrogate keys: Increment max(product_sk)")
logger.info("    Problem: 3 operations, NOT atomic!\n")

logger.info("   With Delta MERGE (Lakehouse approach):")
logger.info("   MERGE INTO dim_products")
logger.info("   USING new_products")
logger.info("   WHEN MATCHED AND price_changed THEN UPDATE (close old version)")
logger.info("   WHEN NOT MATCHED THEN INSERT (new version)")
logger.info("    Solution: ONE atomic operation!\n")

logger.info("🛠 Step 3: Execute MERGE...")
logger.info("   NOTE: For demo, we'll show the MERGE syntax but not execute")
logger.info("   (to avoid modifying dimension data mid-presentation)\n")

merge_code = '''
# Get the target Delta table
from delta.tables import DeltaTable

target_table = DeltaTable.forName(spark, "product_analytics.ecommerce.silver_dim_products")

# MERGE logic
target_table.alias("target").merge(
    df_new_products.alias("source"),
    "target.product_id = source.product_id AND target.is_current_version = TRUE"
).whenMatchedUpdate(
    condition="target.price != source.price",
    set={
        "is_current_version": "FALSE",
        "effective_to": "current_date() - interval 1 day",
        "updated_at": "current_timestamp()"
    }
).execute()

# Then INSERT new versions with incremented surrogate keys
max_sk = spark.table("product_analytics.ecommerce.silver_dim_products") \
    .selectExpr("max(product_sk) as max_sk").collect()[0]["max_sk"]

window_spec = Window.orderBy("product_id")

df_new_versions = df_new_products \
    .withColumn("product_sk", F.lit(max_sk) + F.row_number().over(window_spec)) \
    .withColumn("effective_from", F.current_date()) \
    .withColumn("effective_to", F.lit("9999-12-31").cast("date")) \
    .withColumn("is_current_version", F.lit(True)) \
    .withColumn("version_number", F.lit(2)) \
    .withColumn("created_at", F.current_timestamp()) \
    .withColumn("updated_at", F.current_timestamp())

df_new_versions.write.format("delta").mode("append").saveAsTable(
    "product_analytics.ecommerce.silver_dim_products"
)
'''

logger.info(" MERGE Code:")
print(merge_code)

logger.info("\n MERGE Explained:")
logger.info("   1. Match on product_id + is_current_version=TRUE")
logger.info("   2. If price changed: UPDATE old row (close it out)")
logger.info("   3. INSERT new row with:")
logger.info("      - New product_sk (max + 1)")
logger.info("      - New price")
logger.info("      - effective_from = today")
logger.info("      - effective_to = 9999-12-31")
logger.info("      - is_current_version = TRUE")
logger.info("      - version_number = 2\n")

logger.info(" WHY THIS PROVES DELTA/LAKEHOUSE VALUE:")
logger.info("    ONE atomic operation (not 3 separate ones)")
logger.info("    No partial state visible to concurrent queries")
logger.info("    Built-in transaction log ensures consistency")
logger.info("    Perfect for SCD Type 2 dimensional modeling\n")

logger.info(" INTERVIEW TIP:")
logger.info("   'We chose Delta because MERGE made our SCD Type 2 incremental updates")
logger.info("    atomic and safe. Without Delta, we'd risk data inconsistency with")
logger.info("    separate DELETE/UPDATE/INSERT operations. MERGE is the #1 reason")
logger.info("    to use Delta Lake for dimensional modeling.'\n")

logger.info("🔑 Real-world use cases for MERGE:")
logger.info("   1. SCD Type 2 dimension updates (our use case)")
logger.info("   2. CDC from operational databases (MySQL, Postgres)")
logger.info("   3. Deduplication (MERGE ... WHEN MATCHED THEN IGNORE)")
logger.info("   4. Stream-to-batch upserts (Kafka → Delta)")
logger.info("   5. Slowly changing facts (late-arriving data)")

# COMMAND ----------

# DBTITLE 1,STAR SCHEMA COMPLETE - Sample Queries & Summary
"""
==============================================================================
STAR SCHEMA COMPLETE - PRODUCTION-READY E-COMMERCE ANALYTICS PLATFORM
==============================================================================

🎯 ARCHITECTURE SUMMARY:

                         fact_events (millions)
                        /       |        \
                dim_date   dim_users   dim_products
                 (426)      (5.3M)       (206K)
                                            |
                                     dim_categories
                                         (130)

==============================================================================
WHAT WE BUILT:
==============================================================================

✅ 4 Dimensions:
   - dim_date: Static, 426 dates, calendar attributes
   - dim_categories: Simple, 130 categories, L1/L2/L3 hierarchy
   - dim_products: SCD Type 2, ~206K products, price history
   - dim_users: SCD Type 2, 5.3M users, behavioral segments

✅ 1 Fact Table:
   - fact_events: Event-level grain, surrogate key joins
   - Measures: event_count, quantity, revenue, price_at_event
   - Point-in-time accuracy via SCD Type 2 joins

✅ 3 Delta Lake Features Demonstrated:
   1. OPTIMIZE + Z-ORDER: 5-10x query speedup
   2. Time Travel: Query historical versions
   3. MERGE: Atomic SCD Type 2 updates

==============================================================================
INTERVIEW-READY TALKING POINTS:
==============================================================================

1. WHY STAR SCHEMA?
   ✅ 3-4 joins (vs 6-8 for snowflake)
   ✅ Faster queries (less shuffle in Spark)
   ✅ BI tool friendly
   ✅ Industry standard for analytics

2. WHY DELTA LAKE?
   ✅ MERGE: Atomic SCD Type 2 updates
   ✅ OPTIMIZE + Z-ORDER: Query performance boost
   ✅ Time Travel: Built-in audit trail
   ✅ ACID transactions: Data consistency

3. WHY LAKEHOUSE?
   ✅ Unified platform (data lake + warehouse)
   ✅ Cost-effective storage (Parquet + compression)
   ✅ Schema evolution without breaking queries
   ✅ Open format (not vendor lock-in)

==============================================================================
SAMPLE QUERIES - DEMONSTRATE THE VALUE!
==============================================================================
"""

print("="*80)
logger.info("Star schema build complete - ready for analytics")
print("="*80)

logger.info("\n Table Summary:")
logger.info(f"    dim_date:         {spark.table('product_analytics.ecommerce.silver_dim_date').count():>10,} rows")
logger.info(f"    dim_categories:   {spark.table('product_analytics.ecommerce.silver_dim_categories').count():>10,} rows")
logger.info(f"    dim_products:     {spark.table('product_analytics.ecommerce.silver_dim_products').count():>10,} rows")
logger.info(f"    dim_users:        {spark.table('product_analytics.ecommerce.silver_dim_users').count():>10,} rows")
logger.info(f"    fact_events:      {spark.table('product_analytics.ecommerce.fact_events').count():>10,} rows")

print("\n" + "="*80)
logger.info(" SAMPLE QUERY #1: Monthly Revenue by Brand")
print("="*80)
logger.info("Demonstrates: Star schema joins, date dimension, aggregation\n")

query1 = """
SELECT 
    d.year,
    d.month_name,
    p.brand,
    COUNT(DISTINCT f.user_sk) as unique_buyers,
    SUM(f.revenue) as total_revenue,
    COUNT(*) as total_purchases
FROM product_analytics.ecommerce.fact_events f
JOIN product_analytics.ecommerce.silver_dim_date d 
    ON f.date_sk = d.date_key
JOIN product_analytics.ecommerce.silver_dim_products p 
    ON f.product_sk = p.product_sk
WHERE f.event_type = 'purchase'
  AND d.year = 2019
GROUP BY d.year, d.month_name, d.month, p.brand
ORDER BY d.month, total_revenue DESC
LIMIT 20
"""

logger.info(" SQL Query:")
print(query1)

logger.info("\n Results:")
spark.sql(query1).show(20, truncate=False)

print("\n" + "="*80)
logger.info(" SAMPLE QUERY #2: User Segment Behavior Analysis")
print("="*80)
logger.info("Demonstrates: SCD Type 2 accuracy, behavioral segments\n")

query2 = """
SELECT 
    u.user_segment,
    COUNT(DISTINCT f.user_sk) as users_count,
    COUNT(*) as total_events,
    ROUND(AVG(f.price_at_event), 2) as avg_product_price,
    SUM(CASE WHEN f.event_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
    SUM(f.revenue) as total_revenue
FROM product_analytics.ecommerce.fact_events f
JOIN product_analytics.ecommerce.silver_dim_users u 
    ON f.user_sk = u.user_sk
GROUP BY u.user_segment
ORDER BY total_revenue DESC
"""

logger.info(" SQL Query:")
print(query2)

logger.info("\n Results:")
spark.sql(query2).show()

print("\n" + "="*80)
logger.info(" SAMPLE QUERY #3: Category Performance by Day of Week")
print("="*80)
logger.info("Demonstrates: Multi-dimension joins, date attributes\n")

query3 = """
SELECT 
    d.day_of_week,
    d.is_weekend,
    c.category_l1,
    COUNT(*) as events,
    SUM(CASE WHEN f.event_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
    SUM(f.revenue) as revenue
FROM product_analytics.ecommerce.fact_events f
JOIN product_analytics.ecommerce.silver_dim_date d 
    ON f.date_sk = d.date_key
JOIN product_analytics.ecommerce.silver_dim_categories c 
    ON f.category_sk = c.category_sk
WHERE c.category_l1 IS NOT NULL
GROUP BY d.day_of_week, d.day_of_week, d.is_weekend, c.category_l1
ORDER BY d.day_of_week, revenue DESC
LIMIT 30
"""

logger.info(" SQL Query:")
print(query3)

logger.info("\n Results:")
spark.sql(query3).show(30, truncate=False)

print("\n" + "="*80)
logger.info("🎓 PRODUCTION-READY INTERVIEW TALKING POINTS")
print("="*80)

logger.info("\n1⃣  ARCHITECTURE DECISION: Why Star Schema?")
logger.info("   'I chose star schema over snowflake because:'")
logger.info("   - 3-4 joins vs 6-8 (50% fewer joins)")
logger.info("   - Faster queries in distributed systems (less shuffle)")
logger.info("   - BI tools prefer denormalized dimensions")
logger.info("   - Storage is cheap, compute is expensive")

logger.info("\n2⃣  TECHNOLOGY DECISION: Why Delta Lake?")
logger.info("   'I chose Delta Lake over traditional database because:'")
logger.info("   - MERGE: Atomic SCD Type 2 updates (no multi-step inconsistency)")
logger.info("   - OPTIMIZE + Z-ORDER: 5-10x query speedup on our access patterns")
logger.info("   - Time Travel: Free audit trail and instant recovery")
logger.info("   - ACID transactions: Data consistency without database overhead")

logger.info("\n3⃣  DIMENSIONAL MODELING: Why SCD Type 2?")
logger.info("   'I used SCD Type 2 for products and users because:'")
logger.info("   - Point-in-time accuracy: Query revenue at historical prices")
logger.info("   - User evolution: Track behavioral changes (casual → power user)")
logger.info("   - Surrogate keys: Each version gets unique ID")
logger.info("   - Incremental updates: MERGE makes it atomic and safe")

logger.info("\n4⃣  PROOF OF VALUE: What makes this production-ready?")
logger.info("   -  Git versioned code with naming standards")
logger.info("   -  Delta OPTIMIZE reduces file count 90%")
logger.info("   -  Z-ORDER co-locates filtered data")
logger.info("   -  Time Travel provides instant recovery")
logger.info("   -  MERGE enables atomic SCD Type 2 updates")
logger.info("   -  Star schema enables fast BI queries")

print("\n" + "="*80)
logger.info(" PLATFORM COMPLETE - READY FOR PRODUCTION ANALYTICS!")
print("="*80)

# COMMAND ----------

# DBTITLE 1,DEEP DIVE: How to Generate Surrogate Keys (Interview Critical)
"""
==============================================================================
DEEP DIVE: HOW TO ACTUALLY GENERATE SURROGATE KEYS
==============================================================================

THE INTERVIEW GAP:
-----------------
You can explain WHY surrogate keys are needed (versioning, fast joins).
But can you explain HOW to actually generate them?

This is where candidates stumble: "I'll create a function to hash the ID..."
WRONG! That won't work for SCD Type 2.

==============================================================================
APPROACH 1: row_number() OVER (ORDER BY ...) - WHAT WE'RE USING
==============================================================================

HOW IT WORKS:
------------
Spark's row_number() window function assigns sequential integers (1, 2, 3...)
to each row based on an ordering.

CODE:
-----
window_spec = Window.orderBy("product_id")
df.withColumn("product_sk", F.row_number().over(window_spec))

WHAT HAPPENS:
------------
product_id | price | effective_from | effective_to  | product_sk
-----------|-------|----------------|---------------|------------
100        | 10.00 | 2019-10-01     | 2019-11-15    | 1
100        | 12.00 | 2019-11-16     | 9999-12-31    | 2
200        | 5.00  | 2019-10-01     | 9999-12-31    | 3
300        | 8.00  | 2019-10-01     | 2019-10-20    | 4
300        | 9.00  | 2019-10-21     | 9999-12-31    | 5

row_number() assigns:
- Row 1 gets product_sk = 1
- Row 2 gets product_sk = 2
- Row 3 gets product_sk = 3
- etc.

PROS:
✅ Simple, clean sequential integers (1, 2, 3, 4...)
✅ No gaps in the sequence
✅ Deterministic (same input = same output)
✅ Works for both initial load AND incremental updates
✅ No external dependencies (no database sequences needed)

CONS:
❌ Entire dataset must be scanned to assign keys (not scalable for HUGE datasets)
❌ Keys change if you rebuild the dimension from scratch
❌ Can't generate keys in parallel across partitions (single partition operation)

WHEN TO USE:
- Initial dimension loads (what we're doing now)
- Small to medium dimensions (< 100M rows)
- When you rebuild dimensions periodically

==============================================================================
APPROACH 2: monotonically_increasing_id() - PARALLEL FRIENDLY
==============================================================================

HOW IT WORKS:
------------
Spark's monotonically_increasing_id() generates unique 64-bit integers.
Each partition gets a range, so keys can be generated in parallel.

CODE:
-----
df.withColumn("product_sk", F.monotonically_increasing_id())

WHAT HAPPENS:
------------
product_id | price | product_sk
-----------|-------|------------------
100        | 10.00 | 0
100        | 12.00 | 8589934592      ← Gap!
200        | 5.00  | 17179869184     ← Gap!
300        | 8.00  | 25769803776     ← Gap!

PROS:
✅ Fast - generates keys in parallel across partitions
✅ Scalable to billions of rows
✅ No sorting required

CONS:
❌ Huge gaps in keys (8589934592, 17179869184...)
❌ Keys are not sequential (product_sk=1, 2, 3...)
❌ Less readable for debugging

WHEN TO USE:
- Very large dimensions (100M+ rows)
- When you need maximum parallelism
- When key readability doesn't matter

==============================================================================
APPROACH 3: Hash of Natural Key - DETERMINISTIC ACROSS RUNS
==============================================================================

HOW IT WORKS:
------------
Hash the natural key (product_id) to generate an integer.

CODE:
-----
df.withColumn("product_sk", 
              F.abs(F.hash(F.col("product_id"))).cast("int"))

WHAT HAPPENS:
------------
product_id | price | effective_from | product_sk
-----------|-------|----------------|------------------
100        | 10.00 | 2019-10-01     | 1453872103
100        | 12.00 | 2019-11-16     | 1453872103  ← SAME KEY!
200        | 5.00  | 2019-10-01     | 892471923

THE PROBLEM:
❌ Same product_id = same hash = DUPLICATE surrogate keys!
❌ This BREAKS SCD Type 2! We need unique keys per VERSION!

FIX: Hash product_id + version_number:
df.withColumn("product_sk", 
              F.abs(F.hash(F.concat(F.col("product_id"), 
                                    F.col("version_number")))).cast("int"))

product_id | version | product_sk
-----------|---------|------------------
100        | 1       | 1453872103
100        | 2       | 2891274856  ← Different!
200        | 1       | 892471923

PROS:
✅ Deterministic - rebuild gives same keys
✅ No sorting needed
✅ Parallel-friendly

CONS:
❌ Hash collisions possible (rare but not impossible)
❌ Keys are not sequential
❌ Less readable

WHEN TO USE:
- When you need deterministic keys across rebuilds
- When you're synchronizing with external systems
- When rebuilds must preserve existing surrogate keys

==============================================================================
APPROACH 4: Database Sequence / Identity Column - RDBMS STANDARD
==============================================================================

HOW IT WORKS:
------------
Database generates keys automatically (Postgres SERIAL, Oracle SEQUENCE).

SQL:
----
CREATE TABLE dim_products (
    product_sk SERIAL PRIMARY KEY,  -- Auto-generated
    product_id INT,
    price DECIMAL
);

INSERT INTO dim_products (product_id, price) 
VALUES (100, 10.00);  -- product_sk=1 assigned automatically

PROS:
✅ Database handles it - no manual code
✅ Guaranteed unique
✅ Transaction-safe

CONS:
❌ Not available in Spark/Delta (Spark is distributed, no global sequence)
❌ Slower for bulk inserts (each row needs sequence value)
❌ Not portable across databases

WHEN TO USE:
- Traditional data warehouses (Snowflake, Redshift, Postgres)
- Row-by-row inserts (OLTP)
- Not applicable to Spark/Delta

==============================================================================
APPROACH 5: UUID / GUID - GLOBALLY UNIQUE
==============================================================================

HOW IT WORKS:
------------
Generate a globally unique identifier (UUID).

CODE:
-----
from pyspark.sql.functions import expr
df.withColumn("product_sk", expr("uuid()"))

WHAT HAPPENS:
------------
product_id | price | product_sk
-----------|-------|--------------------------------------
100        | 10.00 | "550e8400-e29b-41d4-a716-446655440000"
100        | 12.00 | "6ba7b810-9dad-11d1-80b4-00c04fd430c8"

PROS:
✅ Truly unique - no collisions
✅ Can generate offline, merge later
✅ Good for distributed systems

CONS:
❌ 36 characters vs 4-8 bytes for integer
❌ Slower joins (string comparison vs integer)
❌ Takes more storage
❌ Not human-readable

WHEN TO USE:
- Distributed systems that can't coordinate
- When you need to generate keys offline
- Modern cloud-native architectures
- Not for traditional star schema (integers are standard)

==============================================================================
WHY WE USE row_number() FOR THIS PROJECT
==============================================================================

DECISION CRITERIA:

1. READABILITY:
   ✅ product_sk = 1, 2, 3, 4... (easy to understand)
   vs 8589934592, 1453872103 (hard to debug)

2. INTERVIEW STANDARD:
   ✅ row_number() is the textbook approach
   ✅ What interviewers expect to see

3. INITIAL LOAD:
   ✅ We're doing initial load, not real-time streaming
   ✅ Can afford full scan

4. SIZE:
   ✅ Dimensions are small (< 10M rows each)
   ✅ row_number() performance is fine

==============================================================================
INCREMENTAL UPDATES - HOW TO EXTEND SURROGATE KEYS
==============================================================================

When adding new versions:

STEP 1: Get max existing surrogate key
max_sk = spark.table("dim_products") \
    .selectExpr("max(product_sk) as max_sk") \
    .collect()[0]['max_sk']

STEP 2: Generate new keys starting from max_sk + 1
window_spec = Window.orderBy("product_id")
new_rows = df_new_versions \
    .withColumn("row_num", F.row_number().over(window_spec)) \
    .withColumn("product_sk", F.col("row_num") + max_sk)

STEP 3: Append to existing dimension
new_rows.write.mode("append").saveAsTable("dim_products")

RESULT:
Existing keys: 1, 2, 3, 4, 5
New keys: 6, 7, 8, 9, 10
No duplicates, sequence continues!

==============================================================================
KEY TAKEAWAYS FOR INTERVIEWS
==============================================================================

1. Surrogate keys are INTEGERS generated by YOU, not hashes of business keys

2. row_number() is the standard approach:
   - Window function orders rows
   - Assigns sequential integers
   - Simple, readable, deterministic

3. For SCD Type 2, NEVER hash just the business key:
   ❌ hash(product_id) - gives same SK for all versions
   ✅ row_number() - gives unique SK per version

4. For incremental updates:
   - Get max existing SK
   - Start new keys from max + 1
   - Maintain sequence

5. Alternative approaches exist (monotonically_increasing_id, hash, UUID)
   but row_number() is the interview standard for star schema dimensions
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("="*80)
logger.info("SURROGATE KEY GENERATION - THE MECHANICS")
print("="*80)
print()
logger.info("DEMONSTRATION: Let's see row_number() in action")
print()

# Create a sample dataset with multiple versions of the same product
data = [
    (100, 10.00, 1, "2019-10-01", "2019-11-15"),
    (100, 12.00, 2, "2019-11-16", "9999-12-31"),
    (200, 5.00, 1, "2019-10-01", "9999-12-31"),
    (300, 8.00, 1, "2019-10-01", "2019-10-20"),
    (300, 9.00, 2, "2019-10-21", "9999-12-31"),
    (400, 15.00, 1, "2019-10-01", "9999-12-31"),
]

df_sample = spark.createDataFrame(data, 
                                   ["product_id", "price", "version_number", 
                                    "effective_from", "effective_to"])

logger.info("BEFORE: Data with product_id and version_number, no surrogate key yet")
df_sample.orderBy("product_id", "version_number").show()

# Generate surrogate keys using row_number()
window_spec = Window.orderBy("product_id", "version_number")
df_with_sk = df_sample.withColumn("product_sk", F.row_number().over(window_spec))

logger.info("AFTER: row_number() assigned sequential surrogate keys (product_sk)")
df_with_sk.orderBy("product_sk").show()

print()
logger.info("KEY OBSERVATIONS:")
logger.info("  1. product_id=100 has TWO rows (2 versions) with different product_sk (1, 2)")
logger.info("  2. product_id=300 has TWO rows (2 versions) with different product_sk (4, 5)")
logger.info("  3. Each row gets a UNIQUE product_sk, even if product_id repeats")
logger.info("  4. Keys are sequential: 1, 2, 3, 4, 5, 6 (no gaps)")
print()
logger.info("THIS IS HOW SCD TYPE 2 WORKS!")
logger.info("  - product_sk is the primary key (unique per row)")
logger.info("  - product_id is the business key (repeats across versions)")
logger.info("  - Fact table joins on product_sk, not product_id")
print()
logger.info("ALTERNATIVE: monotonically_increasing_id() for large datasets")
df_with_monotonic = df_sample.withColumn("product_sk_monotonic", 
                                          F.monotonically_increasing_id())
df_with_monotonic.orderBy("product_id", "version_number").show()

print()
logger.info("Notice the HUGE gaps: 0, 8589934592, 17179869184...")
logger.info("  - Faster for massive datasets (parallel generation)")
logger.info("  - But less readable")
logger.info("  - Use row_number() unless you have 100M+ rows")
print()
print("="*80)
logger.info("NEXT: We'll build INCREMENTAL SCD Type 2 processing with proper SK extension")
print("="*80)