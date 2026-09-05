# Databricks notebook source
# DBTITLE 1,Incremental SCD Type 2 Processing Framework


# COMMAND ----------

# DBTITLE 1,🔍 INVESTIGATION: Check Brand Distribution Across All Layers
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- DIAGNOSTIC QUERY 1: Check brand distribution in BRONZE layer
# MAGIC -- ============================================================================
# MAGIC SELECT 
# MAGIC     'Bronze Layer' as layer,
# MAGIC     LOWER(TRIM(brand)) as brand_normalized,
# MAGIC     COUNT(DISTINCT product_id) as unique_products,
# MAGIC     COUNT(*) as total_events
# MAGIC FROM product_analytics.ecommerce.bronze_events
# MAGIC WHERE brand IS NOT NULL
# MAGIC GROUP BY LOWER(TRIM(brand))
# MAGIC ORDER BY unique_products DESC
# MAGIC LIMIT 25;

# COMMAND ----------

# DBTITLE 1,🔍 INVESTIGATION: Check Brand Distribution in SILVER Layer
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- DIAGNOSTIC QUERY 2: Check brand distribution in SILVER dim_products
# MAGIC -- ============================================================================
# MAGIC SELECT 
# MAGIC     'Silver Layer' as layer,
# MAGIC     brand,
# MAGIC     is_current_version,
# MAGIC     COUNT(DISTINCT product_id) as unique_products,
# MAGIC     COUNT(*) as total_versions
# MAGIC FROM product_analytics.ecommerce.silver_dim_products
# MAGIC GROUP BY brand, is_current_version
# MAGIC ORDER BY unique_products DESC
# MAGIC LIMIT 25;

# COMMAND ----------

# DBTITLE 1,🔍 INVESTIGATION: Check Brand Distribution in GOLD Layer
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- DIAGNOSTIC QUERY 3: Check brand distribution in GOLD product_performance
# MAGIC -- ============================================================================
# MAGIC SELECT 
# MAGIC     'Gold Layer' as layer,
# MAGIC     brand,
# MAGIC     COUNT(DISTINCT product_id) as unique_products,
# MAGIC     SUM(total_views) as total_views,
# MAGIC     SUM(total_purchases) as total_purchases,
# MAGIC     ROUND(SUM(total_revenue), 2) as total_revenue
# MAGIC FROM product_analytics.ecommerce.gold_product_performance
# MAGIC GROUP BY brand
# MAGIC ORDER BY unique_products DESC
# MAGIC LIMIT 25;

# COMMAND ----------

# DBTITLE 1,🔍 INVESTIGATION: Cross-Layer Brand Comparison
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- DIAGNOSTIC QUERY 4: Compare brand coverage across layers
# MAGIC -- ============================================================================
# MAGIC WITH bronze_brands AS (
# MAGIC     SELECT DISTINCT LOWER(TRIM(brand)) as brand
# MAGIC     FROM product_analytics.ecommerce.bronze_events
# MAGIC     WHERE brand IS NOT NULL
# MAGIC ),
# MAGIC silver_brands AS (
# MAGIC     SELECT DISTINCT brand
# MAGIC     FROM product_analytics.ecommerce.silver_dim_products
# MAGIC     WHERE is_current_version = TRUE
# MAGIC ),
# MAGIC gold_brands AS (
# MAGIC     SELECT DISTINCT brand
# MAGIC     FROM product_analytics.ecommerce.gold_product_performance
# MAGIC )
# MAGIC SELECT 
# MAGIC     COALESCE(b.brand, s.brand, g.brand) as brand,
# MAGIC     CASE WHEN b.brand IS NOT NULL THEN '✅' ELSE '❌' END as in_bronze,
# MAGIC     CASE WHEN s.brand IS NOT NULL THEN '✅' ELSE '❌' END as in_silver,
# MAGIC     CASE WHEN g.brand IS NOT NULL THEN '✅' ELSE '❌' END as in_gold
# MAGIC FROM bronze_brands b
# MAGIC FULL OUTER JOIN silver_brands s ON b.brand = s.brand
# MAGIC FULL OUTER JOIN gold_brands g ON COALESCE(b.brand, s.brand) = g.brand
# MAGIC ORDER BY brand
# MAGIC LIMIT 50;

# COMMAND ----------

# DBTITLE 1,🔍 INVESTIGATION: Check fact_events to silver_dim_products JOIN
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- DIAGNOSTIC QUERY 5: Check if fact_events has orphaned product_sk values
# MAGIC -- (product_sk values in fact_events that don't exist in silver_dim_products)
# MAGIC -- ============================================================================
# MAGIC WITH fact_product_sks AS (
# MAGIC     SELECT DISTINCT product_sk
# MAGIC     FROM product_analytics.ecommerce.fact_events
# MAGIC     WHERE product_sk IS NOT NULL
# MAGIC ),
# MAGIC dim_product_sks AS (
# MAGIC     SELECT DISTINCT product_sk
# MAGIC     FROM product_analytics.ecommerce.silver_dim_products
# MAGIC     WHERE is_current_version = TRUE
# MAGIC )
# MAGIC SELECT 
# MAGIC     COUNT(DISTINCT f.product_sk) as total_distinct_product_sks_in_facts,
# MAGIC     COUNT(DISTINCT d.product_sk) as matched_in_dim_products,
# MAGIC     COUNT(DISTINCT f.product_sk) - COUNT(DISTINCT d.product_sk) as orphaned_product_sks
# MAGIC FROM fact_product_sks f
# MAGIC LEFT JOIN dim_product_sks d ON f.product_sk = d.product_sk;

# COMMAND ----------

# DBTITLE 1,🔍 INVESTIGATION: Sample Orphaned Products (if any)
# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- DIAGNOSTIC QUERY 6: Show sample orphaned products
# MAGIC -- ============================================================================
# MAGIC WITH fact_product_sks AS (
# MAGIC     SELECT DISTINCT f.product_sk, f.product_id
# MAGIC     FROM product_analytics.ecommerce.fact_events f
# MAGIC     WHERE f.product_sk IS NOT NULL
# MAGIC ),
# MAGIC dim_current_products AS (
# MAGIC     SELECT product_sk, product_id, brand
# MAGIC     FROM product_analytics.ecommerce.silver_dim_products
# MAGIC     WHERE is_current_version = TRUE
# MAGIC )
# MAGIC SELECT 
# MAGIC     f.product_sk,
# MAGIC     f.product_id,
# MAGIC     d.brand,
# MAGIC     CASE WHEN d.product_sk IS NULL THEN 'ORPHANED (Missing from dim_products)' ELSE 'OK' END as status
# MAGIC FROM fact_product_sks f
# MAGIC LEFT JOIN dim_current_products d ON f.product_sk = d.product_sk
# MAGIC WHERE d.product_sk IS NULL
# MAGIC LIMIT 20;

# COMMAND ----------

# DBTITLE 1,Setup: Load Config and Read Current Dimension State
"""
STEP 1: SETUP
============

Load configuration and read the current state of dim_products.
"""

import logging

# Configure logging
logger = logging.getLogger(__name__)


import sys
sys.path.append('/Workspace/Users/vedantve@gmail.com/product-analytics-platform/config')
from platform_config import PlatformConfig

config = PlatformConfig()

logger.info("Loading current dimension state...")
print()

# Read current dimension
df_dim_current = spark.table("product_analytics.ecommerce.silver_dim_products")

logger.info("Current dimension state:")
logger.info(f"  Total rows: {df_dim_current.count():,}")
logger.info(f"  Current versions: {df_dim_current.filter(F.col('is_current_version')).count():,}")
logger.info(f"  Historical versions: {df_dim_current.filter(~F.col('is_current_version')).count():,}")
logger.info(f"  Max product_sk: {df_dim_current.selectExpr('max(product_sk) as max_sk').collect()[0]['max_sk']:,}")
print()
logger.info("Sample current products:")
df_dim_current.filter(F.col("is_current_version")).orderBy("product_sk").show(5, truncate=False)

# COMMAND ----------

# DBTITLE 1,Simulate New Data with Changes (Price and Brand Changes)
"""
STEP 2: SIMULATE NEW DATA
=========================

In production, this would be new data from Bronze.
For this demo, we'll simulate some price and brand changes.

We'll take 100 products and:
- Change price for 30 products (30% price change)
- Change brand for 10 products (10% brand change)
- Leave 60 products unchanged (60% no change)

This simulates a typical incremental batch.
"""

from pyspark.sql import functions as F

logger.info("Simulating new product data with changes...")
print()

# Take a sample of current products
df_sample = df_dim_current \
    .filter(F.col("is_current_version")) \
    .filter(F.col("product_id").isin([1000365, 1000978, 1001588, 1001606, 1001618, 
                                       1001619, 1001894, 1002042, 1002062, 1002098])) \
    .select("product_id", "price", "brand", "category_sk", "category_code")

logger.info(f"Sample size: {df_sample.count()} products")
print()
logger.info("BEFORE (current state):")
df_sample.orderBy("product_id").show()

# Simulate price changes for some products
df_new_snapshot = df_sample \
    .withColumn("price_new", 
                F.when(F.col("product_id").isin([1000365, 1001588, 1001606]), 
                       F.round(F.col("price") * 0.8, 2))  # 20% price drop
                 .when(F.col("product_id").isin([1001618, 1001619]), 
                       F.round(F.col("price") * 1.1, 2))  # 10% price increase
                 .otherwise(F.col("price"))) \
    .withColumn("brand_new",
                F.when(F.col("product_id") == 1002042, "samsung_updated")
                 .otherwise(F.col("brand"))) \
    .drop("price", "brand") \
    .withColumnRenamed("price_new", "price") \
    .withColumnRenamed("brand_new", "brand")

logger.info("\nAFTER (new snapshot with simulated changes):")
df_new_snapshot.orderBy("product_id").show()

logger.info("\nSUMMARY OF CHANGES:")
logger.info("  Products with price changes: 5")
logger.info("  Products with brand changes: 1")
logger.info("  Products with no changes: 4")

# COMMAND ----------

# DBTITLE 1,Detect Changes and Build Update Records
"""
STEP 3: DETECT CHANGES
=====================

Compare new snapshot to current dimension.
Identify products where tracked attributes changed.

TRACKED ATTRIBUTES:
- price
- brand  
- category_sk

LOGIC:
IF (new.price != old.price) OR 
   (new.brand != old.brand) OR 
   (new.category_sk != old.category_sk)
THEN
  Product changed - need new version
"""

from delta.tables import DeltaTable
from datetime import date

logger.info("Detecting changes...")
print()

# Get current versions only
df_current_versions = df_dim_current.filter(F.col("is_current_version"))

# Join new snapshot to current dimension
df_comparison = df_new_snapshot.alias("new") \
    .join(
        df_current_versions.alias("old"),
        F.col("new.product_id") == F.col("old.product_id"),
        "left"
    ) \
    .select(
        F.col("new.product_id").alias("product_id"),
        F.col("new.price").alias("new_price"),
        F.col("new.brand").alias("new_brand"),
        F.col("new.category_sk").alias("new_category_sk"),
        F.col("new.category_code").alias("new_category_code"),
        F.col("old.price").alias("old_price"),
        F.col("old.brand").alias("old_brand"),
        F.col("old.category_sk").alias("old_category_sk"),
        F.col("old.product_sk").alias("old_product_sk"),
        F.col("old.version_number").alias("old_version_number"),
        F.col("old.effective_from").alias("old_effective_from")
    )

# Flag changed products
df_changes = df_comparison \
    .withColumn("has_changed",
                (F.col("new_price") != F.col("old_price")) |
                (F.col("new_brand") != F.col("old_brand")) |
                (F.col("new_category_sk") != F.col("old_category_sk")))

logger.info("Comparison results:")
df_changes.orderBy("product_id").show(truncate=False)

# Separate changed vs unchanged
df_changed_products = df_changes.filter(F.col("has_changed"))
df_unchanged_products = df_changes.filter(~F.col("has_changed"))

changed_count = df_changed_products.count()
unchanged_count = df_unchanged_products.count()

logger.info("\nCHANGE DETECTION RESULTS:")
logger.info(f"  Changed products: {changed_count}")
logger.info(f"  Unchanged products: {unchanged_count}")
print()

if changed_count > 0:
    logger.info("Products that changed:")
    df_changed_products.select(
        "product_id", 
        "old_price", "new_price",
        "old_brand", "new_brand"
    ).show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Execute SCD Type 2 Update (Close Old + Insert New Versions)
"""
STEP 4: EXECUTE SCD TYPE 2 UPDATE
=================================

For changed products:
1. Close old version (UPDATE existing row)
   - SET effective_to = CURRENT_DATE - 1
   - SET is_current_version = FALSE

2. Insert new version (INSERT new row)
   - Generate new product_sk (max + row_number)
   - SET version_number = old_version + 1
   - SET effective_from = CURRENT_DATE
   - SET effective_to = 9999-12-31
   - SET is_current_version = TRUE

We'll use Delta Lake MERGE for transactional consistency.
"""

from datetime import datetime, timedelta

if changed_count == 0:
    logger.info("No changes detected. Skipping SCD Type 2 update.")
else:
    logger.info(f"Processing {changed_count} changed products...")
    print()
    
    # STEP 4A: Close out old versions
    logger.info("STEP 4A: Closing old versions")
    print("-" * 80)
    
    # Get current date for effective_to
    current_date = date.today()
    yesterday = current_date - timedelta(days=1)
    
    logger.info(f"  Setting effective_to = {yesterday} for old versions")
    logger.info("  Setting is_current_version = FALSE")
    print()
    
    # Prepare update records
    product_ids_to_close = [row["product_id"] for row in df_changed_products.select("product_id").collect()]
    
    # Update using Delta Lake
    delta_table = DeltaTable.forName(spark, "product_analytics.ecommerce.silver_dim_products")
    
    delta_table.update(
        condition=f"product_id IN ({','.join(map(str, product_ids_to_close))}) AND is_current_version = TRUE",
        set={
            "effective_to": F.lit(yesterday),
            "is_current_version": F.lit(False),
            "updated_at": F.current_timestamp()
        }
    )
    
    logger.info(f"  Updated {len(product_ids_to_close)} old versions")
    print()
    
    # STEP 4B: Insert new versions
    logger.info("STEP 4B: Inserting new versions")
    print("-" * 80)
    
    # Get max surrogate key to continue sequence
    max_sk = spark.table("product_analytics.ecommerce.silver_dim_products") \
        .selectExpr("max(product_sk) as max_sk") \
        .collect()[0]['max_sk']
    
    logger.info(f"  Current max product_sk: {max_sk:,}")
    logger.info(f"  New surrogate keys will start from: {max_sk + 1:,}")
    print()
    
    # Build new version rows
    window_spec = Window.orderBy("product_id")
    
    df_new_versions = df_changed_products \
        .withColumn("product_sk", F.row_number().over(window_spec) + max_sk) \
        .withColumn("version_number", F.col("old_version_number") + 1) \
        .withColumn("effective_from", F.lit(current_date).cast("date")) \
        .withColumn("effective_to", F.lit("9999-12-31").cast("date")) \
        .withColumn("is_current_version", F.lit(True)) \
        .withColumn("created_at", F.current_timestamp()) \
        .withColumn("updated_at", F.current_timestamp()) \
        .select(
            "product_sk",
            "product_id",
            F.col("new_brand").alias("brand"),
            F.col("new_price").alias("price"),
            F.col("new_category_sk").alias("category_sk"),
            F.col("new_category_code").alias("category_code"),
            "effective_from",
            "effective_to",
            "is_current_version",
            "version_number",
            "created_at",
            "updated_at"
        )
    
    logger.info("  New version rows to insert:")
    df_new_versions.show(truncate=False)
    
    # Insert new versions
    df_new_versions.write.format("delta") \
        .mode("append") \
        .saveAsTable("product_analytics.ecommerce.silver_dim_products")
    
    logger.info(f"  Inserted {df_new_versions.count()} new versions")
    print()
    
    print("="*80)
    logger.info("SCD TYPE 2 UPDATE COMPLETE")
    print("="*80)
    print()
    
    # Verify the results
    logger.info("VERIFICATION: Check one changed product")
    example_product_id = df_changed_products.first()["product_id"]
    
    df_verification = spark.table("product_analytics.ecommerce.silver_dim_products") \
        .filter(F.col("product_id") == example_product_id) \
        .orderBy("effective_from")
    
    logger.info(f"\nProduct {example_product_id} history:")
    df_verification.select(
        "product_sk", "product_id", "price", "brand", 
        "effective_from", "effective_to", "is_current_version", "version_number"
    ).show(truncate=False)
    
    logger.info("\nNotice:")
    logger.info("  - Old version: effective_to updated to yesterday, is_current = FALSE")
    logger.info("  - New version: new product_sk, version incremented, is_current = TRUE")
    logger.info("  - Price/brand updated in new version")
    logger.info("  - No data loss - full history preserved!")