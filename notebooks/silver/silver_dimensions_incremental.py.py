# Databricks notebook source
# DBTITLE 1,Incremental SCD Type 2 Processing Framework


# COMMAND ----------

# DBTITLE 1,Setup: Load Config and Read Current Dimension State
"""
STEP 1: SETUP
============

Load configuration and read the current state of dim_products.
"""

import sys
sys.path.append('/Workspace/Users/vedantve@gmail.com/product-analytics-platform/config')
from platform_config import PlatformConfig

config = PlatformConfig()

print("Loading current dimension state...")
print()

# Read current dimension
df_dim_current = spark.table("product_analytics.ecommerce.silver_dim_products")

print(f"Current dimension state:")
print(f"  Total rows: {df_dim_current.count():,}")
print(f"  Current versions: {df_dim_current.filter(F.col('is_current_version')).count():,}")
print(f"  Historical versions: {df_dim_current.filter(~F.col('is_current_version')).count():,}")
print(f"  Max product_sk: {df_dim_current.selectExpr('max(product_sk) as max_sk').collect()[0]['max_sk']:,}")
print()
print("Sample current products:")
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

print("Simulating new product data with changes...")
print()

# Take a sample of current products
df_sample = df_dim_current \
    .filter(F.col("is_current_version")) \
    .filter(F.col("product_id").isin([1000365, 1000978, 1001588, 1001606, 1001618, 
                                       1001619, 1001894, 1002042, 1002062, 1002098])) \
    .select("product_id", "price", "brand", "category_sk", "category_code")

print(f"Sample size: {df_sample.count()} products")
print()
print("BEFORE (current state):")
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

print("\nAFTER (new snapshot with simulated changes):")
df_new_snapshot.orderBy("product_id").show()

print("\nSUMMARY OF CHANGES:")
print("  Products with price changes: 5")
print("  Products with brand changes: 1")
print("  Products with no changes: 4")

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

print("Detecting changes...")
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

print("Comparison results:")
df_changes.orderBy("product_id").show(truncate=False)

# Separate changed vs unchanged
df_changed_products = df_changes.filter(F.col("has_changed"))
df_unchanged_products = df_changes.filter(~F.col("has_changed"))

changed_count = df_changed_products.count()
unchanged_count = df_unchanged_products.count()

print(f"\nCHANGE DETECTION RESULTS:")
print(f"  Changed products: {changed_count}")
print(f"  Unchanged products: {unchanged_count}")
print()

if changed_count > 0:
    print("Products that changed:")
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
    print("No changes detected. Skipping SCD Type 2 update.")
else:
    print(f"Processing {changed_count} changed products...")
    print()
    
    # STEP 4A: Close out old versions
    print("STEP 4A: Closing old versions")
    print("-" * 80)
    
    # Get current date for effective_to
    current_date = date.today()
    yesterday = current_date - timedelta(days=1)
    
    print(f"  Setting effective_to = {yesterday} for old versions")
    print(f"  Setting is_current_version = FALSE")
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
    
    print(f"  Updated {len(product_ids_to_close)} old versions")
    print()
    
    # STEP 4B: Insert new versions
    print("STEP 4B: Inserting new versions")
    print("-" * 80)
    
    # Get max surrogate key to continue sequence
    max_sk = spark.table("product_analytics.ecommerce.silver_dim_products") \
        .selectExpr("max(product_sk) as max_sk") \
        .collect()[0]['max_sk']
    
    print(f"  Current max product_sk: {max_sk:,}")
    print(f"  New surrogate keys will start from: {max_sk + 1:,}")
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
    
    print("  New version rows to insert:")
    df_new_versions.show(truncate=False)
    
    # Insert new versions
    df_new_versions.write.format("delta") \
        .mode("append") \
        .saveAsTable("product_analytics.ecommerce.silver_dim_products")
    
    print(f"  Inserted {df_new_versions.count()} new versions")
    print()
    
    print("="*80)
    print("SCD TYPE 2 UPDATE COMPLETE")
    print("="*80)
    print()
    
    # Verify the results
    print("VERIFICATION: Check one changed product")
    example_product_id = df_changed_products.first()["product_id"]
    
    df_verification = spark.table("product_analytics.ecommerce.silver_dim_products") \
        .filter(F.col("product_id") == example_product_id) \
        .orderBy("effective_from")
    
    print(f"\nProduct {example_product_id} history:")
    df_verification.select(
        "product_sk", "product_id", "price", "brand", 
        "effective_from", "effective_to", "is_current_version", "version_number"
    ).show(truncate=False)
    
    print("\nNotice:")
    print("  - Old version: effective_to updated to yesterday, is_current = FALSE")
    print("  - New version: new product_sk, version incremented, is_current = TRUE")
    print("  - Price/brand updated in new version")
    print("  - No data loss - full history preserved!")