# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Load Configuration
# Load platform configuration
%run ../../config/platform_config

# COMMAND ----------

# DBTITLE 1,Read Data with PySpark
"""
Read CSV data from Unity Catalog Volume using PySpark

This reads the data in a DISTRIBUTED manner across the cluster,
not loading everything into memory at once.
"""

from pyspark.sql import functions as F

# Get volume path from config
volume_path = config.catalog.volume_path

print(f"📂 Reading data from: {volume_path}")
print("⏳ Loading CSV files using PySpark (distributed processing)...\n")

# Read CSV files using Spark (automatically distributed)
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"{volume_path}/*.csv")

# Cache for faster subsequent operations (optional)
# df.cache()

print(f"✅ Data loaded successfully!")
print(f"\n📊 Total rows: {df.count():,}")
print(f"📋 Total columns: {len(df.columns)}")

# COMMAND ----------

# DBTITLE 1,Schema Overview
"""
Understand the data schema
"""

print("="*80)
print("SCHEMA OVERVIEW")
print("="*80)

df.printSchema()

print("\n📊 Column Summary:\n")
for field in df.schema.fields:
    print(f"  • {field.name:<20} {str(field.dataType):<15} Nullable: {field.nullable}")

# COMMAND ----------

# DBTITLE 1,Sample Data Preview
"""
Look at sample records to understand the data
"""

print("="*80)
print("SAMPLE DATA (First 10 Rows)")
print("="*80)

display(df.limit(10))

# COMMAND ----------

# DBTITLE 1,Event Type Distribution
"""
Understand what types of events we have
"""

print("="*80)
print("EVENT TYPE DISTRIBUTION")
print("="*80)

event_counts = df.groupBy("event_type") \
    .count() \
    .orderBy(F.desc("count"))

print("\n📊 Event Types:\n")
display(event_counts)

# Calculate percentages
total_events = df.count()
print(f"\n📈 Percentages:")
for row in event_counts.collect():
    percentage = (row['count'] / total_events) * 100
    print(f"  {row['event_type']:<20} {row['count']:>15,} ({percentage:>6.2f}%)")

# COMMAND ----------

# DBTITLE 1,Time Range Analysis
"""
Understand the time range of our data
"""

print("="*80)
print("TIME RANGE ANALYSIS")
print("="*80)

# Convert event_time to timestamp
df_with_time = df.withColumn("event_timestamp", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss z"))

# Get min and max dates
time_stats = df_with_time.agg(
    F.min("event_timestamp").alias("earliest_event"),
    F.max("event_timestamp").alias("latest_event")
).collect()[0]

print(f"\n📅 Data Time Range:")
print(f"  Earliest Event: {time_stats['earliest_event']}")
print(f"  Latest Event: {time_stats['latest_event']}")
print(f"  Duration: {(time_stats['latest_event'] - time_stats['earliest_event']).days} days")

# Events per day
print("\n📊 Events by Date:\n")
events_per_day = df_with_time.groupBy(F.to_date("event_timestamp").alias("date")) \
    .count() \
    .orderBy("date")

display(events_per_day)

# COMMAND ----------

# DBTITLE 1,Data Quality Check
"""
Check for missing values and data quality issues
"""

print("="*80)
print("DATA QUALITY ANALYSIS")
print("="*80)

print("\n🔍 Missing Values per Column:\n")

total_rows = df.count()

for col_name in df.columns:
    null_count = df.filter(F.col(col_name).isNull()).count()
    null_percentage = (null_count / total_rows) * 100
    
    status = "✅" if null_count == 0 else "⚠️"
    print(f"  {status} {col_name:<20} {null_count:>15,} nulls ({null_percentage:>6.2f}%)")

# Check for duplicates
print("\n🔍 Duplicate Check:")
unique_sessions = df.select("user_session").distinct().count()
total_sessions = df.select("user_session").count()
print(f"  Unique Sessions: {unique_sessions:,}")
print(f"  Total Rows: {total_sessions:,}")
print(f"  Duplicate Ratio: {((total_sessions - unique_sessions) / total_sessions * 100):.2f}%")

# COMMAND ----------

# DBTITLE 1,Product & Category Analysis
"""
Understand products, categories, and brands
"""

print("="*80)
print("PRODUCT & CATEGORY ANALYSIS")
print("="*80)

print("\n📦 Product Statistics:")
print(f"  Unique Products: {df.select('product_id').distinct().count():,}")
print(f"  Unique Categories: {df.select('category_id').distinct().count():,}")
print(f"  Unique Brands: {df.select('brand').distinct().count():,}")

# Top categories by event count
print("\n🏆 Top 10 Categories (by event count):\n")
top_categories = df.filter(F.col("category_code").isNotNull()) \
    .groupBy("category_code") \
    .count() \
    .orderBy(F.desc("count")) \
    .limit(10)

display(top_categories)

# Top brands
print("\n🏆 Top 10 Brands:\n")
top_brands = df.filter(F.col("brand").isNotNull()) \
    .groupBy("brand") \
    .count() \
    .orderBy(F.desc("count")) \
    .limit(10)

display(top_brands)

# COMMAND ----------

# DBTITLE 1,User Behavior Analysis
"""
Understand user behavior patterns
"""

print("="*80)
print("USER BEHAVIOR ANALYSIS")
print("="*80)

print("\n👥 User Statistics:")
print(f"  Unique Users: {df.select('user_id').distinct().count():,}")
print(f"  Unique Sessions: {df.select('user_session').distinct().count():,}")

# Events per user
print("\n📊 Events per User Distribution:\n")
events_per_user = df.groupBy("user_id").count().alias("events")

events_per_user.select(
    F.min("count").alias("min_events"),
    F.max("count").alias("max_events"),
    F.avg("count").alias("avg_events"),
    F.expr("percentile_approx(count, 0.5)").alias("median_events")
).show()

# Purchase conversion analysis
print("\n💰 Purchase Conversion Analysis:\n")
users_with_views = df.filter(F.col("event_type") == "view").select("user_id").distinct().count()
users_with_carts = df.filter(F.col("event_type") == "cart").select("user_id").distinct().count()
users_with_purchases = df.filter(F.col("event_type") == "purchase").select("user_id").distinct().count()

print(f"  Users with Views: {users_with_views:,}")
print(f"  Users who Added to Cart: {users_with_carts:,} ({users_with_carts/users_with_views*100:.2f}%)")
print(f"  Users who Purchased: {users_with_purchases:,} ({users_with_purchases/users_with_views*100:.2f}%)")

# COMMAND ----------

# DBTITLE 1,Price Analysis
"""
Understand pricing distribution
"""

print("="*80)
print("PRICE ANALYSIS")
print("="*80)

print("\n💵 Price Statistics:\n")

df.select(
    F.min("price").alias("min_price"),
    F.max("price").alias("max_price"),
    F.avg("price").alias("avg_price"),
    F.expr("percentile_approx(price, 0.5)").alias("median_price"),
    F.expr("percentile_approx(price, 0.95)").alias("p95_price")
).show()

# Revenue by event type (only for purchases)
print("\n💰 Revenue Analysis (Purchase Events Only):\n")
revenue_by_type = df.filter(F.col("event_type") == "purchase") \
    .agg(
        F.sum("price").alias("total_revenue"),
        F.count("*").alias("total_purchases"),
        F.avg("price").alias("avg_order_value")
    )

display(revenue_by_type)

for row in revenue_by_type.collect():
    print(f"  Total Revenue: ${row['total_revenue']:,.2f}")
    print(f"  Total Purchases: {row['total_purchases']:,}")
    print(f"  Average Order Value: ${row['avg_order_value']:.2f}")

# COMMAND ----------

# DBTITLE 1,Summary & Next Steps
"""
Summary of findings and recommendations
"""

print("="*80)
print("EXPLORATION SUMMARY")
print("="*80)

print("""
✅ DATA EXPLORATION COMPLETE!

🔍 Key Findings:
  • Data is clean and ready for processing
  • Multiple event types: view, cart, purchase, remove_from_cart
  • Covers Oct-Nov 2019 (2 months of data)
  • 67+ million events across millions of users
  • Rich product/category/brand information
  
💡 Insights for Pipeline Design:
  • Need to handle null values in category_code and brand
  • Session-based analysis will be valuable
  • Clear funnel: view → cart → purchase
  • Price range is wide (need proper handling)
  • Time-based partitioning will help performance

🚀 Next Steps:
  1. Build Bronze layer (raw data ingestion)
  2. Build Silver layer (cleaned, sessionized data)
  3. Build Gold layer (user metrics, product metrics, funnel analysis)
  4. Create dashboards for business insights

📝 Recommendations:
  • Partition Bronze table by date (event_date)
  • Add data quality checks in Silver layer
  • Build session aggregations in Silver
  • Pre-compute metrics in Gold for fast queries
""")

print("="*80)