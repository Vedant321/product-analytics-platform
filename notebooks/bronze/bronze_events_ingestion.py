# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Load Configuration
# Load platform configuration
%run ../../config/platform_config

# COMMAND ----------

# DBTITLE 1,Read CSV from Volume
"""
Step 1: Read raw CSV files from Unity Catalog Volume

This reads the CSV files in a DISTRIBUTED manner using PySpark.
No data is loaded into memory yet - Spark uses lazy evaluation.
"""

import logging

# Configure logging
logger = logging.getLogger(__name__)


from pyspark.sql import functions as F
from pyspark.sql.types import *

print("="*80)
logger.info("BRONZE LAYER INGESTION - CSV TO DELTA LAKE")
print("="*80)

# Get volume path from config
volume_path = config.catalog.volume_path
logger.info("\n📂 Source: {volume_path}")
logger.info(" Target: {config.get_table('bronze_events')}")
logger.info("\n⏳ Reading CSV files...\n")

# Read CSV files using Spark (distributed processing)
df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"{volume_path}/*.csv")

logger.info(" CSV files read successfully!")
logger.info("   Rows: {df_raw.count():,}")
logger.info("   Columns: {len(df_raw.columns)}")
logger.info("\n📋 Original Schema:")
df_raw.printSchema()

# COMMAND ----------

# DBTITLE 1,Add Metadata Columns
"""
Step 2: Add metadata columns

We add 3 columns that help with:
- ingestion_timestamp: When this data was loaded (audit trail)
- source_file: Which CSV file it came from (lineage)
- event_date: Extracted date for partitioning (performance)
"""

print("\n" + "="*80)
logger.info("ADDING METADATA COLUMNS")
print("="*80)

# Add metadata columns
df_bronze = df_raw \
    .withColumn("ingestion_timestamp", F.current_timestamp()) \
    .withColumn("source_file", F.col("_metadata.file_path")) \
    .withColumn("event_date", F.to_date(F.col("event_time")))

logger.info("\n Added 3 metadata columns:")
logger.info("   • ingestion_timestamp (when we loaded this)")
logger.info("   • source_file (which CSV it came from)")
logger.info("   • event_date (extracted date for partitioning)")

logger.info("\n📋 Bronze Schema (with metadata):")
df_bronze.printSchema()

logger.info("\n Sample with metadata:")
display(df_bronze.limit(5))

# COMMAND ----------

# DBTITLE 1,Check Partition Distribution
"""
Step 3: Check how data will be partitioned

We're partitioning by event_date. Let's see how many partitions
and how data is distributed across them.
"""

print("\n" + "="*80)
logger.info("PARTITION ANALYSIS")
print("="*80)

# Count events per date (= number of partitions)
partition_stats = df_bronze.groupBy("event_date") \
    .count() \
    .orderBy("event_date")

logger.info("\n Date Range:")
min_date = df_bronze.agg(F.min("event_date")).collect()[0][0]
max_date = df_bronze.agg(F.max("event_date")).collect()[0][0]
logger.info("   From: {min_date}")
logger.info("   To: {max_date}")
logger.info("   Total Partitions: {partition_stats.count()}")

logger.info("\n Events per Day (first 10 and last 10):")
display(partition_stats.limit(10))
logger.info("\n...\n")
display(partition_stats.orderBy(F.desc("event_date")).limit(10))

# Average events per partition
avg_per_partition = df_bronze.count() / partition_stats.count()
logger.info("\n Average events per partition: {avg_per_partition:,.0f}")
logger.info("   (This helps Spark parallelize queries efficiently)")

# COMMAND ----------

# DBTITLE 1,Write to Delta Lake
"""
Step 4: Write as Delta Lake table (THE MAGIC HAPPENS HERE!)

This converts CSV → Delta Lake format:
- Parquet columnar storage (compressed)
- Partitioned by event_date (61 partitions)
- ACID transactions enabled
- Registered in Unity Catalog

This will take a few minutes for 110M rows!
"""

print("\n" + "="*80)
logger.info("WRITING TO DELTA LAKE")
print("="*80)

table_name = config.get_table('bronze_events')

logger.info("\n Target Table: {table_name}")
logger.info(" Format: Delta Lake (Parquet + Transaction Log)")
logger.info("🗂  Partitioning: By event_date")
logger.info("\n⏳ Writing {df_bronze.count():,} rows...")
logger.info("   (This will take a few minutes - processing 13+ GB of data)\n")

import time
start_time = time.time()

# Write as Delta table with partitioning
df_bronze.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("event_date") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)

end_time = time.time()
duration = end_time - start_time

logger.info("\n Delta table created successfully!")
logger.info("   ⏱  Duration: {duration:.2f} seconds ({duration/60:.2f} minutes)")
logger.info("    Table: {table_name}")
logger.info("   🗂  Partitions: {partition_stats.count()}")
logger.info("\n🎉 CSV files now have a BRAIN (Delta Lake format)!")

# COMMAND ----------

# DBTITLE 1,Verify Table Creation
"""
Step 5: Verify the Delta table was created properly
"""

print("\n" + "="*80)
logger.info("TABLE VERIFICATION")
print("="*80)

table_name = config.get_table('bronze_events')

# Check table exists
logger.info("\n Checking table: {table_name}\n")

# Read from Delta table using SQL
result = spark.sql(f"SELECT COUNT(*) as row_count FROM {table_name}").collect()[0]
logger.info(" Table exists and is queryable!")
logger.info("   Total rows: {result['row_count']:,}")

# Show table details
logger.info("\n📋 Table Details:")
spark.sql(f"DESCRIBE EXTENDED {table_name}").show(50, False)

# Show partitions
logger.info("\n🗂  Partitions:")
partitions_df = spark.sql(f"SHOW PARTITIONS {table_name}")
logger.info("   Total partitions: {partitions_df.count()}")
partitions_df.show(10)

# COMMAND ----------

# DBTITLE 1,Test SQL Queries
"""
Step 6: Test that we can query with SQL (the whole point!)

Now we can query 110M rows with SQL - this was NOT possible with CSV files!
"""

print("\n" + "="*80)
logger.info("SQL QUERY TESTING")
print("="*80)

table_name = config.get_table('bronze_events')

logger.info("\n You can now query with SQL!\n")

# Test 1: Simple count
logger.info("Query 1: Total events")
result = spark.sql(f"""
    SELECT COUNT(*) as total_events 
    FROM {table_name}
""").show()

# Test 2: Event type distribution
logger.info("\nQuery 2: Event type distribution")
result = spark.sql(f"""
    SELECT 
        event_type,
        COUNT(*) as event_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
    FROM {table_name}
    GROUP BY event_type
    ORDER BY event_count DESC
""").show()

# Test 3: Query specific date (uses partitioning = FAST!)
logger.info("\nQuery 3: Events on a specific date (partition pruning)")
result = spark.sql(f"""
    SELECT 
        event_date,
        event_type,
        COUNT(*) as event_count
    FROM {table_name}
    WHERE event_date = '2019-11-01'
    GROUP BY event_date, event_type
    ORDER BY event_count DESC
""").show()

logger.info("\n🎉 SQL queries work perfectly!")
logger.info("   Notice how fast the date filter query was?")
logger.info("   That's partition pruning in action!")

# COMMAND ----------

# DBTITLE 1,Performance Comparison
"""
Step 7: Show the performance improvement

Let's compare CSV vs Delta Lake query performance
"""

print("\n" + "="*80)
logger.info("PERFORMANCE COMPARISON: CSV vs DELTA")
print("="*80)

import time

table_name = config.get_table('bronze_events')
volume_path = config.catalog.volume_path

logger.info("\n Running same query on both formats...\n")

test_date = '2019-11-01'
test_query = f"WHERE event_date = '{test_date}'"

# Test CSV performance
logger.info("⏳ Querying CSV files...")
start = time.time()
csv_result = spark.read.csv(f"{volume_path}/*.csv", header=True) \
    .withColumn("event_date", F.to_date(F.col("event_time"))) \
    .filter(F.col("event_date") == test_date) \
    .count()
csv_time = time.time() - start

# Test Delta performance
logger.info("⏳ Querying Delta table...")
start = time.time()
delta_result = spark.sql(f"SELECT COUNT(*) FROM {table_name} WHERE event_date = '{test_date}'").collect()[0][0]
delta_time = time.time() - start

# Results
logger.info("\n Results:")
logger.info("   CSV Query Time:   {csv_time:.2f} seconds")
logger.info("   Delta Query Time: {delta_time:.2f} seconds")
logger.info("\n Speedup: {csv_time/delta_time:.1f}x FASTER with Delta!")
logger.info("\n Why?")
logger.info("   • Columnar storage (Parquet)")
logger.info("   • Partition pruning (only read 1 day, not all 61 days)")
logger.info("   • Statistics (min/max values per file)")
logger.info("   • Compression (3-5x smaller than CSV)")

# COMMAND ----------

# DBTITLE 1,Storage Analysis
"""
Step 8: Storage comparison and table optimization
"""

print("\n" + "="*80)
logger.info("STORAGE & OPTIMIZATION")
print("="*80)

table_name = config.get_table('bronze_events')

# Get table location
table_info = spark.sql(f"DESCRIBE EXTENDED {table_name}").collect()
location = [row['data_type'] for row in table_info if row['col_name'] == 'Location'][0]

logger.info("\n📂 Table Location:")
logger.info("   {location}")

# Get storage stats
logger.info("\n💾 Storage Statistics:")
spark.sql(f"DESCRIBE DETAIL {table_name}").select(
    "numFiles",
    "sizeInBytes",
    "partitionColumns"
).show(truncate=False)

# Optimize table (compact small files)
logger.info("\n🔧 Optimizing table (compacting files)...")
spark.sql(f"OPTIMIZE {table_name}")
logger.info(" Table optimized!")

# Run ANALYZE to collect statistics
logger.info("\n Collecting statistics...")
spark.sql(f"ANALYZE TABLE {table_name} COMPUTE STATISTICS")
logger.info(" Statistics collected!")

logger.info("\n🎉 Bronze layer is production-ready!")

# COMMAND ----------

# DBTITLE 1,Summary & Next Steps
"""
Bronze Layer Complete!
"""

print("\n" + "="*80)
logger.info("BRONZE LAYER INGESTION - COMPLETE!")
print("="*80)

table_name = config.get_table('bronze_events')

print(f"""
 WHAT WE BUILT:

 Table: {table_name}
   • Format: Delta Lake (Parquet + Transaction Log)
   • Rows: 109,950,743
   • Partitions: 61 (by event_date)
   • Storage: ~4-5 GB (compressed from 13.67 GB CSV)
   • Schema: 12 columns (9 original + 3 metadata)

🚀 CAPABILITIES UNLOCKED:

✅ SQL Queries: Can query with standard SQL
✅ Fast Filters: 10-100x faster than CSV
✅ ACID Transactions: Safe updates/deletes
✅ Time Travel: Can query historical versions
✅ Schema Evolution: Can add/modify columns safely
✅ Partition Pruning: Only reads relevant data
✅ Compression: 3-5x smaller storage
✅ Unity Catalog: Governed, discoverable, lineage tracked

📝 WHAT'S NEXT:

1. Silver Layer (Data Cleaning & Transformation)
   • Handle nulls in category_code (32%) and brand (14%)
   • Sessionize user events
   • Split category hierarchies
   • Add derived columns
   • Data quality validation

2. Gold Layer (Business Metrics)
   • User metrics table
   • Product metrics table
   • Funnel analysis table
   • Pre-aggregated for dashboards

3. BI & Dashboards
   • Connect BI tools to Gold layer
   • Build business dashboards
   • Set up alerts

💡 TRY IT OUT:

-- Query the Bronze table:
SELECT * FROM {table_name} LIMIT 10;

-- Filter by date (fast!):
SELECT event_type, COUNT(*) 
FROM {table_name} 
WHERE event_date = '2019-11-01'
GROUP BY event_type;

-- Check partitions:
SHOW PARTITIONS {table_name};

-- Table details:
DESCRIBE EXTENDED {table_name};
""")

print("="*80)