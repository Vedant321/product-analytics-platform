# Databricks notebook source
# DBTITLE 1,Gold Layer: Daily Metrics
# MAGIC %md
# MAGIC # Gold Daily Metrics
# MAGIC
# MAGIC Core business metrics aggregated by date for dashboard consumption.
# MAGIC
# MAGIC **Table:** `product_analytics.ecommerce.gold_daily_metrics`  
# MAGIC **Grain:** One row per date  
# MAGIC **Update Strategy:** Full refresh (small table ~426 rows)
# MAGIC
# MAGIC **Metrics:**
# MAGIC - Daily Active Users (DAU)
# MAGIC - Events by type (views, carts, purchases)
# MAGIC - Revenue & AOV
# MAGIC - Conversion rates
# MAGIC - Product & category activity

# COMMAND ----------

# DBTITLE 1,Imports and Config
from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Create Gold Daily Metrics Table
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE product_analytics.ecommerce.gold_daily_metrics
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT 
# MAGIC     -- Date attributes
# MAGIC     d.date_key,
# MAGIC     d.full_date,
# MAGIC     d.year,
# MAGIC     d.quarter,
# MAGIC     d.month,
# MAGIC     d.month_name,
# MAGIC     d.day_of_month,
# MAGIC     d.day_name,
# MAGIC     d.day_of_week,
# MAGIC     d.is_weekend,
# MAGIC     
# MAGIC     -- User metrics
# MAGIC     COUNT(DISTINCT f.user_sk) as daily_active_users,
# MAGIC     
# MAGIC     -- Event counts by type
# MAGIC     COUNT(*) as total_events,
# MAGIC     SUM(CASE WHEN f.event_type = 'view' THEN 1 ELSE 0 END) as total_views,
# MAGIC     SUM(CASE WHEN f.event_type = 'cart' THEN 1 ELSE 0 END) as total_carts,
# MAGIC     SUM(CASE WHEN f.event_type = 'remove_from_cart' THEN 1 ELSE 0 END) as total_removes,
# MAGIC     SUM(CASE WHEN f.event_type = 'purchase' THEN 1 ELSE 0 END) as total_purchases,
# MAGIC     
# MAGIC     -- Revenue metrics
# MAGIC     ROUND(SUM(f.revenue), 2) as total_revenue,
# MAGIC     ROUND(AVG(CASE WHEN f.event_type = 'purchase' THEN f.revenue END), 2) as avg_order_value,
# MAGIC     SUM(CASE WHEN f.event_type = 'purchase' THEN f.quantity ELSE 0 END) as total_quantity_sold,
# MAGIC     
# MAGIC     -- Conversion metrics
# MAGIC     ROUND(
# MAGIC         SUM(CASE WHEN f.event_type = 'cart' THEN 1 ELSE 0 END) * 100.0 / 
# MAGIC         NULLIF(SUM(CASE WHEN f.event_type = 'view' THEN 1 ELSE 0 END), 0),
# MAGIC         2
# MAGIC     ) as view_to_cart_rate,
# MAGIC     
# MAGIC     ROUND(
# MAGIC         SUM(CASE WHEN f.event_type = 'purchase' THEN 1 ELSE 0 END) * 100.0 / 
# MAGIC         NULLIF(SUM(CASE WHEN f.event_type = 'cart' THEN 1 ELSE 0 END), 0),
# MAGIC         2
# MAGIC     ) as cart_to_purchase_rate,
# MAGIC     
# MAGIC     ROUND(
# MAGIC         SUM(CASE WHEN f.event_type = 'purchase' THEN 1 ELSE 0 END) * 100.0 / 
# MAGIC         NULLIF(SUM(CASE WHEN f.event_type = 'view' THEN 1 ELSE 0 END), 0),
# MAGIC         2
# MAGIC     ) as overall_conversion_rate,
# MAGIC     
# MAGIC     -- Product metrics
# MAGIC     COUNT(DISTINCT f.product_sk) as unique_products_viewed,
# MAGIC     COUNT(DISTINCT CASE WHEN f.event_type = 'purchase' THEN f.product_sk END) as unique_products_sold,
# MAGIC     
# MAGIC     -- Category metrics
# MAGIC     COUNT(DISTINCT f.category_sk) as unique_categories_active,
# MAGIC     
# MAGIC     -- User segment breakdown
# MAGIC     COUNT(DISTINCT CASE WHEN u.user_segment = 'power_user' THEN f.user_sk END) as dau_power_users,
# MAGIC     COUNT(DISTINCT CASE WHEN u.user_segment = 'engaged' THEN f.user_sk END) as dau_engaged,
# MAGIC     COUNT(DISTINCT CASE WHEN u.user_segment = 'casual' THEN f.user_sk END) as dau_casual,
# MAGIC     
# MAGIC     -- Load metadata
# MAGIC     current_timestamp() as created_at
# MAGIC     
# MAGIC FROM product_analytics.ecommerce.fact_events f
# MAGIC JOIN product_analytics.ecommerce.silver_dim_date d 
# MAGIC     ON f.date_sk = d.date_key
# MAGIC JOIN product_analytics.ecommerce.silver_dim_users u 
# MAGIC     ON f.user_sk = u.user_sk
# MAGIC GROUP BY 
# MAGIC     d.date_key, d.full_date, d.year, d.quarter, d.month, d.month_name,
# MAGIC     d.day_of_month, d.day_name, d.day_of_week, d.is_weekend
# MAGIC ORDER BY d.full_date;

# COMMAND ----------

# DBTITLE 1,Optimize Table
# MAGIC %sql
# MAGIC -- Optimize and Z-ORDER by date_key (primary query filter)
# MAGIC OPTIMIZE product_analytics.ecommerce.gold_daily_metrics
# MAGIC ZORDER BY (date_key);

# COMMAND ----------

# DBTITLE 1,Enable Auto-Optimize
# MAGIC %sql
# MAGIC -- Enable Auto-Optimize for future writes
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_daily_metrics
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Add Table and Column Comments
# MAGIC %sql
# MAGIC -- Table comment
# MAGIC COMMENT ON TABLE product_analytics.ecommerce.gold_daily_metrics IS 
# MAGIC 'Daily aggregated business metrics for dashboard consumption. Grain: one row per date. Full refresh daily.';
# MAGIC
# MAGIC -- Column comments
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_daily_metrics ALTER COLUMN date_key COMMENT 'Surrogate key from dim_date';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_daily_metrics ALTER COLUMN full_date COMMENT 'Calendar date (YYYY-MM-DD)';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_daily_metrics ALTER COLUMN daily_active_users COMMENT 'Count of distinct users with any event on this date (DAU)';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_daily_metrics ALTER COLUMN total_events COMMENT 'Total events across all types';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_daily_metrics ALTER COLUMN total_views COMMENT 'Count of view events';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_daily_metrics ALTER COLUMN total_carts COMMENT 'Count of cart events';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_daily_metrics ALTER COLUMN total_removes COMMENT 'Count of remove_from_cart events';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_daily_metrics ALTER COLUMN total_purchases COMMENT 'Count of purchase events';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_daily_metrics ALTER COLUMN total_revenue COMMENT 'Sum of revenue from purchase events';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_daily_metrics ALTER COLUMN avg_order_value COMMENT 'Average revenue per purchase event (AOV)';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_daily_metrics ALTER COLUMN total_quantity_sold COMMENT 'Total quantity sold across all purchases';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_daily_metrics ALTER COLUMN view_to_cart_rate COMMENT 'Conversion rate from views to carts (%)';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_daily_metrics ALTER COLUMN cart_to_purchase_rate COMMENT 'Conversion rate from carts to purchases (%)';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_daily_metrics ALTER COLUMN overall_conversion_rate COMMENT 'Overall conversion rate from views to purchases (%)';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_daily_metrics ALTER COLUMN unique_products_viewed COMMENT 'Count of distinct products with any interaction';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_daily_metrics ALTER COLUMN unique_products_sold COMMENT 'Count of distinct products purchased';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_daily_metrics ALTER COLUMN unique_categories_active COMMENT 'Count of distinct categories with activity';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_daily_metrics ALTER COLUMN dau_power_users COMMENT 'DAU count for power_user segment';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_daily_metrics ALTER COLUMN dau_engaged COMMENT 'DAU count for engaged segment';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_daily_metrics ALTER COLUMN dau_casual COMMENT 'DAU count for casual segment';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_daily_metrics ALTER COLUMN created_at COMMENT 'Timestamp when row was created';

# COMMAND ----------

# DBTITLE 1,Validation: Row Count and Sample
# MAGIC %sql
# MAGIC -- Check row count (should be ~426 days)
# MAGIC SELECT COUNT(*) as total_days FROM product_analytics.ecommerce.gold_daily_metrics;
# MAGIC
# MAGIC -- Sample recent days
# MAGIC SELECT 
# MAGIC     full_date,
# MAGIC     daily_active_users,
# MAGIC     total_events,
# MAGIC     total_purchases,
# MAGIC     total_revenue,
# MAGIC     avg_order_value,
# MAGIC     overall_conversion_rate
# MAGIC FROM product_analytics.ecommerce.gold_daily_metrics
# MAGIC ORDER BY full_date DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Validation: Business Logic Checks
# MAGIC %sql
# MAGIC -- Check for any nulls in key metrics
# MAGIC SELECT 
# MAGIC     COUNT(*) as total_rows,
# MAGIC     SUM(CASE WHEN daily_active_users IS NULL THEN 1 ELSE 0 END) as null_dau,
# MAGIC     SUM(CASE WHEN total_revenue IS NULL THEN 1 ELSE 0 END) as null_revenue,
# MAGIC     SUM(CASE WHEN overall_conversion_rate IS NULL THEN 1 ELSE 0 END) as null_conversion
# MAGIC FROM product_analytics.ecommerce.gold_daily_metrics;
# MAGIC
# MAGIC -- Check metric ranges (spot check reasonableness)
# MAGIC SELECT 
# MAGIC     MIN(daily_active_users) as min_dau,
# MAGIC     MAX(daily_active_users) as max_dau,
# MAGIC     AVG(daily_active_users) as avg_dau,
# MAGIC     MIN(total_revenue) as min_revenue,
# MAGIC     MAX(total_revenue) as max_revenue,
# MAGIC     AVG(overall_conversion_rate) as avg_conversion_rate
# MAGIC FROM product_analytics.ecommerce.gold_daily_metrics;

# COMMAND ----------

# DBTITLE 1,Validation: Compare with Source
# MAGIC %sql
# MAGIC -- Cross-check total events against fact table
# MAGIC SELECT 
# MAGIC     'Gold Table' as source,
# MAGIC     SUM(total_events) as total_events,
# MAGIC     SUM(total_purchases) as total_purchases,
# MAGIC     ROUND(SUM(total_revenue), 2) as total_revenue
# MAGIC FROM product_analytics.ecommerce.gold_daily_metrics
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'Fact Table' as source,
# MAGIC     COUNT(*) as total_events,
# MAGIC     SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as total_purchases,
# MAGIC     ROUND(SUM(revenue), 2) as total_revenue
# MAGIC FROM product_analytics.ecommerce.fact_events;

# COMMAND ----------

# DBTITLE 1,Sample Query: Revenue Trend
# MAGIC %sql
# MAGIC -- Revenue trend over time
# MAGIC SELECT 
# MAGIC     year,
# MAGIC     month_name,
# MAGIC     SUM(total_purchases) as purchases,
# MAGIC     ROUND(SUM(total_revenue), 2) as revenue,
# MAGIC     ROUND(AVG(avg_order_value), 2) as avg_aov,
# MAGIC     ROUND(AVG(overall_conversion_rate), 2) as avg_conversion
# MAGIC FROM product_analytics.ecommerce.gold_daily_metrics
# MAGIC GROUP BY year, month, month_name
# MAGIC ORDER BY year, month;

# COMMAND ----------

# DBTITLE 1,Sample Query: Weekend vs Weekday Performance
# MAGIC %sql
# MAGIC -- Compare weekend vs weekday performance
# MAGIC SELECT 
# MAGIC     CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type,
# MAGIC     COUNT(*) as days,
# MAGIC     ROUND(AVG(daily_active_users), 0) as avg_dau,
# MAGIC     ROUND(AVG(total_purchases), 0) as avg_purchases,
# MAGIC     ROUND(AVG(total_revenue), 2) as avg_revenue,
# MAGIC     ROUND(AVG(overall_conversion_rate), 2) as avg_conversion_rate
# MAGIC FROM product_analytics.ecommerce.gold_daily_metrics
# MAGIC GROUP BY is_weekend
# MAGIC ORDER BY is_weekend;

# COMMAND ----------

