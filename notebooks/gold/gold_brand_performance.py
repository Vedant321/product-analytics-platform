# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Gold Layer: Brand Performance
# MAGIC %md
# MAGIC # Gold Brand Performance
# MAGIC
# MAGIC Brand-level performance metrics for merchandising, vendor management, and brand strategy.
# MAGIC
# MAGIC **Table:** `product_analytics.ecommerce.gold_brand_performance`  
# MAGIC **Grain:** One row per brand  
# MAGIC **Update Strategy:** Full refresh daily
# MAGIC
# MAGIC **Metrics:**
# MAGIC - Event counts (views, carts, purchases) per brand
# MAGIC - Revenue and product portfolio size
# MAGIC - Conversion rates and engagement
# MAGIC - Brand rankings and market share
# MAGIC - Cross-category presence

# COMMAND ----------

# DBTITLE 1,Imports and Config
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Create Gold Brand Performance Table
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE product_analytics.ecommerce.gold_brand_performance
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH brand_metrics AS (
# MAGIC     SELECT 
# MAGIC         -- Brand identifier
# MAGIC         p.brand,
# MAGIC         
# MAGIC         -- Event counts
# MAGIC         COUNT(*) as total_events,
# MAGIC         SUM(CASE WHEN f.event_type = 'view' THEN 1 ELSE 0 END) as total_views,
# MAGIC         SUM(CASE WHEN f.event_type = 'cart' THEN 1 ELSE 0 END) as total_carts,
# MAGIC         SUM(CASE WHEN f.event_type = 'remove_from_cart' THEN 1 ELSE 0 END) as total_removes,
# MAGIC         SUM(CASE WHEN f.event_type = 'purchase' THEN 1 ELSE 0 END) as total_purchases,
# MAGIC         
# MAGIC         -- Revenue metrics
# MAGIC         ROUND(SUM(CASE WHEN f.event_type = 'purchase' THEN f.revenue ELSE 0 END), 2) as total_revenue,
# MAGIC         ROUND(AVG(CASE WHEN f.event_type = 'purchase' THEN f.revenue END), 2) as avg_revenue_per_purchase,
# MAGIC         SUM(CASE WHEN f.event_type = 'purchase' THEN f.quantity ELSE 0 END) as total_quantity_sold,
# MAGIC         
# MAGIC         -- Product portfolio
# MAGIC         COUNT(DISTINCT f.product_sk) as unique_products,
# MAGIC         COUNT(DISTINCT CASE WHEN f.event_type = 'purchase' THEN f.product_sk END) as products_purchased,
# MAGIC         
# MAGIC         -- Category presence
# MAGIC         COUNT(DISTINCT c.category_l1) as categories_l1_count,
# MAGIC         COUNT(DISTINCT c.category_sk) as total_categories,
# MAGIC         
# MAGIC         -- User engagement
# MAGIC         COUNT(DISTINCT f.user_sk) as unique_users_interacted,
# MAGIC         COUNT(DISTINCT CASE WHEN f.event_type = 'view' THEN f.user_sk END) as unique_viewers,
# MAGIC         COUNT(DISTINCT CASE WHEN f.event_type = 'cart' THEN f.user_sk END) as unique_carts,
# MAGIC         COUNT(DISTINCT CASE WHEN f.event_type = 'purchase' THEN f.user_sk END) as unique_purchasers,
# MAGIC         
# MAGIC         -- Temporal metrics
# MAGIC         MIN(f.event_time) as first_event_date,
# MAGIC         MAX(f.event_time) as last_event_date,
# MAGIC         COUNT(DISTINCT f.date_sk) as days_active,
# MAGIC         
# MAGIC         -- Price range
# MAGIC         MIN(p.price) as min_price,
# MAGIC         MAX(p.price) as max_price,
# MAGIC         ROUND(AVG(p.price), 2) as avg_price
# MAGIC         
# MAGIC     FROM product_analytics.ecommerce.fact_events f
# MAGIC     JOIN product_analytics.ecommerce.silver_dim_products p 
# MAGIC         ON f.product_sk = p.product_sk
# MAGIC     JOIN product_analytics.ecommerce.silver_dim_categories c
# MAGIC         ON f.category_sk = c.category_sk
# MAGIC     WHERE p.is_current_version = TRUE
# MAGIC     GROUP BY p.brand
# MAGIC ),
# MAGIC brand_conversions AS (
# MAGIC     SELECT 
# MAGIC         *,
# MAGIC         -- Conversion rates
# MAGIC         ROUND(
# MAGIC             total_carts * 100.0 / NULLIF(total_views, 0),
# MAGIC             2
# MAGIC         ) as view_to_cart_rate,
# MAGIC         ROUND(
# MAGIC             total_purchases * 100.0 / NULLIF(total_carts, 0),
# MAGIC             2
# MAGIC         ) as cart_to_purchase_rate,
# MAGIC         ROUND(
# MAGIC             total_purchases * 100.0 / NULLIF(total_views, 0),
# MAGIC             2
# MAGIC         ) as view_to_purchase_rate,
# MAGIC         
# MAGIC         -- Revenue per view (brand monetization efficiency)
# MAGIC         ROUND(
# MAGIC             total_revenue / NULLIF(total_views, 0),
# MAGIC             2
# MAGIC         ) as revenue_per_view,
# MAGIC         
# MAGIC         -- Market share
# MAGIC         ROUND(
# MAGIC             total_revenue * 100.0 / SUM(total_revenue) OVER (),
# MAGIC             2
# MAGIC         ) as revenue_market_share,
# MAGIC         
# MAGIC         -- Overall rank by revenue
# MAGIC         DENSE_RANK() OVER (ORDER BY total_revenue DESC) as rank_by_revenue,
# MAGIC         
# MAGIC         -- Rank by product portfolio size
# MAGIC         DENSE_RANK() OVER (ORDER BY unique_products DESC) as rank_by_product_count
# MAGIC         
# MAGIC     FROM brand_metrics
# MAGIC )
# MAGIC SELECT 
# MAGIC     *,
# MAGIC     current_timestamp() as created_at
# MAGIC FROM brand_conversions;

# COMMAND ----------

# DBTITLE 1,Optimize Table
# MAGIC %sql
# MAGIC -- Optimize and Z-ORDER by brand
# MAGIC OPTIMIZE product_analytics.ecommerce.gold_brand_performance
# MAGIC ZORDER BY (brand);

# COMMAND ----------

# DBTITLE 1,Enable Auto-Optimize
# MAGIC %sql
# MAGIC -- Enable Auto-Optimize for future writes
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_brand_performance
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Add Table and Column Comments
# MAGIC %sql
# MAGIC -- Table comment
# MAGIC COMMENT ON TABLE product_analytics.ecommerce.gold_brand_performance IS 
# MAGIC 'Brand-level performance metrics for merchandising, vendor management, and brand strategy. Grain: one row per brand. Full refresh daily.';
# MAGIC
# MAGIC -- Column comments
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_brand_performance ALTER COLUMN brand COMMENT 'Brand name';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_brand_performance ALTER COLUMN total_events COMMENT 'Total events across all types';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_brand_performance ALTER COLUMN total_views COMMENT 'Total view events';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_brand_performance ALTER COLUMN total_carts COMMENT 'Total cart events';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_brand_performance ALTER COLUMN total_removes COMMENT 'Total remove_from_cart events';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_brand_performance ALTER COLUMN total_purchases COMMENT 'Total purchase events';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_brand_performance ALTER COLUMN total_revenue COMMENT 'Total revenue from purchases';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_brand_performance ALTER COLUMN avg_revenue_per_purchase COMMENT 'Average revenue per purchase';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_brand_performance ALTER COLUMN total_quantity_sold COMMENT 'Total quantity sold';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_brand_performance ALTER COLUMN unique_products COMMENT 'Count of distinct products in brand portfolio';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_brand_performance ALTER COLUMN products_purchased COMMENT 'Count of distinct products actually purchased';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_brand_performance ALTER COLUMN categories_l1_count COMMENT 'Count of distinct L1 categories brand appears in';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_brand_performance ALTER COLUMN total_categories COMMENT 'Count of distinct categories at all levels';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_brand_performance ALTER COLUMN unique_users_interacted COMMENT 'Count of distinct users with any interaction';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_brand_performance ALTER COLUMN unique_viewers COMMENT 'Count of distinct users who viewed';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_brand_performance ALTER COLUMN unique_carts COMMENT 'Count of distinct users who added to cart';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_brand_performance ALTER COLUMN unique_purchasers COMMENT 'Count of distinct users who purchased';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_brand_performance ALTER COLUMN first_event_date COMMENT 'Timestamp of first event';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_brand_performance ALTER COLUMN last_event_date COMMENT 'Timestamp of last event';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_brand_performance ALTER COLUMN days_active COMMENT 'Count of distinct dates with activity';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_brand_performance ALTER COLUMN min_price COMMENT 'Minimum price across brand portfolio';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_brand_performance ALTER COLUMN max_price COMMENT 'Maximum price across brand portfolio';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_brand_performance ALTER COLUMN avg_price COMMENT 'Average price across brand portfolio';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_brand_performance ALTER COLUMN view_to_cart_rate COMMENT 'Conversion rate: views to cart (%)';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_brand_performance ALTER COLUMN cart_to_purchase_rate COMMENT 'Conversion rate: cart to purchase (%)';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_brand_performance ALTER COLUMN view_to_purchase_rate COMMENT 'Conversion rate: views to purchase (%)';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_brand_performance ALTER COLUMN revenue_per_view COMMENT 'Revenue per view (brand monetization efficiency)';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_brand_performance ALTER COLUMN revenue_market_share COMMENT 'Brand market share by revenue (%)';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_brand_performance ALTER COLUMN rank_by_revenue COMMENT 'Brand rank by total revenue';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_brand_performance ALTER COLUMN rank_by_product_count COMMENT 'Brand rank by product portfolio size';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_brand_performance ALTER COLUMN created_at COMMENT 'Timestamp when row was created';

# COMMAND ----------

# DBTITLE 1,Validation: Row Count and Sample
# MAGIC %sql
# MAGIC -- Check row count
# MAGIC SELECT COUNT(*) as total_brands FROM product_analytics.ecommerce.gold_brand_performance;
# MAGIC
# MAGIC -- Sample top brands by revenue
# MAGIC SELECT 
# MAGIC     brand,
# MAGIC     unique_products,
# MAGIC     categories_l1_count,
# MAGIC     total_views,
# MAGIC     total_purchases,
# MAGIC     total_revenue,
# MAGIC     view_to_purchase_rate,
# MAGIC     revenue_market_share,
# MAGIC     rank_by_revenue
# MAGIC FROM product_analytics.ecommerce.gold_brand_performance
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# DBTITLE 1,Validation: Business Logic Checks
# MAGIC %sql
# MAGIC -- Check for nulls and metric ranges
# MAGIC SELECT 
# MAGIC     COUNT(*) as total_rows,
# MAGIC     SUM(CASE WHEN total_revenue IS NULL THEN 1 ELSE 0 END) as null_revenue,
# MAGIC     SUM(CASE WHEN brand IS NULL THEN 1 ELSE 0 END) as null_brand,
# MAGIC     MIN(total_views) as min_views,
# MAGIC     MAX(total_views) as max_views,
# MAGIC     ROUND(AVG(total_views), 2) as avg_views,
# MAGIC     ROUND(AVG(view_to_purchase_rate), 2) as avg_conversion,
# MAGIC     ROUND(SUM(revenue_market_share), 2) as total_market_share
# MAGIC FROM product_analytics.ecommerce.gold_brand_performance;

# COMMAND ----------

# DBTITLE 1,Validation: Compare with Source
# MAGIC %sql
# MAGIC -- Cross-check totals against fact table
# MAGIC SELECT 
# MAGIC     'Gold Table' as source,
# MAGIC     COUNT(DISTINCT brand) as distinct_brands,
# MAGIC     SUM(total_views) as total_views,
# MAGIC     SUM(total_purchases) as total_purchases,
# MAGIC     ROUND(SUM(total_revenue), 2) as total_revenue
# MAGIC FROM product_analytics.ecommerce.gold_brand_performance
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'Fact Table' as source,
# MAGIC     COUNT(DISTINCT p.brand) as distinct_brands,
# MAGIC     SUM(CASE WHEN f.event_type = 'view' THEN 1 ELSE 0 END) as total_views,
# MAGIC     SUM(CASE WHEN f.event_type = 'purchase' THEN 1 ELSE 0 END) as total_purchases,
# MAGIC     ROUND(SUM(f.revenue), 2) as total_revenue
# MAGIC FROM product_analytics.ecommerce.fact_events f
# MAGIC JOIN product_analytics.ecommerce.silver_dim_products p ON f.product_sk = p.product_sk
# MAGIC WHERE p.is_current_version = TRUE;

# COMMAND ----------

# DBTITLE 1,Sample Query: Multi-Category Brands
# MAGIC %sql
# MAGIC -- Brands with presence across multiple L1 categories
# MAGIC SELECT 
# MAGIC     brand,
# MAGIC     categories_l1_count,
# MAGIC     unique_products,
# MAGIC     total_revenue,
# MAGIC     view_to_purchase_rate,
# MAGIC     revenue_market_share
# MAGIC FROM product_analytics.ecommerce.gold_brand_performance
# MAGIC WHERE categories_l1_count >= 2
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# DBTITLE 1,Sample Query: High-Performing Brands
# MAGIC %sql
# MAGIC -- Brands with high conversion and significant volume
# MAGIC SELECT 
# MAGIC     brand,
# MAGIC     unique_products,
# MAGIC     total_views,
# MAGIC     total_purchases,
# MAGIC     view_to_purchase_rate,
# MAGIC     total_revenue,
# MAGIC     revenue_per_view,
# MAGIC     revenue_market_share
# MAGIC FROM product_analytics.ecommerce.gold_brand_performance
# MAGIC WHERE total_views >= 10000  -- Significant sample size
# MAGIC     AND view_to_purchase_rate >= 1.5  -- High conversion
# MAGIC ORDER BY revenue_per_view DESC
# MAGIC LIMIT 20;

# COMMAND ----------

