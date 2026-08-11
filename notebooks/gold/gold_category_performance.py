# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Gold Layer: Category Performance
# MAGIC %md
# MAGIC # Gold Category Performance
# MAGIC
# MAGIC Category-level performance metrics for merchandising and inventory planning.
# MAGIC
# MAGIC **Table:** `product_analytics.ecommerce.gold_category_performance`  
# MAGIC **Grain:** One row per category (L1, L2, L3 hierarchy)  
# MAGIC **Update Strategy:** Full refresh daily
# MAGIC
# MAGIC **Metrics:**
# MAGIC - Event counts (views, carts, purchases) per category
# MAGIC - Revenue and product counts
# MAGIC - Conversion rates per category
# MAGIC - User engagement metrics
# MAGIC - Category rankings and share of business

# COMMAND ----------

# DBTITLE 1,Imports and Config
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Create Gold Category Performance Table
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE product_analytics.ecommerce.gold_category_performance
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH category_metrics AS (
# MAGIC     SELECT 
# MAGIC         -- Category identifiers
# MAGIC         c.category_sk,
# MAGIC         c.category_l1,
# MAGIC         c.category_l2,
# MAGIC         c.category_l3,
# MAGIC         c.category_full_path,
# MAGIC         c.category_depth,
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
# MAGIC         -- Product counts
# MAGIC         COUNT(DISTINCT f.product_sk) as unique_products,
# MAGIC         COUNT(DISTINCT CASE WHEN f.event_type = 'purchase' THEN f.product_sk END) as products_purchased,
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
# MAGIC         COUNT(DISTINCT f.date_sk) as days_active
# MAGIC         
# MAGIC     FROM product_analytics.ecommerce.fact_events f
# MAGIC     JOIN product_analytics.ecommerce.silver_dim_categories c
# MAGIC         ON f.category_sk = c.category_sk
# MAGIC     GROUP BY 
# MAGIC         c.category_sk, c.category_l1, c.category_l2, c.category_l3,
# MAGIC         c.category_full_path, c.category_depth
# MAGIC ),
# MAGIC category_conversions AS (
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
# MAGIC         -- Revenue per view
# MAGIC         ROUND(
# MAGIC             total_revenue / NULLIF(total_views, 0),
# MAGIC             2
# MAGIC         ) as revenue_per_view,
# MAGIC         
# MAGIC         -- Share of business (by L1 category)
# MAGIC         ROUND(
# MAGIC             total_revenue * 100.0 / SUM(total_revenue) OVER (PARTITION BY category_l1),
# MAGIC             2
# MAGIC         ) as revenue_share_within_l1,
# MAGIC         
# MAGIC         -- Overall rank by revenue
# MAGIC         DENSE_RANK() OVER (ORDER BY total_revenue DESC) as overall_rank_by_revenue,
# MAGIC         
# MAGIC         -- L1 category rank by revenue
# MAGIC         DENSE_RANK() OVER (
# MAGIC             PARTITION BY category_l1 
# MAGIC             ORDER BY total_revenue DESC
# MAGIC         ) as rank_within_l1_by_revenue
# MAGIC         
# MAGIC     FROM category_metrics
# MAGIC )
# MAGIC SELECT 
# MAGIC     *,
# MAGIC     current_timestamp() as created_at
# MAGIC FROM category_conversions;

# COMMAND ----------

# DBTITLE 1,Optimize Table
# MAGIC %sql
# MAGIC -- Optimize and Z-ORDER by category_sk and category_l1
# MAGIC OPTIMIZE product_analytics.ecommerce.gold_category_performance
# MAGIC ZORDER BY (category_sk, category_l1);

# COMMAND ----------

# DBTITLE 1,Enable Auto-Optimize
# MAGIC %sql
# MAGIC -- Enable Auto-Optimize for future writes
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_category_performance
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Add Table and Column Comments
# MAGIC %sql
# MAGIC -- Table comment
# MAGIC COMMENT ON TABLE product_analytics.ecommerce.gold_category_performance IS 
# MAGIC 'Category-level performance metrics for merchandising and inventory planning. Grain: one row per category. Full refresh daily.';
# MAGIC
# MAGIC -- Column comments
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_category_performance ALTER COLUMN category_sk COMMENT 'Surrogate key from dim_categories';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_category_performance ALTER COLUMN category_l1 COMMENT 'Category level 1 (top level)';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_category_performance ALTER COLUMN category_l2 COMMENT 'Category level 2';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_category_performance ALTER COLUMN category_l3 COMMENT 'Category level 3';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_category_performance ALTER COLUMN category_full_path COMMENT 'Full category hierarchy path';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_category_performance ALTER COLUMN category_depth COMMENT 'Depth level in category hierarchy';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_category_performance ALTER COLUMN total_events COMMENT 'Total events across all types';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_category_performance ALTER COLUMN total_views COMMENT 'Total view events';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_category_performance ALTER COLUMN total_carts COMMENT 'Total cart events';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_category_performance ALTER COLUMN total_removes COMMENT 'Total remove_from_cart events';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_category_performance ALTER COLUMN total_purchases COMMENT 'Total purchase events';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_category_performance ALTER COLUMN total_revenue COMMENT 'Total revenue from purchases';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_category_performance ALTER COLUMN avg_revenue_per_purchase COMMENT 'Average revenue per purchase';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_category_performance ALTER COLUMN total_quantity_sold COMMENT 'Total quantity sold';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_category_performance ALTER COLUMN unique_products COMMENT 'Count of distinct products in category';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_category_performance ALTER COLUMN products_purchased COMMENT 'Count of distinct products actually purchased';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_category_performance ALTER COLUMN unique_users_interacted COMMENT 'Count of distinct users with any interaction';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_category_performance ALTER COLUMN unique_viewers COMMENT 'Count of distinct users who viewed';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_category_performance ALTER COLUMN unique_carts COMMENT 'Count of distinct users who added to cart';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_category_performance ALTER COLUMN unique_purchasers COMMENT 'Count of distinct users who purchased';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_category_performance ALTER COLUMN first_event_date COMMENT 'Timestamp of first event';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_category_performance ALTER COLUMN last_event_date COMMENT 'Timestamp of last event';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_category_performance ALTER COLUMN days_active COMMENT 'Count of distinct dates with activity';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_category_performance ALTER COLUMN view_to_cart_rate COMMENT 'Conversion rate: views to cart (%)';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_category_performance ALTER COLUMN cart_to_purchase_rate COMMENT 'Conversion rate: cart to purchase (%)';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_category_performance ALTER COLUMN view_to_purchase_rate COMMENT 'Conversion rate: views to purchase (%)';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_category_performance ALTER COLUMN revenue_per_view COMMENT 'Revenue per view';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_category_performance ALTER COLUMN revenue_share_within_l1 COMMENT 'Revenue share within L1 category (%)';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_category_performance ALTER COLUMN overall_rank_by_revenue COMMENT 'Overall rank across all categories by revenue';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_category_performance ALTER COLUMN rank_within_l1_by_revenue COMMENT 'Rank within L1 category by revenue';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_category_performance ALTER COLUMN created_at COMMENT 'Timestamp when row was created';

# COMMAND ----------

# DBTITLE 1,Validation: Row Count and Sample
# MAGIC %sql
# MAGIC -- Check row count
# MAGIC SELECT COUNT(*) as total_categories FROM product_analytics.ecommerce.gold_category_performance;
# MAGIC
# MAGIC -- Sample top categories by revenue
# MAGIC SELECT 
# MAGIC     category_l1,
# MAGIC     category_l2,
# MAGIC     category_l3,
# MAGIC     unique_products,
# MAGIC     total_views,
# MAGIC     total_purchases,
# MAGIC     total_revenue,
# MAGIC     view_to_purchase_rate,
# MAGIC     overall_rank_by_revenue
# MAGIC FROM product_analytics.ecommerce.gold_category_performance
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# DBTITLE 1,Validation: Business Logic Checks
# MAGIC %sql
# MAGIC -- Check for nulls and metric ranges
# MAGIC SELECT 
# MAGIC     COUNT(*) as total_rows,
# MAGIC     SUM(CASE WHEN total_revenue IS NULL THEN 1 ELSE 0 END) as null_revenue,
# MAGIC     SUM(CASE WHEN category_l1 IS NULL THEN 1 ELSE 0 END) as null_l1,
# MAGIC     MIN(total_views) as min_views,
# MAGIC     MAX(total_views) as max_views,
# MAGIC     ROUND(AVG(total_views), 2) as avg_views,
# MAGIC     ROUND(AVG(view_to_purchase_rate), 2) as avg_conversion
# MAGIC FROM product_analytics.ecommerce.gold_category_performance;

# COMMAND ----------

# DBTITLE 1,Validation: L1 Category Rollup
# MAGIC %sql
# MAGIC -- Rollup by L1 category
# MAGIC SELECT 
# MAGIC     category_l1,
# MAGIC     COUNT(*) as subcategory_count,
# MAGIC     SUM(unique_products) as total_products,
# MAGIC     SUM(total_views) as total_views,
# MAGIC     SUM(total_purchases) as total_purchases,
# MAGIC     ROUND(SUM(total_revenue), 2) as total_revenue,
# MAGIC     ROUND(AVG(view_to_purchase_rate), 2) as avg_conversion
# MAGIC FROM product_analytics.ecommerce.gold_category_performance
# MAGIC GROUP BY category_l1
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# DBTITLE 1,Validation: Compare with Source
# MAGIC %sql
# MAGIC -- Cross-check totals against fact table
# MAGIC SELECT 
# MAGIC     'Gold Table' as source,
# MAGIC     COUNT(DISTINCT category_sk) as distinct_categories,
# MAGIC     SUM(total_views) as total_views,
# MAGIC     SUM(total_purchases) as total_purchases,
# MAGIC     ROUND(SUM(total_revenue), 2) as total_revenue
# MAGIC FROM product_analytics.ecommerce.gold_category_performance
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'Fact Table' as source,
# MAGIC     COUNT(DISTINCT category_sk) as distinct_categories,
# MAGIC     SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as total_views,
# MAGIC     SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as total_purchases,
# MAGIC     ROUND(SUM(revenue), 2) as total_revenue
# MAGIC FROM product_analytics.ecommerce.fact_events;

# COMMAND ----------

# DBTITLE 1,Sample Query: Top Subcategories by L1
# MAGIC %sql
# MAGIC -- Top 3 subcategories within each L1 category
# MAGIC WITH ranked_categories AS (
# MAGIC     SELECT 
# MAGIC         category_l1,
# MAGIC         category_l2,
# MAGIC         category_l3,
# MAGIC         total_revenue,
# MAGIC         total_purchases,
# MAGIC         view_to_purchase_rate,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY category_l1 
# MAGIC             ORDER BY total_revenue DESC
# MAGIC         ) as rn
# MAGIC     FROM product_analytics.ecommerce.gold_category_performance
# MAGIC     WHERE category_l2 IS NOT NULL
# MAGIC )
# MAGIC SELECT 
# MAGIC     category_l1,
# MAGIC     category_l2,
# MAGIC     category_l3,
# MAGIC     total_revenue,
# MAGIC     total_purchases,
# MAGIC     view_to_purchase_rate
# MAGIC FROM ranked_categories
# MAGIC WHERE rn <= 3
# MAGIC ORDER BY category_l1, rn;

# COMMAND ----------

# DBTITLE 1,Sample Query: High-Converting Categories
# MAGIC %sql
# MAGIC -- Categories with high conversion and significant volume
# MAGIC SELECT 
# MAGIC     category_l1,
# MAGIC     category_l2,
# MAGIC     category_l3,
# MAGIC     total_views,
# MAGIC     total_purchases,
# MAGIC     view_to_purchase_rate,
# MAGIC     total_revenue,
# MAGIC     revenue_per_view
# MAGIC FROM product_analytics.ecommerce.gold_category_performance
# MAGIC WHERE total_views >= 1000  -- Significant sample size
# MAGIC     AND view_to_purchase_rate >= 1.0  -- High conversion
# MAGIC ORDER BY view_to_purchase_rate DESC
# MAGIC LIMIT 20;

# COMMAND ----------

