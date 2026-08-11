# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Gold Layer: Product Performance
# MAGIC %md
# MAGIC # Gold Product Performance
# MAGIC
# MAGIC Product-level performance metrics for inventory decisions and product analytics.
# MAGIC
# MAGIC **Table:** `product_analytics.ecommerce.gold_product_performance`  
# MAGIC **Grain:** One row per product  
# MAGIC **Update Strategy:** Full refresh daily (table ~89K rows)
# MAGIC
# MAGIC **Note:** Filters to `is_current_version = TRUE` products only. Historical product versions in fact_events are excluded.
# MAGIC
# MAGIC **Metrics:**
# MAGIC - Event counts (views, carts, purchases) per product
# MAGIC - Conversion rates (view→cart, cart→purchase)
# MAGIC - Revenue and pricing metrics
# MAGIC - User engagement (unique viewers/purchasers)
# MAGIC - Product ranking within category

# COMMAND ----------

# DBTITLE 1,Imports and Config
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Create Gold Product Performance Table
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE product_analytics.ecommerce.gold_product_performance
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH product_metrics AS (
# MAGIC     SELECT 
# MAGIC         -- Product identifiers
# MAGIC         p.product_sk,
# MAGIC         p.product_id,
# MAGIC         p.brand,
# MAGIC         c.category_full_path as category_code,
# MAGIC         c.category_l1,
# MAGIC         c.category_l2,
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
# MAGIC         -- Pricing (current price from dimension - using most recent)
# MAGIC         MAX(p.price) as current_price,
# MAGIC         MIN(p.price) as min_price_seen,
# MAGIC         MAX(p.price) as max_price_seen
# MAGIC         
# MAGIC     FROM product_analytics.ecommerce.fact_events f
# MAGIC     JOIN product_analytics.ecommerce.silver_dim_products p 
# MAGIC         ON f.product_sk = p.product_sk
# MAGIC     JOIN product_analytics.ecommerce.silver_dim_categories c
# MAGIC         ON f.category_sk = c.category_sk
# MAGIC     WHERE p.is_current_version = TRUE  -- Only current product version
# MAGIC     GROUP BY 
# MAGIC         p.product_sk, p.product_id, p.brand, 
# MAGIC         c.category_full_path, c.category_l1, c.category_l2
# MAGIC ),
# MAGIC product_conversions AS (
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
# MAGIC         -- Revenue per view (product monetization efficiency)
# MAGIC         ROUND(
# MAGIC             total_revenue / NULLIF(total_views, 0),
# MAGIC             2
# MAGIC         ) as revenue_per_view,
# MAGIC         
# MAGIC         -- Category rank by revenue
# MAGIC         DENSE_RANK() OVER (
# MAGIC             PARTITION BY category_l1 
# MAGIC             ORDER BY total_revenue DESC
# MAGIC         ) as rank_in_category_by_revenue,
# MAGIC         
# MAGIC         -- Category rank by purchases
# MAGIC         DENSE_RANK() OVER (
# MAGIC             PARTITION BY category_l1 
# MAGIC             ORDER BY total_purchases DESC
# MAGIC         ) as rank_in_category_by_purchases
# MAGIC         
# MAGIC     FROM product_metrics
# MAGIC )
# MAGIC SELECT 
# MAGIC     *,
# MAGIC     current_timestamp() as created_at
# MAGIC FROM product_conversions;

# COMMAND ----------

# DBTITLE 1,Optimize Table
# MAGIC %sql
# MAGIC -- Optimize and Z-ORDER by product_sk and category_l1 (primary query filters)
# MAGIC OPTIMIZE product_analytics.ecommerce.gold_product_performance
# MAGIC ZORDER BY (product_sk, category_l1);

# COMMAND ----------

# DBTITLE 1,Enable Auto-Optimize
# MAGIC %sql
# MAGIC -- Enable Auto-Optimize for future writes
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_product_performance
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Add Table and Column Comments
# MAGIC %sql
# MAGIC -- Table comment
# MAGIC COMMENT ON TABLE product_analytics.ecommerce.gold_product_performance IS 
# MAGIC 'Product-level performance metrics for inventory decisions and product analytics. Grain: one row per product. Full refresh daily.';
# MAGIC
# MAGIC -- Column comments
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_product_performance ALTER COLUMN product_sk COMMENT 'Surrogate key from dim_products';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_product_performance ALTER COLUMN product_id COMMENT 'Natural product identifier';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_product_performance ALTER COLUMN brand COMMENT 'Product brand';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_product_performance ALTER COLUMN category_code COMMENT 'Full category code path';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_product_performance ALTER COLUMN category_l1 COMMENT 'Category level 1 (top level)';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_product_performance ALTER COLUMN category_l2 COMMENT 'Category level 2';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_product_performance ALTER COLUMN total_events COMMENT 'Total events across all types';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_product_performance ALTER COLUMN total_views COMMENT 'Total view events';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_product_performance ALTER COLUMN total_carts COMMENT 'Total cart events';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_product_performance ALTER COLUMN total_removes COMMENT 'Total remove_from_cart events';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_product_performance ALTER COLUMN total_purchases COMMENT 'Total purchase events';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_product_performance ALTER COLUMN total_revenue COMMENT 'Total revenue from all purchases';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_product_performance ALTER COLUMN avg_revenue_per_purchase COMMENT 'Average revenue per purchase event';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_product_performance ALTER COLUMN total_quantity_sold COMMENT 'Total quantity sold';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_product_performance ALTER COLUMN unique_users_interacted COMMENT 'Count of distinct users with any interaction';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_product_performance ALTER COLUMN unique_viewers COMMENT 'Count of distinct users who viewed';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_product_performance ALTER COLUMN unique_carts COMMENT 'Count of distinct users who added to cart';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_product_performance ALTER COLUMN unique_purchasers COMMENT 'Count of distinct users who purchased';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_product_performance ALTER COLUMN first_event_date COMMENT 'Timestamp of first event (any type)';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_product_performance ALTER COLUMN last_event_date COMMENT 'Timestamp of last event (any type)';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_product_performance ALTER COLUMN days_active COMMENT 'Count of distinct dates with activity';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_product_performance ALTER COLUMN current_price COMMENT 'Current product price';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_product_performance ALTER COLUMN min_price_seen COMMENT 'Minimum price observed';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_product_performance ALTER COLUMN max_price_seen COMMENT 'Maximum price observed';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_product_performance ALTER COLUMN view_to_cart_rate COMMENT 'Conversion rate: views to cart (%)';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_product_performance ALTER COLUMN cart_to_purchase_rate COMMENT 'Conversion rate: cart to purchase (%)';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_product_performance ALTER COLUMN view_to_purchase_rate COMMENT 'Conversion rate: views to purchase (%)';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_product_performance ALTER COLUMN revenue_per_view COMMENT 'Revenue per view (product monetization efficiency)';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_product_performance ALTER COLUMN rank_in_category_by_revenue COMMENT 'Product rank within category L1 by total revenue';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_product_performance ALTER COLUMN rank_in_category_by_purchases COMMENT 'Product rank within category L1 by total purchases';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_product_performance ALTER COLUMN created_at COMMENT 'Timestamp when row was created';

# COMMAND ----------

# DBTITLE 1,Validation: Row Count and Sample
# MAGIC %sql
# MAGIC -- Check row count (should be ~207K products)
# MAGIC SELECT COUNT(*) as total_products FROM product_analytics.ecommerce.gold_product_performance;
# MAGIC
# MAGIC -- Sample top products by revenue
# MAGIC SELECT 
# MAGIC     product_id,
# MAGIC     brand,
# MAGIC     category_l1,
# MAGIC     total_views,
# MAGIC     total_purchases,
# MAGIC     total_revenue,
# MAGIC     view_to_purchase_rate,
# MAGIC     rank_in_category_by_revenue
# MAGIC FROM product_analytics.ecommerce.gold_product_performance
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# DBTITLE 1,Validation: Business Logic Checks
# MAGIC %sql
# MAGIC -- Check for any nulls in key metrics
# MAGIC SELECT 
# MAGIC     COUNT(*) as total_rows,
# MAGIC     SUM(CASE WHEN total_revenue IS NULL THEN 1 ELSE 0 END) as null_revenue,
# MAGIC     SUM(CASE WHEN view_to_purchase_rate IS NULL THEN 1 ELSE 0 END) as null_conversion,
# MAGIC     SUM(CASE WHEN brand IS NULL THEN 1 ELSE 0 END) as null_brand
# MAGIC FROM product_analytics.ecommerce.gold_product_performance;
# MAGIC
# MAGIC -- Check metric ranges
# MAGIC SELECT 
# MAGIC     MIN(total_views) as min_views,
# MAGIC     MAX(total_views) as max_views,
# MAGIC     ROUND(AVG(total_views), 2) as avg_views,
# MAGIC     MIN(total_revenue) as min_revenue,
# MAGIC     MAX(total_revenue) as max_revenue,
# MAGIC     ROUND(AVG(total_revenue), 2) as avg_revenue,
# MAGIC     ROUND(AVG(view_to_purchase_rate), 2) as avg_conversion
# MAGIC FROM product_analytics.ecommerce.gold_product_performance;

# COMMAND ----------

# DBTITLE 1,Validation: Category Distribution
# MAGIC %sql
# MAGIC -- Check category distribution
# MAGIC SELECT 
# MAGIC     category_l1,
# MAGIC     COUNT(*) as product_count,
# MAGIC     SUM(total_views) as total_views,
# MAGIC     SUM(total_purchases) as total_purchases,
# MAGIC     ROUND(SUM(total_revenue), 2) as total_revenue,
# MAGIC     ROUND(AVG(view_to_purchase_rate), 2) as avg_conversion
# MAGIC FROM product_analytics.ecommerce.gold_product_performance
# MAGIC GROUP BY category_l1
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# DBTITLE 1,Validation: Compare with Source
# MAGIC %sql
# MAGIC -- Cross-check totals against fact table
# MAGIC SELECT 
# MAGIC     'Gold Table' as source,
# MAGIC     COUNT(DISTINCT product_sk) as distinct_products,
# MAGIC     SUM(total_views) as total_views,
# MAGIC     SUM(total_purchases) as total_purchases,
# MAGIC     ROUND(SUM(total_revenue), 2) as total_revenue
# MAGIC FROM product_analytics.ecommerce.gold_product_performance
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'Fact Table' as source,
# MAGIC     COUNT(DISTINCT f.product_sk) as distinct_products,
# MAGIC     SUM(CASE WHEN f.event_type = 'view' THEN 1 ELSE 0 END) as total_views,
# MAGIC     SUM(CASE WHEN f.event_type = 'purchase' THEN 1 ELSE 0 END) as total_purchases,
# MAGIC     ROUND(SUM(f.revenue), 2) as total_revenue
# MAGIC FROM product_analytics.ecommerce.fact_events f
# MAGIC JOIN product_analytics.ecommerce.silver_dim_products p ON f.product_sk = p.product_sk
# MAGIC WHERE p.is_current_version = TRUE;

# COMMAND ----------

# DBTITLE 1,Sample Query: Top Products by Category
# MAGIC %sql
# MAGIC -- Top 5 products in each category by revenue
# MAGIC WITH ranked_products AS (
# MAGIC     SELECT 
# MAGIC         category_l1,
# MAGIC         product_id,
# MAGIC         brand,
# MAGIC         total_revenue,
# MAGIC         total_purchases,
# MAGIC         view_to_purchase_rate,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY category_l1 
# MAGIC             ORDER BY total_revenue DESC
# MAGIC         ) as rn
# MAGIC     FROM product_analytics.ecommerce.gold_product_performance
# MAGIC     WHERE total_revenue > 0
# MAGIC )
# MAGIC SELECT 
# MAGIC     category_l1,
# MAGIC     product_id,
# MAGIC     brand,
# MAGIC     total_revenue,
# MAGIC     total_purchases,
# MAGIC     view_to_purchase_rate
# MAGIC FROM ranked_products
# MAGIC WHERE rn <= 5
# MAGIC ORDER BY category_l1, rn;

# COMMAND ----------

# DBTITLE 1,Sample Query: High-Converting Products
# MAGIC %sql
# MAGIC -- Products with high conversion rates and significant views
# MAGIC SELECT 
# MAGIC     product_id,
# MAGIC     brand,
# MAGIC     category_l1,
# MAGIC     total_views,
# MAGIC     total_purchases,
# MAGIC     view_to_purchase_rate,
# MAGIC     total_revenue,
# MAGIC     revenue_per_view
# MAGIC FROM product_analytics.ecommerce.gold_product_performance
# MAGIC WHERE total_views >= 100  -- Significant sample size
# MAGIC     AND view_to_purchase_rate >= 3.0  -- High conversion
# MAGIC ORDER BY revenue_per_view DESC
# MAGIC LIMIT 20;

# COMMAND ----------

