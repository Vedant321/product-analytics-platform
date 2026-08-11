# Databricks notebook source
# MAGIC %md
# MAGIC # Gold User Metrics
# MAGIC
# MAGIC Lifetime metrics aggregated by user for customer segmentation and retention analysis.
# MAGIC
# MAGIC **Table:** `product_analytics.ecommerce.gold_user_metrics`  
# MAGIC **Grain:** One row per user  
# MAGIC **Update Strategy:** Full refresh daily (large table ~5.3M rows)
# MAGIC
# MAGIC **Metrics:**
# MAGIC - Lifetime activity (events, purchases, revenue)
# MAGIC - First/last event dates, days active
# MAGIC - RFM scores (Recency, Frequency, Monetary)
# MAGIC - User segment
# MAGIC - Lifetime value (LTV)

# COMMAND ----------

from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE product_analytics.ecommerce.gold_user_metrics
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT 
# MAGIC     -- User identifiers
# MAGIC     u.user_sk,
# MAGIC     u.user_id,
# MAGIC     u.user_segment,
# MAGIC     
# MAGIC     -- Activity metrics
# MAGIC     COUNT(*) as total_events,
# MAGIC     SUM(CASE WHEN f.event_type = 'view' THEN 1 ELSE 0 END) as total_views,
# MAGIC     SUM(CASE WHEN f.event_type = 'cart' THEN 1 ELSE 0 END) as total_carts,
# MAGIC     SUM(CASE WHEN f.event_type = 'remove_from_cart' THEN 1 ELSE 0 END) as total_removes,
# MAGIC     SUM(CASE WHEN f.event_type = 'purchase' THEN 1 ELSE 0 END) as total_purchases,
# MAGIC     
# MAGIC     -- Revenue metrics (Monetary)
# MAGIC     ROUND(SUM(f.revenue), 2) as lifetime_revenue,
# MAGIC     ROUND(AVG(CASE WHEN f.event_type = 'purchase' THEN f.revenue END), 2) as avg_order_value,
# MAGIC     SUM(CASE WHEN f.event_type = 'purchase' THEN f.quantity ELSE 0 END) as total_quantity_purchased,
# MAGIC     
# MAGIC     -- Temporal metrics
# MAGIC     MIN(f.event_time) as first_event_date,
# MAGIC     MAX(f.event_time) as last_event_date,
# MAGIC     DATEDIFF(MAX(f.event_time), MIN(f.event_time)) as days_between_first_last,
# MAGIC     COUNT(DISTINCT f.date_sk) as days_active,
# MAGIC     
# MAGIC     -- Recency (days since last event, as of '2019-11-30' - last date in dataset)
# MAGIC     DATEDIFF(CAST('2019-11-30' AS DATE), MAX(DATE(f.event_time))) as recency_days,
# MAGIC     
# MAGIC     -- Frequency (same as total_purchases, but explicit for RFM)
# MAGIC     SUM(CASE WHEN f.event_type = 'purchase' THEN 1 ELSE 0 END) as frequency_purchases,
# MAGIC     
# MAGIC     -- Conversion metrics
# MAGIC     ROUND(
# MAGIC         SUM(CASE WHEN f.event_type = 'purchase' THEN 1 ELSE 0 END) * 100.0 / 
# MAGIC         NULLIF(SUM(CASE WHEN f.event_type = 'view' THEN 1 ELSE 0 END), 0),
# MAGIC         2
# MAGIC     ) as personal_conversion_rate,
# MAGIC     
# MAGIC     -- Product engagement
# MAGIC     COUNT(DISTINCT f.product_sk) as unique_products_viewed,
# MAGIC     COUNT(DISTINCT CASE WHEN f.event_type = 'purchase' THEN f.product_sk END) as unique_products_purchased,
# MAGIC     
# MAGIC     -- Category engagement
# MAGIC     COUNT(DISTINCT f.category_sk) as unique_categories_browsed,
# MAGIC     
# MAGIC     -- Load metadata
# MAGIC     current_timestamp() as created_at
# MAGIC     
# MAGIC FROM product_analytics.ecommerce.fact_events f
# MAGIC JOIN product_analytics.ecommerce.silver_dim_users u 
# MAGIC     ON f.user_sk = u.user_sk
# MAGIC WHERE u.is_current_version = TRUE  -- Only current user version for segment
# MAGIC GROUP BY 
# MAGIC     u.user_sk, u.user_id, u.user_segment;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize and Z-ORDER by user_sk (primary query filter)
# MAGIC OPTIMIZE product_analytics.ecommerce.gold_user_metrics
# MAGIC ZORDER BY (user_sk);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enable Auto-Optimize for future writes
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_user_metrics
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Table comment
# MAGIC COMMENT ON TABLE product_analytics.ecommerce.gold_user_metrics IS 
# MAGIC 'User lifetime metrics for customer segmentation and retention analysis. Grain: one row per user. Full refresh daily.';
# MAGIC
# MAGIC -- Column comments
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_user_metrics ALTER COLUMN user_sk COMMENT 'Surrogate key from dim_users';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_user_metrics ALTER COLUMN user_id COMMENT 'Natural user identifier';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_user_metrics ALTER COLUMN user_segment COMMENT 'Current user segment (power_user, engaged, casual)';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_user_metrics ALTER COLUMN total_events COMMENT 'Lifetime total events across all types';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_user_metrics ALTER COLUMN total_views COMMENT 'Lifetime view events';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_user_metrics ALTER COLUMN total_carts COMMENT 'Lifetime cart events';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_user_metrics ALTER COLUMN total_removes COMMENT 'Lifetime remove_from_cart events';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_user_metrics ALTER COLUMN total_purchases COMMENT 'Lifetime purchase events';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_user_metrics ALTER COLUMN lifetime_revenue COMMENT 'Total revenue from all purchases (LTV - Lifetime Value)';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_user_metrics ALTER COLUMN avg_order_value COMMENT 'Average revenue per purchase event (AOV)';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_user_metrics ALTER COLUMN total_quantity_purchased COMMENT 'Total quantity purchased across all orders';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_user_metrics ALTER COLUMN first_event_date COMMENT 'Timestamp of first event (any type)';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_user_metrics ALTER COLUMN last_event_date COMMENT 'Timestamp of last event (any type)';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_user_metrics ALTER COLUMN days_between_first_last COMMENT 'Days between first and last event';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_user_metrics ALTER COLUMN days_active COMMENT 'Count of distinct dates with activity';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_user_metrics ALTER COLUMN recency_days COMMENT 'RFM Recency: Days since last event (as of 2019-11-30)';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_user_metrics ALTER COLUMN frequency_purchases COMMENT 'RFM Frequency: Total purchase count';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_user_metrics ALTER COLUMN personal_conversion_rate COMMENT 'Personal conversion rate: purchases / views (%)';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_user_metrics ALTER COLUMN unique_products_viewed COMMENT 'Count of distinct products viewed';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_user_metrics ALTER COLUMN unique_products_purchased COMMENT 'Count of distinct products purchased';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_user_metrics ALTER COLUMN unique_categories_browsed COMMENT 'Count of distinct categories browsed';
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_user_metrics ALTER COLUMN created_at COMMENT 'Timestamp when row was created';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check row count (should be ~5.3M users)
# MAGIC SELECT COUNT(*) as total_users FROM product_analytics.ecommerce.gold_user_metrics;
# MAGIC
# MAGIC -- Sample high-value users
# MAGIC SELECT 
# MAGIC     user_id,
# MAGIC     user_segment,
# MAGIC     total_events,
# MAGIC     total_purchases,
# MAGIC     lifetime_revenue,
# MAGIC     recency_days,
# MAGIC     frequency_purchases,
# MAGIC     days_active
# MAGIC FROM product_analytics.ecommerce.gold_user_metrics
# MAGIC ORDER BY lifetime_revenue DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for any nulls in key metrics
# MAGIC SELECT 
# MAGIC     COUNT(*) as total_rows,
# MAGIC     SUM(CASE WHEN lifetime_revenue IS NULL THEN 1 ELSE 0 END) as null_revenue,
# MAGIC     SUM(CASE WHEN recency_days IS NULL THEN 1 ELSE 0 END) as null_recency,
# MAGIC     SUM(CASE WHEN first_event_date IS NULL THEN 1 ELSE 0 END) as null_first_event
# MAGIC FROM product_analytics.ecommerce.gold_user_metrics;
# MAGIC
# MAGIC -- Check metric ranges (spot check reasonableness)
# MAGIC SELECT 
# MAGIC     MIN(total_events) as min_events,
# MAGIC     MAX(total_events) as max_events,
# MAGIC     ROUND(AVG(total_events), 2) as avg_events,
# MAGIC     MIN(lifetime_revenue) as min_ltv,
# MAGIC     MAX(lifetime_revenue) as max_ltv,
# MAGIC     ROUND(AVG(lifetime_revenue), 2) as avg_ltv,
# MAGIC     MIN(days_active) as min_days_active,
# MAGIC     MAX(days_active) as max_days_active,
# MAGIC     ROUND(AVG(days_active), 2) as avg_days_active
# MAGIC FROM product_analytics.ecommerce.gold_user_metrics;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check user segment distribution
# MAGIC SELECT 
# MAGIC     user_segment,
# MAGIC     COUNT(*) as user_count,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as pct_of_users,
# MAGIC     ROUND(AVG(total_purchases), 2) as avg_purchases,
# MAGIC     ROUND(AVG(lifetime_revenue), 2) as avg_ltv,
# MAGIC     ROUND(AVG(days_active), 2) as avg_days_active
# MAGIC FROM product_analytics.ecommerce.gold_user_metrics
# MAGIC GROUP BY user_segment
# MAGIC ORDER BY user_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cross-check totals against fact table
# MAGIC SELECT 
# MAGIC     'Gold Table' as source,
# MAGIC     COUNT(DISTINCT user_sk) as distinct_users,
# MAGIC     SUM(total_events) as total_events,
# MAGIC     SUM(total_purchases) as total_purchases,
# MAGIC     ROUND(SUM(lifetime_revenue), 2) as total_revenue
# MAGIC FROM product_analytics.ecommerce.gold_user_metrics
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'Fact Table' as source,
# MAGIC     COUNT(DISTINCT f.user_sk) as distinct_users,
# MAGIC     COUNT(*) as total_events,
# MAGIC     SUM(CASE WHEN f.event_type = 'purchase' THEN 1 ELSE 0 END) as total_purchases,
# MAGIC     ROUND(SUM(f.revenue), 2) as total_revenue
# MAGIC FROM product_analytics.ecommerce.fact_events f
# MAGIC JOIN product_analytics.ecommerce.silver_dim_users u ON f.user_sk = u.user_sk
# MAGIC WHERE u.is_current_version = TRUE;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- RFM segmentation example
# MAGIC -- Assign quintiles for Recency, Frequency, Monetary
# MAGIC WITH rfm_scores AS (
# MAGIC     SELECT 
# MAGIC         user_id,
# MAGIC         user_segment,
# MAGIC         recency_days,
# MAGIC         frequency_purchases,
# MAGIC         lifetime_revenue,
# MAGIC         -- Lower recency is better (more recent), so reverse the score
# MAGIC         6 - NTILE(5) OVER (ORDER BY recency_days) as r_score,
# MAGIC         NTILE(5) OVER (ORDER BY frequency_purchases) as f_score,
# MAGIC         NTILE(5) OVER (ORDER BY lifetime_revenue) as m_score
# MAGIC     FROM product_analytics.ecommerce.gold_user_metrics
# MAGIC     WHERE total_purchases > 0  -- Only customers who purchased
# MAGIC )
# MAGIC SELECT 
# MAGIC     CONCAT(r_score, f_score, m_score) as rfm_segment,
# MAGIC     COUNT(*) as user_count,
# MAGIC     ROUND(AVG(recency_days), 1) as avg_recency,
# MAGIC     ROUND(AVG(frequency_purchases), 1) as avg_frequency,
# MAGIC     ROUND(AVG(lifetime_revenue), 2) as avg_monetary
# MAGIC FROM rfm_scores
# MAGIC WHERE r_score >= 4 AND f_score >= 4 AND m_score >= 4  -- Top customers
# MAGIC GROUP BY CONCAT(r_score, f_score, m_score)
# MAGIC ORDER BY user_count DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customer lifetime value tiers
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN lifetime_revenue = 0 THEN '0. Never Purchased'
# MAGIC         WHEN lifetime_revenue < 100 THEN '1. Low Value ($0-$100)'
# MAGIC         WHEN lifetime_revenue < 500 THEN '2. Medium Value ($100-$500)'
# MAGIC         WHEN lifetime_revenue < 1000 THEN '3. High Value ($500-$1K)'
# MAGIC         ELSE '4. VIP ($1K+)'
# MAGIC     END as ltv_tier,
# MAGIC     COUNT(*) as users,
# MAGIC     ROUND(AVG(total_purchases), 1) as avg_purchases,
# MAGIC     ROUND(AVG(lifetime_revenue), 2) as avg_ltv,
# MAGIC     ROUND(AVG(days_active), 1) as avg_days_active,
# MAGIC     ROUND(AVG(personal_conversion_rate), 2) as avg_conversion_rate
# MAGIC FROM product_analytics.ecommerce.gold_user_metrics
# MAGIC GROUP BY 
# MAGIC     CASE 
# MAGIC         WHEN lifetime_revenue = 0 THEN '0. Never Purchased'
# MAGIC         WHEN lifetime_revenue < 100 THEN '1. Low Value ($0-$100)'
# MAGIC         WHEN lifetime_revenue < 500 THEN '2. Medium Value ($100-$500)'
# MAGIC         WHEN lifetime_revenue < 1000 THEN '3. High Value ($500-$1K)'
# MAGIC         ELSE '4. VIP ($1K+)'
# MAGIC     END
# MAGIC ORDER BY ltv_tier;
