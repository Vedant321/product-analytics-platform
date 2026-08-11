# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Gold Layer: Funnel Metrics
# MAGIC %md
# MAGIC # Gold Funnel Metrics
# MAGIC
# MAGIC Conversion funnel performance tracking: view → cart → purchase drop-offs.
# MAGIC
# MAGIC **Table:** `product_analytics.ecommerce.gold_funnel_metrics`  
# MAGIC **Grain:** One row per date per category L1  
# MAGIC **Update Strategy:** Full refresh daily
# MAGIC
# MAGIC **Metrics:**
# MAGIC - Stage progression counts (views, carts, purchases)
# MAGIC - Stage-to-stage conversion rates
# MAGIC - Drop-off rates and abandonment metrics
# MAGIC - Funnel completion rate
# MAGIC - Category-level funnel performance

# COMMAND ----------

# DBTITLE 1,Imports and Config
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Create Gold Funnel Metrics Table
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE product_analytics.ecommerce.gold_funnel_metrics
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH daily_category_funnel AS (
# MAGIC     SELECT 
# MAGIC         -- Dimensions
# MAGIC         f.date_sk,
# MAGIC         d.full_date,
# MAGIC         c.category_l1,
# MAGIC         c.category_l2,
# MAGIC         
# MAGIC         -- Funnel stage counts
# MAGIC         COUNT(DISTINCT CASE WHEN f.event_type = 'view' THEN f.user_sk END) as users_viewed,
# MAGIC         COUNT(DISTINCT CASE WHEN f.event_type = 'cart' THEN f.user_sk END) as users_carted,
# MAGIC         COUNT(DISTINCT CASE WHEN f.event_type = 'purchase' THEN f.user_sk END) as users_purchased,
# MAGIC         
# MAGIC         SUM(CASE WHEN f.event_type = 'view' THEN 1 ELSE 0 END) as total_views,
# MAGIC         SUM(CASE WHEN f.event_type = 'cart' THEN 1 ELSE 0 END) as total_carts,
# MAGIC         SUM(CASE WHEN f.event_type = 'remove_from_cart' THEN 1 ELSE 0 END) as total_removes,
# MAGIC         SUM(CASE WHEN f.event_type = 'purchase' THEN 1 ELSE 0 END) as total_purchases,
# MAGIC         
# MAGIC         -- Revenue
# MAGIC         ROUND(SUM(CASE WHEN f.event_type = 'purchase' THEN f.revenue ELSE 0 END), 2) as total_revenue,
# MAGIC         
# MAGIC         -- Product engagement
# MAGIC         COUNT(DISTINCT f.product_sk) as unique_products_in_funnel
# MAGIC         
# MAGIC     FROM product_analytics.ecommerce.fact_events f
# MAGIC     JOIN product_analytics.ecommerce.silver_dim_products p 
# MAGIC         ON f.product_sk = p.product_sk
# MAGIC     JOIN product_analytics.ecommerce.silver_dim_categories c
# MAGIC         ON f.category_sk = c.category_sk
# MAGIC     JOIN product_analytics.ecommerce.silver_dim_date d
# MAGIC         ON f.date_sk = d.date_key
# MAGIC     WHERE p.is_current_version = TRUE
# MAGIC     GROUP BY f.date_sk, d.full_date, c.category_l1, c.category_l2
# MAGIC ),
# MAGIC funnel_conversions AS (
# MAGIC     SELECT 
# MAGIC         *,
# MAGIC         -- Stage-to-stage conversion rates
# MAGIC         ROUND(
# MAGIC             users_carted * 100.0 / NULLIF(users_viewed, 0),
# MAGIC             2
# MAGIC         ) as view_to_cart_user_rate,
# MAGIC         ROUND(
# MAGIC             users_purchased * 100.0 / NULLIF(users_carted, 0),
# MAGIC             2
# MAGIC         ) as cart_to_purchase_user_rate,
# MAGIC         ROUND(
# MAGIC             users_purchased * 100.0 / NULLIF(users_viewed, 0),
# MAGIC             2
# MAGIC         ) as overall_conversion_user_rate,
# MAGIC         
# MAGIC         -- Event-level conversion rates
# MAGIC         ROUND(
# MAGIC             total_carts * 100.0 / NULLIF(total_views, 0),
# MAGIC             2
# MAGIC         ) as view_to_cart_event_rate,
# MAGIC         ROUND(
# MAGIC             total_purchases * 100.0 / NULLIF(total_carts, 0),
# MAGIC             2
# MAGIC         ) as cart_to_purchase_event_rate,
# MAGIC         ROUND(
# MAGIC             total_purchases * 100.0 / NULLIF(total_views, 0),
# MAGIC             2
# MAGIC         ) as overall_conversion_event_rate,
# MAGIC         
# MAGIC         -- Drop-off metrics (users who didn't progress)
# MAGIC         users_viewed - users_carted as users_dropped_at_cart,
# MAGIC         users_carted - users_purchased as users_dropped_at_purchase,
# MAGIC         
# MAGIC         -- Drop-off rates
# MAGIC         ROUND(
# MAGIC             (users_viewed - users_carted) * 100.0 / NULLIF(users_viewed, 0),
# MAGIC             2
# MAGIC         ) as cart_abandonment_rate,
# MAGIC         ROUND(
# MAGIC             (users_carted - users_purchased) * 100.0 / NULLIF(users_carted, 0),
# MAGIC             2
# MAGIC         ) as purchase_abandonment_rate,
# MAGIC         
# MAGIC         -- Cart removal impact
# MAGIC         ROUND(
# MAGIC             total_removes * 100.0 / NULLIF(total_carts, 0),
# MAGIC             2
# MAGIC         ) as cart_removal_rate,
# MAGIC         
# MAGIC         -- Revenue per funnel stage user
# MAGIC         ROUND(
# MAGIC             total_revenue / NULLIF(users_viewed, 0),
# MAGIC             2
# MAGIC         ) as revenue_per_viewer,
# MAGIC         ROUND(
# MAGIC             total_revenue / NULLIF(users_purchased, 0),
# MAGIC             2
# MAGIC         ) as revenue_per_purchaser
# MAGIC         
# MAGIC     FROM daily_category_funnel
# MAGIC )
# MAGIC SELECT 
# MAGIC     *,
# MAGIC     current_timestamp() as created_at
# MAGIC FROM funnel_conversions;

# COMMAND ----------

# DBTITLE 1,Optimize Table
# MAGIC %sql
# MAGIC -- Optimize and Z-ORDER by date and category
# MAGIC OPTIMIZE product_analytics.ecommerce.gold_funnel_metrics
# MAGIC ZORDER BY (date_sk, category_l1);

# COMMAND ----------

# DBTITLE 1,Enable Auto-Optimize
# MAGIC %sql
# MAGIC -- Enable Auto-Optimize for future writes
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_funnel_metrics
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Add Table Comment
# MAGIC %sql
# MAGIC -- Table comment
# MAGIC COMMENT ON TABLE product_analytics.ecommerce.gold_funnel_metrics IS 
# MAGIC 'Conversion funnel performance: view → cart → purchase progression by date and category. Grain: one row per date per category L1 + L2. Full refresh daily.';

# COMMAND ----------

# DBTITLE 1,Validation: Row Count and Sample
# MAGIC %sql
# MAGIC -- Check row count
# MAGIC SELECT COUNT(*) as total_rows FROM product_analytics.ecommerce.gold_funnel_metrics;
# MAGIC
# MAGIC -- Sample recent funnel performance
# MAGIC SELECT 
# MAGIC     full_date,
# MAGIC     category_l1,
# MAGIC     category_l2,
# MAGIC     users_viewed,
# MAGIC     users_carted,
# MAGIC     users_purchased,
# MAGIC     view_to_cart_user_rate,
# MAGIC     cart_to_purchase_user_rate,
# MAGIC     overall_conversion_user_rate,
# MAGIC     cart_abandonment_rate,
# MAGIC     total_revenue
# MAGIC FROM product_analytics.ecommerce.gold_funnel_metrics
# MAGIC ORDER BY full_date DESC, total_revenue DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# DBTITLE 1,Validation: Aggregate Funnel by Category
# MAGIC %sql
# MAGIC -- Overall funnel performance by L1 category
# MAGIC SELECT 
# MAGIC     category_l1,
# MAGIC     SUM(users_viewed) as total_users_viewed,
# MAGIC     SUM(users_carted) as total_users_carted,
# MAGIC     SUM(users_purchased) as total_users_purchased,
# MAGIC     ROUND(SUM(users_carted) * 100.0 / NULLIF(SUM(users_viewed), 0), 2) as avg_view_to_cart_rate,
# MAGIC     ROUND(SUM(users_purchased) * 100.0 / NULLIF(SUM(users_carted), 0), 2) as avg_cart_to_purchase_rate,
# MAGIC     ROUND(SUM(users_purchased) * 100.0 / NULLIF(SUM(users_viewed), 0), 2) as avg_overall_conversion,
# MAGIC     ROUND(SUM(total_revenue), 2) as total_revenue
# MAGIC FROM product_analytics.ecommerce.gold_funnel_metrics
# MAGIC GROUP BY category_l1
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# DBTITLE 1,Sample Query: Worst Performing Funnels
# MAGIC %sql
# MAGIC -- Categories with highest abandonment rates (min 100 viewers for significance)
# MAGIC SELECT 
# MAGIC     full_date,
# MAGIC     category_l1,
# MAGIC     category_l2,
# MAGIC     users_viewed,
# MAGIC     users_carted,
# MAGIC     users_purchased,
# MAGIC     cart_abandonment_rate,
# MAGIC     purchase_abandonment_rate,
# MAGIC     overall_conversion_user_rate,
# MAGIC     total_revenue
# MAGIC FROM product_analytics.ecommerce.gold_funnel_metrics
# MAGIC WHERE users_viewed >= 100
# MAGIC ORDER BY cart_abandonment_rate DESC, purchase_abandonment_rate DESC
# MAGIC LIMIT 20;

# COMMAND ----------

