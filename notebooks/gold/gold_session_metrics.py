# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Gold Layer: Session Metrics
# MAGIC %md
# MAGIC # Gold Session Metrics
# MAGIC
# MAGIC Session-level aggregations derived from event sequences (30-minute inactivity window).
# MAGIC
# MAGIC **Table:** `product_analytics.ecommerce.gold_session_metrics`  
# MAGIC **Grain:** One row per date  
# MAGIC **Update Strategy:** Full refresh daily
# MAGIC
# MAGIC **Session Definition:** Events grouped by user with ≤30 minutes between events  
# MAGIC
# MAGIC **Metrics:**
# MAGIC - Total sessions per day
# MAGIC - Session duration and event counts
# MAGIC - Session conversion metrics
# MAGIC - Session engagement patterns

# COMMAND ----------

# DBTITLE 1,Imports and Config
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Create Gold Session Metrics Table
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE product_analytics.ecommerce.gold_session_metrics
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH events_with_session AS (
# MAGIC     SELECT 
# MAGIC         *,
# MAGIC         -- Create session boundary: new session if >30 min since last event
# MAGIC         SUM(
# MAGIC             CASE 
# MAGIC                 WHEN UNIX_TIMESTAMP(event_time) - LAG(UNIX_TIMESTAMP(event_time)) OVER (
# MAGIC                     PARTITION BY user_sk 
# MAGIC                     ORDER BY event_time
# MAGIC                 ) > 1800 OR LAG(UNIX_TIMESTAMP(event_time)) OVER (
# MAGIC                     PARTITION BY user_sk 
# MAGIC                     ORDER BY event_time
# MAGIC                 ) IS NULL
# MAGIC                 THEN 1 
# MAGIC                 ELSE 0 
# MAGIC             END
# MAGIC         ) OVER (
# MAGIC             PARTITION BY user_sk 
# MAGIC             ORDER BY event_time
# MAGIC             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC         ) as session_number
# MAGIC     FROM product_analytics.ecommerce.fact_events
# MAGIC ),
# MAGIC session_aggregates AS (
# MAGIC     SELECT 
# MAGIC         user_sk,
# MAGIC         session_number,
# MAGIC         date_sk,
# MAGIC         
# MAGIC         -- Session timing
# MAGIC         MIN(event_time) as session_start,
# MAGIC         MAX(event_time) as session_end,
# MAGIC         (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60.0 as session_duration_minutes,
# MAGIC         
# MAGIC         -- Event counts
# MAGIC         COUNT(*) as total_events_in_session,
# MAGIC         SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as views_in_session,
# MAGIC         SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) as carts_in_session,
# MAGIC         SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases_in_session,
# MAGIC         
# MAGIC         -- Session outcome flags
# MAGIC         MAX(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) as had_cart_action,
# MAGIC         MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as had_purchase,
# MAGIC         
# MAGIC         -- Revenue
# MAGIC         ROUND(SUM(CASE WHEN event_type = 'purchase' THEN revenue ELSE 0 END), 2) as session_revenue,
# MAGIC         
# MAGIC         -- Product engagement
# MAGIC         COUNT(DISTINCT product_sk) as unique_products_in_session,
# MAGIC         COUNT(DISTINCT category_sk) as unique_categories_in_session
# MAGIC         
# MAGIC     FROM events_with_session
# MAGIC     GROUP BY user_sk, session_number, date_sk
# MAGIC ),
# MAGIC daily_session_metrics AS (
# MAGIC     SELECT 
# MAGIC         sa.date_sk,
# MAGIC         d.full_date,
# MAGIC         
# MAGIC         -- Session counts
# MAGIC         COUNT(*) as total_sessions,
# MAGIC         COUNT(DISTINCT sa.user_sk) as unique_users_with_sessions,
# MAGIC         
# MAGIC         -- Session duration metrics
# MAGIC         ROUND(AVG(sa.session_duration_minutes), 2) as avg_session_duration_minutes,
# MAGIC         ROUND(PERCENTILE(sa.session_duration_minutes, 0.5), 2) as median_session_duration_minutes,
# MAGIC         MAX(sa.session_duration_minutes) as max_session_duration_minutes,
# MAGIC         
# MAGIC         -- Events per session
# MAGIC         ROUND(AVG(sa.total_events_in_session), 2) as avg_events_per_session,
# MAGIC         ROUND(AVG(sa.views_in_session), 2) as avg_views_per_session,
# MAGIC         ROUND(AVG(sa.unique_products_in_session), 2) as avg_products_per_session,
# MAGIC         
# MAGIC         -- Session outcomes
# MAGIC         SUM(sa.had_cart_action) as sessions_with_cart,
# MAGIC         SUM(sa.had_purchase) as sessions_with_purchase,
# MAGIC         ROUND(SUM(sa.had_cart_action) * 100.0 / COUNT(*), 2) as pct_sessions_with_cart,
# MAGIC         ROUND(SUM(sa.had_purchase) * 100.0 / COUNT(*), 2) as pct_sessions_with_purchase,
# MAGIC         
# MAGIC         -- Revenue metrics
# MAGIC         ROUND(SUM(sa.session_revenue), 2) as total_session_revenue,
# MAGIC         ROUND(AVG(CASE WHEN sa.had_purchase = 1 THEN sa.session_revenue END), 2) as avg_revenue_per_converting_session,
# MAGIC         
# MAGIC         -- Engagement distribution
# MAGIC         SUM(CASE WHEN sa.total_events_in_session = 1 THEN 1 ELSE 0 END) as single_event_sessions,
# MAGIC         SUM(CASE WHEN sa.total_events_in_session BETWEEN 2 AND 5 THEN 1 ELSE 0 END) as low_engagement_sessions,
# MAGIC         SUM(CASE WHEN sa.total_events_in_session BETWEEN 6 AND 15 THEN 1 ELSE 0 END) as medium_engagement_sessions,
# MAGIC         SUM(CASE WHEN sa.total_events_in_session > 15 THEN 1 ELSE 0 END) as high_engagement_sessions
# MAGIC         
# MAGIC     FROM session_aggregates sa
# MAGIC     JOIN product_analytics.ecommerce.silver_dim_date d ON sa.date_sk = d.date_key
# MAGIC     GROUP BY sa.date_sk, d.full_date
# MAGIC )
# MAGIC SELECT 
# MAGIC     *,
# MAGIC     current_timestamp() as created_at
# MAGIC FROM daily_session_metrics;

# COMMAND ----------

# DBTITLE 1,Optimize Table
# MAGIC %sql
# MAGIC -- Optimize and Z-ORDER by date
# MAGIC OPTIMIZE product_analytics.ecommerce.gold_session_metrics
# MAGIC ZORDER BY (date_sk);

# COMMAND ----------

# DBTITLE 1,Enable Auto-Optimize
# MAGIC %sql
# MAGIC -- Enable Auto-Optimize for future writes
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_session_metrics
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Add Table Comment
# MAGIC %sql
# MAGIC -- Table comment
# MAGIC COMMENT ON TABLE product_analytics.ecommerce.gold_session_metrics IS 
# MAGIC 'Daily session-level aggregations derived from event sequences (30-min inactivity window). Grain: one row per date. Full refresh daily.';

# COMMAND ----------

# DBTITLE 1,Validation: Row Count and Sample
# MAGIC %sql
# MAGIC -- Check row count
# MAGIC SELECT COUNT(*) as total_rows FROM product_analytics.ecommerce.gold_session_metrics;
# MAGIC
# MAGIC -- Sample recent session metrics
# MAGIC SELECT 
# MAGIC     full_date,
# MAGIC     total_sessions,
# MAGIC     unique_users_with_sessions,
# MAGIC     avg_session_duration_minutes,
# MAGIC     avg_events_per_session,
# MAGIC     pct_sessions_with_cart,
# MAGIC     pct_sessions_with_purchase,
# MAGIC     total_session_revenue,
# MAGIC     avg_revenue_per_converting_session
# MAGIC FROM product_analytics.ecommerce.gold_session_metrics
# MAGIC ORDER BY full_date DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# DBTITLE 1,Validation: Session Engagement Distribution
# MAGIC %sql
# MAGIC -- Session engagement breakdown
# MAGIC SELECT 
# MAGIC     full_date,
# MAGIC     total_sessions,
# MAGIC     single_event_sessions,
# MAGIC     low_engagement_sessions,
# MAGIC     medium_engagement_sessions,
# MAGIC     high_engagement_sessions,
# MAGIC     ROUND(single_event_sessions * 100.0 / total_sessions, 2) as pct_single_event,
# MAGIC     ROUND(high_engagement_sessions * 100.0 / total_sessions, 2) as pct_high_engagement
# MAGIC FROM product_analytics.ecommerce.gold_session_metrics
# MAGIC ORDER BY full_date DESC
# MAGIC LIMIT 20;

# COMMAND ----------

