# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Gold Layer: Cohort Analysis
# MAGIC %md
# MAGIC # Gold Cohort Analysis
# MAGIC
# MAGIC User cohorts by first activity month with retention and LTV tracking.
# MAGIC
# MAGIC **Table:** `product_analytics.ecommerce.gold_cohort_analysis`  
# MAGIC **Grain:** One row per cohort month per activity period  
# MAGIC **Update Strategy:** Full refresh daily
# MAGIC
# MAGIC **Metrics:**
# MAGIC - Cohort size and composition
# MAGIC - Period-over-period retention rates
# MAGIC - Cumulative revenue and LTV by cohort
# MAGIC - User activity patterns by cohort age

# COMMAND ----------

# DBTITLE 1,Imports and Config
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import *

# COMMAND ----------

# DBTITLE 1,Create Gold Cohort Analysis Table
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE product_analytics.ecommerce.gold_cohort_analysis
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH user_first_activity AS (
# MAGIC     SELECT 
# MAGIC         user_sk,
# MAGIC         MIN(event_time) as first_activity_date,
# MAGIC         DATE_TRUNC('MONTH', MIN(event_time)) as cohort_month
# MAGIC     FROM product_analytics.ecommerce.fact_events
# MAGIC     GROUP BY user_sk
# MAGIC ),
# MAGIC user_monthly_activity AS (
# MAGIC     SELECT 
# MAGIC         f.user_sk,
# MAGIC         ufa.cohort_month,
# MAGIC         DATE_TRUNC('MONTH', f.event_time) as activity_month,
# MAGIC         MONTHS_BETWEEN(DATE_TRUNC('MONTH', f.event_time), ufa.cohort_month) as periods_since_cohort,
# MAGIC         
# MAGIC         -- Activity metrics per user per month
# MAGIC         COUNT(DISTINCT CASE WHEN f.event_type = 'view' THEN f.event_time END) as views,
# MAGIC         COUNT(DISTINCT CASE WHEN f.event_type = 'cart' THEN f.event_time END) as carts,
# MAGIC         COUNT(DISTINCT CASE WHEN f.event_type = 'purchase' THEN f.event_time END) as purchases,
# MAGIC         ROUND(SUM(CASE WHEN f.event_type = 'purchase' THEN f.revenue ELSE 0 END), 2) as revenue
# MAGIC         
# MAGIC     FROM product_analytics.ecommerce.fact_events f
# MAGIC     JOIN user_first_activity ufa ON f.user_sk = ufa.user_sk
# MAGIC     GROUP BY f.user_sk, ufa.cohort_month, DATE_TRUNC('MONTH', f.event_time)
# MAGIC ),
# MAGIC cohort_metrics AS (
# MAGIC     SELECT 
# MAGIC         cohort_month,
# MAGIC         periods_since_cohort,
# MAGIC         activity_month,
# MAGIC         
# MAGIC         -- Cohort size (users in cohort)
# MAGIC         COUNT(DISTINCT user_sk) as cohort_size,
# MAGIC         
# MAGIC         -- Active users in this period
# MAGIC         COUNT(DISTINCT user_sk) as active_users,
# MAGIC         
# MAGIC         -- Activity metrics
# MAGIC         SUM(views) as total_views,
# MAGIC         SUM(carts) as total_carts,
# MAGIC         SUM(purchases) as total_purchases,
# MAGIC         ROUND(SUM(revenue), 2) as period_revenue,
# MAGIC         
# MAGIC         -- Average per active user
# MAGIC         ROUND(AVG(views), 2) as avg_views_per_user,
# MAGIC         ROUND(AVG(revenue), 2) as avg_revenue_per_user
# MAGIC         
# MAGIC     FROM user_monthly_activity
# MAGIC     GROUP BY cohort_month, periods_since_cohort, activity_month
# MAGIC ),
# MAGIC cohort_size_base AS (
# MAGIC     SELECT 
# MAGIC         cohort_month,
# MAGIC         COUNT(DISTINCT user_sk) as total_cohort_users
# MAGIC     FROM user_first_activity
# MAGIC     GROUP BY cohort_month
# MAGIC ),
# MAGIC cohort_with_retention AS (
# MAGIC     SELECT 
# MAGIC         cm.*,
# MAGIC         csb.total_cohort_users,
# MAGIC         
# MAGIC         -- Retention rate (active users / total cohort size)
# MAGIC         ROUND(
# MAGIC             cm.active_users * 100.0 / csb.total_cohort_users,
# MAGIC             2
# MAGIC         ) as retention_rate,
# MAGIC         
# MAGIC         -- Cumulative revenue for cohort up to this period
# MAGIC         ROUND(
# MAGIC             SUM(cm.period_revenue) OVER (
# MAGIC                 PARTITION BY cm.cohort_month 
# MAGIC                 ORDER BY cm.periods_since_cohort
# MAGIC                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC             ),
# MAGIC             2
# MAGIC         ) as cumulative_revenue,
# MAGIC         
# MAGIC         -- LTV (cumulative revenue / total cohort size)
# MAGIC         ROUND(
# MAGIC             SUM(cm.period_revenue) OVER (
# MAGIC                 PARTITION BY cm.cohort_month 
# MAGIC                 ORDER BY cm.periods_since_cohort
# MAGIC                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC             ) / csb.total_cohort_users,
# MAGIC             2
# MAGIC         ) as ltv_to_date
# MAGIC         
# MAGIC     FROM cohort_metrics cm
# MAGIC     JOIN cohort_size_base csb ON cm.cohort_month = csb.cohort_month
# MAGIC )
# MAGIC SELECT 
# MAGIC     *,
# MAGIC     current_timestamp() as created_at
# MAGIC FROM cohort_with_retention;

# COMMAND ----------

# DBTITLE 1,Optimize Table
# MAGIC %sql
# MAGIC -- Optimize and Z-ORDER by cohort month and period
# MAGIC OPTIMIZE product_analytics.ecommerce.gold_cohort_analysis
# MAGIC ZORDER BY (cohort_month, periods_since_cohort);

# COMMAND ----------

# DBTITLE 1,Enable Auto-Optimize
# MAGIC %sql
# MAGIC -- Enable Auto-Optimize for future writes
# MAGIC ALTER TABLE product_analytics.ecommerce.gold_cohort_analysis
# MAGIC SET TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

# DBTITLE 1,Add Table Comment
# MAGIC %sql
# MAGIC -- Table comment
# MAGIC COMMENT ON TABLE product_analytics.ecommerce.gold_cohort_analysis IS 
# MAGIC 'User cohorts by first activity month with retention curves and LTV tracking. Grain: one row per cohort month per activity period. Full refresh daily.';

# COMMAND ----------

# DBTITLE 1,Validation: Row Count and Sample
# MAGIC %sql
# MAGIC -- Check row count
# MAGIC SELECT COUNT(*) as total_rows FROM product_analytics.ecommerce.gold_cohort_analysis;
# MAGIC
# MAGIC -- Sample recent cohorts with retention
# MAGIC SELECT 
# MAGIC     cohort_month,
# MAGIC     periods_since_cohort,
# MAGIC     activity_month,
# MAGIC     total_cohort_users,
# MAGIC     active_users,
# MAGIC     retention_rate,
# MAGIC     period_revenue,
# MAGIC     cumulative_revenue,
# MAGIC     ltv_to_date
# MAGIC FROM product_analytics.ecommerce.gold_cohort_analysis
# MAGIC WHERE cohort_month >= '2019-10-01'
# MAGIC ORDER BY cohort_month DESC, periods_since_cohort ASC
# MAGIC LIMIT 30;

# COMMAND ----------

# DBTITLE 1,Validation: Retention Curve by Cohort
# MAGIC %sql
# MAGIC -- Retention curve for select cohorts (Month 0-6)
# MAGIC SELECT 
# MAGIC     cohort_month,
# MAGIC     periods_since_cohort,
# MAGIC     total_cohort_users,
# MAGIC     active_users,
# MAGIC     retention_rate,
# MAGIC     cumulative_revenue,
# MAGIC     ltv_to_date
# MAGIC FROM product_analytics.ecommerce.gold_cohort_analysis
# MAGIC WHERE cohort_month IN ('2019-10-01', '2019-11-01', '2019-12-01', '2020-01-01')
# MAGIC     AND periods_since_cohort <= 6
# MAGIC ORDER BY cohort_month, periods_since_cohort;

# COMMAND ----------

# DBTITLE 1,Sample Query: Best Performing Cohorts
# MAGIC %sql
# MAGIC -- Cohorts with highest LTV after 3 months
# MAGIC SELECT 
# MAGIC     cohort_month,
# MAGIC     total_cohort_users,
# MAGIC     retention_rate as month_3_retention,
# MAGIC     ltv_to_date as month_3_ltv,
# MAGIC     cumulative_revenue as total_revenue_month_3
# MAGIC FROM product_analytics.ecommerce.gold_cohort_analysis
# MAGIC WHERE periods_since_cohort = 3
# MAGIC ORDER BY ltv_to_date DESC
# MAGIC LIMIT 20;

# COMMAND ----------

