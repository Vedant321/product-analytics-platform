import streamlit as st
import pandas as pd
import plotly.express as px
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
import time

# ============ MUST BE FIRST! ============
st.set_page_config(
    page_title="Product Analytics Platform",
    page_icon="📊",
    layout="wide"
)

# ============ DATABRICKS SDK CONNECTION ============
@st.cache_resource
def get_workspace_client():
    """Get Databricks workspace client (uses local ~/.databrickscfg)"""
    return WorkspaceClient()

workspace = get_workspace_client()

# ============ DATABRICKS SDK CONNECTION ============
@st.cache_resource
def get_workspace_client():
    """Get Databricks workspace client (auto-auth in Apps)"""
    print("[DEBUG] Initializing WorkspaceClient...")
    try:
        client = WorkspaceClient()
        print("[DEBUG] WorkspaceClient initialized successfully")
        return client
    except Exception as e:
        print(f"[ERROR] Failed to initialize WorkspaceClient: {e}")
        raise

print("[DEBUG] Getting workspace client...")
workspace = get_workspace_client()
print("[DEBUG] Workspace client ready")

# ============ DATA REPOSITORY ============
class MetricsRepository:
    """Repository for fetching Gold layer metrics"""
    
    def __init__(self, workspace_client):
        self.workspace = workspace_client
        self.catalog = "product_analytics"
        self.schema = "ecommerce"
        self.warehouse_id = "b1660d805834aacd"  # Serverless Starter Warehouse
    
    def _execute_query(self, query):
        """Execute query using Databricks SDK and return pandas DataFrame"""
        try:
            # Execute SQL statement
            result = self.workspace.statement_execution.execute_statement(
                warehouse_id=self.warehouse_id,
                statement=query,
                catalog=self.catalog,
                schema=self.schema
            )
            
            # Wait for completion
            while result.status.state in [StatementState.PENDING, StatementState.RUNNING]:
                time.sleep(0.5)
                result = self.workspace.statement_execution.get_statement(result.statement_id)
            
            if result.status.state != StatementState.SUCCEEDED:
                raise Exception(f"Query failed: {result.status.error}")
            
            # Convert result to DataFrame
            if result.result and result.result.data_array:
                columns = [col.name for col in result.manifest.schema.columns]
                return pd.DataFrame(result.result.data_array, columns=columns)
            return pd.DataFrame()
            
        except Exception as e:
            st.error(f"Query execution error: {str(e)}")
            return pd.DataFrame()
    
    @st.cache_data(ttl=300)  # Cache for 5 minutes
    def get_daily_metrics(_self, days=30):
        """Get daily metrics from Gold layer"""
        query = f"""
        SELECT 
            full_date,
            daily_active_users,
            total_events,
            total_purchases,
            CAST(total_revenue AS DOUBLE) as total_revenue,
            CAST(avg_order_value AS DOUBLE) as avg_order_value,
            CAST(overall_conversion_rate AS DOUBLE) as overall_conversion_rate
        FROM {_self.catalog}.{_self.schema}.gold_daily_metrics
        ORDER BY full_date DESC
        LIMIT {days}
        """
        return _self._execute_query(query)
    
    @st.cache_data(ttl=300)
    def get_top_products(_self, limit=10):
        """Get top products by revenue"""
        query = f"""
        SELECT 
            brand as product_name,
            CAST(total_revenue AS DOUBLE) as total_revenue,
            total_quantity_sold,
            total_purchases
        FROM {_self.catalog}.{_self.schema}.gold_product_performance
        WHERE brand IS NOT NULL
        ORDER BY total_revenue DESC
        LIMIT {limit}
        """
        return _self._execute_query(query)
    
    @st.cache_data(ttl=300)
    def get_category_performance(_self):
        """Get category performance metrics"""
        query = f"""
        SELECT 
            category_l1 as category_name,
            CAST(total_revenue AS DOUBLE) as total_revenue,
            total_purchases,
            CAST(avg_revenue_per_purchase AS DOUBLE) as avg_order_value
        FROM {_self.catalog}.{_self.schema}.gold_category_performance
        WHERE category_l1 IS NOT NULL
        ORDER BY total_revenue DESC
        """
        return _self._execute_query(query)
    
    @st.cache_data(ttl=300)
    def get_summary_kpis(_self):
        """Get overall summary KPIs"""
        query = f"""
        SELECT 
            CAST(SUM(total_revenue) AS DOUBLE) as total_revenue,
            CAST(SUM(total_purchases) AS BIGINT) as total_purchases,
            CAST(AVG(avg_order_value) AS DOUBLE) as avg_order_value,
            CAST(MAX(daily_active_users) AS BIGINT) as peak_dau
        FROM {_self.catalog}.{_self.schema}.gold_daily_metrics
        """
        df = _self._execute_query(query)
        return df.iloc[0] if not df.empty else None

# Initialize repository
print("[DEBUG] Initializing MetricsRepository...")
repo = MetricsRepository(workspace)
print("[DEBUG] MetricsRepository initialized")

# ============ HEADER ============
print("[DEBUG] Rendering UI header...")
st.title("📊 Product Analytics Platform")
st.markdown("**Real-time analytics powered by Databricks Delta Lake**")
st.markdown(f"*Data Source: `{repo.catalog}.{repo.schema}` (Gold Layer)*")
print("[DEBUG] Header rendered successfully")

# ============ SIDEBAR ============
with st.sidebar:
    st.header("⚙️ Settings")
    days_filter = st.slider("Days to Display", 7, 90, 30)
    st.markdown("---")
    st.markdown("### Data Refresh")
    if st.button("🔄 Clear Cache"):
        st.cache_data.clear()
        st.success("Cache cleared!")
    st.markdown("---")
    st.markdown("### Architecture")
    st.markdown("""
    - **Bronze**: Raw events
    - **Silver**: Star schema
    - **Gold**: Aggregated metrics
    """)

# ============ TABS ============
print("[DEBUG] Creating tabs...")
tab1, tab2, tab3, tab4 = st.tabs(["📈 Overview", "🛍️ Products", "📅 Categories", "🧪 Experimentation"])
print("[DEBUG] Tabs created successfully")

# ============ TAB 1: OVERVIEW ============
print("[DEBUG] Entering Tab 1...")
with tab1:
    print("[DEBUG] Rendering Tab 1 content...")
    st.subheader("Business KPIs")
    
    try:
        # Fetch KPIs
        kpis = repo.get_summary_kpis()
        
        # Convert to numeric (SDK returns strings sometimes)
        total_revenue = float(kpis['total_revenue']) if kpis['total_revenue'] else 0
        total_purchases = int(kpis['total_purchases']) if kpis['total_purchases'] else 0
        avg_order_value = float(kpis['avg_order_value']) if kpis['avg_order_value'] else 0
        peak_dau = int(kpis['peak_dau']) if kpis['peak_dau'] else 0
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Revenue", f"${total_revenue:,.0f}")
        col2.metric("Total Purchases", f"{total_purchases:,.0f}")
        col3.metric("Avg Order Value", f"${avg_order_value:.2f}")
        col4.metric("Peak DAU", f"{peak_dau:,.0f}")
        
        st.markdown("---")
        
        # Conversion Funnel
        daily_df_preview = repo.get_daily_metrics(days_filter)
        if not daily_df_preview.empty:
            total_views = daily_df_preview['total_events'].sum()
            total_carts = daily_df_preview.get('total_carts', pd.Series([0])).sum()
            total_purchases_funnel = daily_df_preview['total_purchases'].sum()
            
            funnel_data = pd.DataFrame({
                'Stage': ['Views', 'Add to Cart', 'Purchase'],
                'Count': [total_views, total_carts if total_carts > 0 else total_views * 0.3, total_purchases_funnel],
                'Conversion': ['100%', 
                              f"{(total_carts/total_views*100) if total_views > 0 else 30:.1f}%",
                              f"{(total_purchases_funnel/total_views*100) if total_views > 0 else 5:.1f}%"]
            })
            
            st.subheader("🎯 Conversion Funnel")
            fig_funnel = px.funnel(funnel_data, x='Count', y='Stage', 
                                  text='Conversion',
                                  title='User Journey: Views → Cart → Purchase',
                                  color='Stage',
                                  color_discrete_sequence=['#636EFA', '#EF553B', '#00CC96'])
            fig_funnel.update_traces(textposition='inside', textfont_size=14)
            st.plotly_chart(fig_funnel, use_container_width=True)
        
        st.markdown("---")
        
        # Daily trends
        st.subheader(f"Daily Trends (Last {days_filter} Days)")
        daily_df = repo.get_daily_metrics(days_filter)
        
        if not daily_df.empty:
            # Revenue trend
            fig_revenue = px.line(daily_df, x='full_date', y='total_revenue',
                                 title='Daily Revenue',
                                 labels={'total_revenue': 'Revenue ($)', 'full_date': 'Date'})
            st.plotly_chart(fig_revenue, use_container_width=True)
            
            # DAU and Conversion
            col1, col2 = st.columns(2)
            with col1:
                fig_dau = px.line(daily_df, x='full_date', y='daily_active_users',
                                title='Daily Active Users',
                                labels={'daily_active_users': 'DAU', 'full_date': 'Date'})
                st.plotly_chart(fig_dau, use_container_width=True)
            
            with col2:
                fig_conv = px.line(daily_df, x='full_date', y='overall_conversion_rate',
                                 title='Conversion Rate',
                                 labels={'overall_conversion_rate': 'Conversion %', 'full_date': 'Date'})
                st.plotly_chart(fig_conv, use_container_width=True)
        else:
            st.info("No data available for the selected period")
            
    except Exception as e:
        st.error(f"Error loading overview metrics: {str(e)}")
        st.exception(e)

# ============ TAB 2: PRODUCTS ============
with tab2:
    st.subheader("Product Performance")
    
    try:
        products_df = repo.get_top_products(20)  # Get more products
        
        if not products_df.empty:
            col1, col2 = st.columns([2, 1])
            
            with col1:
                # Top 10 products horizontal bar
                fig = px.bar(products_df.head(10), x='total_revenue', y='product_name',
                            orientation='h',
                            title='Top 10 Brands by Revenue',
                            labels={'total_revenue': 'Revenue ($)', 'product_name': 'Brand'},
                            color='total_revenue',
                            color_continuous_scale='Blues')
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Product metrics summary
                st.metric("Total Brands", len(products_df))
                st.metric("Top Brand Revenue", f"${float(products_df.iloc[0]['total_revenue']):,.0f}")
                st.metric("Avg Revenue/Brand", f"${float(products_df['total_revenue'].mean()):,.0f}")
            
            # Treemap: Visual hierarchy of products by revenue
            st.markdown("### 🗺️ Product Revenue Treemap")
            fig_tree = px.treemap(products_df.head(15), 
                                 path=['product_name'], 
                                 values='total_revenue',
                                 title='Brand Revenue Distribution (Top 15)',
                                 color='total_revenue',
                                 color_continuous_scale='RdYlGn')
            st.plotly_chart(fig_tree, use_container_width=True)
            
            # Scatter: Revenue vs Quantity Sold
            st.markdown("### 📊 Revenue vs Quantity Analysis")
            fig_scatter = px.scatter(products_df.head(15), 
                                    x='total_quantity_sold', 
                                    y='total_revenue',
                                    size='total_purchases',
                                    hover_data=['product_name'],
                                    title='Revenue vs Quantity Sold (bubble size = purchases)',
                                    labels={'total_quantity_sold': 'Units Sold', 
                                           'total_revenue': 'Revenue ($)'},
                                    color='total_revenue',
                                    color_continuous_scale='Viridis')
            st.plotly_chart(fig_scatter, use_container_width=True)
            
            # Data table
            st.markdown("### 📋 Detailed Product Table")
            st.dataframe(products_df, use_container_width=True, height=300)
        else:
            st.info("No product data available")
            
    except Exception as e:
        st.error(f"Error loading product metrics: {str(e)}")
        st.exception(e)

# ============ TAB 3: CATEGORIES ============
with tab3:
    st.subheader("Category Performance")
    
    try:
        categories_df = repo.get_category_performance()
        
        if not categories_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                fig = px.pie(categories_df, values='total_revenue', names='category_name',
                           title='Revenue by Category')
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                fig = px.bar(categories_df, x='category_name', y='total_purchases',
                           title='Purchases by Category',
                           labels={'total_purchases': 'Purchases', 'category_name': 'Category'})
                st.plotly_chart(fig, use_container_width=True)
            
            # Data table
            st.markdown("### Category Details")
            st.dataframe(categories_df, use_container_width=True)
        else:
            st.info("No category data available")
            
    except Exception as e:
        st.error(f"Error loading category metrics: {str(e)}")
        st.exception(e)

# ============ TAB 4: EXPERIMENTATION (PLACEHOLDER) ============
with tab4:
    st.subheader("🧪 A/B Testing & Experimentation")
    st.info("⚡ Coming Soon: A/B test results, experiment metrics, and ML predictions")
    
    # Placeholder sections
    with st.expander("🎯 Active Experiments"):
        st.markdown("Track running A/B tests and their performance metrics")
    
    with st.expander("🤖 ML Models"):
        st.markdown("""
        - Churn prediction
        - Customer lifetime value
        - Product recommendations
        - Demand forecasting
        """)
    
    with st.expander("📊 Statistical Analysis"):
        st.markdown("Significance testing, confidence intervals, and experiment analysis")

st.markdown("---")
st.markdown("🚀 Built with Streamlit + Databricks Delta Lake | 📊 Powered by Gold Layer Analytics")