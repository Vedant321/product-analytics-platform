import streamlit as st
import pandas as pd
import plotly.express as px
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
import time


# Google-inspired color palette for charts
color_palette = ["#1a73e8", "#34a853", "#fbbc04", "#ea4335", "#9334e6", "#00acc1"]

# ============ MUST BE FIRST! ============
st.set_page_config(
    page_title="Analytics Platform",
    page_icon="📊",
    layout="wide"
)

# ============ CUSTOM CSS - GOOGLE LIGHT THEME (SIMPLIFIED) ============
st.markdown("""
<style>
    /* === MAIN COLORS === */
    .stApp {
        background-color: #ffffff !important;
    }
    
    /* Force all text to be dark */
    * {
        color: #202124 !important;
    }
    
    .block-container {
        padding-top: 1.5rem !important;
        padding-bottom: 1rem !important;
        max-width: 95% !important;
        background-color: #ffffff !important;
        position: relative !important;
        z-index: 10 !important;
    }
    
    /* === TITLE === */
    h1, h2, h3, h4, h5, h6 {
        color: #202124 !important;
        font-weight: 400 !important;
    }
    
    h1 {
        font-size: 2rem !important;
        margin-top: 0.5rem !important;
        margin-bottom: 1rem !important;
    }
    
    /* === SIDEBAR === */
    [data-testid="stSidebar"] {
        background-color: #f8f9fa !important;
        border-right: 1px solid #e8eaed !important;
    }
    
    [data-testid="stSidebar"] > div:first-child {
        background-color: #f8f9fa !important;
    }
    
    [data-testid="stSidebar"] h3 {
        color: #202124 !important;
        font-size: 1.3rem !important;
        padding: 0 0 1rem 0 !important;
    }
    
    /* === NAVIGATION BUTTONS - FULL WIDTH === */
    div[role="radiogroup"] {
        gap: 0 !important;
        display: flex !important;
        flex-direction: column !important;
    }
    
    div[role="radiogroup"] label {
        background-color: transparent !important;
        border: none !important;
        border-left: 3px solid transparent !important;
        border-radius: 0 !important;
        padding: 12px 24px !important;
        cursor: pointer !important;
        transition: all 0.2s ease !important;
        margin: 0 !important;
        width: 100% !important;
    }
    
    div[role="radiogroup"] label:hover {
        background-color: #e8f0fe !important;
        border-left-color: #1a73e8 !important;
    }
    
    div[role="radiogroup"] label > div:first-child {
        display: none !important;
    }
    
    div[role="radiogroup"] label p {
        font-size: 0.95rem !important;
        font-weight: 500 !important;
        margin: 0 !important;
        color: #5f6368 !important;
    }
    
    div[role="radiogroup"] label[data-checked="true"],
    div[role="radiogroup"] label:has(input:checked) {
        background-color: #e8f0fe !important;
        border-left: 3px solid #1a73e8 !important;
    }
    
    div[role="radiogroup"] label[data-checked="true"] p,
    div[role="radiogroup"] label:has(input:checked) p {
        color: #1a73e8 !important;
        font-weight: 600 !important;
    }
    
    /* === METRICS === */
    [data-testid="stMetric"] {
        background-color: #ffffff !important;
        border: 1px solid #e8eaed !important;
        border-radius: 8px !important;
        padding: 16px !important;
        box-shadow: 0 1px 2px 0 rgba(60,64,67,0.1) !important;
    }
    
    [data-testid="stMetricLabel"] {
        color: #5f6368 !important;
        font-size: 0.875rem !important;
    }
    
    [data-testid="stMetricValue"] {
        color: #202124 !important;
        font-size: 1.75rem !important;
        font-weight: 400 !important;
    }
    
    /* === CHARTS === */
    .plotly {
        border-radius: 8px !important;
        border: 1px solid #e8eaed !important;
        background-color: #ffffff !important;
        box-shadow: 0 1px 2px 0 rgba(60,64,67,0.1) !important;
    }
    
    /* === BUTTONS === */
    .stButton > button {
        background-color: #1a73e8 !important;
        color: #ffffff !important;
        border: none !important;
        border-radius: 4px !important;
        font-weight: 500 !important;
        box-shadow: 0 1px 2px 0 rgba(60,64,67,0.3) !important;
    }
    
    .stButton > button:hover {
        background-color: #1765cc !important;
    }
    
    /* === TABLES === */
    .dataframe {
        border: 1px solid #e8eaed !important;
        border-radius: 8px !important;
    }
    
    .dataframe th {
        background-color: #f8f9fa !important;
        color: #202124 !important;
    }
    
    .dataframe td {
        color: #202124 !important;
    }
    
    /* === SUBTLE BACKGROUND DECORATIONS (BEHIND EVERYTHING) === */
    [data-testid="stAppViewContainer"]::before {
        content: '';
        position: fixed;
        width: 500px;
        height: 500px;
        background: radial-gradient(circle, #1a73e8 0%, transparent 70%);
        opacity: 0.02;
        top: -200px;
        right: -200px;
        z-index: 0;
        pointer-events: none;
    }
    
    [data-testid="stAppViewContainer"]::after {
        content: '';
        position: fixed;
        width: 400px;
        height: 400px;
        background: radial-gradient(circle, #34a853 0%, transparent 70%);
        opacity: 0.02;
        bottom: -150px;
        left: -150px;
        z-index: 0;
        pointer-events: none;
    }
</style>
""", unsafe_allow_html=True)



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
            
            # Convert result to DataFrame with proper types
            if result.result and result.result.data_array:
                columns = [col.name for col in result.manifest.schema.columns]
                df = pd.DataFrame(result.result.data_array, columns=columns)
                
                # Convert numeric columns properly
                for col in df.columns:
                    try:
                        # Try to convert to numeric, keep original if fails
                        df[col] = pd.to_numeric(df[col], errors='ignore')
                    except:
                        pass
                
                return df
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

# ============ SIDEBAR - VERTICAL NAVIGATION ============
with st.sidebar:
    st.markdown("### 📊 Analytics")
    st.markdown("")  # spacing
    
    # Navigation - Clean button-style
    selected_page = st.radio(
        "Navigation",
        ["Overview", "Products", "Categories", "Experimentation"],
        label_visibility="collapsed"
    )
    
    
    
    # Settings
    st.subheader("⚙️ Settings")
    days_filter = st.slider("Days to Display", 7, 90, 30)
    
    # Data Refresh
    st.markdown("")
    if st.button("🔄 Clear Cache", use_container_width=True):
        st.cache_data.clear()
        st.success("Cache cleared!")
    
    
    
    # Data Source (compact)
    st.caption(f"📦 Source: {repo.catalog}.{repo.schema}")

# ============ COMPACT HEADER ============
print("[DEBUG] Rendering UI header...")
st.markdown("# Analytics Platform")
print("[DEBUG] Header rendered successfully")

# ============ PAGE: OVERVIEW ============
if selected_page == "Overview":
    <div style="position: fixed; top: 20%; right: 10%; opacity: 0.03; font-size: 250px; color: #1a73e8; z-index: 0; pointer-events: none;">
        📊
    </div>
    """, unsafe_allow_html=True)
    
    print("[DEBUG] Rendering Overview page...")
    # KPIs section
    
    try:
        # Fetch KPIs
        kpis = repo.get_summary_kpis()
        
        # Convert to numeric (SDK returns strings sometimes)
        total_revenue = float(kpis['total_revenue']) if kpis['total_revenue'] else 0
        total_purchases = int(kpis['total_purchases']) if kpis['total_purchases'] else 0
        avg_order_value = float(kpis['avg_order_value']) if kpis['avg_order_value'] else 0
        peak_dau = int(kpis['peak_dau']) if kpis['peak_dau'] else 0
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Revenue", f"${total_revenue:,.0f}", help="Total revenue in USD")
        col2.metric("Total Purchases", f"{total_purchases:,.0f} orders", help="Total number of purchase transactions")
        col3.metric("Avg Order Value", f"${avg_order_value:.2f}", help="Average value per order in USD")
        col4.metric("Peak DAU", f"{peak_dau:,.0f} users", help="Peak Daily Active Users")
        
        
        
        # Conversion Funnel
        daily_df_preview = repo.get_daily_metrics(days_filter)
        if not daily_df_preview.empty:
            # Convert to native Python numbers to avoid type issues
            total_views = float(daily_df_preview['total_events'].sum())
            total_carts_col = daily_df_preview.get('total_carts', pd.Series([0]))
            total_carts = float(total_carts_col.sum()) if len(total_carts_col) > 0 else 0.0
            total_purchases_funnel = float(daily_df_preview['total_purchases'].sum())
            
            # Use actual cart data if available, else estimate
            cart_count = total_carts if total_carts > 0 else total_views * 0.3
            
            funnel_data = pd.DataFrame({
                'Stage': ['Views', 'Add to Cart', 'Purchase'],
                'Count': [int(total_views), int(cart_count), int(total_purchases_funnel)],
                'Conversion': ['100%', 
                              f"{(cart_count/total_views*100) if total_views > 0 else 30:.1f}%",
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
        
        
        
        # Daily trends
        st.subheader(f"Daily Trends (Last {days_filter} Days)")
        daily_df = repo.get_daily_metrics(days_filter)
        
        if not daily_df.empty:
            # Revenue trend
            fig_revenue = px.line(daily_df, x='full_date', y='total_revenue',
                                 title='Daily Revenue',
                                 labels={'total_revenue': 'Revenue (USD)', 'full_date': 'Date'})
            st.plotly_chart(fig_revenue, use_container_width=True)
            
            # DAU and Conversion
            col1, col2 = st.columns(2)
            with col1:
                fig_dau = px.line(daily_df, x='full_date', y='daily_active_users',
                                title='Daily Active Users',
                                labels={'daily_active_users': 'Daily Active Users', 'full_date': 'Date'})
                st.plotly_chart(fig_dau, use_container_width=True)
            
            with col2:
                fig_conv = px.line(daily_df, x='full_date', y='overall_conversion_rate',
                                 title='Conversion Rate',
                                 labels={'overall_conversion_rate': 'Conversion Rate (%)', 'full_date': 'Date'})
                st.plotly_chart(fig_conv, use_container_width=True)
        else:
            st.info("No data available for the selected period")
            
    except Exception as e:
        st.error(f"Error loading overview metrics: {str(e)}")
        st.exception(e)

# ============ PAGE: PRODUCTS ============
elif selected_page == "Products":
    <div style="position: fixed; bottom: 15%; left: 5%; opacity: 0.025; font-size: 200px; color: #34a853; z-index: 0; pointer-events: none;">
        🛒
    </div>
    """, unsafe_allow_html=True)
    
    # Products section
    
    try:
        products_df = repo.get_top_products(20)  # Get more products
        
        if not products_df.empty:
            col1, col2 = st.columns([2, 1])
            
            with col1:
                # Top 10 products horizontal bar
                fig = px.bar(products_df.head(10), x='total_revenue', y='product_name',
                            orientation='h',
                            title='Top 10 Brands by Revenue',
                            labels={'total_revenue': 'Revenue (USD)', 'product_name': 'Brand'},
                            color='total_revenue',
                            color_continuous_scale='Blues')
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Product metrics summary
                st.metric("Total Brands", f"{len(products_df)} brands", help="Number of brands in dataset")
                
                # Safely convert revenue to numeric
                try:
                    top_brand_rev = pd.to_numeric(products_df.iloc[0]['total_revenue'], errors='coerce')
                    st.metric("Top Brand Revenue", f"${top_brand_rev:,.0f}", help="Revenue of #1 brand in USD")
                except:
                    st.metric("Top Brand Revenue", "Data error")
                
                try:
                    # Convert entire column to numeric, then calculate mean
                    revenue_numeric = pd.to_numeric(products_df['total_revenue'], errors='coerce')
                    avg_rev = revenue_numeric.mean()
                    st.metric("Avg Revenue/Brand", f"${avg_rev:,.0f}", help="Average revenue per brand in USD")
                except:
                    st.metric("Avg Revenue/Brand", "Data error")
            
            # === ROW 2: Treemap + Scatter in 2 columns ===
            col_tree, col_scatter = st.columns([1, 1])
            
            with col_tree:
                st.markdown("### 🗺️ Revenue Treemap")
                fig_tree = px.treemap(products_df.head(15), 
                                     path=['product_name'], 
                                     values='total_revenue',
                                     title='Brand Revenue Distribution',
                                     color='total_revenue',
                                     color_continuous_scale='RdYlGn',
                                     height=450)
                st.plotly_chart(fig_tree, use_container_width=True)
            
            with col_scatter:
                st.markdown("### 📊 Revenue vs Quantity")
                fig_scatter = px.scatter(products_df.head(15), 
                                        x='total_quantity_sold', 
                                        y='total_revenue',
                                        size='total_purchases',
                                        hover_data=['product_name'],
                                        title='Revenue vs Units Sold',
                                    labels={'total_quantity_sold': 'Units Sold (Quantity)', 
                                           'total_revenue': 'Revenue (USD)'},
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

# ============ PAGE: CATEGORIES ============
elif selected_page == "Categories":
    <div style="position: fixed; top: 30%; right: 8%; opacity: 0.025; font-size: 220px; color: #fbbc04; z-index: 0; pointer-events: none;">
        📦
    </div>
    """, unsafe_allow_html=True)
    
    # Categories section
    
    try:
        categories_df = repo.get_category_performance()
        
        if not categories_df.empty:
            # Top row: Key metrics
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Categories", len(categories_df))
            with col2:
                st.metric("Top Category", categories_df.iloc[0]['category_name'])
            with col3:
                top_cat_revenue = float(categories_df.iloc[0]['total_revenue'])
                st.metric("Top Cat Revenue", f"${top_cat_revenue:,.0f}")
            
            
            
            # Row 1: Pie + Sunburst
            col1, col2 = st.columns(2)
            
            with col1:
                # Donut chart for better readability
                fig = px.pie(categories_df, values='total_revenue', names='category_name',
                           title='🍰 Revenue Distribution by Category',
                           hole=0.4)  # Makes it a donut chart
                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Bar chart with colors
                fig = px.bar(categories_df, x='category_name', y='total_purchases',
                           title='🛍️ Purchases by Category',
                           labels={'total_purchases': 'Number of Purchases', 'category_name': 'Category'},
                           color='total_purchases',
                           color_continuous_scale='Oranges')
                fig.update_xaxes(tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
            
            # === ROW 2: Performance Comparison + AOV in 2 columns ===
            col_perf, col_aov = st.columns([1, 1])
            
            with col_perf:
                st.markdown("### 📈 Performance")
                # Multi-metric bar chart
                fig_multi = px.bar(categories_df, 
                                  x='category_name', 
                                  y=['total_revenue', 'total_purchases'],
                                  title='Revenue vs Purchases',
                                  labels={'value': 'Amount (USD/Count)', 'variable': 'Metric', 'category_name': 'Category'},
                                  barmode='group',
                                  color_discrete_map={'total_revenue': '#636EFA', 'total_purchases': '#EF553B'},
                                  height=400)
                fig_multi.update_xaxes(tickangle=-45)
                st.plotly_chart(fig_multi, use_container_width=True)
            
            with col_aov:
                st.markdown("### 💰 Avg Order Value")
                categories_df['aov_numeric'] = pd.to_numeric(categories_df['avg_order_value'], errors='coerce')
                fig_aov = px.bar(categories_df.sort_values('aov_numeric', ascending=False), 
                                x='category_name', y='aov_numeric',
                                title='AOV by Category',
                                labels={'aov_numeric': 'Avg Order Value (USD)', 'category_name': 'Category'},
                                color='aov_numeric',
                                color_continuous_scale='Greens',
                                height=400)
                fig_aov.update_xaxes(tickangle=-45)
                st.plotly_chart(fig_aov, use_container_width=True)
            
            # Data table
            st.markdown("### 📋 Detailed Category Table")
            st.dataframe(categories_df, use_container_width=True, height=300)
        else:
            st.info("No category data available")
            
    except Exception as e:
        st.error(f"Error loading category metrics: {str(e)}")
        st.exception(e)

# ============ PAGE: EXPERIMENTATION ============
elif selected_page == "Experimentation":
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


st.markdown("🚀 Built with Streamlit + Databricks Delta Lake | 📊 Powered by Gold Layer Analytics")