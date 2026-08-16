# Product Analytics Platform - Frontend Dashboard

Streamlit dashboard for real-time product analytics powered by Databricks Delta Lake.

---

## 🚀 Quick Start (Local Setup)

### Prerequisites
- Python 3.9+
- Databricks workspace access
- Access to `product_analytics.ecommerce` catalog

### Step 1: Clone Repository
```bash
git clone <your-repo-url>
cd product-analytics-platform/frontend
```

### Step 2: Setup Python Environment
```bash
# Create virtual environment
python3 -m venv venv

# Activate it
source venv/bin/activate  # Mac/Linux
# OR
venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt
```

### Step 3: Configure Databricks Authentication

Create `~/.databrickscfg` file:
```ini
[DEFAULT]
host = https://dbc-eedc1a2c-39d4.cloud.databricks.com
token = <YOUR_DATABRICKS_TOKEN>
```

**Get your Databricks token:**
1. Go to Databricks UI → Settings (top right)
2. Click "User Settings"
3. Go to "Access tokens" tab
4. Click "Generate new token"
5. Copy the token and paste it in `~/.databrickscfg`

### Step 4: Run the Dashboard
```bash
streamlit run app.py
```

The app will open at **http://localhost:8501** 🎉

---

## 📊 Dashboard Features

### 📈 Overview Tab
- Total Revenue, Purchases, AOV, Peak DAU
- Daily revenue trends
- Daily Active Users (DAU) chart
- Conversion rate trends

### 🛍️ Products Tab
- Top 10 products by revenue
- Product performance table
- Revenue breakdown

### 📅 Categories Tab
- Revenue by category (pie chart)
- Purchases by category (bar chart)
- Category performance metrics

### 🧪 Experimentation Tab
- Placeholder for A/B testing
- ML models section
- Statistical analysis (coming soon)

---

## 🗂️ Data Sources

The dashboard connects to these Gold layer tables:

- `product_analytics.ecommerce.gold_daily_metrics`
- `product_analytics.ecommerce.gold_product_performance`
- `product_analytics.ecommerce.gold_category_performance`

**Catalog:** `product_analytics`  
**Schema:** `ecommerce`

---

## ⚙️ Configuration

### Filters
- **Days to Display**: 7-90 days (slider in sidebar)
- **Data Refresh**: Clear cache button to force reload

### Caching
- Query results cached for 5 minutes (TTL=300s)
- Use "Clear Cache" button to refresh data immediately

---

## 🏗️ Architecture

```
frontend/
├── app.py                 # Main Streamlit app
├── requirements.txt       # Python dependencies
├── app.yaml              # Databricks Apps config (for cloud deployment)
└── README.md             # This file
```

**Key Components:**
- `WorkspaceClient`: Databricks SDK for authentication
- `MetricsRepository`: Data access layer with query methods
- `Streamlit`: UI framework
- `Plotly`: Interactive charts

---

## 🔒 Environment Variables

**Required in `~/.databrickscfg`:**
- `host`: Your Databricks workspace URL
- `token`: Your Databricks access token

**No `.env` file needed** - Databricks SDK reads from `~/.databrickscfg` automatically.

---

## 🐛 Troubleshooting

### "No module named 'databricks'"  
```bash
pip install databricks-sdk
```

### "Authentication failed"  
- Check your `~/.databrickscfg` file exists
- Verify token is valid (regenerate if expired)
- Ensure host URL is correct

### "Table not found"  
- Verify you have access to `product_analytics.ecommerce` catalog
- Run a test query in Databricks SQL to confirm table exists

### "Query execution error"  
- Ensure you have a SQL warehouse running
- Check warehouse permissions

---

## 📦 Dependencies

- `streamlit` - Web app framework
- `pandas` - Data manipulation
- `plotly` - Interactive charts
- `databricks-sdk` - Databricks API client

---

## 🚀 Deploying to Databricks Apps (Optional)

This dashboard can also run on Databricks Apps (cloud hosted), but requires additional setup for authentication.

For local development, use the instructions above for the best experience.

---

## 📝 Notes

- **Local is recommended**: Running locally gives you full access to Delta tables without authentication complexity
- **Fast queries**: Serverless SQL warehouse executes queries in seconds
- **Real-time updates**: Data refreshes every 5 minutes (configurable)
- **Professional patterns**: Repository pattern, caching, error handling

---

**Built with ❤️ on Databricks**