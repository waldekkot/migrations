# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "streamlit>=1.51.0",
#     "pyodbc>=5.0.0",
#     "pandas>=2.0.0",
# ]
# ///

# Sales Performance Dashboard - SQL Server Edition
# AdventureWorks: Salesperson Performance by Territory

import os
import streamlit as st
import pandas as pd
import pyodbc

# ─────────────────────────────────────────────────────────────────────────────
# Page Configuration
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Sales Performance Dashboard",
    page_icon="📊",
    layout="wide"
)

# ─────────────────────────────────────────────────────────────────────────────
# SQL Server Connection (credentials from environment variables)
# ─────────────────────────────────────────────────────────────────────────────
SQL_SERVER = os.getenv("SQL_SERVER", "127.0.0.1,1433")
SQL_DATABASE = os.getenv("SQL_DATABASE", "AdventureWorks2017")
SQL_USER = "sa"
SQL_PASSWORD = os.getenv("ADMIN_PASS", "")

@st.cache_resource
def get_connection():
    """Create a connection to SQL Server running in Docker"""
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        f"UID={SQL_USER};"
        f"PWD={SQL_PASSWORD};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)

# ─────────────────────────────────────────────────────────────────────────────
# Data Query
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=600)
def load_salesperson_data():
    """Load salesperson performance data from AdventureWorks"""
    query = """
    WITH salesperson_metrics AS (
        SELECT 
            sp.BusinessEntityID,
            CONCAT(per.FirstName, ' ', per.LastName) AS salesperson_name,
            st.Name AS territory_name,
            st.[Group] AS region_group,
            sp.SalesQuota,
            sp.SalesYTD,
            sp.SalesLastYear,
            COALESCE(sp.SalesYTD, 0) - COALESCE(sp.SalesLastYear, 0) AS yoy_change,
            CASE 
                WHEN sp.SalesQuota > 0 
                THEN ROUND((sp.SalesYTD / sp.SalesQuota) * 100, 2)
                ELSE NULL 
            END AS quota_achievement_pct
        FROM Sales.SalesPerson sp
        INNER JOIN HumanResources.Employee e 
            ON sp.BusinessEntityID = e.BusinessEntityID
        INNER JOIN Person.Person per 
            ON e.BusinessEntityID = per.BusinessEntityID
        LEFT JOIN Sales.SalesTerritory st 
            ON sp.TerritoryID = st.TerritoryID
    )
    SELECT 
        territory_name,
        region_group,
        salesperson_name,
        SalesYTD AS sales_ytd,
        SalesLastYear AS sales_last_year,
        yoy_change,
        quota_achievement_pct,
        RANK() OVER (PARTITION BY territory_name ORDER BY SalesYTD DESC) AS rank_in_territory
    FROM salesperson_metrics
    WHERE SalesYTD > 0
    ORDER BY region_group, territory_name, SalesYTD DESC
    """
    conn = get_connection()
    return pd.read_sql(query, conn)

# Load data
try:
    df = load_salesperson_data()
    data_loaded = True
except Exception as e:
    st.error(f"❌ Failed to connect to SQL Server: {e}")
    st.info("💡 Make sure SQL Server is running in Docker on 127.0.0.1:1433")
    st.markdown("""
**Required environment variables:**
- `ADMIN_PASS` - SQL Server password *(required)*
- `SQL_SERVER` - Server address (default: 127.0.0.1,1433)
- `SQL_DATABASE` - Database name (default: AdventureWorks2017)
    """)
    st.code("""
# Source setenv.sh and run
source ../setenv.sh && streamlit run app.py
    """, language="bash")
    data_loaded = False
    df = pd.DataFrame()

# ─────────────────────────────────────────────────────────────────────────────
# App Header
# ─────────────────────────────────────────────────────────────────────────────
st.title("📊 Sales Performance Dashboard")
st.markdown("**AdventureWorks** | Salesperson Performance by Territory")
st.caption("🐳 Connected to SQL Server (Docker)")
st.divider()

if not data_loaded or df.empty:
    st.warning("No data available. Please check your SQL Server connection.")
    st.stop()

# ─────────────────────────────────────────────────────────────────────────────
# Sidebar Filters
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("🔍 Filters")
    
    # Region filter
    all_regions = df['region_group'].dropna().unique().tolist()
    selected_regions = st.multiselect(
        "Select Region(s)",
        options=all_regions,
        default=all_regions,
        help="Filter by geographic region"
    )
    
    # Territory filter (dependent on region selection)
    available_territories = df[df['region_group'].isin(selected_regions)]['territory_name'].dropna().unique().tolist()
    selected_territories = st.multiselect(
        "Select Territory(ies)",
        options=available_territories,
        default=available_territories,
        help="Filter by sales territory"
    )
    
    # Sales threshold slider
    min_sales = float(df['sales_ytd'].min())
    max_sales = float(df['sales_ytd'].max())
    sales_threshold = st.slider(
        "Minimum Sales YTD ($)",
        min_value=min_sales,
        max_value=max_sales,
        value=min_sales,
        format="$%.0f",
        help="Filter salespeople by minimum YTD sales"
    )
    
    # Show only top performers toggle
    show_top_only = st.checkbox(
        "Show only #1 ranked per territory",
        value=False,
        help="Display only the top performer in each territory"
    )
    
    st.divider()
    st.caption("💡 Tip: Use filters to drill down into specific regions or territories")

# ─────────────────────────────────────────────────────────────────────────────
# Apply Filters
# ─────────────────────────────────────────────────────────────────────────────
filtered_df = df[
    (df['region_group'].isin(selected_regions)) &
    (df['territory_name'].isin(selected_territories)) &
    (df['sales_ytd'] >= sales_threshold)
]

if show_top_only:
    filtered_df = filtered_df[filtered_df['rank_in_territory'] == 1]

# ─────────────────────────────────────────────────────────────────────────────
# KPI Metrics Row
# ─────────────────────────────────────────────────────────────────────────────
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        label="Total Salespeople",
        value=len(filtered_df),
        delta=f"{len(filtered_df) - len(df)} filtered" if len(filtered_df) != len(df) else None
    )

with col2:
    total_ytd = filtered_df['sales_ytd'].sum()
    st.metric(
        label="Total Sales YTD",
        value=f"${total_ytd:,.0f}"
    )

with col3:
    avg_quota = filtered_df['quota_achievement_pct'].mean()
    st.metric(
        label="Avg Quota Achievement",
        value=f"{avg_quota:.1f}%" if pd.notna(avg_quota) else "N/A"
    )

with col4:
    total_yoy = filtered_df['yoy_change'].sum()
    st.metric(
        label="Total YoY Change",
        value=f"${total_yoy:,.0f}",
        delta=f"{'↑' if total_yoy > 0 else '↓'} vs Last Year"
    )

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# Charts Section
# ─────────────────────────────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["📊 By Salesperson", "🗺️ By Territory", "📈 YoY Comparison"])

with tab1:
    st.subheader("Sales YTD by Salesperson")
    
    # Prepare chart data - sorted by sales
    chart_data = filtered_df[['salesperson_name', 'sales_ytd', 'territory_name']].copy()
    chart_data = chart_data.sort_values('sales_ytd', ascending=True)
    
    # Horizontal bar chart
    st.bar_chart(
        chart_data.set_index('salesperson_name')['sales_ytd'],
        horizontal=True,
        height=max(400, len(chart_data) * 35)
    )

with tab2:
    st.subheader("Total Sales by Territory")
    
    # Aggregate by territory
    territory_data = filtered_df.groupby('territory_name').agg({
        'sales_ytd': 'sum',
        'salesperson_name': 'count'
    }).rename(columns={'salesperson_name': 'num_salespeople'}).reset_index()
    territory_data = territory_data.sort_values('sales_ytd', ascending=False)
    
    col_chart, col_table = st.columns([2, 1])
    
    with col_chart:
        st.bar_chart(
            territory_data.set_index('territory_name')['sales_ytd'],
            height=400
        )
    
    with col_table:
        st.dataframe(
            territory_data,
            column_config={
                "territory_name": st.column_config.TextColumn("Territory"),
                "sales_ytd": st.column_config.NumberColumn("Total Sales", format="$%.0f"),
                "num_salespeople": st.column_config.NumberColumn("# Salespeople")
            },
            hide_index=True,
            width='stretch'
        )

with tab3:
    st.subheader("Year-over-Year Performance")
    
    # Prepare comparison data
    comparison_data = filtered_df[['salesperson_name', 'sales_ytd', 'sales_last_year']].copy()
    comparison_data = comparison_data.sort_values('sales_ytd', ascending=False).head(10)
    comparison_data = comparison_data.set_index('salesperson_name')
    comparison_data.columns = ['This Year', 'Last Year']
    
    st.bar_chart(comparison_data, height=400)
    
    st.caption("📌 Showing top 10 salespeople by current year sales")

# ─────────────────────────────────────────────────────────────────────────────
# Detailed Data Table
# ─────────────────────────────────────────────────────────────────────────────
st.divider()
st.subheader("📋 Detailed Performance Data")

# Display options
col_display1, col_display2 = st.columns([1, 3])
with col_display1:
    sort_by = st.selectbox(
        "Sort by",
        options=['sales_ytd', 'yoy_change', 'quota_achievement_pct', 'salesperson_name'],
        format_func=lambda x: {
            'sales_ytd': 'Sales YTD',
            'yoy_change': 'YoY Change',
            'quota_achievement_pct': 'Quota Achievement',
            'salesperson_name': 'Name'
        }.get(x, x)
    )

# Sort and display
display_df = filtered_df.sort_values(sort_by, ascending=(sort_by == 'salesperson_name'))

st.dataframe(
    display_df,
    column_config={
        "territory_name": st.column_config.TextColumn("Territory", width="medium"),
        "region_group": st.column_config.TextColumn("Region", width="small"),
        "salesperson_name": st.column_config.TextColumn("Salesperson", width="medium"),
        "sales_ytd": st.column_config.NumberColumn("Sales YTD", format="$%.0f"),
        "sales_last_year": st.column_config.NumberColumn("Sales Last Year", format="$%.0f"),
        "yoy_change": st.column_config.NumberColumn(
            "YoY Change", 
            format="$%.0f",
            help="Year-over-Year change in sales"
        ),
        "quota_achievement_pct": st.column_config.ProgressColumn(
            "Quota %",
            min_value=0,
            max_value=200,
            format="%.1f%%"
        ),
        "rank_in_territory": st.column_config.NumberColumn("Rank", width="small")
    },
    hide_index=True,
    width='stretch',
    height=400
)

# ─────────────────────────────────────────────────────────────────────────────
# Footer
# ─────────────────────────────────────────────────────────────────────────────
st.divider()
st.caption("Data source: AdventureWorks Sample Database | Connected to SQL Server 🐳")

