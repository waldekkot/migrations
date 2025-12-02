# Sales Performance Dashboard - Streamlit in Snowflake
# AdventureWorks: Salesperson Performance by Territory

import streamlit as st
import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
# Page Configuration
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Sales Performance Dashboard",
    page_icon="📊",
    layout="wide"
)

# ─────────────────────────────────────────────────────────────────────────────
# Snowflake Connection
# ─────────────────────────────────────────────────────────────────────────────
session = st.connection('snowflake').session()

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
            st."Group" AS region_group,
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
    return session.sql(query).to_pandas()

# Load data
df = load_salesperson_data()

# ─────────────────────────────────────────────────────────────────────────────
# App Header
# ─────────────────────────────────────────────────────────────────────────────
st.title("📊 Sales Performance Dashboard")
st.markdown("**AdventureWorks** | Salesperson Performance by Territory")
st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# Sidebar Filters
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("🔍 Filters")
    
    # Region filter
    all_regions = df['REGION_GROUP'].dropna().unique().tolist()
    selected_regions = st.multiselect(
        "Select Region(s)",
        options=all_regions,
        default=all_regions,
        help="Filter by geographic region"
    )
    
    # Territory filter (dependent on region selection)
    available_territories = df[df['REGION_GROUP'].isin(selected_regions)]['TERRITORY_NAME'].dropna().unique().tolist()
    selected_territories = st.multiselect(
        "Select Territory(ies)",
        options=available_territories,
        default=available_territories,
        help="Filter by sales territory"
    )
    
    # Sales threshold slider
    min_sales = float(df['SALES_YTD'].min())
    max_sales = float(df['SALES_YTD'].max())
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
    (df['REGION_GROUP'].isin(selected_regions)) &
    (df['TERRITORY_NAME'].isin(selected_territories)) &
    (df['SALES_YTD'] >= sales_threshold)
]

if show_top_only:
    filtered_df = filtered_df[filtered_df['RANK_IN_TERRITORY'] == 1]

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
    total_ytd = filtered_df['SALES_YTD'].sum()
    st.metric(
        label="Total Sales YTD",
        value=f"${total_ytd:,.0f}"
    )

with col3:
    avg_quota = filtered_df['QUOTA_ACHIEVEMENT_PCT'].mean()
    st.metric(
        label="Avg Quota Achievement",
        value=f"{avg_quota:.1f}%" if pd.notna(avg_quota) else "N/A"
    )

with col4:
    total_yoy = filtered_df['YOY_CHANGE'].sum()
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
    chart_data = filtered_df[['SALESPERSON_NAME', 'SALES_YTD', 'TERRITORY_NAME']].copy()
    chart_data = chart_data.sort_values('SALES_YTD', ascending=True)
    
    # Horizontal bar chart
    st.bar_chart(
        chart_data.set_index('SALESPERSON_NAME')['SALES_YTD'],
        horizontal=True,
        height=max(400, len(chart_data) * 35)
    )

with tab2:
    st.subheader("Total Sales by Territory")
    
    # Aggregate by territory
    territory_data = filtered_df.groupby('TERRITORY_NAME').agg({
        'SALES_YTD': 'sum',
        'SALESPERSON_NAME': 'count'
    }).rename(columns={'SALESPERSON_NAME': 'NUM_SALESPEOPLE'}).reset_index()
    territory_data = territory_data.sort_values('SALES_YTD', ascending=False)
    
    col_chart, col_table = st.columns([2, 1])
    
    with col_chart:
        st.bar_chart(
            territory_data.set_index('TERRITORY_NAME')['SALES_YTD'],
            height=400
        )
    
    with col_table:
        st.dataframe(
            territory_data,
            column_config={
                "TERRITORY_NAME": st.column_config.TextColumn("Territory"),
                "SALES_YTD": st.column_config.NumberColumn("Total Sales", format="$%.0f"),
                "NUM_SALESPEOPLE": st.column_config.NumberColumn("# Salespeople")
            },
            hide_index=True,
            use_container_width=True
        )

with tab3:
    st.subheader("Year-over-Year Performance")
    
    # Prepare comparison data
    comparison_data = filtered_df[['SALESPERSON_NAME', 'SALES_YTD', 'SALES_LAST_YEAR']].copy()
    comparison_data = comparison_data.sort_values('SALES_YTD', ascending=False).head(10)
    comparison_data = comparison_data.set_index('SALESPERSON_NAME')
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
        options=['SALES_YTD', 'YOY_CHANGE', 'QUOTA_ACHIEVEMENT_PCT', 'SALESPERSON_NAME'],
        format_func=lambda x: {
            'SALES_YTD': 'Sales YTD',
            'YOY_CHANGE': 'YoY Change',
            'QUOTA_ACHIEVEMENT_PCT': 'Quota Achievement',
            'SALESPERSON_NAME': 'Name'
        }.get(x, x)
    )

# Sort and display
display_df = filtered_df.sort_values(sort_by, ascending=(sort_by == 'SALESPERSON_NAME'))

st.dataframe(
    display_df,
    column_config={
        "TERRITORY_NAME": st.column_config.TextColumn("Territory", width="medium"),
        "REGION_GROUP": st.column_config.TextColumn("Region", width="small"),
        "SALESPERSON_NAME": st.column_config.TextColumn("Salesperson", width="medium"),
        "SALES_YTD": st.column_config.NumberColumn("Sales YTD", format="$%.0f"),
        "SALES_LAST_YEAR": st.column_config.NumberColumn("Sales Last Year", format="$%.0f"),
        "YOY_CHANGE": st.column_config.NumberColumn(
            "YoY Change", 
            format="$%.0f",
            help="Year-over-Year change in sales"
        ),
        "QUOTA_ACHIEVEMENT_PCT": st.column_config.ProgressColumn(
            "Quota %",
            min_value=0,
            max_value=200,
            format="%.1f%%"
        ),
        "RANK_IN_TERRITORY": st.column_config.NumberColumn("Rank", width="small")
    },
    hide_index=True,
    use_container_width=True,
    height=400
)

# ─────────────────────────────────────────────────────────────────────────────
# Footer
# ─────────────────────────────────────────────────────────────────────────────
st.divider()
st.caption("Data source: AdventureWorks Sample Database | Built with Streamlit in Snowflake ❄️")

