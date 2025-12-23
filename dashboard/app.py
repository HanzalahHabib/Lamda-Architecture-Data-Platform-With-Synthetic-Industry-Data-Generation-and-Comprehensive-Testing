import streamlit as st
import pandas as pd
import time
import sys
import os
import plotly.express as px

# Check if parent dir is in path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from serving_layer.query_engine import ServingLayer

st.set_page_config(page_title="Lambda Architecture Dashboard", layout="wide")

st.title("Enterprise Data Platform - Real-time Dashboard")
st.markdown("### Lambda Architecture: Batch (Historical) + Speed (Real-time)")

# Initialize Serving Layer
sl = ServingLayer()

# Auto-refresh mechanism
if st.button("Refresh Data"):
    st.rerun()

# Placeholders for KPIs
col1, col2, col3 = st.columns(3)
kpi1 = col1.empty()
kpi2 = col2.empty()
kpi3 = col3.empty()

# Main Chart
st.subheader("Sales Trend (Unified View)")
chart_placeholder = st.empty()

# Recent Data Table
st.subheader("Live Transaction Feed")
table_placeholder = st.empty()


def load_data():
    kpis = sl.get_kpis()
    recent = sl.get_recent_transactions(15)
    
    # Calculate trend
    df_all = sl.get_unified_view()
    
    return kpis, recent, df_all

# Load
kpis, recent, df_all = load_data()

# Metric Cards
kpi1.metric("Total Sales Volume", f"${kpis['total_sales']:,.2f}")
kpi2.metric("Total Transactions", f"{kpis['transaction_count']:,}")
kpi3.metric("Avg Order Value", f"${kpis['avg_order_value']:.2f}")

# Charts
if df_all is not None and not df_all.empty:
    # Ensure timestamp is datetime
    df_all['timestamp'] = pd.to_datetime(df_all['timestamp'])
    
    # Resample for clean chart (Hourly)
    df_chart = df_all.set_index('timestamp').resample('H')['amount'].sum().reset_index()
    
    fig = px.line(df_chart, x='timestamp', y='amount', title='hourly Sales Trend', template="plotly_dark")
    chart_placeholder.plotly_chart(fig, use_container_width=True)
else:
    chart_placeholder.info("No data available yet. Run the pipeline!")

# Table
table_placeholder.dataframe(recent)

# Footer
st.markdown("---")
st.caption("System Status: Online | Architecture: Lambda | Serving: DuckDB")
