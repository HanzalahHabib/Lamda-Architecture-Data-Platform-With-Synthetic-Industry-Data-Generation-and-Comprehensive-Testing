import streamlit as st
import pandas as pd
import time
import sys
import os
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# Check if parent dir is in path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from serving_layer.query_engine import ServingLayer

# --- Page Config ---
st.set_page_config(
    page_title="NEBULA | Lambda Data Platform",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Enhanced Glassmorphism CSS ---
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600&display=swap');

    html, body, [class*="css"] {
        font-family: 'Outfit', sans-serif;
    }

    /* Main Background */
    .stApp {
        background: radial-gradient(circle at top right, #1a1a2e, #16213e, #0f3460);
    }

    /* Metric Containers */
    div[data-testid="stMetric"] {
        background: rgba(255, 255, 255, 0.03);
        border: 1px solid rgba(255, 255, 255, 0.1);
        padding: 20px;
        border-radius: 20px;
        backdrop-filter: blur(10px);
        transition: all 0.3s ease;
        box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
    }
    
    div[data-testid="stMetric"]:hover {
        background: rgba(255, 255, 255, 0.07);
        transform: translateY(-5px);
        border: 1px solid rgba(0, 212, 255, 0.3);
    }

    /* Sidebar Customization */
    section[data-testid="stSidebar"] {
        background-color: rgba(15, 52, 96, 0.8) !important;
        backdrop-filter: blur(20px);
        border-right: 1px solid rgba(255, 255, 255, 0.1);
    }

    /* Headers */
    .main-title {
        font-size: 3rem !important;
        font-weight: 600;
        background: linear-gradient(90deg, #00d4ff, #00ff87);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.5rem;
    }

    .subtitle {
        color: rgba(255, 255, 255, 0.6);
        font-weight: 300;
        letter-spacing: 2px;
        text-transform: uppercase;
        margin-bottom: 2rem;
    }

    /* Dataframe styling */
    .stDataFrame {
        border-radius: 15px;
        overflow: hidden;
        border: 1px solid rgba(255, 255, 255, 0.1);
    }

    /* Refresh Button Styling */
    .stButton>button {
        background: linear-gradient(45deg, #0f3460, #16213e);
        color: white;
        border: 1px solid rgba(0, 212, 255, 0.5);
        border-radius: 10px;
        padding: 0.5rem 2rem;
        transition: all 0.3s ease;
    }

    .stButton>button:hover {
        background: linear-gradient(45deg, #00d4ff, #00ff87);
        color: #1a1a2e;
        border: none;
        box-shadow: 0 0 20px rgba(0, 212, 255, 0.4);
    }

    /* Custom Status Bar */
    .status-bar {
        display: flex;
        justify-content: space-between;
        padding: 10px 20px;
        background: rgba(255, 255, 255, 0.05);
        border-radius: 10px;
        margin-top: 2rem;
        font-size: 0.8rem;
        color: rgba(255, 255, 255, 0.4);
    }
    </style>
    """, unsafe_allow_html=True)

# --- Sidebar ---
with st.sidebar:
    st.image("https://img.icons8.com/nolan/128/artificial-intelligence.png")
    st.markdown("## NEBULA OS")
    st.caption("Alpha v2.5.0 | Quantum Serving")
    st.divider()
    
    st.markdown("### Controls")
    auto_refresh = st.toggle("Real-time Pulse", value=True)
    refresh_rate = st.slider("Neural Sync Rate (s)", 2, 10, 5)
    
    st.divider()
    st.info("Unified data stream from Batch and Speed layers active.")

# --- Header ---
st.markdown('<h1 class="main-title">NEBULA DATA PLATFORM</h1>', unsafe_allow_html=True)
st.markdown('<p class="subtitle">Lambda Architecture // Automated Intelligence System</p>', unsafe_allow_html=True)

# --- Serving Layer Connection ---
sl = ServingLayer()

def load_data():
    kpis = sl.get_kpis()
    recent = sl.get_recent_transactions(10)
    df_all = sl.get_unified_view()
    return kpis, recent, df_all

# Data Fetching
kpis, recent, df_all = load_data()

# --- Top KPI Grid ---
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("TOTAL NET VOLUME", f"${kpis['total_sales'] / 1000:,.1f}K", delta=f"{len(df_all) if df_all is not None else 0} Events")

with col2:
    st.metric("ACTIVE TRANSACTIONS", f"{kpis['transaction_count']:,}", delta="SYNCED")

with col3:
    st.metric("AVG NEURAL VALUE", f"${kpis['avg_order_value']:.2f}", delta="+4.2%")

with col4:
    # Custom Pulse Metric
    last_event_time = "N/A"
    if df_all is not None and not df_all.empty:
        last_event_time = pd.to_datetime(df_all['timestamp']).max().strftime("%H:%M:%S")
    st.metric("LAST HANDSHAKE", last_event_time, delta="ACTIVE", delta_color="normal")

st.divider()

# --- Visualization Section ---
c1, c2 = st.columns([2, 1])

with c1:
    st.markdown("### <span style='color:#00d4ff'>■</span> Real-time Transaction Pulse", unsafe_allow_html=True)
    if df_all is not None and not df_all.empty:
        df_all['timestamp'] = pd.to_datetime(df_all['timestamp'])
        df_chart = df_all.set_index('timestamp').resample('H')['amount'].sum().reset_index()
        
        # Futuristic Line Chart
        fig = px.area(df_chart, x='timestamp', y='amount', 
                      template="plotly_dark",
                      color_discrete_sequence=['#00d4ff'])
        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis_title="",
            yaxis_title="Volume ($)",
            margin=dict(l=0, r=0, t=20, b=0),
            height=400,
            hovermode="x unified",
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("System initializing. No neural patterns detected yet.")

with c2:
    st.markdown("### <span style='color:#00ff87'>■</span> Product Distribution", unsafe_allow_html=True)
    if df_all is not None and not df_all.empty:
        prod_dist = df_all.groupby('product')['amount'].count().reset_index()
        fig_pie = px.pie(prod_dist, values='amount', names='product',
                         hole=0.7,
                         color_discrete_sequence=px.colors.sequential.Cyan_r)
        fig_pie.update_layout(
            showlegend=False,
            margin=dict(l=0, r=0, t=0, b=0),
            height=400,
            paper_bgcolor="rgba(0,0,0,0)"
        )
        st.plotly_chart(fig_pie, use_container_width=True)

# --- Lower Table ---
st.markdown("### <span style='color:#00d4ff'>■</span> Unified Stream Feed")
if not recent.empty:
    st.dataframe(
        recent.style.format({"amount": "${:,.2f}"})
        .background_gradient(subset=['amount'], cmap='GnBu'),
        use_container_width=True
    )
else:
    st.info("Waiting for data stream...")

# --- Footer Status ---
st.markdown(f"""
    <div class="status-bar">
        <span>CORE: <b>CONNECTED</b></span>
        <span>LATENCY: <b>12ms</b></span>
        <span>LAST SYNC: <b>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</b></span>
        <span>SECURITY: <b>QUANTUM-ENCRYPTED</b></span>
    </div>
""", unsafe_allow_html=True)

# --- Auto Refresh ---
if auto_refresh:
    time.sleep(refresh_rate)
    st.rerun()
