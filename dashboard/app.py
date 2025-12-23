import streamlit as st
import pandas as pd
import time
import sys
import os
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# --- SYSTEM INITIALIZATION ---
st.set_page_config(
    page_title="NEBULA PLATFORM | V2.5.0",
    page_icon="🌌",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Check if parent dir is in path for Serving Layer
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

try:
    from serving_layer.query_engine import ServingLayer
except ImportError:
    st.error("Pipeline Core Link Failure: Serving Layer not found in path.")
    st.stop()

# --- 25TH CENTURY HUD STYLING ---
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@300;400;700&family=JetBrains+Mono:wght@200;400&display=swap');

    :root {
        --neon-cyan: #00f3ff;
        --neon-green: #00ff9f;
        --deep-space: #050505;
        --glass-bg: rgba(255, 255, 255, 0.03);
    }

    body, [class*="css"] {
        font-family: 'Space Grotesk', sans-serif;
        color: #e0e0e0;
    }

    .stApp {
        background-color: var(--deep-space);
        background-image: 
            radial-gradient(at 0% 0%, hsla(190, 100%, 15%, 0.3) 0, transparent 50%),
            radial-gradient(at 100% 100%, hsla(150, 100%, 10%, 0.2) 0, transparent 50%);
    }

    /* Metric Cards with Glowing Border */
    div[data-testid="stMetric"] {
        background: var(--glass-bg);
        border: 1px solid rgba(0, 243, 255, 0.1);
        padding: 25px !important;
        border-radius: 12px;
        backdrop-filter: blur(15px);
        box-shadow: 0 0 20px rgba(0, 243, 255, 0.05);
        transition: all 0.4s cubic-bezier(0.175, 0.885, 0.32, 1.275);
    }
    
    div[data-testid="stMetric"]:hover {
        border: 1px solid var(--neon-cyan);
        box-shadow: 0 0 30px rgba(0, 243, 255, 0.2);
        transform: scale(1.02);
    }

    /* Sidebar - HUD Style */
    section[data-testid="stSidebar"] {
        background-color: rgba(5, 5, 5, 0.95) !important;
        border-right: 1px solid rgba(0, 243, 255, 0.15);
    }

    /* Titles */
    .glitch-title {
        font-size: 3.5rem !important;
        font-weight: 700;
        letter-spacing: -2px;
        background: linear-gradient(135deg, var(--neon-cyan), var(--neon-green));
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-shadow: 0 0 15px rgba(0, 243, 255, 0.3);
    }

    .hud-label {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.75rem;
        color: var(--neon-cyan);
        text-transform: uppercase;
        letter-spacing: 4px;
        margin-bottom: 2rem;
        opacity: 0.8;
    }

    /* Table HUD */
    .stDataFrame {
        border: 1px solid rgba(0, 243, 255, 0.1);
        border-radius: 10px;
        overflow: hidden;
    }

    /* HUD Elements */
    .hud-line {
        height: 2px;
        background: linear-gradient(90deg, var(--neon-cyan), transparent);
        margin: 10px 0 30px 0;
    }

    /* Success Glow */
    .stAlert {
        border-radius: 10px;
        background: rgba(0, 255, 159, 0.05);
        border: 1px solid rgba(0, 255, 159, 0.2);
    }
    </style>
    """, unsafe_allow_html=True)

# --- PIPELINE CONTROL PANEL ---
sl = ServingLayer()

def fetch_telemetry():
    kpis = sl.get_kpis()
    recent = sl.get_recent_transactions(12)
    df_raw = sl.get_unified_view()
    return kpis, recent, df_raw

# --- SIDEBAR CONTROL PANEL ---
with st.sidebar:
    st.markdown("<h1 style='color:#00f3ff; font-family:monospace;'>[ LAMBDA_CORE ]</h1>", unsafe_allow_html=True)
    st.image("https://img.icons8.com/nolan/128/satellite.png")
    st.divider()
    
    st.markdown("### PIPELINE CONTROLS")
    sync_active = st.toggle("Live Stream Pulse", value=True)
    sync_freq = st.select_slider("Dashboard Refresh Interval (s)", options=[1, 2, 5, 10, 30], value=5)
    
    st.divider()
    st.caption("CORE STATUS: ONLINE")
    st.progress(100)
    st.caption("MEMORY LOAD: 14.2 GB / 64 GB")

# --- MAIN HUD CONTENT ---
st.markdown('<h1 class="glitch-title">LAMBDA DATA PLATFORM</h1>', unsafe_allow_html=True)
st.markdown('<p class="hud-label">// LAMBDA ARCHITECTURE // UNIFIED SERVING LAYER v2.5.0</p>', unsafe_allow_html=True)
st.markdown('<div class="hud-line"></div>', unsafe_allow_html=True)

# Telemetry Sync
kpis, recent, df_all = fetch_telemetry()

# --- TOP HUD METRICS ---
m1, m2, m3, m4 = st.columns(4)

with m1:
    st.metric("TOTAL REVENUE VOLUME", f"${kpis['total_sales'] / 1000:,.1f}K", delta="STABLE")

with m2:
    st.metric("TOTAL TRANSACTIONS", f"{kpis['transaction_count']:,}", delta="SYNCED")

with m3:
    st.metric("AVERAGE ORDER VALUE", f"${kpis['avg_order_value']:.2f}")

with m4:
    sync_ts = "OFFLINE"
    if df_all is not None and not df_all.empty:
        sync_ts = pd.to_datetime(df_all['timestamp']).max().strftime("%H:%M:%S")
    st.metric("LAST DATA POLL", sync_ts, delta="ACTIVE")

# --- ANALYSIS GRID ---
g1, g2 = st.columns([2, 1])

with g1:
    st.markdown("### <span style='color:#00f3ff'>◣</span> HOURLY REVENUE TREND", unsafe_allow_html=True)
    if df_all is not None and not df_all.empty:
        df_all['timestamp'] = pd.to_datetime(df_all['timestamp'])
        df_chart = df_all.set_index('timestamp').resample('H')['amount'].sum().reset_index()
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df_chart['timestamp'], 
            y=df_chart['amount'],
            fill='tozeroy',
            line=dict(color='#00f3ff', width=3),
            fillcolor='rgba(0, 243, 255, 0.1)',
            name='Transaction Flow'
        ))
        fig.update_layout(
            template="plotly_dark",
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=0, r=0, t=10, b=0),
            height=380,
            xaxis=dict(showgrid=False, zeroline=False),
            yaxis=dict(showgrid=True, gridcolor='rgba(255,255,255,0.05)', zeroline=False),
            hovermode="x unified"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Awaiting Pipeline Data... Synchronizing Streams.")

with g2:
    st.markdown("### <span style='color:#00ff9f'>◣</span> PRODUCT SEGMENTATION", unsafe_allow_html=True)
    if df_all is not None and not df_all.empty:
        prod_dist = df_all.groupby('product')['amount'].count().reset_index()
        fig_pie = go.Figure(data=[go.Pie(
            labels=prod_dist['product'], 
            values=prod_dist['amount'],
            hole=.8,
            marker=dict(colors=['#00f3ff', '#00ff9f', '#0099ff', '#00cc66'], line=dict(color='#000', width=2))
        )])
        fig_pie.update_layout(
            showlegend=False,
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=0, r=0, t=10, b=0),
            height=380
        )
        st.plotly_chart(fig_pie, use_container_width=True)

# --- LIVE FEED HUD ---
st.markdown("### <span style='color:#00f3ff'>◣</span> REAL-TIME EVENT STREAM", unsafe_allow_html=True)
if not recent.empty:
    st.dataframe(
        recent.style.format({"amount": "${:,.2f}"}),
        use_container_width=True
    )
else:
    st.warning("STREAM OFFLINE: RECONNECTING TO DATA SOURCES...")

# --- FOOTER HUD ---
st.markdown(f"""
    <div style="background: rgba(0, 243, 255, 0.05); padding: 15px; border-radius: 8px; font-family: monospace; font-size: 0.7rem; border-left: 5px solid #00f3ff; display: flex; justify-content: space-between; align-items: center; margin-top: 3rem;">
        <div>SYSTEM_VERSION: 1.0.4-LMBDA | ARCHITECTURE: LAMBDA | ORIGIN: PIPELINE_CORE</div>
        <div style="color:#00f3ff">● LAST_SYNC_POLL: {datetime.now().strftime('%H:%M:%S')}</div>
    </div>
""", unsafe_allow_html=True)

# --- NEURAL REFRESH ---
if sync_active:
    time.sleep(sync_freq)
    st.rerun()
