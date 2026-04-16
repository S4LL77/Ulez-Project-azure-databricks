import os
import time
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

st.set_page_config(page_title="AutoTrader ULEZ Local Lakehouse", layout="wide", page_icon="🎯")

# Custom CSS for Premium LIGHT Look
st.markdown("""
    <style>
    /* Main Background - Clean White */
    .stApp {
        background-color: #ffffff;
    }

    /* Metric Cards (Light Premium) */
    [data-testid="stMetric"] {
        background-color: #f8f9fa !important;
        border: 1px solid #e9ecef !important;
        padding: 20px !important;
        border-radius: 12px !important;
        box-shadow: 0 2px 10px rgba(0, 0, 0, 0.05) !important;
    }

    /* Force Metric Text Colors for Light Mode */
    [data-testid="stMetricLabel"] {
        color: #6c757d !important; /* Dark Gray Label */
        font-size: 0.9rem !important;
        font-weight: 600 !important;
    }
    [data-testid="stMetricValue"] {
        color: #212529 !important; /* Almost Black Value */
        font-size: 2.2rem !important;
        font-weight: 700 !important;
    }

    /* Headers Styling */
    h1, h2, h3 {
        color: #212529 !important;
        font-family: 'Inter', sans-serif;
    }

    /* Subheaders and Captions */
    .stMarkdown p {
        color: #495057 !important;
    }

    /* Dataframe Styling */
    .stDataFrame {
        border-radius: 10px;
        border: 1px solid #dee2e6;
    }

    /* Sidebar Background */
    [data-testid="stSidebar"] {
        background-color: #f1f3f5;
    }
    </style>
    """, unsafe_allow_html=True)

st.title("🎯 London ULEZ: Local Market Lakehouse Tracking")
st.markdown("---")

# --- SIDEBAR: Controls ---
with st.sidebar:
    st.header("Project Status")
    st.success("Connected to Local Lakehouse")
    st.info("Engine: DuckDB (No Java Required)")
    st.caption("Storage: `data` directory")

    if st.button("Refresh Dashboard"):
        st.cache_data.clear()
        st.rerun()

# --- LOCAL DATA HELPERS ---
@st.cache_data(ttl=60)
def load_parquet_layer(file_path: str):
    """
    Helper to read Parquet layers.
    """
    path = Path(file_path)
    if not path.exists():
        return pd.DataFrame()
    
    try:
        return pd.read_parquet(path)
    except Exception as e:
        st.error(f"Error reading {file_path}: {e}")
        return pd.DataFrame()

# Load Layers
df_impact = load_parquet_layer("data/gold/mart_market_impact.parquet")
df_diesel = load_parquet_layer("data/gold/mart_diesel_devaluation.parquet")
df_clusters = load_parquet_layer("data/gold/mart_market_clusters.parquet")
df_silver = load_parquet_layer("data/silver/fct_cars.parquet")

# Handle Empty Data
if df_impact.empty:
    st.warning("⚠️ No data found in the Gold Marts.")
    st.info("Please follow these steps to generate data:\n1. Run `python 01_ingestion/data_engine.py`\n2. Run `python 02_processing/databricks_pipeline.py` (Now uses DuckDB)\n3. Run `python 02_processing/ml_clustering.py` for ML profiles")
    st.stop()

# --- METRICS ---
st.subheader("Live Market Insights")
m1, m2, m3 = st.columns(3)

total_listings = len(df_silver) if not df_silver.empty else 0
avg_penalty = df_impact["percent_diff"].mean() if not df_impact.empty else 0

m1.metric("Listings Processed", f"{total_listings}", "Real-time")
m2.metric("Avg. ULEZ Impact", f"{avg_penalty:.1f}%", "Price Gap")
m3.metric("Data Engine", "DuckDB SQL", "Parquet Format")

st.markdown("---")

# --- VISUALS: MARKET IMPACT ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("Price Gap: Compliant vs Non-Compliant")
    
    # Melt for grouped bar chart
    df_melted = df_impact.melt(
        id_vars=["brand"], 
        value_vars=["avg_price_compliant", "avg_price_non_compliant"],
        var_name="Category",
        value_name="Average Price (£)"
    )
    
    df_melted["Category"] = df_melted["Category"].map({
        "avg_price_compliant": "Compliant Price",
        "avg_price_non_compliant": "Non-Compliant Price"
    })

    fig = px.bar(
        df_melted,
        x="brand",
        y="Average Price (£)",
        color="Category",
        barmode="group",
        color_discrete_map={"Compliant Price": "#00CC96", "Non-Compliant Price": "#EF553B"},
        template="plotly_white"
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Market Impact Index by Brand")
    st.dataframe(
        df_impact[["brand", "percent_diff"]]
        .rename(columns={"brand": "Brand", "percent_diff": "Disvaluation %"})
        .sort_values("Disvaluation %")
        .style.background_gradient(subset=["Disvaluation %"], cmap="Reds"),
        hide_index=True,
        use_container_width=True
    )

st.markdown("---")

# --- VISUALS: DIESEL DEVALUATION ---
if not df_diesel.empty:
    st.subheader("⚠️ Top 10 Diesel Devaluation (ULEZ Impact)")
    st.info("Ranking specific models by the highest negative impact (Percentage drop between compliant vs non-compliant versions).")
    
    df_diesel_refined = df_diesel.rename(columns={
        "brand": "Brand",
        "model": "Model",
        "avg_price_compliant": "Compliant Avg Price",
        "avg_price_non_compliant": "Non-Compliant Avg Price",
        "devaluation_percent": "Devaluation (%)"
    })

    st.dataframe(
        df_diesel_refined.style.background_gradient(
            subset=["Devaluation (%)"], cmap="Reds"
        ).format({"Compliant Avg Price": "£{:,.0f}", "Non-Compliant Avg Price": "£{:,.0f}", "Devaluation (%)": "{:.1f}%"}),
        use_container_width=True,
        hide_index=True,
    )
    st.divider()

# --- VISUALS: ML CLUSTERS ---
if not df_clusters.empty:
    st.subheader("🤖 Machine Learning: Market Segmentation (K-Means)")
    
    cl1, cl2 = st.columns([2, 1])

    with cl1:
        fig_cluster = px.scatter(
            df_clusters,
            x="MILEAGE",
            y="PRICE",
            color="CLUSTER_NAME",
            hover_data=["BRAND", "MODEL", "YEAR"],
            title="Market Profiles (Price vs Mileage)",
            color_discrete_sequence=px.colors.qualitative.Set2,
            template="plotly_white"
        )
        st.plotly_chart(fig_cluster, use_container_width=True)

    with cl2:
        st.subheader("Cluster Distribution")
        distribution = df_clusters["CLUSTER_NAME"].value_counts().reset_index()
        distribution.columns = ["Profile", "Count"]
        st.table(distribution)

    st.divider()

# --- DRILL DOWN ---
st.subheader("🔍 Detailed Listing Explorer (Live Market Data)")
st.caption("Below are the top 20 most recent vehicle listings processed and enriched with ULEZ status.")
if not df_silver.empty:
    st.dataframe(
        df_silver[["brand", "model", "year", "price", "fuel_type", "is_ulez_compliant"]]
        .sort_values("price", ascending=False)
        .head(20),
        use_container_width=True,
        hide_index=True
    )
else:
    st.info("Silver layer is currently empty.")

st.sidebar.markdown("---")
st.sidebar.caption("ULEZ Analytics - Local Lakehouse Simulator v1.2")
