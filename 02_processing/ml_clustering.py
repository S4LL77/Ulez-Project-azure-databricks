import logging
import os
from pathlib import Path

import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

# Enterprise Logging Configuration
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("ML_ClusteringPipeline")

SILVER_FILE = Path("data/silver/fct_cars.parquet")
GOLD_FILE = Path("data/gold/mart_market_clusters.parquet")

def extract_features() -> pd.DataFrame:
    """Extract operational data from the Local Silver layer."""
    logger.info(f"Extracting data from {SILVER_FILE}...")
    
    if not SILVER_FILE.exists():
        logger.error("Silver layer file not found. Run the Processing pipeline first.")
        return pd.DataFrame()

    try:
        df = pd.read_parquet(SILVER_FILE)
        
        # Filter and select features
        df = df[df['price'].notnull() & df['mileage'].notnull() & df['year'].notnull()]
        
        # Standardize column names for ML logic
        df.columns = [c.upper() for c in df.columns]
        
        logger.info(f"Extracted {len(df)} records for training.")
        return df
    except Exception as e:
        logger.error(f"Failed to read silver file: {e}")
        return pd.DataFrame()


def train_and_predict(df: pd.DataFrame, n_clusters: int = 3) -> pd.DataFrame:
    """Train K-Means model and predict clusters."""
    logger.info("Initializing Feature Engineering and Model Training...")

    # Select features for clustering
    features = ["PRICE", "MILEAGE", "YEAR"]
    X = df[features].copy()

    # 1. Scale Features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # 2. Train Model
    logger.info(f"Training K-Means with K={n_clusters}...")
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    df["CLUSTER_ID"] = kmeans.fit_predict(X_scaled)

    # 3. Profiling / Naming Clusters based on Business Logic
    cluster_means = df.groupby("CLUSTER_ID")[features].mean()

    # Basic logic: Sort by price to identify "Premium" vs "Value"
    sorted_by_price = cluster_means.sort_values(by="PRICE")
    value_cluster_id = sorted_by_price.index[0]
    mid_cluster_id = sorted_by_price.index[1]
    premium_cluster_id = sorted_by_price.index[2]

    def map_cluster_name(row):
        is_compliant = row["IS_ULEZ_COMPLIANT"]
        cid = row["CLUSTER_ID"]

        if cid == premium_cluster_id:
            return "Premium Segment (Compliant)" if is_compliant else "Premium Segment (Non-Compliant)"
        elif cid == mid_cluster_id:
            return "Standard Market (Compliant)" if is_compliant else "Standard Market (Non-Compliant)"
        elif cid == value_cluster_id:
            return "Budget Entry (Compliant)" if is_compliant else "Value Entry (Non-Compliant)"

    df["CLUSTER_NAME"] = df.apply(map_cluster_name, axis=1)
    logger.info("Clustering completed and business profiles assigned.")
    return df


def load_to_gold(df: pd.DataFrame):
    """Load results into the Local Gold layer (Parquet)."""
    GOLD_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Writing {len(df)} records to {GOLD_FILE}...")
    df.to_parquet(GOLD_FILE, index=False)
    logger.info(f"Successfully loaded into Gold layer.")


def run_pipeline():
    logger.info("--- Starting Local ML Pipeline (DuckDB Source) ---")
    try:
        df_raw = extract_features()

        if df_raw.empty:
            logger.warning("No data extracted. Aborting pipeline.")
            return

        df_scored = train_and_predict(df_raw, n_clusters=3)
        load_to_gold(df_scored)

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    logger.info("--- Pipeline Execution Finished ---")


if __name__ == "__main__":
    run_pipeline()
