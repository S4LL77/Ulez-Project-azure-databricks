"""
ML Pipeline: Market Segmentation (K-Means Clustering)
-----------------------------------------------------
Segments used car listings into market profiles (Premium / Standard / Budget)
using K-Means clustering with automated cluster evaluation.
"""

import logging
from pathlib import Path

import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score

# Logging Configuration
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


def find_optimal_k(X_scaled: np.ndarray, k_range: range = range(2, 8)) -> int:
    """
    Evaluate K values using Silhouette Score and select the optimal K.
    Falls back to K=3 if the dataset is too small for evaluation.
    """
    if len(X_scaled) < max(k_range):
        logger.warning(f"Dataset too small ({len(X_scaled)} rows) for elbow analysis. Using K=3.")
        return 3

    results = []
    for k in k_range:
        kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = kmeans.fit_predict(X_scaled)
        sil = silhouette_score(X_scaled, labels)
        inertia = kmeans.inertia_
        results.append({"k": k, "silhouette": sil, "inertia": inertia})
        logger.info(f"  K={k} | Silhouette={sil:.3f} | Inertia={inertia:.0f}")

    # Select K with the highest silhouette score
    best = max(results, key=lambda x: x["silhouette"])
    logger.info(f"Optimal K={best['k']} selected (silhouette={best['silhouette']:.3f})")
    return best["k"]


def train_and_predict(df: pd.DataFrame) -> pd.DataFrame:
    """Train K-Means model with automated K selection and predict clusters."""
    logger.info("Initializing Feature Engineering and Model Training...")

    # Select features for clustering
    features = ["PRICE", "MILEAGE", "YEAR"]
    X = df[features].copy()

    # 1. Scale Features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # 2. Find optimal K via silhouette analysis
    logger.info("Running cluster evaluation (K=2..7):")
    optimal_k = find_optimal_k(X_scaled)

    # 3. Train final model with optimal K
    logger.info(f"Training final K-Means with K={optimal_k}...")
    kmeans = KMeans(n_clusters=optimal_k, random_state=42, n_init=10)
    df["CLUSTER_ID"] = kmeans.fit_predict(X_scaled)

    # Final silhouette score for reporting
    final_sil = silhouette_score(X_scaled, df["CLUSTER_ID"])
    logger.info(f"Final model silhouette score: {final_sil:.3f}")

    # 4. Profiling / Naming Clusters based on Business Logic
    cluster_means = df.groupby("CLUSTER_ID")[features].mean()
    sorted_by_price = cluster_means.sort_values(by="PRICE")

    # Map cluster IDs to business names (ascending price order)
    cluster_labels = {}
    label_names = ["Budget Entry", "Standard Market", "Premium Segment"]
    for i, cluster_id in enumerate(sorted_by_price.index):
        if i < len(label_names):
            cluster_labels[cluster_id] = label_names[i]
        else:
            cluster_labels[cluster_id] = f"Segment {i + 1}"

    def map_cluster_name(row):
        base_name = cluster_labels.get(row["CLUSTER_ID"], "Unknown")
        suffix = "(Compliant)" if row["IS_ULEZ_COMPLIANT"] else "(Non-Compliant)"
        return f"{base_name} {suffix}"

    df["CLUSTER_NAME"] = df.apply(map_cluster_name, axis=1)

    # Log cluster profile summary
    logger.info("Cluster profiles:")
    for cid, name in cluster_labels.items():
        means = cluster_means.loc[cid]
        count = (df["CLUSTER_ID"] == cid).sum()
        logger.info(f"  {name}: n={count}, avg_price=£{means['PRICE']:.0f}, avg_mileage={means['MILEAGE']:.0f}")

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

        df_scored = train_and_predict(df_raw)
        load_to_gold(df_scored)

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise
    logger.info("--- Pipeline Execution Finished ---")


if __name__ == "__main__":
    run_pipeline()
