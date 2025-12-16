"""
Optimized ETL Basics + Airflow-style DAG using Scikit-learn
Performance Optimized Version
"""

import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from datetime import datetime

# ---------------------------------------------------
# EXTRACT (Optimized)
# ---------------------------------------------------
def extract_data():
    print("🔹 [Extract] Loading data...")

    df = pd.DataFrame(
        {
            "customer_id": np.arange(1, 6, dtype=np.int32),
            "age": np.array([22, 25, 47, 52, 46], dtype=np.int16),
            "annual_income": np.array([15000, 32000, 58000, 72000, 61000], dtype=np.int32),
            "spending_score": np.array([39, 81, 6, 77, 40], dtype=np.int16),
        }
    )

    return df


# ---------------------------------------------------
# TRANSFORM (Optimized ML)
# ---------------------------------------------------
def transform_data(df):
    print("🔹 [Transform] Running ML transformation...")

    # Convert directly to NumPy for faster computation
    X = df[["age", "annual_income", "spending_score"]].to_numpy(dtype=np.float32)

    # Scale once
    scaler = StandardScaler(copy=False)
    X_scaled = scaler.fit_transform(X)

    # Optimized KMeans
    kmeans = KMeans(
        n_clusters=2,
        n_init=5,        # fewer initializations = faster
        max_iter=200,
        random_state=42
    )

    df["customer_segment"] = kmeans.fit_predict(X_scaled)

    return df


# ---------------------------------------------------
# LOAD (Optimized)
# ---------------------------------------------------
def load_data(df):
    print("🔹 [Load] Saving output...")

    file_name = f"customer_segments_optimized_{datetime.now():%Y%m%d_%H%M%S}.csv"

    df.to_csv(
        file_name,
        index=False,
        encoding="utf-8",
        chunksize=1000   # scalable for large datasets
    )

    print(f"✅ Data saved: {file_name}")


# ---------------------------------------------------
# AIRFLOW-STYLE DAG EXECUTION
# ---------------------------------------------------
def etl_pipeline():
    print("\n🚀 Optimized ETL Pipeline Started\n")

    df = extract_data()
    df = transform_data(df)
    load_data(df)

    print("\n🎯 Optimized ETL Pipeline Completed")


# ---------------------------------------------------
# ENTRY POINT (Scheduler-like)
# ---------------------------------------------------
if __name__ == "__main__":
    etl_pipeline()
