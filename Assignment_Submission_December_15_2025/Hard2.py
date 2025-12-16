"""
Mini Project: ETL Basics & Airflow (End-to-End)
Author: Nikhilesh
"""

import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from datetime import datetime

# =====================================================
# EXTRACT TASK
# =====================================================
def extract_data():
    print("🔹 [Extract] Reading customer data")

    data = {
        "customer_id": np.arange(1, 11),
        "age": [22, 25, 47, 52, 46, 21, 23, 45, 53, 33],
        "annual_income": [15000, 32000, 58000, 72000, 61000,
                          18000, 29000, 60000, 79000, 40000],
        "spending_score": [39, 81, 6, 77, 40, 76, 94, 5, 70, 55]
    }

    df = pd.DataFrame(data)
    print(df.head())
    return df


# =====================================================
# TRANSFORM TASK (ML)
# =====================================================
def transform_data(df):
    print("\n🔹 [Transform] Applying ML transformation")

    features = df[["age", "annual_income", "spending_score"]].to_numpy()

    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(features)

    kmeans = KMeans(
        n_clusters=3,
        n_init=5,
        random_state=42
    )

    df["customer_segment"] = kmeans.fit_predict(scaled_features)

    print(df.head())
    return df


# =====================================================
# LOAD TASK
# =====================================================
def load_data(df):
    print("\n🔹 [Load] Writing analytics output")

    filename = f"customer_segments_{datetime.now():%Y%m%d_%H%M%S}.csv"
    df.to_csv(filename, index=False)

    print(f"✅ Output saved as {filename}")


# =====================================================
# AIRFLOW-STYLE DAG
# =====================================================
def etl_dag():
    print("\n🚀 ETL DAG Started\n")

    df = extract_data()
    df = transform_data(df)
    load_data(df)

    print("\n🎯 ETL DAG Completed Successfully")


# =====================================================
# DAG TRIGGER (Scheduler)
# =====================================================
if __name__ == "__main__":
    etl_dag()
