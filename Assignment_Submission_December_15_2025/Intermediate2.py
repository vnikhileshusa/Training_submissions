"""
ETL Basics + Airflow-style Pipeline using Scikit-learn
Author: Nikhilesh
Run: python etl_airflow_style_ml.py
"""

import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from datetime import datetime

# ---------------------------------------------------
# RAW DATA (Extract)
# ---------------------------------------------------
def extract_data():
    print("🔹 Extracting data...")
    data = {
        "customer_id": [1, 2, 3, 4, 5],
        "age": [22, 25, 47, 52, 46],
        "annual_income": [15000, 32000, 58000, 72000, 61000],
        "spending_score": [39, 81, 6, 77, 40]
    }
    df = pd.DataFrame(data)
    print(df)
    return df


# ---------------------------------------------------
# TRANSFORM (Scikit-learn)
# ---------------------------------------------------
def transform_data(df):
    print("\n🔹 Transforming data using Scikit-learn...")

    features = df[["age", "annual_income", "spending_score"]]

    # Scaling
    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(features)

    # Clustering (ML transformation)
    kmeans = KMeans(n_clusters=2, random_state=42)
    df["customer_segment"] = kmeans.fit_predict(scaled_features)

    print(df)
    return df


# ---------------------------------------------------
# LOAD
# ---------------------------------------------------
def load_data(df):
    print("\n🔹 Loading data...")
    file_name = f"customer_segments_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(file_name, index=False)
    print(f"✅ Data successfully saved to {file_name}")


# ---------------------------------------------------
# AIRFLOW-STYLE DAG EXECUTION
# ---------------------------------------------------
def etl_pipeline():
    print("\n🚀 ETL Pipeline Started\n")
    data = extract_data()
    transformed_data = transform_data(data)
    load_data(transformed_data)
    print("\n🎯 ETL Pipeline Completed Successfully")


# ---------------------------------------------------
# ENTRY POINT (like Airflow scheduler)
# ---------------------------------------------------
if __name__ == "__main__":
    etl_pipeline()
