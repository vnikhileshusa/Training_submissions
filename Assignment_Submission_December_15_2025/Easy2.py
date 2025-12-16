import pandas as pd

# ---------------- DATA ---------------- #
SALES_DATA = [
    {"order_id": 1, "product": "Apple",  "quantity": 2, "price": 100, "status": "completed"},
    {"order_id": 2, "product": "Banana", "quantity": 5, "price": 20,  "status": "pending"},
    {"order_id": 3, "product": "Orange", "quantity": 3, "price": 50,  "status": "completed"},
    {"order_id": 4, "product": "Mango",  "quantity": 1, "price": 150, "status": "completed"},
]

RAW_FILE = "sales_raw.csv"
FINAL_FILE = "sales_final.csv"

# ---------------- ETL ---------------- #

def extract():
    print("Extracting data...")
    df = pd.DataFrame(SALES_DATA)
    df.to_csv(RAW_FILE, index=False)
    return df

def transform(df):
    print("Transforming data...")
    df = df[df["status"] == "completed"]
    df["total_price"] = df["quantity"] * df["price"]
    return df

def load(df):
    print("Loading data...")
    df.to_csv(FINAL_FILE, index=False)
    print("ETL completed successfully!")

# ---------------- RUN ---------------- #

if __name__ == "__main__":
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)
