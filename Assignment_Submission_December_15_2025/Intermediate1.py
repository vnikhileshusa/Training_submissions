import pandas as pd

# ---------------- REAL DATASET ---------------- #
sales_data = [
    {"order_id": 101, "customer": "Ravi",   "product": "Laptop", "quantity": 1, "price": 60000, "order_status": "Completed"},
    {"order_id": 102, "customer": "Anita",  "product": "Mouse",  "quantity": 2, "price": 500,   "order_status": "Pending"},
    {"order_id": 103, "customer": "Kiran",  "product": "Phone",  "quantity": 1, "price": 30000, "order_status": "Completed"},
    {"order_id": 104, "customer": "Suresh", "product": "Tablet", "quantity": 1, "price": 20000, "order_status": "Cancelled"},
    {"order_id": 105, "customer": "Meena",  "product": "Monitor","quantity": 2, "price": 8000,  "order_status": "Completed"}
]

RAW_FILE = "sales_raw.csv"
FINAL_FILE = "sales_cleaned.csv"

# ---------------- ETL STEPS ---------------- #

def extract():
    print("Extracting data...")
    df = pd.DataFrame(sales_data)
    df.to_csv(RAW_FILE, index=False)
    return df

def transform(df):
    print("Transforming data...")
    df = df[df["order_status"] == "Completed"]
    df["total_amount"] = df["quantity"] * df["price"]
    return df

def load(df):
    print("Loading data...")
    df.to_csv(FINAL_FILE, index=False)
    print("ETL Process Completed")

# ---------------- RUN PIPELINE ---------------- #

if __name__ == "__main__":
    extracted_data = extract()
    transformed_data = transform(extracted_data)
    load(transformed_data)
