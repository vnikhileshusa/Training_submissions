import pandas as pd
from sklearn.preprocessing import MinMaxScaler

# ======================================================
# 1. CREATE REALISTIC DATAFRAMES (Customers, Orders, Products)
# ======================================================

df_customers = pd.DataFrame({
    "customer_id": [1, 2, 3],
    "name": ["Nikhil", "Arjun", "Sophia"],
    "city": ["New York", "Chicago", "San Diego"]
})

df_orders = pd.DataFrame({
    "order_id": [101, 102, 103],
    "customer_id": [1, 1, 2],
    "order_date": ["2025-01-10", "2025-02-14", "2025-01-21"]
})

df_products = pd.DataFrame({
    "product_id": [11, 12, 13],
    "product_name": ["Laptop", "Keyboard", "Mouse"],
    "price": [1200, 80, 40]
})

df_items = pd.DataFrame({
    "item_id": [1, 2, 3, 4],
    "order_id": [101, 101, 102, 103],
    "product_id": [11, 12, 13, 11],
    "quantity": [1, 1, 2, 1]
})

# ======================================================
# 2. ADVANCED SQL JOINS (done using Pandas merges)
# ======================================================

# JOIN Customers → Orders
merged = df_customers.merge(df_orders, on="customer_id", how="left")

# JOIN OrderItems
merged = merged.merge(df_items, on="order_id", how="left")

# JOIN Products
merged = merged.merge(df_products, on="product_id", how="left")

# Compute total price like SQL expression (quantity * price)
merged["total_price"] = merged["quantity"] * merged["price"]

print("\n=== FULL JOIN RESULT (Equivalent to SQL 4-table JOIN) ===")
print(merged)

# ======================================================
# 3. GROUP BY + SUM (SQL Aggregation)
# ======================================================

grouped = merged.groupby("name")["total_price"].sum().reset_index()
grouped = grouped.rename(columns={"total_price": "total_spent"})

print("\n=== TOTAL SPENT PER CUSTOMER (GROUP BY) ===")
print(grouped)

# ======================================================
# 4. HAVING equivalent → filter rows
# ======================================================

filtered = grouped[grouped["total_spent"] > 1000]

print("\n=== CUSTOMERS WHO SPENT > 1000 (HAVING) ===")
print(filtered)

# ======================================================
# 5. WINDOW FUNCTION (RANK) using pandas
# ======================================================

grouped["spend_rank"] = grouped["total_spent"].rank(ascending=False)

print("\n=== CUSTOMER SPENDING RANK (WINDOW FUNCTION) ===")
print(grouped)

# ======================================================
# 6. USING SCIKIT-LEARN WITH SQL OUTPUT
# Normalize spending using MinMaxScaler
# ======================================================

scaler = MinMaxScaler()
grouped["normalized_spending"] = scaler.fit_transform(
    grouped[["total_spent"]]
)

print("\n=== NORMALIZED SPENDING (Scikit-learn applied to SQL results) ===")
print(grouped)
