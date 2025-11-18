import sqlite3
import pandas as pd
import time

# -------------------------------------------
# 1. CONNECT TO (OR CREATE) DATABASE
# -------------------------------------------
conn = sqlite3.connect("advanced_sql.db")
cursor = conn.cursor()

# -------------------------------------------
# 2. CREATE TABLES (CUSTOMERS, ORDERS, PAYMENTS)
# -------------------------------------------
cursor.executescript("""
DROP TABLE IF EXISTS Customers;
DROP TABLE IF EXISTS Orders;
DROP TABLE IF EXISTS Payments;

CREATE TABLE Customers (
    customer_id INTEGER PRIMARY KEY,
    name TEXT,
    country TEXT
);

CREATE TABLE Orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    amount REAL,
    product TEXT,
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
);

CREATE TABLE Payments (
    payment_id INTEGER PRIMARY KEY,
    order_id INTEGER,
    status TEXT,
    FOREIGN KEY (order_id) REFERENCES Orders(order_id)
);
""")

# -------------------------------------------
# 3. INSERT SAMPLE DATA
# -------------------------------------------
customers = [
    (1, "Alice", "USA"),
    (2, "Bob", "Canada"),
    (3, "Charlie", "USA"),
    (4, "Diana", "UK")
]

orders = [
    (101, 1, 250, "Laptop"),
    (102, 1, 120, "Keyboard"),
    (103, 2, 330, "Monitor"),
    (104, 3, 90, "Mouse"),
    (105, 3, 500, "Camera")
]

payments = [
    (1, 101, "Paid"),
    (2, 103, "Paid"),
    (3, 104, "Pending"),
    (4, 105, "Paid")
]

cursor.executemany("INSERT INTO Customers VALUES (?, ?, ?)", customers)
cursor.executemany("INSERT INTO Orders VALUES (?, ?, ?, ?)", orders)
cursor.executemany("INSERT INTO Payments VALUES (?, ?, ?)", payments)
conn.commit()

# -------------------------------------------
# 4. PERFORMANCE OPTIMIZATIONS (INDEXES)
# -------------------------------------------
cursor.executescript("""
CREATE INDEX IF NOT EXISTS idx_orders_customer ON Orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_payments_order ON Payments(order_id);
""")

# -------------------------------------------
# 5. ADVANCED SQL JOINS + AGGREGATION
# -------------------------------------------

query = """
SELECT 
    c.customer_id,
    c.name AS customer_name,
    COUNT(o.order_id) AS total_orders,
    SUM(o.amount) AS total_amount,
    AVG(o.amount) AS avg_order_value,
    SUM(CASE WHEN p.status = 'Paid' THEN 1 ELSE 0 END) AS paid_orders
FROM Customers c
LEFT JOIN Orders o ON c.customer_id = o.customer_id
LEFT JOIN Payments p ON o.order_id = p.order_id
GROUP BY c.customer_id
HAVING total_amount > 200
ORDER BY total_amount DESC;
"""

start = time.time()
df = pd.read_sql_query(query, conn)
end = time.time()

# -------------------------------------------
# 6. PRINT RESULTS
# -------------------------------------------
print("\n🏆 ADVANCED SQL JOIN + AGGREGATION RESULTS:\n")
print(df)

print("\n⏱️ Query Execution Time:", round(end - start, 5), "seconds")

# -------------------------------------------
# 7. CLOSE CONNECTION
# -------------------------------------------
conn.close()
