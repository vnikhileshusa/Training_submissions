import sqlite3

# ======================================================
# 1. CONNECT TO DATABASE (creates file if not exists)
# ======================================================
conn = sqlite3.connect("real_dataset.db")
cur = conn.cursor()

# ======================================================
# 2. DROP TABLES IF THEY EXIST (fresh reset each run)
# ======================================================
cur.executescript("""
DROP TABLE IF EXISTS OrderItems;
DROP TABLE IF EXISTS Products;
DROP TABLE IF EXISTS Orders;
DROP TABLE IF EXISTS Customers;
""")

# ======================================================
# 3. CREATE TABLES
# ======================================================
cur.execute("""
CREATE TABLE Customers (
    customer_id INTEGER PRIMARY KEY,
    name TEXT,
    city TEXT
)
""")

cur.execute("""
CREATE TABLE Orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    order_date TEXT,
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
)
""")

cur.execute("""
CREATE TABLE Products (
    product_id INTEGER PRIMARY KEY,
    product_name TEXT,
    price INTEGER
)
""")

cur.execute("""
CREATE TABLE OrderItems (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    FOREIGN KEY (order_id) REFERENCES Orders(order_id),
    FOREIGN KEY (product_id) REFERENCES Products(product_id)
)
""")

# ======================================================
# 4. INSERT DATA (REALISTIC DATASET)
# ======================================================

# Customers
cur.execute("INSERT INTO Customers VALUES (1, 'Nikhil', 'New York')")
cur.execute("INSERT INTO Customers VALUES (2, 'Arjun', 'Chicago')")
cur.execute("INSERT INTO Customers VALUES (3, 'Sophia', 'San Diego')")

# Orders
cur.execute("INSERT INTO Orders VALUES (101, 1, '2025-01-10')")
cur.execute("INSERT INTO Orders VALUES (102, 1, '2025-02-14')")
cur.execute("INSERT INTO Orders VALUES (103, 2, '2025-01-21')")

# Products
cur.execute("INSERT INTO Products VALUES (11, 'Laptop', 1200)")
cur.execute("INSERT INTO Products VALUES (12, 'Keyboard', 80)")
cur.execute("INSERT INTO Products VALUES (13, 'Mouse', 40)")

# OrderItems
cur.execute("INSERT INTO OrderItems (order_id, product_id, quantity) VALUES (101, 11, 1)")
cur.execute("INSERT INTO OrderItems (order_id, product_id, quantity) VALUES (101, 12, 1)")
cur.execute("INSERT INTO OrderItems (order_id, product_id, quantity) VALUES (102, 13, 2)")
cur.execute("INSERT INTO OrderItems (order_id, product_id, quantity) VALUES (103, 11, 1)")

conn.commit()

# ======================================================
# Helper function for printing query results
# ======================================================
def run_query(title, query):
    print(f"\n=== {title} ===")
    rows = cur.execute(query).fetchall()
    for row in rows:
        print(row)

# ======================================================
# 5. MULTI-TABLE JOIN (4 TABLES)
# ======================================================
run_query(
    "Full Order Details (4-table JOIN)",
    """
    SELECT 
        c.name AS customer,
        o.order_id,
        p.product_name,
        oi.quantity,
        p.price,
        (oi.quantity * p.price) AS total_price
    FROM Customers c
    JOIN Orders o ON c.customer_id = o.customer_id
    JOIN OrderItems oi ON o.order_id = oi.order_id
    JOIN Products p ON oi.product_id = p.product_id
    ORDER BY o.order_id;
    """
)

# ======================================================
# 6. AGGREGATION: Total spending per customer
# ======================================================
run_query(
    "Total Spent by Each Customer",
    """
    SELECT
        c.name,
        SUM(oi.quantity * p.price) AS total_spent
    FROM Customers c
    LEFT JOIN Orders o ON c.customer_id = o.customer_id
    LEFT JOIN OrderItems oi ON o.order_id = oi.order_id
    LEFT JOIN Products p ON oi.product_id = p.product_id
    GROUP BY c.name;
    """
)

# ======================================================
# 7. HAVING: customers who spent > 1000
# ======================================================
run_query(
    "Customers Who Spent > 1000",
    """
    SELECT
        c.name,
        SUM(oi.quantity * p.price) AS total_spent
    FROM Customers c
    LEFT JOIN Orders o ON c.customer_id = o.customer_id
    LEFT JOIN OrderItems oi ON o.order_id = oi.order_id
    LEFT JOIN Products p ON oi.product_id = p.product_id
    GROUP BY c.name
    HAVING total_spent > 1000;
    """
)

# ======================================================
# 8. WINDOW FUNCTION: Rank customers by spending
# ======================================================
run_query(
    "Customer Spending Rank (Window Function)",
    """
    SELECT
        c.name,
        SUM(oi.quantity * p.price) AS total_spent,
        RANK() OVER (ORDER BY SUM(oi.quantity * p.price) DESC) AS spend_rank
    FROM Customers c
    LEFT JOIN Orders o ON c.customer_id = o.customer_id
    LEFT JOIN OrderItems oi ON o.order_id = oi.order_id
    LEFT JOIN Products p ON oi.product_id = p.product_id
    GROUP BY c.name;
    """
)

# Close connection
conn.close()
