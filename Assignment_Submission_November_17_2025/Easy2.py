import sqlite3

# ============================================
# 1. CONNECT TO DATABASE (creates file if not exists)
# ============================================
conn = sqlite3.connect("toy_database.db")
cur = conn.cursor()

# ============================================
# 2. CREATE TABLES
# ============================================
cur.execute("DROP TABLE IF EXISTS Customers")
cur.execute("DROP TABLE IF EXISTS Orders")

cur.execute("""
CREATE TABLE Customers (
    customer_id INTEGER PRIMARY KEY,
    name TEXT
)
""")

cur.execute("""
CREATE TABLE Orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER,
    amount INTEGER,
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
)
""")

# ============================================
# 3. INSERT SAMPLE DATA
# ============================================
cur.execute("INSERT INTO Customers VALUES (1, 'Nikhil')")
cur.execute("INSERT INTO Customers VALUES (2, 'Arjun')")
cur.execute("INSERT INTO Customers VALUES (3, 'Riya')")

cur.execute("INSERT INTO Orders VALUES (101, 1, 300)")
cur.execute("INSERT INTO Orders VALUES (102, 1, 200)")
cur.execute("INSERT INTO Orders VALUES (103, 2, 150)")

conn.commit()

# ============================================
# Helper function to print query results
# ============================================
def run_query(title, query):
    print(f"\n=== {title} ===")
    rows = cur.execute(query).fetchall()
    for row in rows:
        print(row)

# ============================================
# 4. INNER JOIN
# ============================================
run_query(
    "INNER JOIN: Customers WITH orders",
    """
    SELECT c.name, o.order_id, o.amount
    FROM Customers c
    INNER JOIN Orders o
    ON c.customer_id = o.customer_id
    """
)

# ============================================
# 5. LEFT JOIN
# ============================================
run_query(
    "LEFT JOIN: All customers (including those with no orders)",
    """
    SELECT c.name, o.order_id, o.amount
    FROM Customers c
    LEFT JOIN Orders o
    ON c.customer_id = o.customer_id
    """
)

# ============================================
# 6. AGGREGATION (GROUP BY)
# ============================================
run_query(
    "Total spent per customer",
    """
    SELECT c.name, SUM(o.amount) AS total_spent
    FROM Customers c
    LEFT JOIN Orders o
    ON c.customer_id = o.customer_id
    GROUP BY c.name
    """
)

# ============================================
# 7. HAVING (filter groups)
# ============================================
run_query(
    "Customers who spent >= 200",
    """
    SELECT c.name, SUM(o.amount) AS total_spent
    FROM Customers c
    LEFT JOIN Orders o
    ON c.customer_id = o.customer_id
    GROUP BY c.name
    HAVING total_spent >= 200
    """
)

# ============================================
# 8. WINDOW FUNCTION (requires SQLite 3.25+)
# ============================================
run_query(
    "Rank customers by spending (Window Function)",
    """
    SELECT 
        c.name,
        SUM(o.amount) AS total_spent,
        RANK() OVER (ORDER BY SUM(o.amount) DESC) AS spend_rank
    FROM Customers c
    LEFT JOIN Orders o
    ON c.customer_id = o.customer_id
    GROUP BY c.name
    """
)

# Close connection
conn.close()
