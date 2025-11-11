import sqlite3

# Connect to database
conn = sqlite3.connect('company.db')
cursor = conn.cursor()

# Create Employees table
cursor.execute('''
CREATE TABLE IF NOT EXISTS Employees (
    emp_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    department TEXT,
    salary INTEGER,
    age INTEGER
)
''')

# Insert sample data
employees = [
    (1, 'Alice', 'HR', 50000, 30),
    (2, 'Bob', 'IT', 60000, 28),
    (3, 'Charlie', 'IT', 65000, 35),
    (4, 'Diana', 'Finance', 70000, 40),
    (5, 'Eve', 'HR', 52000, 29)
]

cursor.executemany("INSERT OR IGNORE INTO Employees VALUES (?, ?, ?, ?, ?)", employees)
conn.commit()
cursor.execute("SELECT * FROM Employees")
rows = cursor.fetchall()
print("All employees:")
for row in rows:
    print(row)
