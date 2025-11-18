import sqlite3
import pandas as pd

# --------------------------
# Step 1: Connect to SQLite DB
# --------------------------
conn = sqlite3.connect('company.db')
cursor = conn.cursor()

# --------------------------
# Step 2: Create table
# --------------------------
cursor.execute('''
CREATE TABLE IF NOT EXISTS Employees (
    emp_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    department TEXT,
    salary INTEGER,
    age INTEGER
)
''')

# --------------------------
# Step 3: Insert sample data (batch insert)
# --------------------------
employees = [
    (1, 'Alice', 'HR', 50000, 30),
    (2, 'Bob', 'IT', 60000, 28),
    (3, 'Charlie', 'IT', 65000, 35),
    (4, 'Diana', 'Finance', 70000, 40),
    (5, 'Eve', 'HR', 52000, 29),
    (6, 'Frank', 'Finance', 75000, 45),
    (7, 'Grace', 'IT', 62000, 32)
]

cursor.executemany("INSERT OR IGNORE INTO Employees VALUES (?, ?, ?, ?, ?)", employees)
conn.commit()

# --------------------------
# Step 4: Create indexes for performance
# --------------------------
cursor.execute("CREATE INDEX IF NOT EXISTS idx_department ON Employees(department)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_salary ON Employees(salary)")
conn.commit()

# --------------------------
# Step 5: Load data efficiently with pandas
# --------------------------
# Only load needed columns for analysis
df = pd.read_sql_query("SELECT emp_id, name, department, salary, age FROM Employees", conn)

# Convert department to categorical (memory & speed optimization)
df['department'] = df['department'].astype('category')

print("=== All Employees ===")
print(df, "\n")

# --------------------------
# Step 6: SQL-like operations in pandas
# --------------------------

# 1. Filter: IT department
it_employees = df[df['department'] == 'IT']
print("=== IT Employees ===")
print(it_employees, "\n")

# 2. Aggregate: Average salary by department
avg_salary = df.groupby('department')['salary'].mean().reset_index()
print("=== Average Salary by Department ===")
print(avg_salary, "\n")

# 3. Sort by salary descending
sorted_df = df.sort_values(by='salary', ascending=False)
print("=== Employees Sorted by Salary ===")
print(sorted_df, "\n")

# 4. Vectorized update: give a 10% raise to employees under 35
df.loc[df['age'] < 35, 'salary'] *= 1.10
df['salary'] = df['salary'].astype(int)  # convert back to int

print("=== Updated Salaries (Age < 35 got 10% raise) ===")
print(df, "\n")

# --------------------------
# Step 7: Optional: Save changes back to SQLite
# --------------------------
for index, row in df.iterrows():
    cursor.execute('''
        UPDATE Employees
        SET salary = ?
        WHERE emp_id = ?
    ''', (row['salary'], row['emp_id']))
conn.commit()

# Close connection
conn.close()
