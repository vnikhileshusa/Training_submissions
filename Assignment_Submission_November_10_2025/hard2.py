import sqlite3
import pandas as pd

# --------------------------
# Step 1: Connect to SQLite DB
# --------------------------
conn = sqlite3.connect('employee_management.db')
cursor = conn.cursor()

# --------------------------
# Step 2: Create Employees table
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
# Step 3: Insert sample data
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
# Step 4: Create Indexes
# --------------------------
cursor.execute("CREATE INDEX IF NOT EXISTS idx_department ON Employees(department)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_salary ON Employees(salary)")
conn.commit()

# --------------------------
# Step 5: Load data into pandas
# --------------------------
df = pd.read_sql_query("SELECT emp_id, name, department, salary, age FROM Employees", conn)
df['department'] = df['department'].astype('category')

print("=== All Employees ===")
print(df, "\n")

# --------------------------
# Step 6: Basic SQL-like queries
# --------------------------

# 1. Filter by department
it_employees = df[df['department'] == 'IT']
print("=== IT Employees ===")
print(it_employees, "\n")

# 2. Aggregate: average salary by department
avg_salary = df.groupby('department')['salary'].mean().reset_index()
print("=== Average Salary by Department ===")
print(avg_salary, "\n")

# 3. Sort by salary descending
sorted_df = df.sort_values(by='salary', ascending=False)
print("=== Employees Sorted by Salary ===")
print(sorted_df, "\n")

# --------------------------
# Step 7: Update and Delete records
# --------------------------

# Update: give 10% raise to employees under 35
df.loc[df['age'] < 35, 'salary'] *= 1.10
df['salary'] = df['salary'].astype(int)

# Delete: remove employee with emp_id=5 (Eve)
df = df[df['emp_id'] != 5]

# Write updates back to SQLite
for _, row in df.iterrows():
    cursor.execute('''
        UPDATE Employees
        SET salary = ?
        WHERE emp_id = ?
    ''', (row['salary'], row['emp_id']))
cursor.execute("DELETE FROM Employees WHERE emp_id = 5")
conn.commit()

print("=== Updated Employees ===")
print(df, "\n")

# --------------------------
# Step 8: Advanced Queries
# --------------------------

# Count employees per department
dept_count = df.groupby('department')['emp_id'].count().reset_index(name='count')
print("=== Employee Count by Department ===")
print(dept_count, "\n")

# Max salary per department
max_salary = df.groupby('department')['salary'].max().reset_index(name='max_salary')
print("=== Max Salary by Department ===")
print(max_salary, "\n")

# --------------------------
# Step 9: Optional - Export to CSV
# --------------------------
df.to_csv('employees_updated.csv', index=False)
print("Updated data exported to employees_updated.csv")

# Close DB connection
conn.close()
