import sqlite3

# Connect to a database (or create one)
conn = sqlite3.connect('toy.db')
cursor = conn.cursor()

# Create table
cursor.execute('''
CREATE TABLE IF NOT EXISTS Students (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    age INTEGER,
    grade TEXT
)
''')

# Insert data
cursor.execute("INSERT INTO Students (name, age, grade) VALUES ('Alice', 20, 'A')")
cursor.execute("INSERT INTO Students (name, age, grade) VALUES ('Bob', 22, 'B')")
cursor.execute("INSERT INTO Students (name, age, grade) VALUES ('Charlie', 19, 'A')")

# Commit changes
conn.commit()

# Query data
cursor.execute("SELECT * FROM Students")
rows = cursor.fetchall()
for row in rows:
    print(row)

# Close connection
conn.close()
