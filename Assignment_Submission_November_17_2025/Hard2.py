import sqlite3
import pandas as pd
import os

DB_FILE = "school.db"

# Remove existing DB to get a fresh run each time (optional)
if os.path.exists(DB_FILE):
    os.remove(DB_FILE)

# Connect and create cursor
conn = sqlite3.connect(DB_FILE)
cur = conn.cursor()

# -----------------------------
# 1) CREATE SCHEMA
# -----------------------------
cur.executescript("""
PRAGMA foreign_keys = ON;

CREATE TABLE Students (
    student_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    major TEXT,
    year INTEGER
);

CREATE TABLE Courses (
    course_id INTEGER PRIMARY KEY,
    course_code TEXT NOT NULL,
    title TEXT NOT NULL,
    credits INTEGER
);

CREATE TABLE Enrollments (
    enrollment_id INTEGER PRIMARY KEY AUTOINCREMENT,
    student_id INTEGER NOT NULL,
    course_id INTEGER NOT NULL,
    semester TEXT NOT NULL,
    grade TEXT, -- letter grade: A, B, C, D, F or NULL if not graded
    FOREIGN KEY(student_id) REFERENCES Students(student_id),
    FOREIGN KEY(course_id) REFERENCES Courses(course_id)
);
""")

# -----------------------------
# 2) INSERT SAMPLE DATA (batch)
# -----------------------------
students = [
    (1, 'Nikhil', 'Computer Science', 3),
    (2, 'Arjun', 'Mathematics', 2),
    (3, 'Riya', 'Biology', 1),
    (4, 'Sophia', 'Computer Science', 4),
    (5, 'Vikram', 'Physics', 3),
    (6, 'Meera', 'Mathematics', 2)
]

courses = [
    (101, 'CS101', 'Intro to Programming', 4),
    (102, 'CS201', 'Data Structures', 4),
    (201, 'MATH101', 'Calculus I', 3),
    (202, 'MATH201', 'Linear Algebra', 3),
    (301, 'BIO101', 'General Biology', 3),
    (401, 'PHYS201', 'Electromagnetism', 4)
]

# Enrollments: student_id, course_id, semester, grade
# grade is letter; None means not graded yet
enrollments = [
    (1, 101, '2025-Fall', 'A'),
    (1, 102, '2025-Fall', 'B'),
    (1, 201, '2025-Fall', 'A'),
    (2, 201, '2025-Fall', 'B'),
    (2, 202, '2025-Fall', 'A'),
    (3, 301, '2025-Fall', 'C'),
    (4, 101, '2025-Fall', 'B'),
    (4, 102, '2025-Fall', 'A'),
    (4, 201, '2025-Fall', 'A'),
    (5, 401, '2025-Fall', None),
    (6, 201, '2025-Fall', 'B'),
    (6, 202, '2025-Fall', 'B')
]

cur.executemany("INSERT INTO Students VALUES (?, ?, ?, ?)", students)
cur.executemany("INSERT INTO Courses VALUES (?, ?, ?, ?)", courses)
cur.executemany("INSERT INTO Enrollments (student_id, course_id, semester, grade) VALUES (?, ?, ?, ?)", enrollments)
conn.commit()

# -----------------------------
# 3) CREATE INDEXES (optimize joins)
# -----------------------------
cur.executescript("""
CREATE INDEX IF NOT EXISTS idx_enroll_student ON Enrollments(student_id);
CREATE INDEX IF NOT EXISTS idx_enroll_course ON Enrollments(course_id);
CREATE INDEX IF NOT EXISTS idx_students_major ON Students(major);
""")
conn.commit()

# -----------------------------
# 4) USE CASE QUERIES
# We'll run several queries demonstrating advanced joins & aggregations
# -----------------------------

def run_and_print(title, sql):
    print('\n===', title, '===')
    df = pd.read_sql_query(sql, conn)
    if df.empty:
        print('(no rows)')
    else:
        print(df.to_string(index=False))
    return df

# 4.1 Full enrollment details (multi-table join)
q1 = '''
SELECT e.enrollment_id, s.student_id, s.name AS student_name, s.major, c.course_code, c.title, c.credits, e.semester, e.grade
FROM Enrollments e
JOIN Students s ON e.student_id = s.student_id
JOIN Courses c ON e.course_id = c.course_id
ORDER BY s.student_id, c.course_code;
'''
run_and_print('Full Enrollment Details (JOIN Students+Courses+Enrollments)', q1)

# 4.2 Average grade per course
# We'll map letter grades to numeric GPA points (A=4,B=3,C=2,D=1,F=0) inside SQL
q2 = '''
SELECT
  c.course_code,
  c.title,
  COUNT(e.enrollment_id) AS num_students,
  AVG(CASE e.grade
        WHEN 'A' THEN 4
        WHEN 'B' THEN 3
        WHEN 'C' THEN 2
        WHEN 'D' THEN 1
        WHEN 'F' THEN 0
        ELSE NULL END) AS avg_gpa
FROM Courses c
LEFT JOIN Enrollments e ON c.course_id = e.course_id
GROUP BY c.course_id
ORDER BY avg_gpa DESC NULLS LAST;
'''
course_summary = run_and_print('Average GPA per Course (aggregated)', q2)

# Save course summary
course_summary.to_csv('course_summary.csv', index=False)

# 4.3 Pass rate per course (grade >= C considered pass)
q3 = '''
SELECT
  c.course_code,
  c.title,
  SUM(CASE WHEN e.grade IN ('A','B','C') THEN 1 ELSE 0 END) AS passed_count,
  COUNT(e.enrollment_id) AS total_count,
  (CAST(SUM(CASE WHEN e.grade IN ('A','B','C') THEN 1 ELSE 0 END) AS FLOAT) / NULLIF(COUNT(e.enrollment_id),0)) AS pass_rate
FROM Courses c
LEFT JOIN Enrollments e ON c.course_id = e.course_id
GROUP BY c.course_id
HAVING total_count > 0
ORDER BY pass_rate DESC;
'''
run_and_print('Pass Rate per Course (HAVING total_count > 0)', q3)

# 4.4 Students with no enrollments (LEFT JOIN + IS NULL)
q4 = '''
SELECT s.student_id, s.name, s.major
FROM Students s
LEFT JOIN Enrollments e ON s.student_id = e.student_id
WHERE e.enrollment_id IS NULL;
'''
run_and_print('Students with NO Enrollments', q4)

# 4.5 Compute GPA per student (weighted by credits)
# We'll multiply grade points by course credits and divide by total credits taken
q5 = '''
WITH graded AS (
  SELECT
    s.student_id,
    s.name,
    c.course_id,
    c.credits,
    CASE e.grade
      WHEN 'A' THEN 4
      WHEN 'B' THEN 3
      WHEN 'C' THEN 2
      WHEN 'D' THEN 1
      WHEN 'F' THEN 0
      ELSE NULL END AS grade_point
  FROM Students s
  JOIN Enrollments e ON s.student_id = e.student_id
  JOIN Courses c ON e.course_id = c.course_id
  WHERE e.grade IS NOT NULL
)
SELECT
  student_id,
  name,
  SUM(grade_point * credits) / SUM(credits) AS gpa,
  SUM(credits) AS total_credits
FROM graded
GROUP BY student_id
ORDER BY gpa DESC;
'''
student_gpa = run_and_print('Student GPA (weighted by credits)', q5)

# Save student GPA
student_gpa.to_csv('student_gpa.csv', index=False)

# 4.6 Top students by GPA using WINDOW function (RANK)
q6 = '''
WITH graded AS (
  SELECT
    s.student_id,
    s.name,
    c.credits,
    CASE e.grade
      WHEN 'A' THEN 4
      WHEN 'B' THEN 3
      WHEN 'C' THEN 2
      WHEN 'D' THEN 1
      WHEN 'F' THEN 0
      ELSE NULL END AS grade_point
  FROM Students s
  JOIN Enrollments e ON s.student_id = e.student_id
  JOIN Courses c ON e.course_id = c.course_id
  WHERE e.grade IS NOT NULL
),
student_scores AS (
  SELECT
    student_id,
    name,
    SUM(grade_point * credits) / SUM(credits) AS gpa
  FROM graded
  GROUP BY student_id
)
SELECT
  student_id,
  name,
  gpa,
  RANK() OVER (ORDER BY gpa DESC) AS gpa_rank
FROM student_scores
ORDER BY gpa_rank;
'''
run_and_print('Top Students by GPA (WINDOW FUNCTION - RANK)', q6)

# 4.7 Department (major) aggregate: average GPA per major (join student gpa)
q7 = '''
WITH student_gpa AS (
  WITH graded AS (
    SELECT
      s.student_id,
      c.credits,
      CASE e.grade
        WHEN 'A' THEN 4
        WHEN 'B' THEN 3
        WHEN 'C' THEN 2
        WHEN 'D' THEN 1
        WHEN 'F' THEN 0
        ELSE NULL END AS grade_point
    FROM Students s
    JOIN Enrollments e ON s.student_id = e.student_id
    JOIN Courses c ON e.course_id = c.course_id
    WHERE e.grade IS NOT NULL
  )
  SELECT student_id, SUM(grade_point * credits) / SUM(credits) AS gpa
  FROM graded
  GROUP BY student_id
)
SELECT s.major, COUNT(s.student_id) AS num_students, AVG(sg.gpa) AS avg_gpa
FROM Students s
LEFT JOIN student_gpa sg ON s.student_id = sg.student_id
GROUP BY s.major
ORDER BY avg_gpa DESC NULLS LAST;
'''
run_and_print('Average GPA per Major', q7)

# -----------------------------
# 5) CLEANUP
# -----------------------------
conn.close()

print('\nFiles created: course_summary.csv, student_gpa.csv')
print('Database file:', DB_FILE)
print('\nProject complete. Open the CSVs or run the SQL queries yourself in any SQL client.')
