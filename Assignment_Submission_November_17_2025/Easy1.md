1. INNER JOIN → only matched data

Take rows that match in both tables.
“Give me customers who have orders.”

2. LEFT JOIN → everything from left table + matches

Keeps all rows from the left table, even if there’s no match.
“Show all customers, even those with no orders.”

3. RIGHT JOIN → everything from right table + matches

Same as LEFT JOIN but opposite side.
(Not used much.)

4. FULL JOIN → everything from both tables

Shows all rows from both sides, matched or not.

5. CROSS JOIN → every combination

Pairs every row from table A with every row from table B.

6. SELF JOIN → join table with itself

Used when a table compares with itself
“Find employees and their managers.”

Advanced Aggregations (Super Simple Explanation)

1. GROUP BY → group rows

Groups data into categories.
“Total sales per customer.”

2. Aggregation Functions → math on groups

SUM() → adds values

COUNT() → counts rows

AVG() → average

MAX() → largest

MIN() → smallest

3. HAVING → filter groups

Like WHERE, but used after grouping.
“Show customers who spent more than 5000.”

4. Window Functions → calculations without grouping

They keep all rows but add extra info like:
rank
running total
previous row value

Example:
“Show each month’s sales + running total so far.”
