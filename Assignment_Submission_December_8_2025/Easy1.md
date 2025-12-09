Key Concepts of NoSQL (Basics)

1. Not “Only” SQL
   NoSQL databases don’t rely on traditional SQL tables. Instead, they use different data models like documents, key-value pairs, wide-column stores, or graphs. They are flexible compared to fixed row-column structures.

2. Schema-less (Flexible Structure)
   You don’t need to define a strict schema before adding data.
   For example, one record can have 3 fields and another can have 10 fields—NoSQL allows that.

3. Designed for High Scalability
   NoSQL systems are built to handle large amounts of data, high traffic, and real-time applications.
   They can scale horizontally by adding more machines easily.

4. Handles Unstructured & Semi-Structured Data
   Ideal for JSON, logs, user data, sensor data, social media posts, etc.
   No need to convert everything into tables.

5. Types of NoSQL Databases

Document Databases (MongoDB) – store JSON-like documents

Key-Value Stores (Redis) – simple key-value pairs

Column-Family Stores (Cassandra) – large datasets split across columns

Graph Databases (Neo4j) – connected data like networks, friends, routes

6. Prioritizes Availability & Performance
   Instead of strong SQL-like consistency, many NoSQL systems use the CAP theorem principles to stay highly available and fast.
