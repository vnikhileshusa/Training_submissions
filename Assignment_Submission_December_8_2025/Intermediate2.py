# -----------------------------
# 1. Simulate NoSQL dataset
# -----------------------------
books = [
    {"title": "The Alchemist", "author": "Paulo Coelho", "genres": ["Fiction", "Adventure"], "price": 10.99, "in_stock": True},
    {"title": "1984", "author": "George Orwell", "genres": ["Dystopian", "Political Fiction"], "price": 12.5, "in_stock": True},
    {"title": "The Pragmatic Programmer", "author": "Andrew Hunt", "genres": ["Programming"], "price": 30.0, "in_stock": False},
    {"title": "Python Crash Course", "author": "Eric Matthes", "genres": ["Programming", "Education"], "price": 25.0, "in_stock": True},
    {"title": "The Great Gatsby", "author": "F. Scott Fitzgerald", "genres": ["Fiction", "Classic"], "price": 8.5, "in_stock": False}
]

# -----------------------------
# 2. Preprocess NoSQL data for ML
# -----------------------------
import pandas as pd
from sklearn.preprocessing import MultiLabelBinarizer, LabelEncoder
from sklearn.ensemble import RandomForestClassifier

# Convert list of dicts to DataFrame
df = pd.DataFrame(books)

# Encode 'genres' (multi-label)
mlb = MultiLabelBinarizer()
genres_encoded = mlb.fit_transform(df['genres'])
genres_df = pd.DataFrame(genres_encoded, columns=mlb.classes_)

# Encode 'author' (categorical)
le = LabelEncoder()
df['author_encoded'] = le.fit_transform(df['author'])

# Combine features
X = pd.concat([df[['price', 'author_encoded']], genres_df], axis=1)
y = df['in_stock']

# -----------------------------
# 3. Train a simple model
# -----------------------------
model = RandomForestClassifier(n_estimators=50, random_state=42)
model.fit(X, y)

# -----------------------------
# 4. Predict for a new book
# -----------------------------
new_book = {
    "title": "Deep Learning 101",
    "author": "Andrew Hunt",
    "genres": ["Programming", "Education"],
    "price": 28.0
}

# Preprocess new book
author_enc = le.transform([new_book["author"]])[0]
genres_enc = mlb.transform([new_book["genres"]])
X_new = pd.DataFrame([[new_book["price"], author_enc] + list(genres_enc[0])], columns=X.columns)

prediction = model.predict(X_new)[0]
print(f"Predicted in_stock for '{new_book['title']}':", prediction)

# -----------------------------
# 5. Example of NoSQL-style query & update
# -----------------------------
# Query all books priced under $15
cheap_books = [b['title'] for b in books if b['price'] < 15]
print("Books priced under $15:", cheap_books)

# Update: mark all books by 'Andrew Hunt' as in_stock=True
for b in books:
    if b['author'] == 'Andrew Hunt':
        b['in_stock'] = True

print("Updated book list (after marking Andrew Hunt's books in stock):")
for b in books:
    print(b)
