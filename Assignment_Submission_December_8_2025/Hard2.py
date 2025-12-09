# -----------------------------
# Mini Project: Book Inventory System (NoSQL-style + ML)
# -----------------------------
import pandas as pd
import numpy as np
from sklearn.preprocessing import MultiLabelBinarizer, LabelEncoder
from sklearn.ensemble import RandomForestClassifier

# 1. Simulate NoSQL dataset (JSON-like documents)
books = [
    {"title": "The Alchemist", "author": "Paulo Coelho", "genres": ["Fiction", "Adventure"], "price": 10.99, "in_stock": True, "rating": 4.7},
    {"title": "1984", "author": "George Orwell", "genres": ["Dystopian", "Political Fiction"], "price": 12.5, "in_stock": True, "rating": 4.9},
    {"title": "The Pragmatic Programmer", "author": "Andrew Hunt", "genres": ["Programming"], "price": 30.0, "in_stock": False, "rating": 4.8},
    {"title": "Python Crash Course", "author": "Eric Matthes", "genres": ["Programming", "Education"], "price": 25.0, "in_stock": True, "rating": 4.6},
    {"title": "The Great Gatsby", "author": "F. Scott Fitzgerald", "genres": ["Fiction", "Classic"], "price": 8.5, "in_stock": False, "rating": 4.4},
    {"title": "Deep Learning 101", "author": "Ian Goodfellow", "genres": ["Programming", "Education"], "price": 28.0, "in_stock": True, "rating": 4.9},
    {"title": "Brave New World", "author": "Aldous Huxley", "genres": ["Dystopian"], "price": 15.0, "in_stock": False, "rating": 4.5}
]

# Convert to DataFrame
df = pd.DataFrame(books)

# -----------------------------
# 2. Preprocess features for ML
# -----------------------------
mlb = MultiLabelBinarizer()
genres_encoded = mlb.fit_transform(df['genres'])
le = LabelEncoder()
df['author_encoded'] = le.fit_transform(df['author'])
X = np.hstack([df[['price', 'rating']].values, df[['author_encoded']].values, genres_encoded])
y = df['in_stock'].values

# -----------------------------
# 3. Train Random Forest Classifier
# -----------------------------
model = RandomForestClassifier(n_estimators=50, random_state=42, n_jobs=-1)
model.fit(X, y)
print("✅ ML Model trained successfully!\n")

# -----------------------------
# 4. NoSQL-style queries
# -----------------------------
# Query books under $15
cheap_books = df.loc[df['price'] < 15, 'title'].tolist()
print("Books priced under $15:", cheap_books)

# Query all Programming books
programming_books = df[df['genres'].apply(lambda g: 'Programming' in g)]['title'].tolist()
print("Programming books:", programming_books)

# -----------------------------
# 5. NoSQL-style update
# -----------------------------
# Mark all books by 'Andrew Hunt' as in_stock
df.loc[df['author'] == 'Andrew Hunt', 'in_stock'] = True
print("\nUpdated in_stock after marking Andrew Hunt's books in stock:")
print(df[['title', 'author', 'in_stock']])

# -----------------------------
# 6. NoSQL-style delete
# -----------------------------
# Remove books with rating < 4.5
df = df[df['rating'] >= 4.5]
print("\nBooks after deleting low-rated books:")
print(df[['title', 'rating']])

# -----------------------------
# 7. ML Predictions for new books
# -----------------------------
new_books = [
    {"title": "AI for Beginners", "author": "Ian Goodfellow", "genres": ["Programming", "Education"], "price": 26.0, "rating": 4.7},
    {"title": "Classic Tales", "author": "Anonymous", "genres": ["Fiction", "Classic"], "price": 9.0, "rating": 4.3}
]

new_df = pd.DataFrame(new_books)
# Handle unseen authors
authors_enc_new = [le.transform([a])[0] if a in le.classes_ else -1 for a in new_df['author']]
authors_enc_new = np.array(authors_enc_new).reshape(-1,1)
genres_enc_new = mlb.transform(new_df['genres'])
X_new = np.hstack([new_df[['price','rating']].values, authors_enc_new, genres_enc_new])
predictions = model.predict(X_new)
for book, pred in zip(new_books, predictions):
    print(f"Predicted in_stock for '{book['title']}':", pred)

# -----------------------------
# 8. Aggregations / Insights
# -----------------------------
# Count books per genre
genre_counts = df.explode('genres')['genres'].value_counts()
print("\nNumber of books per genre:\n", genre_counts)

# Top 3 rated books
top_books = df.sort_values(by='rating', ascending=False)[['title','rating']].head(3)
print("\nTop 3 rated books:\n", top_books)
