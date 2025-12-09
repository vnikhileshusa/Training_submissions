# -----------------------------
# Optimized NoSQL + ML Implementation
# -----------------------------
import pandas as pd
from sklearn.preprocessing import MultiLabelBinarizer, LabelEncoder
from sklearn.ensemble import RandomForestClassifier
import numpy as np

# 1. Simulate NoSQL dataset (as a list of dicts)
books = [
    {"title": "The Alchemist", "author": "Paulo Coelho", "genres": ["Fiction", "Adventure"], "price": 10.99, "in_stock": True},
    {"title": "1984", "author": "George Orwell", "genres": ["Dystopian", "Political Fiction"], "price": 12.5, "in_stock": True},
    {"title": "The Pragmatic Programmer", "author": "Andrew Hunt", "genres": ["Programming"], "price": 30.0, "in_stock": False},
    {"title": "Python Crash Course", "author": "Eric Matthes", "genres": ["Programming", "Education"], "price": 25.0, "in_stock": True},
    {"title": "The Great Gatsby", "author": "F. Scott Fitzgerald", "genres": ["Fiction", "Classic"], "price": 8.5, "in_stock": False}
]

# Convert to DataFrame once (vectorized)
df = pd.DataFrame(books)

# 2. Preprocess features
# Multi-label encode genres
mlb = MultiLabelBinarizer()
genres_encoded = mlb.fit_transform(df['genres'])
# Label encode authors
le = LabelEncoder()
authors_encoded = le.fit_transform(df['author']).reshape(-1,1)

# Combine all features as NumPy array (fast for ML)
X = np.hstack([df[['price']].values, authors_encoded, genres_encoded])
y = df['in_stock'].values

# 3. Train RandomForest with parallel processing
model = RandomForestClassifier(n_estimators=50, random_state=42, n_jobs=-1)
model.fit(X, y)

# 4. Predict on batch of new books efficiently
new_books = [
    {"title": "Deep Learning 101", "author": "Andrew Hunt", "genres": ["Programming", "Education"], "price": 28.0},
    {"title": "Brave New World", "author": "Aldous Huxley", "genres": ["Dystopian"], "price": 15.0}
]

# Preprocess batch in vectorized way
new_df = pd.DataFrame(new_books)
authors_enc_new = le.transform(new_df['author']).reshape(-1,1)
genres_enc_new = mlb.transform(new_df['genres'])
X_new = np.hstack([new_df[['price']].values, authors_enc_new, genres_enc_new])

predictions = model.predict(X_new)
for book, pred in zip(new_books, predictions):
    print(f"Predicted in_stock for '{book['title']}':", pred)

# 5. Optimized NoSQL-style query (vectorized filter)
cheap_books = df.loc[df['price'] < 15, 'title'].tolist()
print("Books priced under $15:", cheap_books)

# 6. Optimized NoSQL-style update (vectorized)
df.loc[df['author'] == 'Andrew Hunt', 'in_stock'] = True
print("Updated in_stock after vectorized update:")
print(df[['title', 'author', 'in_stock']])
