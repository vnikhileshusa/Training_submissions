from pymongo import MongoClient

# Connect to local MongoDB (make sure MongoDB is running)
client = MongoClient("mongodb://localhost:27017/")

# Create/use database and collection
db = client["bookstore"]
books = db["books"]

# Clear previous data (optional)
books.delete_many({})

# Insert sample books
books.insert_many([
    {
        "title": "The Alchemist",
        "author": "Paulo Coelho",
        "genres": ["Fiction", "Adventure"],
        "price": 10.99,
        "in_stock": True
    },
    {
        "title": "1984",
        "author": "George Orwell",
        "genres": ["Dystopian", "Political Fiction"],
        "price": 12.5,
        "in_stock": True
    },
    {
        "title": "The Pragmatic Programmer",
        "author": "Andrew Hunt",
        "genres": ["Programming"],
        "price": 30.0,
        "in_stock": False
    }
])

# Query: Find all books in stock
print("Books in stock:")
for book in books.find({"in_stock": True}):
    print(book)

# Update: Put "The Pragmatic Programmer" in stock
books.update_one({"title": "The Pragmatic Programmer"}, {"$set": {"in_stock": True}})

# Delete: Remove books priced above $25
books.delete_many({"price": {"$gt": 25}})

# Final list of books
print("\nFinal books in database:")
for book in books.find():
    print(book)
