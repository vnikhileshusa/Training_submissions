from pymongo import MongoClient
import datetime

# Connect to local MongoDB
client = MongoClient("mongodb://localhost:27017/")

# Use sample_mflix database and collections
db = client["sample_mflix"]
movies = db["movies"]
comments = db["comments"]
users = db["users"]

# -----------------------------
# 1. Count total movies
total_movies = movies.count_documents({})
print("Total movies:", total_movies)

# -----------------------------
# 2. Find 5 movies released before 2000
print("\nMovies before 2000:")
for m in movies.find({"year": {"$lt": 2000}}).sort("year", 1).limit(5):
    print(m["title"], "-", m["year"])

# -----------------------------
# 3. Find number of comments for "The Godfather"
godfather = movies.find_one({"title": "The Godfather"})
if godfather:
    movie_id = godfather["_id"]
    comment_count = comments.count_documents({"movie_id": movie_id})
    print("\nNumber of comments for 'The Godfather':", comment_count)

# -----------------------------
# 4. Update: Add a new field 'checked': False to all movies from the 1990s
update_result = movies.update_many(
    {"year": {"$gte": 1990, "$lt": 2000}},
    {"$set": {"checked": False}}
)
print("\nMovies updated with 'checked':", update_result.modified_count)

# -----------------------------
# 5. Delete: Remove comments older than Jan 1, 2005
delete_date = datetime.datetime(2005, 1, 1)
delete_result = comments.delete_many({"date": {"$lt": delete_date}})
print("Old comments deleted:", delete_result.deleted_count)

# -----------------------------
# 6. Aggregation example: Top 5 most commented movies
pipeline = [
    {"$group": {"_id": "$movie_id", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}},
    {"$limit": 5}
]
top_comments = list(comments.aggregate(pipeline))
print("\nTop 5 most commented movies:")
for item in top_comments:
    movie = movies.find_one({"_id": item["_id"]})
    if movie:
        print(movie["title"], "-", item["count"], "comments")

# -----------------------------
# 7. Final check: print first 5 movies to see 'checked' field
print("\nSample movies with 'checked' field:")
for m in movies.find().limit(5):
    print(m.get("title"), "-", m.get("year"), "-", m.get("checked"))
