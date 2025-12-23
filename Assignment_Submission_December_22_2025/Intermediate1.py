"""
Big Data Foundations: Spark
Real Dataset Example - Movie Ratings Analysis
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("BigDataFoundations_MovieRatings") \
        .master("local[*]") \
        .getOrCreate()

    # Load real dataset (CSV)
    ratings_df = spark.read.csv(
        "movies_ratings.csv",
        header=True,
        inferSchema=True
    )

    print("Original Dataset:")
    ratings_df.show()

    # Analyze movie popularity
    movie_stats = ratings_df.groupBy("movie") \
        .agg(
            count("rating").alias("num_ratings"),
            avg("rating").alias("avg_rating")
        ) \
        .orderBy("num_ratings", ascending=False)

    print("Movie Popularity Results:")
    movie_stats.show()

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
