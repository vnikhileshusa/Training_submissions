"""
Big Data Foundations: Spark
Machine Learning Example using Spark MLlib
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("BigDataFoundations_SparkML") \
        .master("local[*]") \
        .getOrCreate()

    # Load dataset
    df = spark.read.csv(
        "movie_ratings_ml.csv",
        header=True,
        inferSchema=True
    )

    print("Original Data:")
    df.show()

    # Create binary label
    df = df.withColumn(
        "label",
        when(df.rating >= 4.0, 1).otherwise(0)
    )

    # Feature vector
    assembler = VectorAssembler(
        inputCols=["rating"],
        outputCol="features"
    )

    data = assembler.transform(df).select("features", "label")

    # Split data
    train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

    # Train logistic regression model
    lr = LogisticRegression()
    model = lr.fit(train_data)

    # Predictions
    predictions = model.transform(test_data)

    print("Predictions:")
    predictions.show()

    # Evaluate model
    evaluator = BinaryClassificationEvaluator()
    accuracy = evaluator.evaluate(predictions)

    print(f"Model Accuracy (AUC): {accuracy:.2f}")

    spark.stop()

if __name__ == "__main__":
    main()
