"""
Optimized Big Data Foundations: Spark Implementation
Focus: Performance, Scalability, and Best Practices
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

def main():
    # Optimized Spark Session
    spark = SparkSession.builder \
        .appName("SparkML_Optimized") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

    # Load data efficiently
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv("movie_ratings_ml.csv")

    # Feature engineering (minimal transformations)
    df = df.withColumn(
        "label",
        when(df.rating >= 4.0, 1).otherwise(0)
    ).select("rating", "label")

    # Cache reused dataset
    df.cache()

    # Feature vector
    assembler = VectorAssembler(
        inputCols=["rating"],
        outputCol="features"
    )

    # Logistic Regression with optimized parameters
    lr = LogisticRegression(
        maxIter=20,
        regParam=0.01,
        elasticNetParam=0.0
    )

    # ML Pipeline (faster & cleaner)
    pipeline = Pipeline(stages=[assembler, lr])

    # Split data
    train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

    # Train model
    model = pipeline.fit(train_data)

    # Predict
    predictions = model.transform(test_data)

    # Evaluate
    evaluator = BinaryClassificationEvaluator()
    auc = evaluator.evaluate(predictions)

    print(f"Optimized Model AUC: {auc:.2f}")

    spark.stop()

if __name__ == "__main__":
    main()
