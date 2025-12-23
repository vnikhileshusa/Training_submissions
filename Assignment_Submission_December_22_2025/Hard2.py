"""
Big Data Foundations: Spark
End-to-End Mini Project
User Activity Analysis
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, avg
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator


def main():
    # --------------------------------------------------
    # 1. Create Spark Session
    # --------------------------------------------------
    spark = SparkSession.builder \
        .appName("BigDataFoundations_EndToEnd_Project") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    print("\n=== Spark Session Created ===\n")

    # --------------------------------------------------
    # 2. Load Dataset
    # --------------------------------------------------
    df = spark.read.csv(
        "user_activity.csv",
        header=True,
        inferSchema=True
    )

    print("=== Original Dataset ===")
    df.show()

    # --------------------------------------------------
    # 3. Data Exploration (Spark SQL)
    # --------------------------------------------------
    df.createOrReplaceTempView("activity")

    print("=== Average User Engagement ===")
    spark.sql("""
        SELECT 
            AVG(session_time) AS avg_session_time,
            AVG(pages_visited) AS avg_pages_visited
        FROM activity
    """).show()

    # --------------------------------------------------
    # 4. Feature Engineering
    # Define active user:
    # session_time >= 30 AND pages_visited >= 5
    # --------------------------------------------------
    df = df.withColumn(
        "label",
        when(
            (df.session_time >= 30) & (df.pages_visited >= 5), 1
        ).otherwise(0)
    )

    print("=== Dataset with Label (Active User) ===")
    df.show()

    # --------------------------------------------------
    # 5. Prepare Features for ML
    # --------------------------------------------------
    assembler = VectorAssembler(
        inputCols=["session_time", "pages_visited"],
        outputCol="features"
    )

    ml_data = assembler.transform(df).select("features", "label")

    # --------------------------------------------------
    # 6. Train/Test Split
    # --------------------------------------------------
    train_data, test_data = ml_data.randomSplit([0.8, 0.2], seed=42)

    # --------------------------------------------------
    # 7. Train ML Model (Logistic Regression)
    # --------------------------------------------------
    lr = LogisticRegression(maxIter=10)
    model = lr.fit(train_data)

    # --------------------------------------------------
    # 8. Make Predictions
    # --------------------------------------------------
    predictions = model.transform(test_data)

    print("=== Model Predictions ===")
    predictions.show()

    # --------------------------------------------------
    # 9. Evaluate Model
    # --------------------------------------------------
    evaluator = BinaryClassificationEvaluator()
    auc = evaluator.evaluate(predictions)

    print(f"Model AUC Score: {auc:.2f}")

    # --------------------------------------------------
    # 10. Stop Spark Session
    # --------------------------------------------------
    spark.stop()
    print("\n=== Spark Session Stopped ===")


if __name__ == "__main__":
    main()
