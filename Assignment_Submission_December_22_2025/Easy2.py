

from pyspark import SparkConf, SparkContext


def main():
    # Spark configuration
    conf = SparkConf() \
        .setAppName("BigDataFoundations_WordCount") \
        .setMaster("local[*]")   # Run locally using all cores

    # Create Spark context
    sc = SparkContext(conf=conf)

    # Toy dataset (simulating big data text)
    text_data = [
        "spark is fast",
        "spark is powerful",
        "big data is powerful",
        "spark handles big data"
    ]

    # Distribute data across Spark cluster
    rdd = sc.parallelize(text_data)

    # Word Count logic
    word_counts = (
        rdd.flatMap(lambda line: line.split(" "))
           .map(lambda word: (word.lower(), 1))
           .reduceByKey(lambda a, b: a + b)
    )

    # Collect and display results
    results = word_counts.collect()

    print("Word Count Results:")
    for word, count in results:
        print(f"{word}: {count}")

    # Stop Spark context
    sc.stop()


if __name__ == "__main__":
    main()
