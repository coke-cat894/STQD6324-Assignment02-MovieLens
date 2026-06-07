# ==========================================================
# Read MovieLens Dataset from HDFS using Apache Spark
# Assignment 02 - Data Management
# ==========================================================

import sys
import pyspark
from pyspark.sql import SparkSession


def main():
    # Create SparkSession in the Hadoop/Spark server environment
    spark = SparkSession.builder \
        .appName("MovieLens Read HDFS Files") \
        .getOrCreate()

    sc = spark.sparkContext

    print("==========================================")
    print("Spark Environment Information")
    print("==========================================")
    print("Python version:", sys.version)
    print("PySpark version:", pyspark.__version__)
    print("Spark version:", spark.version)

    # Define HDFS file paths
    user_path = "hdfs:///user/maria_dev/assignment02/movielens/raw/u.user"
    rating_path = "hdfs:///user/maria_dev/assignment02/movielens/raw/u.data"
    movie_path = "hdfs:///user/maria_dev/assignment02/movielens/raw/u.item"

    print("\n==========================================")
    print("HDFS File Paths")
    print("==========================================")
    print("User file:", user_path)
    print("Rating file:", rating_path)
    print("Movie file:", movie_path)

    # Create RDDs from raw files stored in HDFS
    user_rdd = sc.textFile(user_path)
    rating_rdd = sc.textFile(rating_path)
    movie_rdd = sc.textFile(movie_path)

    print("\n==========================================")
    print("First 5 Records from u.user")
    print("==========================================")
    for line in user_rdd.take(5):
        print(line)

    print("\n==========================================")
    print("First 5 Records from u.data")
    print("==========================================")
    for line in rating_rdd.take(5):
        print(line)

    print("\n==========================================")
    print("First 5 Records from u.item")
    print("==========================================")
    for line in movie_rdd.take(5):
        print(line)

    print("\n==========================================")
    print("Record Counts")
    print("==========================================")
    print("Number of user records:", user_rdd.count())
    print("Number of rating records:", rating_rdd.count())
    print("Number of movie records:", movie_rdd.count())

    print("\n==========================================")
    print("RDD loading from HDFS completed successfully!")
    print("==========================================")

    spark.stop()


if __name__ == "__main__":
    main()