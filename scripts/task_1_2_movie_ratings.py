# ==========================================================
# Task 1 and Task 2: Movie Rating Analysis
# Assignment 02 - Data Management
# ==========================================================

from __future__ import print_function

import sys
import pyspark

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F

from cassandra.cluster import Cluster
from pymongo import MongoClient


# ----------------------------------------------------------
# Read ratings from Cassandra
# ----------------------------------------------------------

def read_ratings_from_cassandra(spark):
    print("==========================================")
    print("Reading ratings data from Cassandra")
    print("==========================================")

    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect("movielens_ks")
    session.default_fetch_size = 5000

    ratings_rows = session.execute(
        "SELECT user_id, movie_id, rating, timestamp FROM ratings"
    )

    ratings_data = []

    for row in ratings_rows:
        ratings_data.append(Row(
            user_id=int(row.user_id),
            movie_id=int(row.movie_id),
            rating=int(row.rating),
            timestamp=int(row.timestamp)
        ))

    session.shutdown()
    cluster.shutdown()

    ratings_df = spark.createDataFrame(ratings_data)

    print("Ratings loaded from Cassandra:", ratings_df.count())
    print("Sample ratings_df:")
    ratings_df.show(5, truncate=False)

    return ratings_df


# ----------------------------------------------------------
# Read movies from MongoDB
# ----------------------------------------------------------

def read_movies_from_mongodb(spark):
    print("==========================================")
    print("Reading movies data from MongoDB")
    print("==========================================")

    client = MongoClient("mongodb://localhost:27017/")
    db = client["movielens_db"]
    collection = db["movies"]

    movies_data = []

    for doc in collection.find():
        movies_data.append(Row(
            movie_id=int(doc.get("movie_id")),
            movie_title=doc.get("movie_title"),
            release_date=doc.get("release_date"),
            video_release_date=doc.get("video_release_date"),
            imdb_url=doc.get("imdb_url"),
            genres=doc.get("genres", [])
        ))

    client.close()

    movies_df = spark.createDataFrame(movies_data)

    print("Movies loaded from MongoDB:", movies_df.count())
    print("Sample movies_df:")
    movies_df.show(5, truncate=False)

    return movies_df


# ----------------------------------------------------------
# Task 1: Calculate average rating for each movie
# ----------------------------------------------------------

def task_1_average_rating_for_each_movie(spark):
    print("==========================================")
    print("Task 1: Average Rating for Each Movie")
    print("==========================================")

    average_movie_ratings_df = spark.sql("""
        SELECT
            m.movie_id,
            m.movie_title,
            ROUND(AVG(r.rating), 3) AS average_rating,
            COUNT(r.rating) AS rating_count
        FROM ratings r
        INNER JOIN movies m
            ON r.movie_id = m.movie_id
        GROUP BY
            m.movie_id,
            m.movie_title
        ORDER BY
            m.movie_id
    """)

    average_movie_ratings_df.show(20, truncate=False)
    print("Total movies with ratings:", average_movie_ratings_df.count())

    return average_movie_ratings_df


# ----------------------------------------------------------
# Task 2: Identify top ten movies with highest average ratings
# ----------------------------------------------------------

def task_2_top_10_movies(spark, average_movie_ratings_df):
    print("==========================================")
    print("Task 2: Top 10 Movies by Average Rating")
    print("==========================================")

    top_10_movies_df = average_movie_ratings_df \
        .orderBy(F.desc("average_rating"), F.desc("rating_count")) \
        .limit(10)

    # Add rank column for Cassandra primary key
    top_10_rows = top_10_movies_df.collect()

    ranked_rows = []
    rank = 1

    for row in top_10_rows:
        ranked_rows.append(Row(
            rank=rank,
            movie_id=int(row.movie_id),
            movie_title=row.movie_title,
            average_rating=float(row.average_rating),
            rating_count=int(row.rating_count)
        ))
        rank += 1

    top_10_ranked_df = spark.createDataFrame(ranked_rows)

    top_10_ranked_df.show(10, truncate=False)

    return top_10_ranked_df


# ----------------------------------------------------------
# Write Task 1 and Task 2 results into Cassandra
# ----------------------------------------------------------

def write_results_to_cassandra(average_movie_ratings_df, top_10_movies_df):
    print("==========================================")
    print("Writing Task 1 and Task 2 results into Cassandra")
    print("==========================================")

    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect("movielens_ks")

    # Clear old result data before inserting new results
    session.execute("TRUNCATE task1_average_movie_ratings")
    session.execute("TRUNCATE task2_top_10_movies")

    insert_task1_sql = """
        INSERT INTO task1_average_movie_ratings
        (movie_id, movie_title, average_rating, rating_count)
        VALUES (?, ?, ?, ?)
    """

    insert_task2_sql = """
        INSERT INTO task2_top_10_movies
        (rank, movie_id, movie_title, average_rating, rating_count)
        VALUES (?, ?, ?, ?, ?)
    """

    task1_stmt = session.prepare(insert_task1_sql)
    task2_stmt = session.prepare(insert_task2_sql)

    # Insert Task 1 result
    task1_count = 0

    for row in average_movie_ratings_df.toLocalIterator():
        session.execute(
            task1_stmt,
            (
                int(row.movie_id),
                row.movie_title,
                float(row.average_rating),
                int(row.rating_count)
            )
        )

        task1_count += 1

        if task1_count % 500 == 0:
            print("Inserted Task 1 records:", task1_count)

    print("Inserted Task 1 records into Cassandra:", task1_count)

    # Insert Task 2 result
    task2_count = 0

    for row in top_10_movies_df.collect():
        session.execute(
            task2_stmt,
            (
                int(row.rank),
                int(row.movie_id),
                row.movie_title,
                float(row.average_rating),
                int(row.rating_count)
            )
        )

        task2_count += 1

    print("Inserted Task 2 records into Cassandra:", task2_count)

    session.shutdown()
    cluster.shutdown()

    print("Task 1 and Task 2 results were written into Cassandra successfully.")


# ----------------------------------------------------------
# Validate Cassandra result tables
# ----------------------------------------------------------

def validate_cassandra_results():
    print("==========================================")
    print("Validating Task 1 and Task 2 Cassandra result tables")
    print("==========================================")

    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect("movielens_ks")

    task1_count = session.execute(
        "SELECT COUNT(*) FROM task1_average_movie_ratings"
    ).one()[0]

    task2_count = session.execute(
        "SELECT COUNT(*) FROM task2_top_10_movies"
    ).one()[0]

    print("Cassandra task1_average_movie_ratings count:", task1_count)
    print("Cassandra task2_top_10_movies count:", task2_count)

    print("Sample records from task1_average_movie_ratings:")
    for row in session.execute("SELECT * FROM task1_average_movie_ratings LIMIT 10"):
        print(row)

    print("Records from task2_top_10_movies:")
    for row in session.execute("SELECT * FROM task2_top_10_movies"):
        print(row)

    session.shutdown()
    cluster.shutdown()


def main():
    spark = SparkSession.builder \
        .appName("Task 1 and Task 2 Movie Rating Analysis") \
        .getOrCreate()

    print("==========================================")
    print("Spark Environment Information")
    print("==========================================")
    print("Python version:", sys.version)
    print("PySpark version:", pyspark.__version__)
    print("Spark version:", spark.version)

    # Read data back from databases
    ratings_df = read_ratings_from_cassandra(spark)
    movies_df = read_movies_from_mongodb(spark)

    # Register Spark SQL temporary views
    ratings_df.createOrReplaceTempView("ratings")
    movies_df.createOrReplaceTempView("movies")

    print("==========================================")
    print("Temporary Views Created")
    print("==========================================")
    print("- ratings")
    print("- movies")

    # Run Task 1
    average_movie_ratings_df = task_1_average_rating_for_each_movie(spark)
    average_movie_ratings_df.createOrReplaceTempView("average_movie_ratings")

    # Run Task 2
    top_10_movies_df = task_2_top_10_movies(spark, average_movie_ratings_df)
    top_10_movies_df.createOrReplaceTempView("top_10_movies")

    # Write results into Cassandra
    write_results_to_cassandra(average_movie_ratings_df, top_10_movies_df)

    # Validate Cassandra result tables
    validate_cassandra_results()

    print("==========================================")
    print("Task 1 and Task 2 completed and stored successfully!")
    print("==========================================")

    spark.stop()


if __name__ == "__main__":
    main()