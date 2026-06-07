# ==========================================================
# Task 3: Active Users and Favourite Movie Genre
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
# Task 3 analysis
# ----------------------------------------------------------

def task_3_active_users_favourite_genre(spark, ratings_df, movies_df):
    print("==========================================")
    print("Task 3: Active Users and Favourite Movie Genre")
    print("==========================================")

    # Register ratings view
    ratings_df.createOrReplaceTempView("ratings")

    # MongoDB stores genres as an array.
    # For genre-level analysis, the genres array is temporarily exploded.
    movie_genres_df = movies_df.select(
        "movie_id",
        "movie_title",
        F.explode("genres").alias("genre")
    )

    movie_genres_df.createOrReplaceTempView("movie_genres")

    print("Sample exploded movie_genres_df:")
    movie_genres_df.show(20, truncate=False)

    # ------------------------------------------------------
    # Step 1: Find users who rated at least 50 movies
    # ------------------------------------------------------

    active_users_df = spark.sql("""
        SELECT
            user_id,
            COUNT(movie_id) AS total_ratings
        FROM ratings
        GROUP BY user_id
        HAVING COUNT(movie_id) >= 50
        ORDER BY user_id
    """)

    active_users_df.createOrReplaceTempView("active_users")

    print("Users who rated at least 50 movies:")
    active_users_df.show(20, truncate=False)

    active_user_count = active_users_df.count()
    print("Number of users who rated at least 50 movies:", active_user_count)

    # ------------------------------------------------------
    # Step 2: Count genre frequency for each active user
    # ------------------------------------------------------

    user_genre_count_df = spark.sql("""
        SELECT
            r.user_id,
            mg.genre,
            COUNT(*) AS genre_rating_count
        FROM ratings r
        INNER JOIN active_users au
            ON r.user_id = au.user_id
        INNER JOIN movie_genres mg
            ON r.movie_id = mg.movie_id
        GROUP BY
            r.user_id,
            mg.genre
    """)

    user_genre_count_df.createOrReplaceTempView("user_genre_counts")

    print("Sample genre rating counts for active users:")
    user_genre_count_df.orderBy("user_id", F.desc("genre_rating_count")).show(30, truncate=False)

    # ------------------------------------------------------
    # Step 3: Rank genres for each user and select top genre
    # ------------------------------------------------------

    favourite_genre_df = spark.sql("""
        SELECT
            user_id,
            favourite_genre,
            genre_rating_count
        FROM (
            SELECT
                user_id,
                genre AS favourite_genre,
                genre_rating_count,
                ROW_NUMBER() OVER (
                    PARTITION BY user_id
                    ORDER BY genre_rating_count DESC, genre ASC
                ) AS genre_rank
            FROM user_genre_counts
        ) ranked_genres
        WHERE genre_rank = 1
        ORDER BY user_id
    """)

    print("Favourite genre for active users:")
    favourite_genre_df.show(50, truncate=False)

    favourite_genre_count = favourite_genre_df.count()
    print("Number of favourite genre results:", favourite_genre_count)

    return active_users_df, user_genre_count_df, favourite_genre_df


# ----------------------------------------------------------
# Write Task 3 result into Cassandra
# ----------------------------------------------------------

def write_task3_result_to_cassandra(favourite_genre_df):
    print("==========================================")
    print("Writing Task 3 result into Cassandra")
    print("==========================================")

    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect("movielens_ks")

    # Clear old result data before inserting new result
    session.execute("TRUNCATE task3_user_favourite_genres")

    insert_task3_sql = """
        INSERT INTO task3_user_favourite_genres
        (user_id, favourite_genre, genre_rating_count)
        VALUES (?, ?, ?)
    """

    task3_stmt = session.prepare(insert_task3_sql)

    task3_count = 0

    for row in favourite_genre_df.toLocalIterator():
        session.execute(
            task3_stmt,
            (
                int(row.user_id),
                row.favourite_genre,
                int(row.genre_rating_count)
            )
        )

        task3_count += 1

        if task3_count % 100 == 0:
            print("Inserted Task 3 records:", task3_count)

    print("Inserted Task 3 records into Cassandra:", task3_count)

    session.shutdown()
    cluster.shutdown()

    print("Task 3 result was written into Cassandra successfully.")


# ----------------------------------------------------------
# Validate Cassandra result table
# ----------------------------------------------------------

def validate_task3_cassandra_result():
    print("==========================================")
    print("Validating Task 3 Cassandra result table")
    print("==========================================")

    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect("movielens_ks")

    task3_count = session.execute(
        "SELECT COUNT(*) FROM task3_user_favourite_genres"
    ).one()[0]

    print("Cassandra task3_user_favourite_genres count:", task3_count)

    print("Sample records from task3_user_favourite_genres:")
    for row in session.execute("SELECT * FROM task3_user_favourite_genres LIMIT 20"):
        print(row)

    session.shutdown()
    cluster.shutdown()


def main():
    spark = SparkSession.builder \
        .appName("Task 3 Favourite Movie Genre") \
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

    # Run Task 3
    active_users_df, user_genre_count_df, favourite_genre_df = task_3_active_users_favourite_genre(
        spark,
        ratings_df,
        movies_df
    )

    # Write Task 3 result into Cassandra
    write_task3_result_to_cassandra(favourite_genre_df)

    # Validate Cassandra result table
    validate_task3_cassandra_result()

    print("==========================================")
    print("Task 3 completed and stored successfully!")
    print("==========================================")

    spark.stop()


if __name__ == "__main__":
    main()