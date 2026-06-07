# ==========================================================
# Store MovieLens Data into Cassandra and MongoDB
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
# Genre columns in MovieLens 100k u.item
# ----------------------------------------------------------

genre_columns = [
    "unknown", "Action", "Adventure", "Animation", "Children", "Comedy",
    "Crime", "Documentary", "Drama", "Fantasy", "Film_Noir", "Horror",
    "Musical", "Mystery", "Romance", "Sci_Fi", "Thriller", "War", "Western"
]


# ----------------------------------------------------------
# Parse u.user
# ----------------------------------------------------------

def parse_user(line):
    fields = line.split("|")

    return Row(
        user_id=int(fields[0]),
        age=int(fields[1]),
        gender=fields[2],
        occupation=fields[3],
        zip_code=fields[4]
    )


# ----------------------------------------------------------
# Parse u.data
# ----------------------------------------------------------

def parse_rating(line):
    fields = line.split()

    return Row(
        user_id=int(fields[0]),
        movie_id=int(fields[1]),
        rating=int(fields[2]),
        timestamp=int(fields[3])
    )


# ----------------------------------------------------------
# Parse u.item for MongoDB document-style movie data
# ----------------------------------------------------------

def parse_movie_for_mongodb(line):
    fields = line.split("|")

    movie_id = int(fields[0])
    movie_title = fields[1]
    release_date = fields[2]
    video_release_date = fields[3]
    imdb_url = fields[4]

    genres = []
    genre_values = fields[5:24]

    for genre_name, genre_value in zip(genre_columns, genre_values):
        if int(genre_value) == 1:
            genres.append(genre_name)

    return Row(
        movie_id=movie_id,
        movie_title=movie_title,
        release_date=release_date,
        video_release_date=video_release_date,
        imdb_url=imdb_url,
        genres=genres
    )


# ----------------------------------------------------------
# Write users and ratings to Cassandra
# ----------------------------------------------------------

def write_to_cassandra(users_clean_df, ratings_clean_df):
    print("==========================================")
    print("Writing data into Cassandra")
    print("==========================================")

    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect("movielens_ks")

    # Clear old data before inserting new data
    session.execute("TRUNCATE users")
    session.execute("TRUNCATE ratings")

    insert_user_sql = """
        INSERT INTO users (user_id, age, gender, occupation, zip_code)
        VALUES (?, ?, ?, ?, ?)
    """

    insert_rating_sql = """
        INSERT INTO ratings (user_id, movie_id, rating, timestamp)
        VALUES (?, ?, ?, ?)
    """

    user_stmt = session.prepare(insert_user_sql)
    rating_stmt = session.prepare(insert_rating_sql)

    # Insert users
    user_count = 0
    for row in users_clean_df.collect():
        session.execute(
            user_stmt,
            (
                int(row.user_id),
                int(row.age),
                row.gender,
                row.occupation,
                row.zip_code
            )
        )
        user_count += 1

    print("Inserted users into Cassandra:", user_count)

    # Insert ratings
    rating_count = 0
    for row in ratings_clean_df.toLocalIterator():
        session.execute(
            rating_stmt,
            (
                int(row.user_id),
                int(row.movie_id),
                int(row.rating),
                int(row.timestamp)
            )
        )
        rating_count += 1

        if rating_count % 10000 == 0:
            print("Inserted ratings:", rating_count)

    print("Inserted ratings into Cassandra:", rating_count)

    session.shutdown()
    cluster.shutdown()

    print("Cassandra writing completed successfully.")


# ----------------------------------------------------------
# Write movies to MongoDB
# ----------------------------------------------------------

def write_to_mongodb(movies_mongo_df):
    print("==========================================")
    print("Writing data into MongoDB")
    print("==========================================")

    client = MongoClient("mongodb://localhost:27017/")
    db = client["movielens_db"]
    collection = db["movies"]

    # Clear old data before inserting new data
    collection.drop()

    movie_documents = []

    for row in movies_mongo_df.collect():
        movie_documents.append({
            "movie_id": int(row.movie_id),
            "movie_title": row.movie_title,
            "release_date": row.release_date,
            "video_release_date": row.video_release_date,
            "imdb_url": row.imdb_url,
            "genres": list(row.genres)
        })

    if len(movie_documents) > 0:
        collection.insert_many(movie_documents)

    print("Inserted movies into MongoDB:", len(movie_documents))

    client.close()

    print("MongoDB writing completed successfully.")


# ----------------------------------------------------------
# Validate Cassandra and MongoDB records
# ----------------------------------------------------------

def validate_databases():
    print("==========================================")
    print("Validating Cassandra Data")
    print("==========================================")

    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect("movielens_ks")

    user_count = session.execute("SELECT COUNT(*) FROM users").one()[0]
    rating_count = session.execute("SELECT COUNT(*) FROM ratings").one()[0]

    print("Cassandra users count:", user_count)
    print("Cassandra ratings count:", rating_count)

    print("Sample users from Cassandra:")
    for row in session.execute("SELECT * FROM users LIMIT 5"):
        print(row)

    print("Sample ratings from Cassandra:")
    for row in session.execute("SELECT * FROM ratings LIMIT 5"):
        print(row)

    session.shutdown()
    cluster.shutdown()

    print("==========================================")
    print("Validating MongoDB Data")
    print("==========================================")

    client = MongoClient("mongodb://localhost:27017/")
    db = client["movielens_db"]
    collection = db["movies"]

    movie_count = collection.count()
    print("MongoDB movies count:", movie_count)

    print("Sample movies from MongoDB:")
    for doc in collection.find().limit(5):
        print(doc)

    client.close()


def main():
    spark = SparkSession.builder \
        .appName("MovieLens Store to Cassandra and MongoDB") \
        .getOrCreate()

    sc = spark.sparkContext

    print("==========================================")
    print("Spark Environment Information")
    print("==========================================")
    print("Python version:", sys.version)
    print("PySpark version:", pyspark.__version__)
    print("Spark version:", spark.version)

    # HDFS paths
    user_path = "hdfs:///user/maria_dev/assignment02/movielens/raw/u.user"
    rating_path = "hdfs:///user/maria_dev/assignment02/movielens/raw/u.data"
    movie_path = "hdfs:///user/maria_dev/assignment02/movielens/raw/u.item"

    # Create RDDs
    user_rdd = sc.textFile(user_path)
    rating_rdd = sc.textFile(rating_path)
    movie_rdd = sc.textFile(movie_path)

    # Convert RDDs to DataFrames
    users_df = spark.createDataFrame(user_rdd.map(parse_user))
    ratings_df = spark.createDataFrame(rating_rdd.map(parse_rating))
    movies_mongo_df = spark.createDataFrame(movie_rdd.map(parse_movie_for_mongodb))

    # Clean DataFrames
    users_clean_df = users_df \
        .dropDuplicates(["user_id"]) \
        .filter(F.col("user_id").isNotNull()) \
        .filter((F.col("age") > 0) & (F.col("age") <= 100))

    ratings_clean_df = ratings_df \
        .dropDuplicates(["user_id", "movie_id", "timestamp"]) \
        .filter(F.col("user_id").isNotNull()) \
        .filter(F.col("movie_id").isNotNull()) \
        .filter((F.col("rating") >= 1) & (F.col("rating") <= 5))

    movies_mongo_df = movies_mongo_df \
        .dropDuplicates(["movie_id"]) \
        .filter(F.col("movie_id").isNotNull()) \
        .filter(F.col("movie_title").isNotNull())

    print("==========================================")
    print("Cleaned DataFrame Counts")
    print("==========================================")
    print("Users:", users_clean_df.count())
    print("Ratings:", ratings_clean_df.count())
    print("Movies:", movies_mongo_df.count())

    # Write data into databases
    write_to_cassandra(users_clean_df, ratings_clean_df)
    write_to_mongodb(movies_mongo_df)

    # Validate database records
    validate_databases()

    print("==========================================")
    print("Data storage into Cassandra and MongoDB completed successfully!")
    print("==========================================")

    spark.stop()


if __name__ == "__main__":
    main()