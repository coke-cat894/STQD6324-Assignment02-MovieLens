# ==========================================================
# Task 4 and Task 5: User Filtering Using CQL
# Assignment 02 - Data Management
# ==========================================================

from __future__ import print_function

from cassandra.cluster import Cluster


# ----------------------------------------------------------
# Task 4: Find all users who are less than 20 years old
# ----------------------------------------------------------

def task_4_users_younger_than_20(session):
    print("==========================================")
    print("Task 4: Users Younger Than 20 Using CQL")
    print("==========================================")

    query = """
        SELECT user_id, age, gender, occupation, zip_code
        FROM users
        WHERE age < 20
        ALLOW FILTERING
    """

    rows = session.execute(query)

    result = []

    for row in rows:
        result.append(row)

    print("Number of users younger than 20:", len(result))

    print("Sample users younger than 20:")
    for row in result[:30]:
        print(row)

    return result


# ----------------------------------------------------------
# Task 5: Find scientist users aged between 30 and 40
# ----------------------------------------------------------

def task_5_scientist_users_30_40(session):
    print("==========================================")
    print("Task 5: Scientist Users Aged 30 to 40 Using CQL")
    print("==========================================")

    query = """
        SELECT user_id, age, gender, occupation, zip_code
        FROM users
        WHERE occupation = 'scientist'
          AND age >= 30
          AND age <= 40
        ALLOW FILTERING
    """

    rows = session.execute(query)

    result = []

    for row in rows:
        result.append(row)

    print("Number of scientist users aged 30 to 40:", len(result))

    print("Scientist users aged 30 to 40:")
    for row in result:
        print(row)

    return result


# ----------------------------------------------------------
# Write Task 4 result into Cassandra
# ----------------------------------------------------------

def write_task4_result_to_cassandra(session, young_users):
    print("==========================================")
    print("Writing Task 4 result into Cassandra")
    print("==========================================")

    # Clear old result data
    session.execute("TRUNCATE task4_young_users")

    insert_sql = """
        INSERT INTO task4_young_users
        (user_id, age, gender, occupation, zip_code)
        VALUES (?, ?, ?, ?, ?)
    """

    insert_stmt = session.prepare(insert_sql)

    inserted_count = 0

    for row in young_users:
        session.execute(
            insert_stmt,
            (
                int(row.user_id),
                int(row.age),
                row.gender,
                row.occupation,
                row.zip_code
            )
        )

        inserted_count += 1

    print("Inserted Task 4 records into Cassandra:", inserted_count)

    return inserted_count


# ----------------------------------------------------------
# Write Task 5 result into Cassandra
# ----------------------------------------------------------

def write_task5_result_to_cassandra(session, scientist_users):
    print("==========================================")
    print("Writing Task 5 result into Cassandra")
    print("==========================================")

    # Clear old result data
    session.execute("TRUNCATE task5_scientist_users")

    insert_sql = """
        INSERT INTO task5_scientist_users
        (user_id, age, gender, occupation, zip_code)
        VALUES (?, ?, ?, ?, ?)
    """

    insert_stmt = session.prepare(insert_sql)

    inserted_count = 0

    for row in scientist_users:
        session.execute(
            insert_stmt,
            (
                int(row.user_id),
                int(row.age),
                row.gender,
                row.occupation,
                row.zip_code
            )
        )

        inserted_count += 1

    print("Inserted Task 5 records into Cassandra:", inserted_count)

    return inserted_count


# ----------------------------------------------------------
# Validate Cassandra result tables
# ----------------------------------------------------------

def validate_task4_task5_results(session):
    print("==========================================")
    print("Validating Task 4 and Task 5 Cassandra result tables")
    print("==========================================")

    task4_count = session.execute(
        "SELECT COUNT(*) FROM task4_young_users"
    ).one()[0]

    task5_count = session.execute(
        "SELECT COUNT(*) FROM task5_scientist_users"
    ).one()[0]

    print("Cassandra task4_young_users count:", task4_count)
    print("Cassandra task5_scientist_users count:", task5_count)

    print("Sample records from task4_young_users:")
    for row in session.execute("SELECT * FROM task4_young_users LIMIT 20"):
        print(row)

    print("Sample records from task5_scientist_users:")
    for row in session.execute("SELECT * FROM task5_scientist_users LIMIT 20"):
        print(row)


def main():
    print("==========================================")
    print("Connecting to Cassandra")
    print("==========================================")

    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect("movielens_ks")

    print("Connected to Cassandra keyspace: movielens_ks")

    # Run Task 4 and Task 5
    young_users = task_4_users_younger_than_20(session)
    scientist_users = task_5_scientist_users_30_40(session)

    # Write results into Cassandra result tables
    write_task4_result_to_cassandra(session, young_users)
    write_task5_result_to_cassandra(session, scientist_users)

    # Validate result tables
    validate_task4_task5_results(session)

    session.shutdown()
    cluster.shutdown()

    print("==========================================")
    print("Task 4 and Task 5 completed and stored successfully!")
    print("==========================================")


if __name__ == "__main__":
    main()