import pandas as pd
from neo4j import GraphDatabase
import os, sys

# Load data
ratings = pd.read_csv("data/raw/ml-100k/u.data", sep="\t", names=["user", "movie", "rating", "timestamp"])
movies = pd.read_csv("data/raw/ml-100k/u.item", sep="|", encoding='latin-1', header=None,
                     usecols=[0, 1], names=["movie", "title"])
print('ratings')
print(ratings.head())

print('movies')
print(movies.head())

# Connect to Neo4j
url = 'neo4j://localhost:7687'
driver = GraphDatabase.driver(url, auth=("neo4j", "test1234"))

# Load function
def load(tx):
    # Create Movie nodes
    for _, row in movies.iterrows():
        tx.run(
            "MERGE (:Movie {id: $id, title: $title})",
            id=int(row["movie"]),
            title=row["title"]
        )

    # Create User nodes and RATED relationships
    for _, row in ratings.iterrows():
        tx.run("""
            MERGE (u:User {id: $uid})
            WITH u
            MATCH (m:Movie {id: $mid})
            MERGE (u)-[r:RATED]->(m)
            SET r.rating = $rating
        """, uid=int(row["user"]), mid=int(row["movie"]), rating=float(row["rating"]))

# Run transaction
with driver.session() as session:
    session.execute_write(load)  # Updated for deprecation warning

# Close the driver
driver.close()
