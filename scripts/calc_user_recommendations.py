from neo4j import GraphDatabase
import pandas as pd
import os

# Config
OUTPUT_DIR = "data/processed"
OUTPUT_FILE = f"{OUTPUT_DIR}/all_user_recommendations.csv"
TOP_N = 10  # Top N recommendations per user

# Connect to Neo4j
driver = GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "test1234"))

# --- Cypher Tasks ---
def drop_projection(tx):
    tx.run("CALL gds.graph.drop('ratingsGraph', false)")

def create_projection(tx):
    tx.run("""
    CALL gds.graph.project(
        'ratingsGraph',
        ['User', 'Movie'],
        {
            RATED: {
                type: 'RATED',
                properties: 'rating'
            }
        }
    )
    """)

def run_node_similarity(tx):
    tx.run("""
    CALL gds.nodeSimilarity.write('ratingsGraph', {
        nodeLabels: ['Movie'],
        relationshipTypes: ['RATED'],
        relationshipWeightProperty: 'rating',
        similarityCutoff: 0.5,
        writeRelationshipType: 'SIMILAR',
        writeProperty: 'score'
    })
    """)


def get_all_users(tx):
    result = tx.run("MATCH (u:User) RETURN u.id AS user_id")
    return [record["user_id"] for record in result]

def get_user_liked_movies(tx, user_id):
    result = tx.run("""
    MATCH (u:User {id: $user_id})-[r:RATED]->(m:Movie)
    WHERE r.rating >= 4
    RETURN m.id AS movie_id
    """, user_id=user_id)
    return [record["movie_id"] for record in result]

def get_user_recommendations(tx, user_id, liked_movie_ids, top_n):
    result = tx.run("""
    UNWIND $liked_movie_ids AS mid
    MATCH (:Movie {id: mid})-[s:SIMILAR]->(rec:Movie)
    WHERE NOT EXISTS {
        MATCH (:User {id: $user_id})-[:RATED]->(rec)
    }
    RETURN DISTINCT $user_id AS user_id, rec.id AS movie_id, rec.title AS title, MAX(s.score) AS similarity
    ORDER BY similarity DESC
    LIMIT $top_n
    """, user_id=user_id, liked_movie_ids=liked_movie_ids, top_n=top_n)
    return result.data()

# --- Main Script ---
all_recommendations = []

with driver.session() as session:
    print("🔄 Resetting GDS graph...")
    session.execute_write(drop_projection)
    session.execute_write(create_projection)
    session.execute_write(run_node_similarity)

    print("👥 Fetching all users...")
    all_users = session.execute_read(get_all_users)

    print(f"🔢 Total users: {len(all_users)}")

    for user_id in all_users:
        liked_movies = session.execute_read(get_user_liked_movies, user_id)
        if not liked_movies:
            continue

        recs = session.execute_read(get_user_recommendations, user_id, liked_movies, TOP_N)
        all_recommendations.extend(recs)

    # Save all to CSV
    if all_recommendations:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        df = pd.DataFrame(all_recommendations)
        df.to_csv(OUTPUT_FILE, index=False)
        print(f"\n✅ All recommendations saved to {OUTPUT_FILE}")
    else:
        print("⚠️ No recommendations generated.")

driver.close()
