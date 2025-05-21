import pandas as pd
from neo4j import GraphDatabase

# Connect to Neo4j
url = 'neo4j://localhost:7687'
driver = GraphDatabase.driver(url, auth=("neo4j", "test1234"))

# PageRank query
query = """
CALL gds.pageRank.stream('ratingsGraph')
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).id AS id, labels(gds.util.asNode(nodeId)) AS labels, score
"""

with driver.session() as session:
    result = session.run(query)
    records = [r.data() for r in result]

# Convert to DataFrame
df = pd.DataFrame(records)

# Filter for Movie nodes only (skip User nodes)
df = df[df['labels'].apply(lambda x: 'Movie' in x)].copy()

# --- Load Movie Metadata from u.item ---
movie_info = pd.read_csv(
    "data/raw/ml-100k/u.item",
    sep="|",
    encoding="latin-1",
    header=None,
    usecols=[0, 1, 2],  # movie id, title, release date
    names=["movie_id", "title", "release_date"]
)

# Merge PageRank scores with movie metadata
df_merged = df.merge(movie_info, how="left", left_on="id", right_on="movie_id")

# Select and rename columns for clarity
df_merged = df_merged[["id", "title", "release_date", "score"]]
df_merged = df_merged.sort_values(by="score", ascending=False)

# Save the enriched results
df_merged.to_csv("data/processed/pagerank_movie_scores.csv", index=False)
