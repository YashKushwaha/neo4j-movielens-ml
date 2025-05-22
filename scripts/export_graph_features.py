import pandas as pd
from neo4j import GraphDatabase

# === 1. Connect to Neo4j and get PageRank scores ===
url = 'neo4j://localhost:7687'
driver = GraphDatabase.driver(url, auth=("neo4j", "test1234"))

query = """
CALL gds.pageRank.stream('ratingsGraph')
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).id AS id, labels(gds.util.asNode(nodeId)) AS labels, score
"""

with driver.session() as session:
    result = session.run(query)
    records = [r.data() for r in result]

pagerank_df = pd.DataFrame(records)

# Filter only Movie nodes
pagerank_df = pagerank_df[pagerank_df['labels'].apply(lambda x: 'Movie' in x)].copy()

# === 2. Load movie metadata ===
movies_df = pd.read_csv(
    "data/raw/ml-100k/u.item",
    sep="|",
    encoding="latin-1",
    header=None,
    usecols=[0, 1, 2],  # movie id, title, release date
    names=["movie_id", "title", "release_date"]
)

# === 3. Load ratings and compute count & avg rating per movie ===
ratings_df = pd.read_csv(
    "data/raw/ml-100k/u.data",
    sep="\t",
    names=["user", "movie", "rating", "timestamp"]
)

rating_stats = ratings_df.groupby('movie').agg(
    rating_count=('rating', 'count'),
    avg_rating=('rating', 'mean')
).reset_index()

# === 4. Merge all datasets ===
# Merge PageRank with movie metadata
merged = pagerank_df.merge(movies_df, how='left', left_on='id', right_on='movie_id')

# Merge rating stats
merged = merged.merge(rating_stats, how='left', left_on='id', right_on='movie')

# Final columns cleanup
final_df = merged[["id", "title", "release_date", "score", "rating_count", "avg_rating"]]
final_df = final_df.sort_values(by="score", ascending=False)

# === 5. Save enriched result ===
final_df.to_csv("data/processed/pagerank_enriched.csv", index=False)
