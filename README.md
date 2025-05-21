# 🎬 Neo4j + MovieLens + XGBoost

This project uses Neo4j's graph database and GDS library to generate graph features for movie recommendations and trains a machine learning model using XGBoost.

## 💾 Dataset

- [MovieLens 100K](https://grouplens.org/datasets/movielens/100k/)

## 🚀 Quickstart

```bash
# Start Neo4j
docker compose -f docker/neo4j-docker-compose.yml up -d

# Install dependencies
pip install -r requirements.txt

# Load data
python scripts/load_data.py
```

## 📊 ML Workflow

- Graph features: PageRank
- Model: XGBoost regressor
- Target: Movie rating

## 🔍 Goals

- Learn graph-based features
- Connect Neo4j and ML ecosystem
- Build a basic recommender
