from neo4j import GraphDatabase
import os
import sys

print(os.getcwd())



# Path to your Cypher script file
#cypher_file = "scripts/run_pagerank.cypher"

cypher_file = "scripts/generate_graph_features.cypher"

print(os.path.exists(cypher_file), cypher_file)

# Connect to Neo4j
driver = GraphDatabase.driver("neo4j://localhost:7687", auth=("neo4j", "test1234"))

with open(cypher_file, "r") as f:
    cypher_commands = f.read().strip().split(";")

with driver.session() as session:
    for command in cypher_commands:
        cleaned = command.strip()
        if cleaned:
            result = session.run(cleaned)
            for record in result:
                print(record.data())  # Optional: print results

driver.close()
