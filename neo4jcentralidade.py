from graphdatascience import GraphDataScience
from neo4j import GraphDatabase

# Conexão com o Neo4j Aura
NEO4J_URI = "bolt+s://c06feaf2.databases.neo4j.io"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "E2yvIk15oJcELi0P7jdmxCIndyyYH3T9WGRx_04elXg"

# Conectando ao GDS
gds = GraphDataScience(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# ------------------------
# 1. Cria grafo projetado
# ------------------------
gds.run_cypher("""
CALL gds.graph.project.cypher(
  'usuarios_similares',
  'Usuario',
  $relationshipQuery
)
""", params={
    'relationshipQuery': """
    MATCH (u1:Usuario)-[:ASSISTIU]->(f:Filme)<-[:ASSISTIU]-(u2:Usuario)
    WHERE u1 <> u2
    RETURN u1 AS source, u2 AS target
    """
})

# -------------------------------
# 2. Algoritmo de PageRank
# -------------------------------
pagerank_result = gds.pageRank.stream("usuarios_similares")
print("\n📊 PageRank:")
for row in pagerank_result.limit(10).to_dataframe().itertuples():
    print(f"{row.nodeName}: {row.score:.4f}")

# -------------------------------
# 3. Algoritmo de Comunidades (Louvain)
# -------------------------------
louvain_result = gds.louvain.stream("usuarios_similares")
print("\n🧩 Comunidades (Louvain):")
for row in louvain_result.limit(10).to_dataframe().itertuples():
    print(f"{row.nodeName}: comunidade {row.communityId}")
