from graphdatascience import GraphDataScience
from pymongo import MongoClient
from bson import ObjectId

# === Conexão com MongoDB ===
mongo_client = MongoClient(
    "mongodb+srv://filipemiguel3m:ho28QZpfrItn9LMC@cluster0.j3k6e.mongodb.net/?retryWrites=true&w=majority&tls=true"
)
db = mongo_client["Cluster0"]
usuarios_collection = db["usuarios"]

# === Conexão com Neo4j Desktop ===
GDS_URI = "bolt://localhost:7687"
GDS_USER = "neo4j"
GDS_PASSWORD = "E2yvIk15oJcELi0P7jdmxCIndyyYH3T9WGRx_04elXg"

# === Inicializa GDS ===
gds = GraphDataScience(GDS_URI, auth=(GDS_USER, GDS_PASSWORD))

# === Limpa grafo anterior, se existir ===
try:
    gds.graph.drop("usuarios_similares", False)
except:
    print("⚠️ Grafo 'usuarios_similares' não existia ou já foi removido.")

# === Projeta um grafo entre usuários com base em filmes assistidos em comum ===
gds.run_cypher("""
CALL gds.graph.project.cypher(
  'usuarios_similares',
  'MATCH (u:Usuario) RETURN id(u) AS id',
  $relQuery
)
""", params={
    'relQuery': """
    MATCH (u1:Usuario)-[:ASSISTIU]->(f:Filme)<-[:ASSISTIU]-(u2:Usuario)
    WHERE u1 <> u2
    RETURN id(u1) AS source, id(u2) AS target
    """
})

# === Executa PageRank ===
pagerank_result = gds.pageRank.stream("usuarios_similares")
print("\n📊 PageRank (usuários mais influentes):")
for row in pagerank_result.limit(10).to_dataframe().itertuples():
    usuario = usuarios_collection.find_one({"_id": ObjectId(str(row.nodeId))}) if ObjectId.is_valid(str(row.nodeId)) else None
    if usuario:
        print(f"👤 {usuario['nome']}: {row.score:.4f}")

# === Executa Louvain (detecção de comunidades) ===
louvain_result = gds.louvain.stream("usuarios_similares")
print("\n🧩 Comunidades detectadas (Louvain):")
for row in louvain_result.limit(10).to_dataframe().itertuples():
    usuario = usuarios_collection.find_one({"_id": ObjectId(str(row.nodeId))}) if ObjectId.is_valid(str(row.nodeId)) else None
    if usuario:
        print(f"👥 {usuario['nome']} → Comunidade {row.communityId}")
