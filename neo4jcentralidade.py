from pymongo import MongoClient
from neo4j import GraphDatabase
from bson import ObjectId

# === Conexão MongoDB com TLS via URI ===
mongo_client = MongoClient(
    "mongodb+srv://filipemiguel3m:ho28QZpfrItn9LMC@cluster0.j3k6e.mongodb.net/?retryWrites=true&w=majority&tls=true"
)
mongo_db = mongo_client["Cluster0"]
usuarios_collection = mongo_db["usuarios"]
filmes_collection = mongo_db["filmes_series"]
buscas_collection = mongo_db["buscas"]

# === Conexão Neo4j ===
uri = "bolt+s://c06feaf2.databases.neo4j.io"
user = "neo4j"
password = "E2yvIk15oJcELi0P7jdmxCIndyyYH3T9WGRx_04elXg"
driver = GraphDatabase.driver(uri, auth=(user, password))

# === Funções Neo4j ===
def criar_usuario(tx, usuario):
    tx.run("""
        MERGE (u:Usuario {id: $id})
        SET u.nome = $nome, u.email = $email
    """, id=str(usuario["_id"]), nome=usuario["nome"], email=usuario["email"])

def criar_filme(tx, filme):
    tx.run("""
        MERGE (f:Filme {id: $id})
        SET f.titulo = $titulo, f.ano = $ano
    """, id=str(filme["_id"]), titulo=filme["titulo"], ano=filme["ano_lancamento"])

def criar_relacao_assistiu(tx, usuario_id, filme_id):
    tx.run("""
        MATCH (u:Usuario {id: $usuario_id})
        MATCH (f:Filme {id: $filme_id})
        MERGE (u)-[:ASSISTIU]->(f)
    """, usuario_id=str(usuario_id), filme_id=str(filme_id))

def criar_relacao_buscou(tx, usuario_id, genero):
    tx.run("""
        MATCH (u:Usuario {id: $usuario_id})
        MERGE (g:Genero {nome: $genero})
        MERGE (u)-[:BUSCOU]->(g)
    """, usuario_id=str(usuario_id), genero=genero)

# === Inserção de dados no grafo ===
with driver.session() as session:
    for usuario in usuarios_collection.find():
        session.execute_write(criar_usuario, usuario)
        for item in usuario.get("historico_visualizacao", []):
            filme_id = item.get("filme_id")
            if filme_id:
                session.execute_write(criar_relacao_assistiu, usuario["_id"], filme_id)

    for filme in filmes_collection.find():
        session.execute_write(criar_filme, filme)

    for busca in buscas_collection.find():
        usuario_id = busca["usuario_id"]
        genero = busca.get("filtros", {}).get("genero")
        if genero:
            session.execute_write(criar_relacao_buscou, usuario_id, genero)

# === Execução dos algoritmos de análise de grafos ===
with driver.session() as session:
    # Remove grafo anterior se existir
    try:
        session.run("CALL gds.graph.drop('streaming_graph', false) YIELD graphName")
    except Exception as e:
        print("⚠️ Nenhum grafo anterior ou erro ao dropar:", e)

    # Criar grafo em memória
    session.run("""
        CALL gds.graph.project(
            'streaming_graph',
            ['Usuario', 'Filme'],
            {
                ASSISTIU: {
                    type: 'ASSISTIU',
                    orientation: 'UNDIRECTED'
                }
            }
        )
    """)

    # Centralidade espacial (grau)
    result = session.run("""
        CALL gds.degree.stream('streaming_graph')
        YIELD nodeId, score AS centrality
        RETURN gds.util.asNode(nodeId).id AS nodeId, centrality
        ORDER BY centrality DESC
        LIMIT 5
    """)
    resultados_centralidade = result.data()

    # Centralidade espectral (PageRank)
    result = session.run("""
        CALL gds.pageRank.stream('streaming_graph')
        YIELD nodeId, score
        RETURN gds.util.asNode(nodeId).id AS nodeId, score
        ORDER BY score DESC
        LIMIT 5
    """)
    resultados_influencia = result.data()

# === Mostrar resultados ===
print("\n=== Filmes mais centrais (assistidos) ===")
for record in resultados_centralidade:
    node_id = record["nodeId"]
    filme = filmes_collection.find_one({"_id": ObjectId(node_id)}) if ObjectId.is_valid(node_id) else None
    if filme:
        print(f"🎬 Filme: {filme['titulo']} | Centralidade: {record['centrality']:.2f}")

print("\n=== Usuários mais influentes (PageRank) ===")
for record in resultados_influencia:
    node_id = record["nodeId"]
    usuario = usuarios_collection.find_one({"_id": ObjectId(node_id)}) if ObjectId.is_valid(node_id) else None
    if usuario:
        print(f"👤 Usuário: {usuario['nome']} | Influência: {record['score']:.2f}")

# === Encerrar conexão Neo4j ===
driver.close()
