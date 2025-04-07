from neo4j import GraphDatabase
from pymongo import MongoClient
from bson import ObjectId

# === CONFIGURAÇÕES ===

# Substitua pelos seus dados do Neo4j Aura
NEO4J_URI = "neo4j+s://c06feaf2.databases.neo4j.io"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "E2yvIk15oJcELi0P7jdmxCIndyyYH3T9WGRx_04elXg"

# MongoDB local (ou URI se estiver na nuvem)
mongo_client = MongoClient("mongodb://filipemiguel3m:ho28QZpfrItn9LMC@cluster0-shard-00-00.j3k6e.mongodb.net:27017,cluster0-shard-00-01.j3k6e.mongodb.net:27017,cluster0-shard-00-02.j3k6e.mongodb.net:27017/?replicaSet=atlas-my90w1-shard-0&ssl=true&authSource=admin&retryWrites=true&w=majority&appName=Cluster0")
mongo_db = mongo_client["Cluster0"]
usuarios = mongo_db["usuarios"]
filmes_series = mongo_db["filmes_series"]
buscas = mongo_db["buscas"]

# === CONEXÃO COM NEO4J ===

neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

# === FUNÇÕES PARA CRIAR NÓS E RELACIONAMENTOS ===

def criar_usuario(tx, usuario):
    tx.run("""
        MERGE (u:Usuario {id: $id})
        SET u.nome = $nome, u.email = $email
    """, id=str(usuario["_id"]), nome=usuario["nome"], email=usuario["email"])

    for genero in usuario.get("preferencias", []):
        tx.run("""
            MERGE (g:Genero {nome: $genero})
            MERGE (u:Usuario {id: $id})-[:GOSTA_DE]->(g)
        """, id=str(usuario["_id"]), genero=genero)

    for viz in usuario.get("historico_visualizacao", []):
        filme_id = str(viz["filme_id"])
        tx.run("""
            MERGE (f:Filme {id: $filme_id})
            MERGE (u:Usuario {id: $usuario_id})-[:ASSISTIU]->(f)
        """, usuario_id=str(usuario["_id"]), filme_id=filme_id)

def criar_filme(tx, filme):
    tx.run("""
        MERGE (f:Filme {id: $id})
        SET f.titulo = $titulo, f.tipo = $tipo,
            f.ano_lancamento = $ano_lancamento,
            f.duracao = $duracao,
            f.classificacao = $classificacao,
            f.avaliacao = $avaliacao
    """, id=str(filme["_id"]), titulo=filme["titulo"], tipo=filme["tipo"],
         ano_lancamento=filme["ano_lancamento"], duracao=filme["duracao_minutos"],
         classificacao=filme["classificacao_indicativa"], avaliacao=filme["avaliacao_media"])

    for genero in filme.get("generos", []):
        tx.run("""
            MERGE (g:Genero {nome: $genero})
            MERGE (f:Filme {id: $id})-[:TEM_GENERO]->(g)
        """, genero=genero, id=str(filme["_id"]))

def criar_busca(tx, busca):
    tx.run("""
        MERGE (u:Usuario {id: $usuario_id})
        CREATE (b:Busca {
            id: $id,
            termo: $termo,
            data: datetime($data)
        })
        MERGE (u)-[:REALIZOU]->(b)
    """, id=str(busca["_id"]),
         usuario_id=str(busca["usuario_id"]),
         termo=busca["termo_busca"],
         data=busca["data_busca"])

    filtros = busca.get("filtros", {})
    if "genero" in filtros:
        tx.run("""
            MERGE (g:Genero {nome: $genero})
            MERGE (b:Busca {id: $busca_id})-[:FILTRA_POR_GENERO]->(g)
        """, genero=filtros["genero"], busca_id=str(busca["_id"]))

# === EXECUÇÃO ===

with neo4j_driver.session() as session:
    # Teste de conexão
    resultado = session.run("RETURN 'Conexão com Neo4j Aura funcionando!' AS msg")
    print(resultado.single()["msg"])

    for usuario in usuarios.find():
        session.execute_write(criar_usuario, usuario)

    for filme in filmes_series.find():
        session.execute_write(criar_filme, filme)

    for busca in buscas.find():
        session.execute_write(criar_busca, busca)

print("✅ Dados importados com sucesso!")

neo4j_driver.close()
