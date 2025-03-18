from pymongo import MongoClient

# Conecta ao MongoDB
client = MongoClient("mongodb://filipemiguel3m:ho28QZpfrItn9LMC@cluster0-shard-00-00.j3k6e.mongodb.net:27017,cluster0-shard-00-01.j3k6e.mongodb.net:27017,cluster0-shard-00-02.j3k6e.mongodb.net:27017/?replicaSet=atlas-my90w1-shard-0&ssl=true&authSource=admin&retryWrites=true&w=majority&appName=Cluster0")
db = client.Cluster0

# Cria índices
db.usuarios.create_index([("email", 1)])  # Índice simples
db.filmes_series.create_index([("titulo", 1), ("ano_lancamento", -1)])  # Índice composto

# 1. Consulta de Usuário por E-mail
print("Consulta de Usuário por E-mail:")
usuario = db.usuarios.find_one({ "email": "joao.silva@example.com" })
print(usuario)  # Exibe o resultado da consulta

# 2. Busca de Filmes por Título e Ano
print("\nBusca de Filmes por Título e Ano:")
filmes = db.filmes_series.find({
    "titulo": "Interestelar",
    "ano_lancamento": { "$gte": 2010, "$lte": 2020 }
}).sort("ano_lancamento", -1)

for filme in filmes:
    print(filme)  # Exibe cada filme encontrado

# 3. Aggregation Pipeline
print("\nAggregation Pipeline:")
pipeline = [
    { "$match": { "historico_visualizacao.filme_id": "flm_1" } },
    { "$unwind": "$historico_visualizacao" },
    { "$sort": { "historico_visualizacao.data_visualizacao": -1 } }
]
result = db.usuarios.aggregate(pipeline)

for doc in result:
    print(doc)  # Exibe cada documento resultante do pipeline