from pymongo import MongoClient
from datetime import datetime

# String de conexão (substitua com a sua)
connection_string = "mongodb+srv://filipemiguel3m:ho28QZpfrItn9LMC@cluster0.j3k6e.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Conectar ao MongoDB
client = MongoClient(connection_string)

# Selecionar o banco de dados (substitua "nome_do_banco" pelo nome do seu banco)
db = client.Cluster0

# Inserir documentos na coleção "usuarios"
usuarios_collection = db.usuarios
documento_usuario = {
    "_id": "usr_12345",
    "nome": "Miguel Araujo",
    "email": "miguel.araujo@ufu.br",
    "senha_hash": "hash_da_senha",
    "data_cadastro": datetime.strptime("2025-02-20T14:30:00Z", "%Y-%m-%dT%H:%M:%SZ"),
    "preferencias": ["Ação", "Ficção Científica"],
    "historico_visualizacao": [
        {
            "filme_id": "flm_98765",
            "data_visualizacao": datetime.strptime("2025-02-18T20:15:00Z", "%Y-%m-%dT%H:%M:%SZ"),
            "progresso": 85
        }
    ]
}
usuarios_collection.insert_one(documento_usuario)

# Inserir documentos na coleção "filmes_series"
filmes_series_collection = db.filmes_series
documento_filme = {
    "_id": "flm_98765",
    "titulo": "O Senhor dos Anéis: A Sociedade do Anel",
    "tipo": "Filme",
    "generos": ["Aventura", "Fantasia"],
    "ano_lancamento": 2001,
    "duracao_minutos": 178,
    "classificacao_indicativa": "12",
    "avaliacao_media": 4.8,
    "disponibilidade": ["Brasil", "Portugal"],
    "elenco": [
        { "ator": "Elijah Wood", "personagem": "Frodo" },
        { "ator": "Ian McKellen", "personagem": "Gandalf" }
    ]
}
filmes_series_collection.insert_one(documento_filme)

# Inserir documentos na coleção "buscas"
buscas_collection = db.buscas
documento_busca = {
    "_id": "bsq_56789",
    "usuario_id": "usr_12345",
    "termo_busca": "Senhor dos Anéis",
    "filtros": {
        "genero": "Fantasia",
        "ano_min": 2000,
        "ano_max": 2010
    },
    "data_busca": datetime.strptime("2025-02-20T14:45:00Z", "%Y-%m-%dT%H:%M:%SZ")
}
buscas_collection.insert_one(documento_busca)

print("Documentos inseridos com sucesso!")