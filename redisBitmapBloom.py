import redis
from pymongo import MongoClient
from datetime import timedelta
import json
from bson import ObjectId

# --- Configurações --- #
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
MONGO_URI = "mongodb://filipemiguel3m:ho28QZpfrItn9LMC@cluster0-shard-00-00.j3k6e.mongodb.net:27017,cluster0-shard-00-01.j3k6e.mongodb.net:27017,cluster0-shard-00-02.j3k6e.mongodb.net:27017/?replicaSet=atlas-my90w1-shard-0&ssl=true&authSource=admin&retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "Cluster0"

# --- Conexões --- #
def conectar_banco():
    try:
        mongo_client = MongoClient(MONGO_URI)
        db = mongo_client[DB_NAME]
        db.command('ping')
        
        redis_client = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True,
            socket_connect_timeout=3
        )
        redis_client.ping()
        
        print("✅ Bancos conectados!")
        return db, redis_client
    except Exception as e:
        print(f"❌ Erro de conexão: {e}")
        return None, None

# --- Bitmap (Visualizações de Filmes) --- #
def marcar_visualizacao(usuario_id, filme_id, redis_client):
    BITMAP_KEY = f"viewed:{usuario_id}"
    hash_id = hash(filme_id) % 1000000  # Garantir tamanho razoável
    redis_client.setbit(BITMAP_KEY, hash_id, 1)

def checar_visualizacao(usuario_id, filme_id, redis_client):
    BITMAP_KEY = f"viewed:{usuario_id}"
    hash_id = hash(filme_id) % 1000000
    return redis_client.getbit(BITMAP_KEY, hash_id) == 1

# --- Bloom Filter (Evitar Busca Desnecessária) --- #
def adicionar_filme_bloom(filme_id, redis_client):
    BLOOM_KEY = "bloom:filmes"
    redis_client.bf().add(BLOOM_KEY, filme_id)

def verificar_filme_bloom(filme_id, redis_client):
    BLOOM_KEY = "bloom:filmes"
    return redis_client.bf().exists(BLOOM_KEY, filme_id)

# --- Uso na Aplicação --- #
if __name__ == "__main__":
    db, redis_client = conectar_banco()
    
    if db is not None and redis_client is not None:
        try:
            usuario = "usr_1"
            filme = "flm_9"
            
            # Bitmap - Marcar e verificar visualização
            print("\n=== BITMAP (Visualizações) ===")
            marcar_visualizacao(usuario, filme, redis_client)
            print(f"Usuário {usuario} já assistiu {filme}?", checar_visualizacao(usuario, filme, redis_client))
            
            # Bloom Filter - Adicionar e verificar existência
            print("\n=== BLOOM FILTER (Filmes Cadastrados) ===")
            adicionar_filme_bloom(filme, redis_client)
            print(f"O filme {filme} já está no sistema?", verificar_filme_bloom(filme, redis_client))
            
        finally:
            redis_client.close()
            db.client.close()
