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
    """Conecta aos bancos MongoDB e Redis"""
    try:
        # MongoDB
        mongo_client = MongoClient(MONGO_URI)
        db = mongo_client[DB_NAME]
        db.command('ping')
        
        # Redis
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

# --- Key-Value (Cache de Filmes) --- #
def cache_filme(filme_id, redis_client, db):
    """Implementação Key-Value para cache de filmes"""
    CACHE_KEY = f"filme:{filme_id}"
    
    # GET
    filme_cache = redis_client.get(CACHE_KEY)
    if filme_cache:
        print("🟢 Cache hit - Dados do Redis")
        return json.loads(filme_cache)
    
    print("🔴 Cache miss - Consultando MongoDB...")
    
    # SET após consulta ao MongoDB
    filme_id_obj = ObjectId(filme_id) if len(filme_id) == 24 else filme_id
    filme = db.filmes_series.find_one({"_id": filme_id_obj})
    
    if filme:
        filme['_id'] = str(filme['_id'])
        redis_client.setex(
            CACHE_KEY,
            timedelta(seconds=3600),
            json.dumps(filme, default=str)
        )
    return filme

# --- Listas (Histórico de Acesso) --- #
def adicionar_historico(usuario_id, filme_id, redis_client):
    """Implementação com Listas para histórico de acesso"""
    HISTORICO_KEY = f"historico:{usuario_id}"
    
    # RPUSH - Adiciona no final da lista
    redis_client.rpush(HISTORICO_KEY, filme_id)
    
    # Mantém apenas os 10 mais recentes
    redis_client.ltrim(HISTORICO_KEY, -10, -1)

def obter_historico(usuario_id, redis_client):
    """LRANGE - Recupera toda a lista"""
    HISTORICO_KEY = f"historico:{usuario_id}"
    return redis_client.lrange(HISTORICO_KEY, 0, -1)

# --- Sets (Tags de Filmes) --- #
def adicionar_tags(filme_id, tags, redis_client):
    """SADD - Adiciona tags únicas"""
    TAGS_KEY = f"tags:{filme_id}"
    redis_client.sadd(TAGS_KEY, *tags)

def obter_tags(filme_id, redis_client):
    """SMEMBERS - Recupera todas as tags"""
    TAGS_KEY = f"tags:{filme_id}"
    return redis_client.smembers(TAGS_KEY)

# --- Uso na Aplicação --- #
if __name__ == "__main__":
    db, redis_client = conectar_banco()
    
    if db is not None and redis_client is not None:
        try:
            # 1. Exemplo Key-Value
            print("\n=== KEY-VALUE (Cache de Filmes) ===")
            filme = cache_filme("flm_9", redis_client, db)
            print("Filme:", filme)
            
            # 2. Exemplo Listas
            print("\n=== LISTAS (Histórico) ===")
            adicionar_historico("usr_1", "flm_9", redis_client)
            adicionar_historico("usr_1", "flm_5", redis_client)
            print("Histórico:", obter_historico("usr_1", redis_client))
            
            # 3. Exemplo Sets
            print("\n=== SETS (Tags) ===")
            adicionar_tags("flm_9", ["ação", "ficção", "aventura"], redis_client)
            print("Tags:", obter_tags("flm_9", redis_client))
            
        finally:
            redis_client.close()
            db.client.close()
