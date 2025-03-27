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
        
        # Testa conexão com MongoDB
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

# --- Função Principal --- #
def buscar_filme_com_cache(filme_id, redis_client, db):
    """
    Busca filme no cache (Redis), se não achar, consulta MongoDB e armazena no cache.
    """
    if redis_client is None or db is None:
        raise ValueError("Conexões com bancos não estabelecidas")
    
    CACHE_KEY = f"filme:{filme_id}"
    
    try:
        # 1. Tentativa de obter do cache
        filme_cache = redis_client.get(CACHE_KEY)
        
        if filme_cache:
            print("🟢 Cache hit - Dados do Redis")
            return json.loads(filme_cache)
        
        print("🔴 Cache miss - Consultando MongoDB...")
        
        # 2. Consulta ao MongoDB
        # Verifica se o ID está no formato correto
        try:
            filme_id_obj = ObjectId(filme_id) if len(filme_id) == 24 else filme_id
        except:
            filme_id_obj = filme_id
            
        filme = db.filmes_series.find_one({"_id": filme_id_obj})
        
        if not filme:
            print("⚠️ Filme não encontrado no MongoDB")
            return None
        
        # Converte ObjectId para string e prepara para cache
        filme['_id'] = str(filme['_id'])
        filme_json = json.dumps(filme, default=str)
        
        # 3. Armazena no Redis por 1 hora (3600 segundos)
        redis_client.setex(
            CACHE_KEY,
            timedelta(seconds=3600),
            filme_json
        )
        
        return filme
        
    except Exception as e:
        print(f"⚠️ Erro durante operação: {e}")
        return None

# --- Uso na Aplicação --- #
if __name__ == "__main__":
    # Conecta aos bancos
    db, redis_client = conectar_banco()
    
    # Verificação correta das conexões
    if db is not None and redis_client is not None:
        try:
            # Teste com um ID que existe no seu banco
            print("\nBusca por filme existente:")
            filme_id_valido = "flm_9"  # Substitua por um ID válido do seu banco
            filme = buscar_filme_com_cache(filme_id_valido, redis_client, db)
            print("Resultado:", filme)
            
            # Teste com ID inválido
            print("\nBusca por filme inexistente:")
            filme = buscar_filme_com_cache("id_inexistente", redis_client, db)
            print("Resultado:", filme)
            
        finally:
            # Fecha conexões
            redis_client.close()
            db.client.close()
    else:
        print("❌ Não foi possível iniciar a aplicação devido a erros de conexão")