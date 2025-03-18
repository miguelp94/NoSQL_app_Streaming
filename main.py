from fastapi import FastAPI, HTTPException
from pymongo import MongoClient
from pydantic import BaseModel
from datetime import datetime, timezone
from typing import List, Optional
from dotenv import load_dotenv
import os

# Carrega as variáveis de ambiente
load_dotenv()

# Configuração do FastAPI
app = FastAPI()

# Conexão com o MongoDB
connection_string = os.getenv("MONGODB_URI")
client = MongoClient(connection_string)
db = client.Cluster0  # Substitua "meu_banco" pelo nome do seu banco de dados

# Coleções
usuarios_collection = db.usuarios
filmes_series_collection = db.filmes_series

# Modelos Pydantic
class Usuario(BaseModel):
    nome: str
    email: str
    senha_hash: str
    preferencias: List[str] = []
    historico_visualizacao: List[dict] = []

class FilmeSerie(BaseModel):
    titulo: str
    tipo: str
    generos: List[str]
    ano_lancamento: int
    duracao_minutos: int
    classificacao_indicativa: str
    avaliacao_media: float
    disponibilidade: List[str]
    elenco: List[dict]

# Endpoint para a raiz (/)
@app.get("/")
def read_root():
    return {"message": "Bem-vindo à API de usuários e filmes!"}

# Endpoint para cadastrar usuário
@app.post("/usuarios/", response_model=Usuario)
def cadastrar_usuario(usuario: Usuario):
    # Verifica se o e-mail já está cadastrado
    if usuarios_collection.find_one({"email": usuario.email}):
        raise HTTPException(status_code=400, detail="E-mail já cadastrado")

    # Converte o modelo Pydantic para dicionário
    usuario_dict = usuario.model_dump()

    # Adiciona a data de cadastro
    usuario_dict["data_cadastro"] = datetime.now(timezone.utc)

    # Insere o usuário no banco de dados
    usuarios_collection.insert_one(usuario_dict)

    return usuario

# Endpoint para buscar filmes
@app.get("/filmes/", response_model=List[FilmeSerie])
def buscar_filmes(titulo: Optional[str] = None, genero: Optional[str] = None):
    query = {}
    if titulo:
        query["titulo"] = {"$regex": titulo, "$options": "i"}  # Busca case-insensitive
    if genero:
        query["generos"] = genero

    filmes = list(filmes_series_collection.find(query, {"_id": 0}))
    return filmes