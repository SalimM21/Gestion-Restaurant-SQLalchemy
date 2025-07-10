from sqlalchemy import Numeric, Text, create_engine, Table, Column, Integer, String, Float, DateTime, ForeignKey, MetaData
from dotenv import load_dotenv
from sqlalchemy import insert
from datetime import datetime
import urllib.parse
import os

# Charger les variables d'environnement
load_dotenv()

# Récupérer les informations de connexion
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = urllib.parse.quote_plus(os.getenv("DB_PASSWORD"))
DB_NAME = os.getenv("DB_NAME")

# Créer la chaîne de connexion et l'engine
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

# Créer un objet MetaData
metadata = MetaData()

# --- 4. Définition des Tables ---
categories = Table(
    "categories",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True), 
    Column("nom", String(50), nullable=False, unique=True)      
)

plats = Table(
    "plats",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True), 
    Column("nom", String(100), nullable=False),                  
    Column("prix", Numeric(10, 2), nullable=False),              
    Column("description", Text, nullable=True),                  
    Column("categorie_id", Integer, ForeignKey("categories.id"), nullable=False) 
)

clients = Table(
    "clients",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True), 
    Column("nom", String(100), nullable=False),                  
    Column("email", String(100), unique=True, nullable=True)     
)


commandes = Table(
    "commandes",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True), 
    Column("client_id", Integer, ForeignKey("clients.id"), nullable=False),
    Column("date_commande", DateTime, nullable=False, default=datetime.now), # Date et heure de la commande, non nulle, par défaut l'heure actuelle
    Column("total", Numeric(10, 2), nullable=False)              
)

commande_plats = Table(
    "commande_plats",
    metadata,
    Column("commande_id", Integer, ForeignKey("commandes.id"), primary_key=True), 
    Column("plat_id", Integer, ForeignKey("plats.id"), primary_key=True),         
    Column("quantite", Integer, nullable=False, default=1)                        
)
