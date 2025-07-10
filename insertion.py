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
metadata = MetaData()
clients = Table("clients", metadata, autoload_with=engine)
destinations = Table("destinations", metadata, autoload_with=engine)
categories = Table("categories", metadata, autoload_with=engine)
plats = Table("plats", metadata, autoload_with=engine)
commandes = Table("commandes", metadata, autoload_with=engine)
commande_plats = Table("commande_plats", metadata, autoload_with=engine)

categories_data = [
    {"id": 1, "nom": "Entrée"},
    {"id": 2, "nom": "Plat principal"},
    {"id": 3, "nom": "Dessert"},
    {"id": 4, "nom": "Boisson"},
]

plats_data = [
    {"id": 1, "nom": "Salade César", "prix": 45.00, "description": "Salade avec poulet", "categorie_id": 1},
    {"id": 2, "nom": "Soupe de légumes", "prix": 30.00, "description": "Soupe chaude de saison", "categorie_id": 1},
    {"id": 3, "nom": "Steak frites", "prix": 90.00, "description": "Viande grillée et frites", "categorie_id": 2},
    {"id": 4, "nom": "Pizza Margherita", "prix": 70.00, "description": "Pizza tomate & mozzarella", "categorie_id": 2},
    {"id": 5, "nom": "Tiramisu", "prix": 35.00, "description": "Dessert italien", "categorie_id": 3},
    {"id": 6, "nom": "Glace 2 boules", "prix": 25.00, "description": "Glace au choix", "categorie_id": 3},
    {"id": 7, "nom": "Coca-Cola", "prix": 15.00, "description": "Boisson gazeuse", "categorie_id": 4},
    {"id": 8, "nom": "Eau minérale", "prix": 10.00, "description": "Eau plate ou gazeuse", "categorie_id": 4},
]

clients_data = [
    {"id": 1, "nom": "Amine Lahmidi", "email": "amine@example.com"},
    {"id": 2, "nom": "Sara Benali", "email": "sara.b@example.com"},
    {"id": 3, "nom": "Youssef El Khalf", "email": "youssef.k@example.com"},
]

commandes_data = [
    {"id": 1, "client_id": 1, "date_commande": datetime(2025, 7, 7, 12, 30, 0), "total": 120.00},
    {"id": 2, "client_id": 2, "date_commande": datetime(2025, 7, 7, 13, 0, 0), "total": 85.00},
    {"id": 3, "client_id": 1, "date_commande": datetime(2025, 7, 8, 19, 45, 0), "total": 150.00},
]

commande_plats_data = [
    {"commande_id": 1, "plat_id": 1, "quantite": 1},
    {"commande_id": 1, "plat_id": 3, "quantite": 1},
    {"commande_id": 1, "plat_id": 7, "quantite": 2},
    {"commande_id": 2, "plat_id": 2, "quantite": 1},
    {"commande_id": 2, "plat_id": 4, "quantite": 1},
    {"commande_id": 2, "plat_id": 8, "quantite": 1},
    {"commande_id": 3, "plat_id": 3, "quantite": 1},
    {"commande_id": 3, "plat_id": 5, "quantite": 1},
    {"commande_id": 3, "plat_id": 7, "quantite": 1},
]

with engine.connect() as connection:
        # Start a transaction
        with connection.begin():
            
            connection.execute(insert(categories), categories_data)
            print("Categories data inserted.")

            connection.execute(insert(plats), plats_data)
            print("Plats data inserted.")

            connection.execute(insert(clients), clients_data)
            print("Clients data inserted.")

            connection.execute(insert(commandes), commandes_data)
            print("Commandes data inserted.")

            connection.execute(insert(commande_plats), commande_plats_data)
            print("Commande_plats data inserted.")

print("All data inserted successfully into PostgreSQL tables.")