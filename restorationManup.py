from turtle import pd
from sqlalchemy import Numeric, Text, create_engine, Table, Column, Integer, String, Float, DateTime, ForeignKey, MetaData, text
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

# Lister tous les plats triés par prix décroissant

with engine.connect() as connection:

    query = text("""
        SELECT
            p.nom AS nom_plat,
            p.prix,
            p.description,
            c.nom AS categorie
        FROM
            plats AS p
        JOIN
            categories AS c ON p.categorie_id = c.id
        WHERE
            p.prix BETWEEN 30 AND 80
        ORDER BY
            p.prix DESC;
        """)

    df_plats_filtres = pd.read_sql_query(query, connection)
    if not df_plats_filtres.empty:
        print(df_plats_filtres.to_string(index=False)) # to_string(index=False) pour un affichage propre sans l'index pandas
    else:
        print("Aucun plat trouvé dans cette fourchette de prix.")

# Afficher les clients dont le nom commence par "S"
with engine.connect() as connection:
        query = text("""
            SELECT
                id,
                nom,
                email
            FROM
                clients
            WHERE
                nom LIKE 'S%';
        """)

        # --- Exécution de la requête et chargement des résultats dans un DataFrame pandas ---
        df_clients_filtres = pd.read_sql_query(query, connection)

        print("\nListe des clients dont le nom commence par 'S' :")
        if not df_clients_filtres.empty:
            print(df_clients_filtres.to_string(index=False))
        else:
            print("Aucun client trouvé dont le nom commence par 'S'.")

