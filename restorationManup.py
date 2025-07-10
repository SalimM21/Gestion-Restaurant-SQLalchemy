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

# Afficher les plats avec leur nom de catégorie
with engine.connect() as connection:
        query = text("""
            SELECT
                p.nom AS nom_plat,
                p.prix,
                p.description,
                c.nom AS nom_categorie
            FROM
                plats AS p
            JOIN
                categories AS c ON p.categorie_id = c.id
            ORDER BY
                p.nom; -- Tri par nom de plat pour une meilleure lisibilité
        """)

        # --- 4. Exécution de la requête et chargement des résultats dans un DataFrame pandas ---
        df_plats_categories = pd.read_sql_query(query, connection)

        print("\nListe des plats avec leur nom de catégorie :")
        if not df_plats_categories.empty:
            print(df_plats_categories.to_string(index=False))
        else:
            print("Aucun plat trouvé.")

# Lister les commandes avec le nom du client et la date
print("Tentative de connexion à la base de données et de récupération des commandes avec le nom du client et la date...")
with engine.connect() as connection:
        query = text("""
            SELECT
                co.id AS commande_id,
                cl.nom AS nom_client,
                co.date_commande,
                co.total
            FROM
                commandes AS co
            JOIN
                clients AS cl ON co.client_id = cl.id
            ORDER BY
                co.date_commande DESC; -- Tri par date de commande décroissante
        """)

        df_commandes_clients = pd.read_sql_query(query, connection)

        print("\nListe des commandes avec le nom du client et la date :")
        if not df_commandes_clients.empty:
            print(df_commandes_clients.to_string(index=False)) 
        else:
            print("Aucune commande trouvée.")

# Pour chaque commande, afficher les plats commandés avec leur quantité
with engine.connect() as connection:
        query = text("""
            SELECT
                co.id AS commande_id,
                cl.nom AS nom_client,
                co.date_commande,
                p.nom AS nom_plat,
                cp.quantite,
                p.prix AS prix_unitaire,
                (cp.quantite * p.prix) AS sous_total_plat
            FROM
                commandes AS co
            JOIN
                clients AS cl ON co.client_id = cl.id
            JOIN
                commande_plats AS cp ON co.id = cp.commande_id
            JOIN
                plats AS p ON cp.plat_id = p.id
            ORDER BY
                co.id, p.nom; -- Tri par ID de commande, puis par nom de plat
        """)
        df_commande_details = pd.read_sql_query(query, connection)

        print("\nDétails de chaque commande (plats commandés et leur quantité) :")
        if not df_commande_details.empty:
            print(df_commande_details.to_string(index=False))
        else:
            print("Aucun détail de commande trouvé.")

# Afficher le nombre de plats pour chaque catégorie
with engine.connect() as connection:
        query = text("""
            SELECT
                c.nom AS nom_categorie,
                AVG(p.prix) AS prix_moyen
            FROM
                categories AS c
            JOIN
                plats AS p ON c.id = p.categorie_id
            GROUP BY
                c.nom
            ORDER BY
                prix_moyen DESC; -- Tri par prix moyen décroissant
        """)
        df_prix_moyen_par_categorie = pd.read_sql_query(query, connection)

        print("\nPrix moyen des plats pour chaque catégorie :")
        if not df_prix_moyen_par_categorie.empty:
            print(df_prix_moyen_par_categorie.to_string(index=False))
        else:
            print("Aucun plat ou catégorie trouvé pour calculer le prix moyen.")

# le prix moyen des plats par catégorie
# Afficher le nombre de commandes par client
with engine.connect() as connection:
        query = text("""
            SELECT
                cl.nom AS nom_client,
                COUNT(co.id) AS nombre_de_commandes
            FROM
                clients AS cl
            JOIN
                commandes AS co ON cl.id = co.client_id
            GROUP BY
                cl.nom
            HAVING
                COUNT(co.id) > 1
            ORDER BY
                nombre_de_commandes DESC; -- Tri par nombre de commandes décroissant
        """)

        df_clients_plus_une_commande = pd.read_sql_query(query, connection)

        print("\nClients ayant passé plus d'une commande :")
        if not df_clients_plus_une_commande.empty:
            print(df_clients_plus_une_commande.to_string(index=False)) 
        else:
            print("Aucun client n'a passé plus d'une commande.")
