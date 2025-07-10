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

# Afficher le nombre de plats pour chaque catégorie
# Afficher le prix moyen des plats par catégorie.
# Afficher le nombre de commandes par client.
# Afficher les clients ayant passé plus d’une commande

# Plats Commandés Plus de Deux Fois
print("Tentative de connexion à la base de données et de récupération des plats commandés plus de deux fois...")
with engine.connect() as connection:
        query = text("""
            SELECT
                p.nom AS nom_plat,
                SUM(cp.quantite) AS total_quantite_commandee
            FROM
                commande_plats AS cp
            JOIN
                plats AS p ON cp.plat_id = p.id
            GROUP BY
                p.id, p.nom
            HAVING
                SUM(cp.quantite) > 2
            ORDER BY
                total_quantite_commandee DESC; -- Tri par quantité totale commandée décroissante
        """)

        df_plats_plus_deux_fois = pd.read_sql_query(query, connection)

        print("\nListe des plats commandés plus de deux fois avec leur total de quantités :")
        if not df_plats_plus_deux_fois.empty:
            print(df_plats_plus_deux_fois.to_string(index=False))
        else:
            print("Aucun plat n'a été commandé plus de deux fois.")


# Lister les commandes du mois de juillet 2025.
# Afficher la commande la plus récente (avec le nom du client).
# Afficher les clients ayant passé une commande d’un montant supérieur à 100.

# Plats Plus Chers que la Moyenne
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
            WHERE
                p.prix > (SELECT AVG(prix) FROM plats)
            ORDER BY
                p.prix DESC; -- Tri par prix décroissant
        """)

        df_plats_above_average = pd.read_sql_query(query, connection)

        print("\nListe des plats plus chers que la moyenne des plats :")
        if not df_plats_above_average.empty:
            print(df_plats_above_average.to_string(index=False))
        else:
            print("Aucun plat trouvé plus cher que la moyenne ou aucune donnée disponible.")

# Mettre à jour le prix du plat "Pizza Margherita" à 75
with engine.connect() as connection:
        update_query = text("""
            UPDATE plats
            SET prix = 75.00
            WHERE nom = 'Pizza Margherita';
        """)

        # Exécuter la requête de mise à jour
        result = connection.execute(update_query)

        print(f"\nLe prix du plat 'Pizza Margherita' a été mis à jour à 75.00. Nombre de lignes affectées : {result.rowcount}")

        # --- Optionnel : Vérifier la mise à jour en affichant le plat ---
        print("\nVérification du plat 'Pizza Margherita' après mise à jour :")
        select_query = text("""
            SELECT
                p.nom AS nom_plat,
                p.prix,
                p.description,
                c.nom AS nom_categorie
            FROM
                plats AS p
            JOIN
                categories AS c ON p.categorie_id = c.id
            WHERE
                p.nom = 'Pizza Margherita';
        """)
        df_pizza_margherita = pd.read_sql_query(select_query, connection)
        if not df_pizza_margherita.empty:
            print(df_pizza_margherita.to_string(index=False))
        else:
            print("Le plat 'Pizza Margherita' n'a pas été trouvé après la mise à jour.")

# Ajouter un nouveau plat dans la catégorie "Boisson"
with engine.connect() as connection:
        # ---  Trouver l'ID de la catégorie "Boisson" ---
        print("Recherche de l'ID de la catégorie 'Boisson'...")
        select_category_id_query = text("SELECT id FROM categories WHERE nom = 'Boisson';")
        category_id_result = connection.execute(select_category_id_query).scalar_one_or_none()

        if category_id_result is None:
            print("Erreur : La catégorie 'Boisson' n'a pas été trouvée dans la base de données. Veuillez la créer d'abord.")
        else:
            boisson_category_id = category_id_result
            print(f"ID de la catégorie 'Boisson' trouvé : {boisson_category_id}")

            # ---  Définition du nouveau plat à insérer ---
            nouveau_plat_nom = "Jus d'orange frais"
            nouveau_plat_prix = 20.00
            nouveau_plat_description = "Jus d'orange pressé fraîchement"

            # --- Requête SQL pour insérer le nouveau plat ---
            insert_plat_query = text("""
                INSERT INTO plats (nom, prix, description, categorie_id)
                VALUES (:nom, :prix, :description, :categorie_id)
                RETURNING id; -- Retourne l'ID du plat inséré
            """)

            # Exécuter la requête d'insertion
            with connection.begin():
                result = connection.execute(insert_plat_query, {
                    "nom": nouveau_plat_nom,
                    "prix": nouveau_plat_prix,
                    "description": nouveau_plat_description,
                    "categorie_id": boisson_category_id
                })
                new_plat_id = result.scalar_one() # Récupère l'ID du nouveau plat inséré
                print(f"\nLe plat '{nouveau_plat_nom}' a été ajouté avec succès (ID: {new_plat_id}).")

            # --- Optionnel : Vérifier l'insertion en affichant les plats de la catégorie "Boisson" ---
            print("\nVérification des plats dans la catégorie 'Boisson' après insertion :")
            select_boisson_plats_query = text("""
                SELECT
                    p.nom AS nom_plat,
                    p.prix,
                    p.description,
                    c.nom AS nom_categorie
                FROM
                    plats AS p
                JOIN
                    categories AS c ON p.categorie_id = c.id
                WHERE
                    c.nom = 'Boisson'
                ORDER BY
                    p.nom;
            """)
            df_boisson_plats = pd.read_sql_query(select_boisson_plats_query, connection)
            if not df_boisson_plats.empty:
                print(df_boisson_plats.to_string(index=False))
            else:
                print("Aucun plat trouvé dans la catégorie 'Boisson'.")

# Supprimer le client "Youssef El Khalfi" et toutes ses commandes associées
with engine.connect() as connection:
        with connection.begin():
            # --- Trouver l'ID du client "Youssef El Khalf" ---
            print(f"Recherche de l'ID du client '{"Youssef El Khalfi"}'...")
            select_client_id_query = text("SELECT id FROM clients WHERE nom = :client_name;")
            client_id_result = connection.execute(select_client_id_query, {"client_name": "Youssef El Khalfi"}).scalar_one_or_none()

            if client_id_result is None:
                print(f"Erreur : Le client '{"Youssef El Khalfi"}' n'a pas été trouvé dans la base de données. Aucune suppression effectuée.")
                # Rollback implicite à la sortie du bloc `with connection.begin()` si une exception est levée
            else:
                client_id = client_id_result
                print(f"ID du client '{"Youssef El Khalfi"}' trouvé : {client_id}")
                print(f"Suppression des détails de plats pour les commandes du client '{"Youssef El Khalfi"}'...")
                delete_commande_plats_query = text("""
                    DELETE FROM commande_plats
                    WHERE commande_id IN (SELECT id FROM commandes WHERE client_id = :client_id);
                """)
                cp_delete_result = connection.execute(delete_commande_plats_query, {"client_id": client_id})
                print(f"{cp_delete_result.rowcount} entrées dans 'commande_plats' supprimées.")


                # ---  Supprimer les commandes associées à ce client dans la table 'commandes' ---
                print(f"Suppression des commandes du client '{"Youssef El Khalfi"}'...")
                delete_commandes_query = text("""
                    DELETE FROM commandes
                    WHERE client_id = :client_id;
                """)
                commandes_delete_result = connection.execute(delete_commandes_query, {"client_id": client_id})
                print(f"{commandes_delete_result.rowcount} commandes supprimées pour le client '{"Youssef El Khalfi"}'.")

                # ---  Supprimer le client de la table 'clients' ---
                print(f"Suppression du client '{"Youssef El Khalfi"}'...")
                delete_client_query = text("""
                    DELETE FROM clients
                    WHERE id = :client_id;
                """)
                client_delete_result = connection.execute(delete_client_query, {"client_id": client_id})
                print(f"{client_delete_result.rowcount} client(s) supprimé(s).")

                print(f"\nLe client '{"Youssef El Khalfi"}' et toutes ses commandes associées ont été supprimés avec succès.")

        # --- Optionnel : Vérifier la suppression ---
        print("\nVérification de la présence du client et de ses commandes après suppression :")

        # Vérifier le client
        check_client_query = text("SELECT * FROM clients WHERE nom = :client_name;")
        df_client_check = pd.read_sql_query(check_client_query, connection, params={"client_name": "Youssef El Khalfi"})
        if df_client_check.empty:
            print(f"Le client '{"Youssef El Khalfi"}' n'est plus présent dans la table 'clients'.")
        else:
            print(f"Le client '{"Youssef El Khalfi"}' est toujours présent :")
            print(df_client_check.to_string(index=False))

        # Vérifier les commandes
        check_commandes_query = text("""
            SELECT co.id AS commande_id, cl.nom AS nom_client, co.date_commande, co.total
            FROM commandes AS co
            JOIN clients AS cl ON co.client_id = cl.id
            WHERE cl.nom = :client_name;
        """)
        df_commandes_check = pd.read_sql_query(check_commandes_query, connection, params={"client_name": "Youssef El Khalfi"})
        if df_commandes_check.empty:
            print(f"Aucune commande trouvée pour le client '{"Youssef El Khalfi"}'.")
        else:
            print(f"Des commandes sont toujours présentes pour le client '{"Youssef El Khalfi"}' :")
            print(df_commandes_check.to_string(index=False))

# Les 3 Plats les Plus Commandés
with engine.connect() as connection:
        query = text("""
            SELECT
                p.nom AS nom_plat,
                SUM(cp.quantite) AS total_quantite_commandee
            FROM
                plats AS p
            JOIN
                commande_plats AS cp ON p.id = cp.plat_id
            GROUP BY
                p.id, p.nom
            ORDER BY
                total_quantite_commandee DESC
            LIMIT 3;
        """)

        df_top_3_plats = pd.read_sql_query(query, connection)

        print("\nLes 3 plats les plus commandés (par quantité totale) :")
        if not df_top_3_plats.empty:
            print(df_top_3_plats.to_string(index=False))
        else:
            print("Aucun plat trouvé ou aucune commande enregistrée.")

# Lister les 3 plats les plus commandés (par quantité totale)
# Afficher les clients et leurs dernières commandes
with engine.connect() as connection:
        query = text("""
            WITH RankedOrders AS (
                SELECT
                    co.id AS commande_id,
                    co.client_id,
                    co.date_commande,
                    co.total,
                    ROW_NUMBER() OVER (PARTITION BY co.client_id ORDER BY co.date_commande DESC) as rn
                FROM
                    commandes AS co
            )
            SELECT
                cl.nom AS nom_client,
                ro.commande_id,
                ro.date_commande,
                ro.total
            FROM
                clients AS cl
            JOIN
                RankedOrders AS ro ON cl.id = ro.client_id
            WHERE
                ro.rn = 1
            ORDER BY
                cl.nom; -- Tri par nom de client pour une meilleure lisibilité
        """)

        df_clients_latest_orders = pd.read_sql_query(query, connection)

        print("\nClients et leurs dernières commandes :")
        if not df_clients_latest_orders.empty:
            print(df_clients_latest_orders.to_string(index=False)) 
            print("Aucune commande trouvée ou aucune donnée disponible.")

# Créer une vue virtuelle (select) qui affiche :
# le nom du client,
# les plats commandés,
# les quantités,
# et la date de la commande

# Définir le nom de la vue
VIEW_NAME = "vue_commandes_clients_plats"

with engine.connect() as connection:
        create_view_query = text(f"""
            CREATE OR REPLACE VIEW {VIEW_NAME} AS
            SELECT
                cl.nom AS nom_client,
                co.id AS commande_id,
                co.date_commande,
                p.nom AS nom_plat,
                cp.quantite
            FROM
                clients AS cl
            JOIN
                commandes AS co ON cl.id = co.client_id
            JOIN
                commande_plats AS cp ON co.id = cp.commande_id
            JOIN
                plats AS p ON cp.plat_id = p.id
            ORDER BY
                co.date_commande DESC, cl.nom, p.nom;
        """)

        print(f"Création ou remplacement de la vue '{VIEW_NAME}'...")
        connection.execute(create_view_query)
        print(f"Vue '{VIEW_NAME}' créée ou mise à jour avec succès.")

        print(f"\nInterrogation de la vue '{VIEW_NAME}' pour afficher les données :")
        select_from_view_query = text(f"SELECT * FROM {VIEW_NAME};")

        df_view_data = pd.read_sql_query(select_from_view_query, connection)

        if not df_view_data.empty:
            print(df_view_data.to_string(index=False)) 
        else:
            print("La vue ne contient aucune donnée.")
