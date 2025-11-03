import hashlib
import pickle
import sqlite3
import time
from urllib.parse import urlparse

import pandas as pd
import requests
from openhexa.sdk import CustomConnection


class MetabaseError(Exception):
    """Custom exception for Metabase-related errors."""

    pass


class SQLiteCache:
    """Gestion du cache persistant avec SQLite."""

    def __init__(self, db_path: str = "metabase_cache.db"):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Initialise la base de données et la table de cache."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS cache (
                key TEXT PRIMARY KEY,
                timestamp REAL NOT NULL,
                data BLOB NOT NULL,
                ttl INTEGER NOT NULL
            )
            """)
            conn.commit()

    def get(self, key: str) -> tuple[float, pd.DataFrame] | None:
        """Récupère une entrée du cache si elle existe et est valide."""  # noqa: DOC201
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
            SELECT timestamp, data, ttl 
            FROM cache 
            WHERE key = ?
            """,
                (key,),
            )
            row = cursor.fetchone()

            if not row:
                return None

            timestamp, data_blob, ttl = row
            current_time = time.time()

            # Vérifie si le cache est expiré
            if current_time - timestamp > ttl:
                self.delete(key)
                return None

            # Désérialise les données
            try:
                df = pickle.loads(data_blob)
                return (timestamp, df)
            except pickle.UnpicklingError:
                self.delete(key)
                return None

    def set(self, key: str, timestamp: float, df: pd.DataFrame, ttl: int):
        """Stocke une entrée dans le cache."""
        data_blob = pickle.dumps(df)

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
            INSERT OR REPLACE INTO cache (key, timestamp, data, ttl)
            VALUES (?, ?, ?, ?)
            """,
                (key, timestamp, data_blob, ttl),
            )
            conn.commit()

    def delete(self, key: str):
        """Supprime une entrée du cache."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM cache WHERE key = ?", (key,))
            conn.commit()

    def clear(self):
        """Vide complètement le cache."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM cache")
            conn.commit()

    def remove_expired(self):
        """Supprime toutes les entrées expirées du cache."""
        current_time = time.time()
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
            DELETE FROM cache 
            WHERE ? - timestamp > ttl
            """,
                (current_time,),
            )
            conn.commit()


class Metabase:
    """Client Metabase avec gestion de cache persistant via SQLite.

    Cette classe permet d'exécuter des requêtes SQL sur Metabase avec pagination,
    gestion du cache, et récupération des résultats sous forme de DataFrame.

    Attributes
    ----------
    api : Api
        Instance de l'API Metabase pour l'authentification et les requêtes.
    default_cache_ttl : int
        Durée de vie par défaut du cache en secondes.
    cache : SQLiteCache
        Instance du cache persistant basé sur SQLite.

    Methods
    -------
    get_data_from_sql_query(...)
        Exécute une requête SQL paginée avec gestion du cache.
    clear_cache()
        Vide complètement le cache.
    remove_expired_cache_entries()
        Supprime les entrées expirées du cache.
    """

    def __init__(
        self,
        connection: CustomConnection,
        default_cache_ttl: int = 10000,
        cache_db_path: str = "metabase_cache.db",
    ):
        """Initialise le client Metabase avec un système de cache persistant SQLite.

        Args:
            connection: Configuration de connexion Metabase
            default_cache_ttl: Durée de vie du cache en secondes (2 heures par défaut)
            cache_db_path: Chemin vers la base de données SQLite
        """
        self.api = Api(connection)
        self.default_cache_ttl = default_cache_ttl
        self.cache = SQLiteCache(cache_db_path)

    def get_data_from_sql_query(
        self,
        sql_query: str,
        database_id: int = 3,
        chunk_size: int = 2000,
        use_cache: bool = True,
        cache_ttl: int | None = None,
    ) -> pd.DataFrame:
        """Exécute une requête SQL sur Metabase avec pagination et cache persistant.

        Args:
            sql_query: Requête SQL avec {limit} et {offset}
            database_id: ID de la base Metabase
            chunk_size: Nombre de lignes par requête
            use_cache: Utiliser le cache pour les requêtes identiques
            cache_ttl: Surcharge du TTL par requête (en secondes)

        Returns:
            DataFrame combinant tous les résultats

        Raises:
            ValueError en cas d'erreur
        """
        try:
            # Préparation de la requête
            sql_query = self._prepare_sql_query(sql_query)

            # Vérification du cache
            cache_key = self._generate_cache_key(sql_query, database_id)
            current_time = time.time()

            if use_cache:
                if cached := self.cache.get(cache_key):
                    _timestamp, cached_df = cached
                    return cached_df.copy()

            # Exécution sans cache ou cache expiré
            data_frames = []
            offset = 0
            names = None

            while True:
                df, names = self._fetch_chunk(sql_query, database_id, chunk_size, offset, names)
                if df.empty:
                    break
                data_frames.append(df)
                offset += len(df)
                if len(df) < chunk_size:
                    break

            result_df = pd.concat(data_frames, ignore_index=True) if data_frames else pd.DataFrame()

            # Mise en cache
            if use_cache:
                ttl = cache_ttl if cache_ttl is not None else self.default_cache_ttl
                self.cache.set(cache_key, current_time, result_df, ttl)

            return result_df

        except Exception as e:
            raise ValueError(f"Erreur lors de la récupération des données: {e}") from e

    def clear_cache(self):
        """Vide complètement le cache."""
        self.cache.clear()

    def remove_expired_cache_entries(self):
        """Supprime automatiquement les entrées de cache expirées."""
        self.cache.remove_expired()

    def _generate_cache_key(self, sql_query: str, database_id: int) -> str:
        """Génère une clé de cache unique pour la requête."""  # noqa: DOC201
        # Normalisation de la requête
        normalized_sql = " ".join(sql_query.strip().split()).lower()

        # Création d'un hash unique
        hash_object = hashlib.sha256()
        hash_object.update(normalized_sql.encode("utf-8"))
        hash_object.update(str(database_id).encode("utf-8"))
        return hash_object.hexdigest()

    def _prepare_sql_query(self, sql_query: str) -> str:
        """Valide et formate la requête SQL avec les paramètres de pagination."""  # noqa: DOC201
        sql_query = sql_query.rstrip(";")
        required_params = {"{limit}", "{offset}"}

        if not required_params.issubset(sql_query):
            if "LIMIT" not in sql_query.upper():
                sql_query += "\nLIMIT {limit}"
            if "OFFSET" not in sql_query.upper():
                sql_query += "\nOFFSET {offset}"

        if not all(param in sql_query for param in required_params):
            raise ValueError("La requête SQL doit contenir les paramètres {limit} et {offset}")

        return sql_query

    def _fetch_chunk(
        self, sql_query: str, database_id: int, chunk_size: int, offset: int, names: list | None
    ) -> tuple[pd.DataFrame, list]:
        """Récupère un segment de données et gère les métadonnées."""  # noqa: DOC201
        try:
            response = self.api.session.post(
                f"{self.api.url}/dataset",
                headers={"Content-Type": "application/json", "X-Metabase-Session": self.api.token},
                json={
                    "database": database_id,
                    "type": "native",
                    "native": {"query": sql_query.format(limit=chunk_size, offset=offset)},
                },
                timeout=15,
            )
            response.raise_for_status()
            data = response.json()["data"]

            # Extraction des noms de colonnes
            if names is None:
                names = [col["display_name"] for col in data["results_metadata"]["columns"]]

            df = pd.DataFrame(data["rows"])
            if not df.empty:
                df.columns = names

            return df, names

        except requests.exceptions.RequestException as e:
            raise ValueError(f"Erreur réseau: {e}") from e
        except (KeyError, TypeError) as e:
            raise ValueError(f"Structure de réponse invalide: {e}") from e


class Api:
    """Client API pour l'authentification et la gestion des sessions Metabase.

    Attributes
    ----------
    url : str
        URL de l'API Metabase.
    token : str
        Jeton d'authentification Metabase.
    session : requests.Session
        Session HTTP authentifiée.

    Methods
    -------
    authenticate(username: str, password: str) -> requests.Session
        Authentifie l'utilisateur et retourne une session HTTP.
    parse_url(url: str) -> str
        Formate l'URL de l'API Metabase.
    """

    def __init__(self, connection: CustomConnection):
        self._validate_connection(connection)
        self.url = self.parse_url(connection.url)
        self.token = None
        self.session = self.authenticate(connection.username, connection.password)

    @staticmethod
    def _validate_connection(connection: CustomConnection):
        """Valide les paramètres de connexion."""
        if not connection:
            raise MetabaseError("Connexion requise")
        if not all([connection.url, connection.username, connection.password]):
            raise MetabaseError("URL, utilisateur et mot de passe requis")

    @staticmethod
    def parse_url(url: str) -> str:
        """Formate l'URL de l'API Metabase."""  # noqa: DOC201
        parsed = urlparse(url)
        if not parsed.scheme or not parsed.netloc:
            raise MetabaseError(f"URL invalide: {url}")
        return f"{parsed.scheme}://{parsed.netloc}/api"

    def authenticate(self, username: str, password: str) -> requests.Session:
        """Authentification avec gestion robuste des erreurs."""  # noqa: DOC201
        session = requests.Session()
        try:
            response = session.post(
                f"{self.url}/session",
                headers={"Content-Type": "application/json"},
                json={"username": username, "password": password},
                timeout=15,
            )
            response.raise_for_status()
            if not (token := response.json().get("id")):
                raise MetabaseError("Token absent de la réponse")
            self.token = token
            return session
        except requests.exceptions.RequestException as e:
            raise MetabaseError(f"Erreur réseau: {e}") from e
        except requests.JSONDecodeError as e:
            raise MetabaseError("Réponse d'authentification invalide") from e
        except KeyError as e:
            raise MetabaseError("Structure de réponse d'authentification invalide") from e
