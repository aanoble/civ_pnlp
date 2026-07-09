from urllib.parse import urlparse

import pandas as pd
import requests
from openhexa.sdk import CustomConnection, current_run


class MetabaseError(Exception):
    """Custom exception class for handling Metabase-specific errors."""

    pass


class Metabase:
    """
    A class to interact with Metabase API and execute SQL queries.

    This class provides functionality to connect to Metabase, execute SQL queries
    with automatic pagination, and retrieve data as pandas DataFrames.
    """

    def __init__(self, connection: CustomConnection):
        self.api = Api(connection)

    def get_data_from_sql_query(
        self, sql_query: str, database_id: int = 3, chunk_size: int = 2000
    ) -> pd.DataFrame:
        """
        Exécute une requête SQL sur Metabase avec pagination automatique.

        Args:
            sql_query: Requête SQL avec {limit} et {offset} comme paramètres de pagination
            database_id: ID de la base Metabase
            chunk_size: Nombre de lignes par requête (2000 par défaut)

        Returns:
            DataFrame combinant tous les résultats

        Raises:
            ValueError en cas d'erreur
        """
        try:
            sql_query = self._prepare_sql_query(sql_query)
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

            return pd.concat(data_frames, ignore_index=True) if data_frames else pd.DataFrame()
        except Exception as e:
            raise ValueError(f"Erreur lors de la récupération des données: {e}") from e

    def _prepare_sql_query(self, sql_query: str) -> str:
        """
        Valide et formate la requête SQL avec les paramètres de pagination.

        Args:
            sql_query: La requête SQL à préparer

        Returns:
            str: La requête SQL formatée avec les paramètres de pagination
        """
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
                # timeout=15
            )
            response.raise_for_status()
            data = response.json()["data"]

            # Extraction des noms de colonnes
            if names is None:
                current_run.log_debug(
                    f"Fetching column names from Metabase response: {[col['display_name'] for col in data['results_metadata']['columns']]}"  # noqa: E501
                )
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
    """
    A class to handle Metabase API authentication and session management.

    This class manages the connection to Metabase, validates connection parameters,
    and maintains an authenticated session for API requests.
    """

    def __init__(self, connection: CustomConnection):
        self._validate_connection(connection)
        self.url = self.parse_url(connection.url)  # type: ignore
        self.token = None
        self.session = self.authenticate(connection.username, connection.password)  # type: ignore

    @staticmethod
    def _validate_connection(connection: CustomConnection):
        """Valide les paramètres de connexion."""
        if not connection:
            raise MetabaseError("Connexion requise")
        if not all([connection.url, connection.username, connection.password]):  # type: ignore
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
        except requests.JSONDecodeError as e:
            raise MetabaseError("Réponse d'authentification invalide") from e

    def ping(self):
        """Vérifie la connectivité avec le serveur Metabase."""
        try:
            response = self.session.get(
                f"{self.url}/health",
                timeout=10,
            )
            response.raise_for_status()
            if response.status_code != 200:
                raise MetabaseError("Serveur Metabase inaccessible")
        except requests.RequestException as e:
            raise MetabaseError(f"Erreur de connectivité Metabase: {e}") from e
