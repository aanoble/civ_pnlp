from datetime import datetime

from metabase import Metabase
from openhexa.sdk import current_run


def parse_cutoff_date(date_str: str) -> datetime:
    """
    Valide et convertit une date ISO en objet datetime.

    Args:
        date_str: Chaîne de date au format YYYY-MM-DD

    Returns:
        Objet datetime correspondant

    Raises:
        ValueError: Format de date invalide
    """
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except (ValueError, TypeError) as e:
        current_run.log_error(f"Format de date invalide: '{date_str}' - {e!s}")
        raise ValueError(f"Format de date invalide: '{date_str}'. Requis: YYYY-MM-DD") from e


def check_metabase_server_health(metabase: Metabase) -> bool:
    """
    Check if the DHIS2 server is responding.

    Parameters
    ----------
    metabase : Metabase
        The Metabase instance to check.

    Returns
    -------
        bool: True if the server is responding, raises ConnectionError otherwise.
    """
    try:
        metabase.api.ping()  # type: ignore
        current_run.log_info(f"✅ Serveur Metabase {metabase.api.url} accessible")
        return True
    except Exception as err:
        current_run.log_error(f"❌ Impossible d'atteindre l'instance Metabase à {metabase.api.url}")
        raise ConnectionError(
            f"Impossible d'atteindre l'instance Metabase à l'URL {metabase.api.url}"
        ) from err
