from datetime import datetime

from metabase import Metabase
from openhexa.sdk import current_run

FRENCH_MONTHS = [
    "",
    "JANVIER",
    "FEVRIER",
    "MARS",
    "AVRIL",
    "MAI",
    "JUIN",
    "JUILLET",
    "AOUT",
    "SEPTEMBRE",
    "OCTOBRE",
    "NOVEMBRE",
    "DECEMBRE",
]

QUARTER_MONTHS = {3, 6, 9, 12}


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


def get_date_report(date_report: datetime) -> list:
    """
    Transforme une date en format de rapport français avec logique trimestrielle.

    Args:
        date_report: Objet datetime représentant la date du rapport.

    Returns:
        Liste de chaînes représentant la/les périodes de rapport en français :
        - Si la date est en fin de trimestre (mois 3, 6, 9, 12) retourne
          [<mois précédent> <mois courant année>, <mois courant année>].
        - Sinon retourne ["('<mois courant année')"].
    """
    month, year = date_report.month, date_report.year
    current_period = f"{FRENCH_MONTHS[month]} {year}"

    if month in QUARTER_MONTHS:
        prev_month = (month - 2) % 12 or 12  # Gestion du cycle annuel
        return [f"{FRENCH_MONTHS[prev_month]} {current_period}", current_period]
    return [f"{current_period}"]


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
