from datetime import datetime

from openhexa.sdk import current_run
from openhexa.toolbox.dhis2 import DHIS2


def check_server_health(dhis2: DHIS2) -> bool:
    """Check if the DHIS2 server is responding.

    Parameters
    ----------
    dhis2 : DHIS2
        The DHIS2 instance to check.

    Returns:
        bool: True if the server is responding, raises ConnectionError otherwise.
    """
    try:
        dhis2.ping()  # type: ignore
        current_run.log_info(f"✅ Serveur DHIS2 {dhis2.api.url} accessible")
        return True
    except ConnectionError as err:
        current_run.log_error(f"❌ Impossible d'atteindre l'instance DHIS2 à {dhis2.api.url}")
        raise ConnectionError(
            f"Impossible d'atteindre l'instance DHIS2 à l'URL {dhis2.api.url}"
        ) from err


def last_analytics_update(dhis2: DHIS2) -> datetime | None:
    """Get the last update date of the analytics tables.

    Parameters
    ----------
    dhis2 : DHIS2
        The DHIS2 instance to check.

    Returns
    -------
    datetime | None
        The last update date of the analytics tables. Returns None if the analytics tables have
        never been updated.
    """
    dtime_str = dhis2.meta.system_info().get("lastAnalyticsTableSuccess")
    return datetime.fromisoformat(dtime_str) if dtime_str else None


def parse_cutoff_date(date_str: str) -> datetime:
    """Valide et convertit une date ISO en objet datetime.

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
