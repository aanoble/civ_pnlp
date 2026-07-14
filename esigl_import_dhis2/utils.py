"""Helpers du pipeline d'import eSIGL → DHIS2 (dates, périodes, santé Metabase)."""

from datetime import datetime

from dateutil.relativedelta import relativedelta
from metabase import Metabase
from openhexa.sdk import current_run


def parse_cutoff_date(date_str: str) -> datetime:
    """Valide et convertit une date ISO en objet datetime.

    Parameters
    ----------
    date_str : str
        Chaîne de date au format ``YYYY-MM-DD``.

    Returns
    -------
    datetime
        Objet datetime correspondant.

    Raises
    ------
    ValueError
        Si le format de date est invalide.
    """
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except (ValueError, TypeError) as e:
        current_run.log_error(f"Format de date invalide: '{date_str}' - {e!s}")
        raise ValueError(f"Format de date invalide: '{date_str}'. Requis: YYYY-MM-DD") from e


def compute_extraction_window(
    start_date: str | None,
    end_date: str | None,
    months_back: int,
    cmm_lookback: int = 3,
) -> tuple[datetime, datetime, datetime]:
    """Calcule les bornes d'extraction et de publication à partir des paramètres.

    - Fenêtre de **publication** ``[pub_start, end]`` : périodes réellement poussées vers DHIS2.
    - Fenêtre d'**extraction** ``[cmm_start, end]`` : élargie de ``cmm_lookback`` mois en amont
      pour que la moyenne glissante (CMM GTC) du premier mois publié soit correcte.

    Parameters
    ----------
    start_date : str | None
        Date de début (``YYYY-MM-DD``). Par défaut : 1er jour du mois courant.
    end_date : str | None
        Date de fin (``YYYY-MM-DD``). Par défaut : fin du mois de ``start_date``.
    months_back : int
        Nombre de mois d'historique à republier avant ``start_date``.
    cmm_lookback : int, optional
        Mois supplémentaires extraits pour le calcul de la CMM (défaut 3).

    Returns
    -------
    tuple[datetime, datetime, datetime]
        ``(cmm_start, pub_start, end)``, toutes normalisées (début/fin de mois).
    """
    start_dt = parse_cutoff_date(start_date) if start_date else datetime.now()
    start_dt = start_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    end_dt = parse_cutoff_date(end_date) if end_date else start_dt
    end_dt = (end_dt + relativedelta(day=31)).replace(hour=23, minute=59, second=59)

    pub_start = start_dt - relativedelta(months=months_back) if months_back else start_dt
    cmm_start = pub_start - relativedelta(months=cmm_lookback)
    return cmm_start, pub_start, end_dt


def check_metabase_server_health(metabase: Metabase) -> bool:
    """Vérifie que le serveur Metabase répond.

    Parameters
    ----------
    metabase : Metabase
        Instance Metabase à interroger.

    Returns
    -------
    bool
        ``True`` si le serveur répond.

    Raises
    ------
    ConnectionError
        Si le serveur est injoignable.
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
