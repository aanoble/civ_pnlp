from datetime import datetime

import polars as pl
from metabase import Metabase
from openhexa.sdk import current_run
from openhexa.toolbox.dhis2 import DHIS2

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


def check_server_health(dhis2: DHIS2) -> bool:
    """Check if the DHIS2 server is responding.

    Parameters
    ----------
    dhis2 : DHIS2
        The DHIS2 instance to check.

    Returns
    -------
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


def check_metabase_server_health(metabase: Metabase) -> bool:
    """Check if the DHIS2 server is responding.

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


def get_date_report(date_report: datetime) -> list:
    """Transforme une date en format de rapport français avec logique trimestrielle.

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


def process_dates_from_df(df: pl.DataFrame) -> pl.DataFrame:
    """Prépare un DataFrame avec des champs temporels normalisés.

    Args:
        df: DataFrame Polars contenant au minimum les colonnes
            "date_report" (Datetime) et "period" (str).

    Returns:
        DataFrame avec les colonnes suivantes:
        - date_report: date d'origine
        - period: période libellée (ex: "MARS 2024")
        - date_order: entier AAAAMM (Int32) pour le tri chronologique
        - annee: année extraite de "date_report"

    Notes:
        - "date_report" doit être de type Datetime.
        - "date_order" facilite l'ordonnancement chronologique dans les rapports.
    """
    if df.is_empty():
        return df.with_columns(
            [
                pl.col("date_report").cast(pl.Datetime),
                pl.col("period").cast(pl.Utf8),
                pl.lit(None).cast(pl.Int32).alias("date_order"),
                pl.lit(None).cast(pl.Int32).alias("annee"),
            ]
        )
    return df.select(
        pl.col("date_report"),
        pl.col("period"),
        (pl.col("date_report").dt.year() * 100 + pl.col("date_report").dt.month())
        .cast(pl.Int32)
        .alias("date_order"),
        pl.col("date_report").dt.year().alias("annee").cast(pl.Int32),
    )
