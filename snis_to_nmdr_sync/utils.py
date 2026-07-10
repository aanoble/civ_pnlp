from datetime import datetime

from openhexa.sdk import current_run
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.periods import Day, Month, Quarter, SixMonth, Week, Year

# DHIS2 periodType -> Period class of the toolbox
PERIOD_TYPE_TO_CLASS = {
    "Daily": Day,
    "Weekly": Week,
    "Monthly": Month,
    "Quarterly": Quarter,
    "SixMonthly": SixMonth,
    "Yearly": Year,
}

# Coarseness ranking used to decide whether an aggregation (fine -> coarse) is possible.
PERIOD_GRANULARITY = {
    "Daily": 0,
    "Weekly": 1,
    "Monthly": 2,
    "Quarterly": 3,
    "SixMonthly": 4,
    "Yearly": 5,
}


def check_server_health(dhis2: DHIS2) -> bool:
    """
    Check if the DHIS2 server is responding.

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


def last_analytics_update(dhis2: DHIS2) -> datetime | None:
    """
    Get the last update date of the analytics tables.

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
    try:
        dtime_str = dhis2.meta.system_info().get("lastAnalyticsTableSuccess")
        return datetime.fromisoformat(dtime_str) if dtime_str else None
    except Exception as e:
        current_run.log_error(f"Error retrieving last analytics update: {e!s}")
        return None


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


def validate_dataset(dhis2: DHIS2, dataset_id: str) -> bool:
    """
    Validate the existence of a dataset in the DHIS2 instance.

    Parameters
    ----------
    dhis2 : DHIS2
        The DHIS2 instance to check.
    dataset_id : str
        The ID of the dataset to validate.

    Returns
    -------
    bool
        True if the dataset exists, False otherwise.
    """
    try:
        dhis2.api.get(endpoint=f"dataSets/{dataset_id}", use_cache=False)
        return True
    except Exception as e:
        current_run.log_error(
            f"❌ Dataset {dataset_id} not found in DHIS2 at {dhis2.api.url}: {e!s}"
        )
        return False


def validate_aoc_exists(dhis2: DHIS2, aoc_id: str) -> bool:
    """
    Validate that an attributeOptionCombo exists in the target instance.

    Parameters
    ----------
    dhis2 : DHIS2
        The DHIS2 instance to check (the target instance).
    aoc_id : str
        The categoryOptionCombo id used as attributeOptionCombo.

    Returns
    -------
    bool
        True if the AOC exists, False otherwise.
    """
    try:
        dhis2.api.get(endpoint=f"categoryOptionCombos/{aoc_id}", use_cache=False)
        return True
    except Exception as e:
        current_run.log_error(
            f"❌ attributeOptionCombo `{aoc_id}` introuvable dans la cible {dhis2.api.url}: {e!s}"
        )
        return False


def get_data_element_cocs(dhis2: DHIS2, data_element_ids: list[str]) -> dict[str, set[str]]:
    """
    Return, for each data element, the set of valid categoryOptionCombo ids.

    The COC set is resolved through the data element ``categoryCombo`` so that we compare
    the *real* disaggregation combos of each instance (and not the raw id strings).

    Parameters
    ----------
    dhis2 : DHIS2
        The DHIS2 instance to query.
    data_element_ids : list[str]
        Data element ids to resolve.

    Returns
    -------
    dict[str, set[str]]
        Mapping ``data_element_id -> {categoryOptionCombo_id, ...}``.
    """
    result: dict[str, set[str]] = {}
    if not data_element_ids:
        return result

    chunk_size = 100
    for index in range(0, len(data_element_ids), chunk_size):
        chunk = data_element_ids[index : index + chunk_size]
        response = dhis2.api.get(
            endpoint="dataElements",
            params={
                "paging": "false",
                "fields": "id,categoryCombo[categoryOptionCombos[id]]",
                "filter": f"id:in:[{','.join(chunk)}]",
            },
            use_cache=False,
        )
        for de in response.get("dataElements", []):
            cocs = de.get("categoryCombo", {}).get("categoryOptionCombos", [])
            result[de["id"]] = {coc["id"] for coc in cocs if coc.get("id")}
    return result


def convert_period_id(period: str, source_type: str, target_type: str) -> str | None:
    """
    Convert a DHIS2 period id from a source period type to a target period type.

    Only aggregation from a finer to a coarser period type is supported (e.g. Monthly ->
    Quarterly). Identity conversion is returned unchanged. Any unsupported direction
    (coarse -> fine, or unknown type) returns ``None``.

    Parameters
    ----------
    period : str
        The source period id (e.g. ``"202401"``).
    source_type : str
        The source DHIS2 periodType (e.g. ``"Monthly"``).
    target_type : str
        The target DHIS2 periodType (e.g. ``"Quarterly"``).

    Returns
    -------
    str | None
        The converted period id, or ``None`` if the conversion is not supported.
    """
    if source_type == target_type:
        return period

    source_cls = PERIOD_TYPE_TO_CLASS.get(source_type)
    target_cls = PERIOD_TYPE_TO_CLASS.get(target_type)
    if source_cls is None or target_cls is None:
        return None

    # Cannot split a coarser period into finer ones.
    if PERIOD_GRANULARITY.get(source_type, -1) >= PERIOD_GRANULARITY.get(target_type, 99):
        return None

    try:
        source_dt = source_cls.from_string(period).start
        return str(target_cls.from_date(source_dt))
    except Exception:
        return None
