"""Pipeline de synchronisation des dataValues de l'instance source vers l'instance cible.

Voir `PLAN_AMELIORATION.md` pour le détail des choix de conception.
"""

import json
import time
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
from constants import DATASET_IDS
from dateutil.relativedelta import relativedelta
from openhexa.sdk import (
    DHIS2Connection,
    current_run,
    parameter,
    pipeline,
    workspace,
)
from openhexa.sdk.pipelines.parameter import DHIS2Widget
from openhexa.toolbox.dhis2 import DHIS2
from utils import (
    check_server_health,
    convert_period_id,
    get_data_element_cocs,
    last_analytics_update,
    parse_cutoff_date,
    validate_aoc_exists,
    validate_dataset,
)


@pipeline("snis_to_dedop_sync", timeout=43200)
@parameter(
    "source_connection",
    type=DHIS2Connection,  # type: ignore
    name="DHIS2 Connection for source",
    help="DHIS2 connection to fetch source data from.",
    default="snis-dhis2",
    required=True,
)
@parameter(
    "target_connection",
    type=DHIS2Connection,  # type: ignore
    name="DHIS2 Connection for target",
    help="DHIS2 connection to push target data to.",
    default="dhis2-nmdr-temp",
    required=True,
)
@parameter(
    "dataset_id",
    type=str,
    widget=DHIS2Widget.DATASETS,
    connection="target_connection",  # type: ignore
    name="Dataset ID in target",
    required=False,
    multiple=True,
)
@parameter(
    "org_unit_id",
    type=str,
    widget=DHIS2Widget.ORG_UNITS,
    connection="target_connection",  # type: ignore
    name="Organisation Unit ID in target",
    help="Optional post-filter on organisation units (subset of the extraction root).",
    required=False,
    multiple=True,
)
@parameter(
    "extraction_root_org_unit",
    type=str,  # type: ignore
    name="Extraction root org unit",
    widget=DHIS2Widget.ORG_UNITS,
    connection="target_connection",  # type: ignore
    help=(
        "Root organisation unit used for extraction (children included). Extracting from a "
        "high-level root is faster than unit-by-unit; a post-filter is applied afterwards."
    ),
    default="ZD44Asc0bAk",
    required=True,
)
@parameter(
    code="start_date",
    type=str,  # type: ignore
    name="Start date (YYYY-MM-DD)",
    help="Start date for DHIS2 extraction (default today).",
    required=False,
)
@parameter(
    code="end_date",
    type=str,  # type: ignore
    name="End date (YYYY-MM-DD)",
    help="End date for the extraction (default last day of start date month).",
    required=False,
)
@parameter(
    "months_back",
    type=int,  # type: ignore
    name="Historical period in months to refresh",
    help="Number of months to look back from current month (only when start date is empty).",
    default=24,
    required=False,
)
@parameter(
    "last_updated",
    type=str,  # type: ignore
    name="Last updated date (YYYY-MM-DD)",
    help="Only fetch records updated since this date (manual backfill).",
    required=False,
)
@parameter(
    "output_directory",
    type=str,  # type: ignore
    name="Output directory",
    help="Directory to save the output files.",
    default="sync SNIS DEDOP/data/output",
    required=True,
)
@parameter(
    "target_aoc",
    type=str,  # type: ignore
    name="Target instance attribute option combo",
    help="Target instance attributeOptionCombo applied to every value.",
    default="HllvX50cXC0",
    required=True,
)
@parameter(
    "create_missing_metadata",
    type=bool,  # type: ignore
    name="Create missing disaggregation metadata in target",
    help=(
        "If enabled, category options / COCs present in the source but missing in the target "
        "are created in the target (UIDs preserved). If disabled, affected values are skipped."
    ),
    default=False,
    required=False,
)
@parameter(
    "sync_orgunit_deletions",
    type=bool,  # type: ignore
    name="Allow org unit deletions from target datasets",
    help=(
        "If enabled, org units present in the target but absent from the source are "
        "unassigned (destructive operation)."
    ),
    default=False,
    required=False,
)
@parameter(
    "use_cache",
    type=bool,  # type: ignore
    name="Use API source cache",
    help="Whether to use cached API responses where possible.",
    default=False,
    required=False,
)
@parameter(
    "automate_sync",
    type=bool,  # type: ignore
    name="Automate synchronization",
    help="Daily incremental mode: fetch records updated today (lastUpdated = today).",
    default=False,
    required=False,
)
@parameter(
    "dry_run",
    type=bool,  # type: ignore
    name="Dry run",
    help="Simulate the import (and metadata creation) without writing to target.",
    default=False,
    required=False,
)
@parameter(
    "import_mode",
    type=str,  # type: ignore
    name="Import strategy",
    help="DHIS2 import strategy for upserts (CREATE, UPDATE, CREATE_AND_UPDATE).",
    default="CREATE_AND_UPDATE",
    required=False,
)
@parameter(
    "post_batch_size",
    type=int,  # type: ignore
    name="Post batch size",
    help="Chunk size for DHIS2 POST requests.",
    default=5000,
    required=False,
)
@parameter(
    "retention_days",
    type=int,  # type: ignore
    name="Report retention (days)",
    help="Number of days of import reports to keep.",
    default=30,
    required=False,
)
def snis_to_dedop_sync(
    source_connection: DHIS2Connection,
    target_connection: DHIS2Connection,
    dataset_id: list[str] | None,
    org_unit_id: list[str] | None,
    extraction_root_org_unit: str,
    start_date: str | None,
    end_date: str | None,
    months_back: int,
    last_updated: str | None,
    output_directory: str,
    target_aoc: str,
    create_missing_metadata: bool,
    sync_orgunit_deletions: bool,
    automate_sync: bool,
    dry_run: bool = False,
    use_cache: bool = False,
    import_mode: str = "CREATE_AND_UPDATE",
    post_batch_size: int = 5000,
    retention_days: int = 30,
):
    """Synchronize data values from source DHIS2 to target DHIS2.

    Parameters
    ----------
    source_connection : DHIS2Connection
        DHIS2 connection to fetch source data from.
    target_connection : DHIS2Connection
        DHIS2 connection to push target data to.
    dataset_id : list[str] | None
        Dataset IDs to synchronize (defaults to the configured DATASET_IDS).
    org_unit_id : list[str] | None
        Optional post-filter on organisation units.
    extraction_root_org_unit : str
        Root organisation unit for extraction (children included).
    start_date, end_date : str | None
        Extraction window bounds (YYYY-MM-DD).
    months_back : int
        Months to look back when start_date is empty.
    last_updated : str | None
        Manual backfill cutoff (YYYY-MM-DD).
    output_directory : str
        Directory for the output reports.
    target_aoc : str
        Target attributeOptionCombo applied to every value.
    create_missing_metadata : bool
        Whether to create missing disaggregation metadata in target.
    sync_orgunit_deletions : bool
        Whether to allow destructive org unit unassignment in target.
    automate_sync : bool
        Daily incremental mode (lastUpdated = today).
    dry_run : bool
        Simulate without writing.
    use_cache : bool
        Use cached source API responses where possible.
    import_mode : str
        DHIS2 import strategy for upserts.
    post_batch_size : int
        Chunk size for POST requests.
    retention_days : int
        Report retention in days.
    """
    source = (
        DHIS2(
            connection=source_connection, cache_dir=Path(workspace.files_path, "source", ".cache")
        )
        if use_cache
        else DHIS2(connection=source_connection)
    )
    target = DHIS2(connection=target_connection)

    check_server_health(source)
    check_server_health(target)

    if not validate_aoc_exists(target, target_aoc):
        raise ValueError(
            f"L'attributeOptionCombo cible `{target_aoc}` est introuvable dans l'instance cible."
        )

    if last_update_source := last_analytics_update(source):
        current_run.log_info(
            f"Dernière mise à jour des tables analytiques source: "
            f"{last_update_source.strftime('%Y-%m-%d %H:%M:%S')}"
        )
    if last_update_target := last_analytics_update(target):
        current_run.log_info(
            f"Dernière mise à jour des tables analytiques target: "
            f"{last_update_target.strftime('%Y-%m-%d %H:%M:%S')}"
        )

    periods_range = process_periods(
        start_date=start_date, end_date=end_date, months_back=months_back
    )
    last_updated_dt = parse_cutoff_date(last_updated) if last_updated else None

    dataset_ids = list(dataset_id) if dataset_id else DATASET_IDS
    org_unit_ids = list(org_unit_id) if org_unit_id else None

    failed_datasets: list[str] = []

    for current_dataset_id in dataset_ids:
        current_run.log_info(f"Traitement du dataset `{current_dataset_id}`")

        valid_source = validate_dataset(source, current_dataset_id)
        valid_target = validate_dataset(target, current_dataset_id)
        if not (valid_source and valid_target):
            failed_datasets.append(current_dataset_id)
            continue

        try:
            sync_dataset_orgunits(
                source=source,
                target=target,
                dataset_id=current_dataset_id,
                org_unit_ids=org_unit_ids,
                allow_deletions=sync_orgunit_deletions,
                dry_run=dry_run,
            )

            metadata_report = ensure_disaggregation_metadata(
                source=source,
                target=target,
                dataset_id=current_dataset_id,
                create_missing_metadata=create_missing_metadata,
                dry_run=dry_run,
            )

            data_source = fetch_dhis2_data(
                source=source,
                dataset_id=current_dataset_id,
                org_unit_ids=org_unit_ids,
                extraction_root=extraction_root_org_unit,
                periods_range=periods_range,
                last_updated=last_updated_dt,
                automate_sync=automate_sync,
                use_cache=use_cache,
                metadata_report=metadata_report,
            )

            data_source = convert_periods(
                source=source, target=target, dataset_id=current_dataset_id, df=data_source
            )

            prepared = prepare_data_for_dhis2(df=data_source, target_aoc=target_aoc)

            summary = push_data_to_dhis2(
                dhis2=target,
                prepared=prepared,
                dataset_id=current_dataset_id,
                dry_run=dry_run,
                import_mode=import_mode,
                post_batch_size=post_batch_size,
            )

            write = write_import_report(
                (Path(output_directory) / current_dataset_id),
                prepared,
                summary,
                metadata_report,
            )
            cleanup_old_directory_files(
                (Path(output_directory) / current_dataset_id), write, retention_days
            )

            raise_on_push_failure(summary, current_dataset_id)

        except Exception as err:
            current_run.log_error(f"Échec du traitement du dataset `{current_dataset_id}`: {err!s}")
            failed_datasets.append(current_dataset_id)

    if failed_datasets:
        raise RuntimeError(
            f"Synchronisation terminée avec des erreurs sur les datasets: "
            f"{', '.join(sorted(set(failed_datasets)))}"
        )


@snis_to_dedop_sync.task
def process_periods(
    start_date: str | None,
    end_date: str | None,
    months_back: int,
) -> list[datetime]:
    """Compute the extraction window [start, end].

    ``months_back`` is only applied when ``start_date`` is empty. An explicit ``end_date`` is
    respected as-is; otherwise the end defaults to the last day of the current month.

    Parameters
    ----------
    start_date : str | None
        Start date (YYYY-MM-DD).
    end_date : str | None
        End date (YYYY-MM-DD).
    months_back : int
        Months to look back when start_date is empty.

    Returns
    -------
    list[datetime]
        ``[start, end]`` (or ``[start]`` when both are equal).
    """
    current_run.log_info("Traitement des périodes d'extraction")

    now = datetime.now()
    start_dt = parse_cutoff_date(start_date) if start_date else now
    if not start_date:
        if months_back:
            start_dt = (now - relativedelta(months=months_back)).replace(day=1)
        current_run.log_info(f"Date de début absente, utilisation: {start_dt.strftime('%Y-%m-%d')}")

    end_dt = parse_cutoff_date(end_date) if end_date else (now + relativedelta(day=31))
    if not end_date:
        current_run.log_info(f"Date de fin absente, utilisation: {end_dt.strftime('%Y-%m-%d')}")

    if start_dt > end_dt:
        current_run.log_error(
            f"Incohérence temporelle: début {start_dt.strftime('%Y-%m-%d')} "
            f"postérieur à fin {end_dt.strftime('%Y-%m-%d')}"
        )
        raise ValueError("La date de début doit être antérieure ou égale à la date de fin.")

    if start_dt == end_dt:
        return [start_dt]
    return [start_dt, end_dt]


@snis_to_dedop_sync.task
def sync_dataset_orgunits(
    source: DHIS2,
    target: DHIS2,
    dataset_id: str,
    org_unit_ids: list[str] | None,
    allow_deletions: bool,
    dry_run: bool,
) -> None:
    """Synchronize the dataset organisation unit assignments between source and target.

    Org unit *existence* in the target is guaranteed upstream by the daily org unit sync pipeline;
    this task only reconciles the dataset<->orgUnit assignment. Deletions are destructive and
    gated behind ``allow_deletions`` and ``dry_run``.

    Parameters
    ----------
    source : DHIS2
        source client.
    target : DHIS2
        target client.
    dataset_id : str
        Dataset identifier.
    org_unit_ids : list[str] | None
        Optional restriction to specific org units.
    allow_deletions : bool
        Whether to unassign org units present in target but absent from source.
    dry_run : bool
        Simulate without writing.
    """
    dataset_units_source = source.api.get(
        endpoint=f"dataSets/{dataset_id}?fields=organisationUnits[id]", use_cache=False
    )
    existing_ids_source = {ou["id"] for ou in dataset_units_source.get("organisationUnits", [])}

    dataset_units_target = target.api.get(
        endpoint=f"dataSets/{dataset_id}?fields=organisationUnits[id]", use_cache=False
    )
    existing_ids_target = {ou["id"] for ou in dataset_units_target.get("organisationUnits", [])}

    if org_unit_ids is None:
        to_add = existing_ids_source - existing_ids_target
        to_delete = existing_ids_target - existing_ids_source
    else:
        scope = set(org_unit_ids)
        to_add = (existing_ids_source & scope) - existing_ids_target
        to_delete = (existing_ids_target & scope) - existing_ids_source

    if to_delete and not allow_deletions:
        current_run.log_info(
            f"{len(to_delete)} orgUnit(s) présents dans l'instance cible mais absents de "
            f"l'instance source pour le dataset {dataset_id} - désassignation désactivée "
            f"(allow_deletions=False)."
        )
    elif to_delete and allow_deletions:
        url = f"{target.api.url}/dataSets/{dataset_id}/organisationUnits"
        for ou in sorted(to_delete):
            if dry_run:
                current_run.log_info(
                    f"[dry_run] désassignation orgUnit {ou} du dataset {dataset_id}"
                )
                continue
            try:
                res = target.api.session.delete(url=f"{url}/{ou}")
                status = getattr(res, "status_code", None)
                if status in (200, 204):
                    existing_ids_target.discard(ou)
                    current_run.log_info(f"orgUnit {ou} désassigné du dataset {dataset_id}.")
                else:
                    body = getattr(res, "text", "")
                    current_run.log_error(
                        f"Échec désassignation orgUnit {ou} (status={status}, body={body})."
                    )
            except Exception as e:
                current_run.log_error(f"Exception désassignation orgUnit {ou}: {e!s}")

    for ou in sorted(to_add):
        if dry_run:
            current_run.log_info(f"[dry_run] assignation orgUnit {ou} au dataset {dataset_id}")
            continue
        endpoint = f"dataSets/{dataset_id}/organisationUnits/{ou}"
        try:
            res_i = target.api.post(endpoint=endpoint)
            status_i = getattr(res_i, "status_code", None)
            if status_i in (200, 201, 409):
                existing_ids_target.add(ou)
            else:
                current_run.log_error(f"Échec assignation orgUnit {ou} (status={status_i}).")
        except Exception as e:
            current_run.log_error(f"Exception assignation orgUnit {ou}: {e!s}")


@snis_to_dedop_sync.task
def ensure_disaggregation_metadata(
    source: DHIS2,
    target: DHIS2,
    dataset_id: str,
    create_missing_metadata: bool,
    dry_run: bool,
) -> dict:
    """Detect (and optionally create) disaggregation metadata missing in target.

    Compares, per data element, the real categoryOptionCombos of source vs target. COCs present
    in the source but missing in the target are created in the target (UIDs preserved) only when
    ``create_missing_metadata`` is enabled and not in ``dry_run``; otherwise affected values
    are skipped and reported.

    Parameters
    ----------
    source : DHIS2
        source client.
    target : DHIS2
        target client.
    dataset_id : str
        Dataset identifier.
    create_missing_metadata : bool
        Whether to create the missing metadata in target.
    dry_run : bool
        Simulate without writing.

    Returns
    -------
    dict
        ``{"data_element_ids", "coc_target", "missing", "created"}``.
    """
    de_source = _dataset_data_element_ids(source, dataset_id)
    de_target = _dataset_data_element_ids(target, dataset_id)
    common = sorted(de_source & de_target)

    only_source = de_source - de_target
    if only_source:
        current_run.log_warning(
            f"{len(only_source)} dataElement(s) présents dans l'instance source mais absents "
            f"du dataset {dataset_id} de l'instance cible - ignorés."
        )

    coc_source = get_data_element_cocs(source, common)
    coc_target = get_data_element_cocs(target, common)

    missing = {de: coc_source.get(de, set()) - coc_target.get(de, set()) for de in common}
    missing = {de: cocs for de, cocs in missing.items() if cocs}

    created: list[str] = []
    if missing:
        missing_coc_ids = sorted({coc for cocs in missing.values() for coc in cocs})
        if create_missing_metadata and not dry_run:
            current_run.log_info(
                f"Création de {len(missing_coc_ids)} categoryOptionCombo(s) manquants dans "
                f"l'instance cible pour le dataset {dataset_id}."
            )
            created = _create_missing_coc_metadata(source, target, missing_coc_ids)
            coc_target = get_data_element_cocs(target, common)
        else:
            current_run.log_warning(
                f"{len(missing_coc_ids)} categoryOptionCombo(s) manquants dans l'instance cible "
                f"pour le dataset {dataset_id} - valeurs correspondantes ignorées "
                f"(create_missing_metadata={create_missing_metadata}, dry_run={dry_run})."
            )

    return {
        "data_element_ids": common,
        "coc_target": {de: sorted(coc_target.get(de, set())) for de in common},
        "missing": {de: sorted(cocs) for de, cocs in missing.items()},
        "created": created,
    }


@snis_to_dedop_sync.task
def fetch_dhis2_data(
    source: DHIS2,
    dataset_id: str,
    org_unit_ids: list[str] | None,
    extraction_root: str,
    periods_range: list[datetime],
    last_updated: datetime | None,
    automate_sync: bool,
    use_cache: bool,
    metadata_report: dict,
) -> pl.DataFrame:
    """Fetch data values from source for the given dataset and window.

    Extraction is performed from ``extraction_root`` with children included, then post-filtered
    on ``org_unit_ids`` (performance-driven strategy). Rows are restricted to the common data
    elements and to the (dataElement, COC) pairs available in target.

    Parameters
    ----------
    source : DHIS2
        source client.
    dataset_id : str
        Dataset identifier.
    org_unit_ids : list[str] | None
        Optional post-filter on organisation units.
    extraction_root : str
        Root org unit for extraction.
    periods_range : list[datetime]
        ``[start, end]`` window.
    last_updated : datetime | None
        Manual backfill cutoff.
    automate_sync : bool
        Daily incremental mode (lastUpdated = today).
    use_cache : bool
        Use cached source responses.
    metadata_report : dict
        Output of ``ensure_disaggregation_metadata``.

    Returns
    -------
    pl.DataFrame
        Filtered data values including the ``deleted`` flag.
    """
    if automate_sync:
        cutoff = last_updated or datetime.now()
    else:
        cutoff = last_updated

    params: dict = {
        "dataSet": dataset_id,
        "orgUnit": extraction_root,
        "children": "true",
        "startDate": periods_range[0].strftime("%Y-%m-%d"),
        "endDate": periods_range[-1].strftime("%Y-%m-%d"),
        "includeDeleted": "true",
    }
    if cutoff is not None:
        params["lastUpdated"] = cutoff.strftime("%Y-%m-%d")

    cutoff_msg = f", lastUpdated={params['lastUpdated']}" if "lastUpdated" in params else ""
    current_run.log_info(
        f"Extraction source dataset `{dataset_id}` racine `{extraction_root}` "
        f"({params['startDate']} → {params['endDate']}{cutoff_msg})"
    )

    response = source.api.get(endpoint="dataValueSets", params=params, use_cache=use_cache)
    data = _build_dataframe(response.get("dataValues", []))

    if data.is_empty():
        current_run.log_info(f"Aucun enregistrement extrait pour le dataset `{dataset_id}`.")
        return data

    # Keep only the latest version of each logical data value.
    subset = [
        "data_element_id",
        "period",
        "organisation_unit_id",
        "category_option_combo_id",
        "attribute_option_combo_id",
    ]
    data = data.sort(by="last_updated").unique(subset=subset, keep="last")

    if org_unit_ids:
        data = data.filter(pl.col("organisation_unit_id").is_in(org_unit_ids))

    selected_de = metadata_report.get("data_element_ids", [])
    data = data.filter(pl.col("data_element_id").is_in(selected_de))

    # Restrict to (dataElement, COC) pairs available in target.
    coc_target = metadata_report.get("coc_target", {})
    allowed_rows = [
        {"data_element_id": de, "category_option_combo_id": coc}
        for de, cocs in coc_target.items()
        for coc in cocs
    ]
    total_before = len(data)
    if allowed_rows:
        allowed = pl.DataFrame(allowed_rows)
        data = data.join(allowed, on=["data_element_id", "category_option_combo_id"], how="semi")
    else:
        data = data.clear()

    blocked = total_before - len(data)
    if blocked:
        current_run.log_warning(
            f"{blocked} enregistrement(s) ignorés (COC absent de l'instance cible) pour le dataset "
            f"`{dataset_id}`."
        )

    current_run.log_info(
        f"Extraction de {len(data)} enregistrement(s) retenus pour le dataset `{dataset_id}`."
    )
    return data


@snis_to_dedop_sync.task
def convert_periods(
    source: DHIS2, target: DHIS2, dataset_id: str, df: pl.DataFrame
) -> pl.DataFrame:
    """Convert period ids when the source and target period types differ.

    Conversion is done at the dataSet level and only supports aggregation from a finer to a
    coarser period type. Unsupported pairs raise (explicit dataset failure).

    Parameters
    ----------
    source : DHIS2
        source client.
    target : DHIS2
        target client.
    dataset_id : str
        Dataset identifier.
    df : pl.DataFrame
        Extracted data values.

    Returns
    -------
    pl.DataFrame
        Data values with periods converted to the target period type.
    """
    if df.is_empty():
        return df

    period_type_source = _dataset_period_type(source, dataset_id)
    period_type_target = _dataset_period_type(target, dataset_id)
    if period_type_source == period_type_target:
        return df

    current_run.log_info(
        f"Conversion de période requise pour `{dataset_id}`: "
        f"{period_type_source} (source) → {period_type_target} (target)"
    )

    periods = df["period"].unique().to_list()
    mapping = {p: convert_period_id(p, period_type_source, period_type_target) for p in periods}
    unsupported = [p for p, v in mapping.items() if v is None]
    if unsupported:
        raise ValueError(
            f"Conversion {period_type_source}→{period_type_target} non supportée pour le dataset "
            f"{dataset_id} (revoir la configuration du dataset dans l'instance cible)."
        )

    df = df.with_columns(pl.col("period").replace_strict(mapping, default=None).alias("period"))

    # Aggregate numeric values into the coarser target period.
    agg_types = _data_element_aggregation_types(target, df["data_element_id"].unique().to_list())
    df = df.with_columns(pl.col("value").cast(pl.Float64, strict=False).alias("_num"))
    non_numeric = df.filter(pl.col("_num").is_null() & pl.col("value").is_not_null()).height
    if non_numeric:
        current_run.log_warning(
            f"{non_numeric} valeur(s) non numériques ignorées lors de l'agrégation de période "
            f"pour `{dataset_id}`."
        )

    keys = [
        "data_element_id",
        "period",
        "organisation_unit_id",
        "category_option_combo_id",
        "attribute_option_combo_id",
    ]
    averaged_de = [de for de, at in agg_types.items() if at and at.upper().startswith("AVERAGE")]
    return (
        df.filter(pl.col("_num").is_not_null())
        .group_by(keys)
        .agg(
            pl.col("_num").sum().alias("_sum"),
            pl.col("_num").mean().alias("_mean"),
            pl.col("deleted").max().alias("deleted"),
            pl.col("last_updated").max().alias("last_updated"),
        )
        .with_columns(
            pl.when(pl.col("data_element_id").is_in(averaged_de))
            .then(pl.col("_mean"))
            .otherwise(pl.col("_sum"))
            .alias("value")
        )
        .with_columns(pl.col("value").cast(pl.String))
        .drop(["_sum", "_mean"])
    )


@snis_to_dedop_sync.task
def prepare_data_for_dhis2(df: pl.DataFrame, target_aoc: str) -> dict:
    """Prepare upsert and delete payloads for DHIS2.

    All values are stamped with the target attributeOptionCombo. Rows flagged ``deleted``
    in the source are partitioned into a delete payload for propagation.

    Parameters
    ----------
    df : pl.DataFrame
        Data values including the ``deleted`` flag.
    target_aoc : str
        Target (target) attributeOptionCombo.

    Returns
    -------
    dict
        ``{"upserts": [...], "deletes": [...]}``.
    """
    empty = {"upserts": [], "deletes": []}
    if df.is_empty():
        return empty

    required_cols = {
        "data_element_id",
        "organisation_unit_id",
        "category_option_combo_id",
        "period",
        "value",
        "deleted",
    }
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        current_run.log_error(f"Colonnes manquantes pour le payload: {', '.join(missing)}")
        return empty

    base = (
        df.with_columns(pl.lit(target_aoc).alias("attributeOptionCombo"))
        .select(
            pl.col("data_element_id").alias("dataElement"),
            pl.col("attributeOptionCombo"),
            pl.col("organisation_unit_id").alias("orgUnit"),
            pl.col("category_option_combo_id").alias("categoryOptionCombo"),
            pl.col("period"),
            pl.col("value").cast(pl.String).alias("value"),
            pl.col("deleted").fill_null(False).alias("deleted"),
        )
        .drop_nulls(["dataElement", "orgUnit", "period", "categoryOptionCombo"])
    )

    deletes_df = base.filter(pl.col("deleted"))
    upserts_df = base.filter(~pl.col("deleted") & pl.col("value").is_not_null())

    upserts = upserts_df.drop("deleted").to_dicts()
    deletes = [
        {**row, "deleted": True}
        for row in deletes_df.with_columns(pl.col("value").fill_null("")).to_dicts()
    ]
    return {"upserts": upserts, "deletes": deletes}


@snis_to_dedop_sync.task
def push_data_to_dhis2(
    dhis2: DHIS2,
    prepared: dict,
    dataset_id: str,
    dry_run: bool,
    import_mode: str = "CREATE_AND_UPDATE",
    post_batch_size: int = 5000,
) -> dict:
    """Push upserts and deletes to DHIS2 with chunking and retries.

    Parameters
    ----------
    dhis2 : DHIS2
        Target DHIS2 client.
    prepared : dict
        ``{"upserts": [...], "deletes": [...]}`` payloads.
    dataset_id : str
        Dataset identifier.
    dry_run : bool
        Simulate without writing.
    import_mode : str
        DHIS2 import strategy for upserts.
    post_batch_size : int
        Chunk size for POST requests.

    Returns
    -------
    dict
        Aggregated import summary including a ``failed`` flag.
    """
    upserts = prepared.get("upserts", [])
    deletes = prepared.get("deletes", [])

    aggregated: dict = {
        "status": "completed",
        "import_strategy": import_mode,
        "dry_run": dry_run,
        "total_upserts": len(upserts),
        "total_deletes": len(deletes),
        "totals": {"imported": 0, "updated": 0, "ignored": 0, "deleted": 0},
        "chunks": [],
        "failed": False,
    }

    if not upserts and not deletes:
        aggregated["status"] = "skipped"
        aggregated["imported"] = 0
        return aggregated

    if upserts:
        _push_chunks(
            dhis2,
            dataset_id,
            upserts,
            {"dryRun": dry_run, "importStrategy": import_mode},
            "upsert",
            post_batch_size,
            aggregated,
        )
    if deletes:
        _push_chunks(
            dhis2,
            dataset_id,
            deletes,
            {"dryRun": dry_run, "importStrategy": "CREATE_AND_UPDATE"},
            "delete",
            post_batch_size,
            aggregated,
        )

    total_success = aggregated["totals"]["imported"] + aggregated["totals"]["updated"]
    aggregated["imported"] = total_success
    current_run.log_info(
        f"dataSet `{dataset_id}`: {total_success} upsert(s), "
        f"{aggregated['totals']['deleted']} suppression(s) (strategy={import_mode})."
    )
    return aggregated


@snis_to_dedop_sync.task
def write_import_report(
    output_dir: Path, prepared: dict, summary: dict, metadata_report: dict
) -> None:
    """Write payload and report files for a dataset run.

    Parameters
    ----------
    output_dir : Path
        Relative output directory.
    prepared : dict
        Prepared payloads.
    summary : dict
        Import summary.
    metadata_report : dict
        Disaggregation metadata report (added to the written report).
    """
    upserts = prepared.get("upserts", [])
    deletes = prepared.get("deletes", [])
    if not upserts and not deletes:
        current_run.log_info("Aucun enregistrement à écrire dans le rapport d'import.")
        return

    summary = {**summary, "metadata": metadata_report}

    base_output_dir = Path(workspace.files_path) / output_dir
    base_output_dir.mkdir(parents=True, exist_ok=True)

    run_dir = base_output_dir / datetime.now(tz=UTC).strftime("%Y-%m-%d_%H-%M-%S_%f")
    run_dir.mkdir(parents=True, exist_ok=True)

    payload_fp = run_dir / "payload.json"
    with payload_fp.open("w", encoding="utf-8") as f:
        json.dump(prepared, f, indent=2)

    report_fp = run_dir / "report.json"
    with report_fp.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    current_run.log_info(f"Rapport d'import écrit dans {run_dir.as_posix()}")
    current_run.add_file_output(payload_fp.as_posix())
    current_run.add_file_output(report_fp.as_posix())


@snis_to_dedop_sync.task
def raise_on_push_failure(summary: dict, dataset_id: str) -> None:
    """Raise if the push summary reports a hard failure.

    Parameters
    ----------
    summary : dict
        Import summary returned by ``push_data_to_dhis2``.
    dataset_id : str
        Dataset identifier (for the error message).
    """
    if summary.get("failed"):
        raise RuntimeError(f"Échec d'import DHIS2 pour le dataset `{dataset_id}`.")


@snis_to_dedop_sync.task
def cleanup_old_directory_files(output_dir: Path, _write: None, retention_days: int = 30) -> None:
    """Delete report directories older than ``retention_days``.

    Parameters
    ----------
    output_dir : Path
        Relative output directory.
    _write : None
        Ordering dependency (unused).
    retention_days : int
        Number of days to keep.
    """
    output_dir = Path(workspace.files_path) / output_dir
    if not output_dir.exists():
        return

    now = datetime.now()
    for item in output_dir.iterdir():
        if not item.is_dir():
            continue
        try:
            try:
                folder_time = datetime.strptime(item.name, "%Y-%m-%d_%H-%M-%S_%f")
            except ValueError:
                folder_time = datetime.strptime(item.name, "%Y-%m-%d_%H-%M-%S")
            if (now - folder_time).days >= retention_days:
                for sub_item in item.iterdir():
                    sub_item.unlink()
                item.rmdir()
                current_run.log_info(f"Ancien rapport supprimé: {item.as_posix()}")
        except Exception:
            continue


# --------------------------------------------------------------------------------------------
# Helpers (non-task)
# --------------------------------------------------------------------------------------------


def _build_dataframe(values: list[dict]) -> pl.DataFrame:
    """Build a data value DataFrame (with the ``deleted`` flag) from raw DHIS2 dataValues.

    Returns
    -------
    pl.DataFrame
        Normalized data values with a boolean ``deleted`` column.
    """
    columns = [
        "data_element_id",
        "period",
        "organisation_unit_id",
        "category_option_combo_id",
        "attribute_option_combo_id",
        "value",
        "last_updated",
        "deleted",
    ]
    if not values:
        return pl.DataFrame({c: pl.Series(c, [], dtype=pl.String) for c in columns}).with_columns(
            pl.col("deleted").cast(pl.Boolean)
        )

    df = pl.from_dicts(values, infer_schema_length=None)
    for src in (
        "dataElement",
        "period",
        "orgUnit",
        "categoryOptionCombo",
        "attributeOptionCombo",
        "value",
        "lastUpdated",
        "deleted",
    ):
        if src not in df.columns:
            df = df.with_columns(pl.lit(None).alias(src))

    return df.select(
        pl.col("dataElement").cast(pl.String).alias("data_element_id"),
        pl.col("period").cast(pl.String),
        pl.col("orgUnit").cast(pl.String).alias("organisation_unit_id"),
        pl.col("categoryOptionCombo").cast(pl.String).alias("category_option_combo_id"),
        pl.col("attributeOptionCombo").cast(pl.String).alias("attribute_option_combo_id"),
        pl.col("value").cast(pl.String),
        pl.col("lastUpdated").cast(pl.String).alias("last_updated"),
        pl.col("deleted").cast(pl.Boolean, strict=False).fill_null(value=False),
    )


def _dataset_data_element_ids(dhis2: DHIS2, dataset_id: str) -> set[str]:
    """Return the set of data element ids assigned to a dataset.

    Returns
    -------
    set[str]
        The data element ids of the dataset.
    """
    response = dhis2.api.get(
        endpoint=f"dataSets/{dataset_id}?fields=dataSetElements[dataElement[id]]",
        use_cache=False,
    )
    return {de["dataElement"]["id"] for de in response.get("dataSetElements", [])}


def _dataset_period_type(dhis2: DHIS2, dataset_id: str) -> str:
    """Return the periodType of a dataset (defaults to Monthly).

    Returns
    -------
    str
        The dataset periodType.
    """
    response = dhis2.api.get(endpoint=f"dataSets/{dataset_id}?fields=periodType", use_cache=False)
    return response.get("periodType", "Monthly")


def _data_element_aggregation_types(dhis2: DHIS2, data_element_ids: list[str]) -> dict[str, str]:
    """Return the aggregationType for each data element.

    Returns
    -------
    dict[str, str]
        Mapping ``data_element_id -> aggregationType``.
    """
    result: dict[str, str] = {}
    if not data_element_ids:
        return result
    chunk_size = 100
    for index in range(0, len(data_element_ids), chunk_size):
        chunk = data_element_ids[index : index + chunk_size]
        response = dhis2.api.get(
            endpoint="dataElements",
            params={
                "paging": "false",
                "fields": "id,aggregationType",
                "filter": f"id:in:[{','.join(chunk)}]",
            },
            use_cache=False,
        )
        for de in response.get("dataElements", []):
            result[de["id"]] = de.get("aggregationType", "SUM")
    return result


def _create_missing_coc_metadata(
    source: DHIS2, target: DHIS2, missing_coc_ids: list[str]
) -> list[str]:
    """Create missing disaggregation metadata in the target, preserving source UIDs.

    Fetches the full definitions (``:owner``) of the missing categoryOptionCombos and their
    referenced categoryOptions / categoryCombos from the source, then imports them into the
    target via the metadata API with ``identifier=UID``. Best-effort; the response is logged.

    Returns
    -------
    list[str]
        The COC ids that were successfully created/updated (empty on failure).
    """
    if not missing_coc_ids:
        return []

    coc_objs: list[dict] = []
    category_option_ids: set[str] = set()
    category_combo_ids: set[str] = set()

    chunk_size = 100
    for index in range(0, len(missing_coc_ids), chunk_size):
        chunk = missing_coc_ids[index : index + chunk_size]
        resp = source.api.get(
            endpoint="categoryOptionCombos",
            params={
                "paging": "false",
                "fields": "id,name,ignoreApproval,categoryCombo[id],categoryOptions[id]",
                "filter": f"id:in:[{','.join(chunk)}]",
            },
            use_cache=False,
        )
        for coc in resp.get("categoryOptionCombos", []):
            coc_objs.append(coc)
            if coc.get("categoryCombo", {}).get("id"):
                category_combo_ids.add(coc["categoryCombo"]["id"])
            category_option_ids.update(
                o["id"] for o in coc.get("categoryOptions", []) if o.get("id")
            )

    category_options = _fetch_owner_objects(source, "categoryOptions", sorted(category_option_ids))
    category_combos = _fetch_owner_objects(source, "categoryCombos", sorted(category_combo_ids))

    payload = {
        "categoryOptions": category_options,
        "categoryCombos": category_combos,
        "categoryOptionCombos": coc_objs,
    }
    try:
        res = target.api.session.post(
            url=f"{target.api.url}/metadata",
            json=payload,
            params={
                "importStrategy": "CREATE_AND_UPDATE",
                "identifier": "UID",
                "atomicMode": "NONE",
                "importMode": "COMMIT",
            },
        )
        status = getattr(res, "status_code", None)
        if status in (200, 201):
            current_run.log_info(
                f"Métadonnées créées/à jour dans l'instance cible: "
                f"{len(category_options)} categoryOption(s), "
                f"{len(category_combos)} categoryCombo(s), {len(coc_objs)} COC."
            )
            return missing_coc_ids
        body = getattr(res, "text", "")
        current_run.log_error(f"Échec création métadonnées target (status={status}): {body}")
    except Exception as e:
        current_run.log_error(f"Exception lors de la création des métadonnées target: {e!s}")
    return []


def _fetch_owner_objects(dhis2: DHIS2, resource: str, ids: list[str]) -> list[dict]:
    """Fetch full (`:owner`) definitions of metadata objects by id.

    Returns
    -------
    list[dict]
        The full metadata object definitions.
    """
    objects: list[dict] = []
    if not ids:
        return objects
    chunk_size = 100
    for index in range(0, len(ids), chunk_size):
        chunk = ids[index : index + chunk_size]
        resp = dhis2.api.get(
            endpoint=resource,
            params={"paging": "false", "fields": ":owner", "filter": f"id:in:[{','.join(chunk)}]"},
            use_cache=False,
        )
        objects.extend(resp.get(resource, []))
    return objects


def _push_chunks(
    dhis2: DHIS2,
    dataset_id: str,
    values: list[dict],
    request_params: dict,
    label: str,
    post_batch_size: int,
    aggregated: dict,
) -> None:
    """Post a list of data values in chunks with retry/backoff, updating ``aggregated``."""
    url = dhis2.api.url + "/dataValueSets"
    max_retries = 3
    backoff_base = 1.0

    for offset in range(0, len(values), post_batch_size):
        chunk = values[offset : offset + post_batch_size]
        idx = offset // post_batch_size + 1

        response = None
        for attempt in range(1, max_retries + 1):
            response = dhis2.api.session.post(
                url=url,
                json={"dataSet": dataset_id, "dataValues": chunk},
                params=request_params,
            )
            status = response.status_code
            if status == 200:
                break
            if status == 429 or 500 <= status < 600:
                sleep_s = backoff_base * (2 ** (attempt - 1))
                current_run.log_warning(
                    f"[{label}] chunk {idx} tentative {attempt}/{max_retries} échouée "
                    f"(status={status}). Nouvel essai dans {sleep_s:.1f}s..."
                )
                time.sleep(sleep_s)
                continue
            break

        if response is None or response.status_code != 200:
            aggregated["failed"] = True
            body = response.text if response is not None else "no response"
            aggregated["chunks"].append(
                {"label": label, "index": idx, "size": len(chunk), "status": "failed"}
            )
            current_run.log_error(
                f"[{label}] échec import dataset {dataset_id} chunk {idx}: {body}"
            )
            continue

        try:
            resp_data = response.json()
        except Exception:
            resp_data = {}

        chunk_summary = resp_data.get("response", resp_data)
        ic = chunk_summary.get("importCount", {}) or {}
        counts = {
            "imported": ic.get("imported", 0),
            "updated": ic.get("updated", 0),
            "ignored": ic.get("ignored", 0),
            "deleted": ic.get("deleted", 0),
        }
        for key, value in counts.items():
            aggregated["totals"][key] += value

        for conflict in chunk_summary.get("conflicts", []) or []:
            current_run.log_warning(
                f"Conflit dataset {dataset_id} chunk {idx} [{label}]: "
                f"{conflict.get('object', '')} - {conflict.get('value', '')}"
            )

        aggregated["chunks"].append(
            {
                "label": label,
                "index": idx,
                "size": len(chunk),
                "importCount": counts,
                "status": "success",
            }
        )


if __name__ == "__main__":
    snis_to_dedop_sync()
