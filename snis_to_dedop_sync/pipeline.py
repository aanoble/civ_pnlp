"""Template for newly generated pipelines."""

import json
import time
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
from constants import DATASET_IDS
from dateutil import rrule
from dateutil.relativedelta import relativedelta
from openhexa.sdk import (
    DHIS2Connection,
    current_run,
    parameter,
    pipeline,
    workspace,
)
from openhexa.sdk.pipelines.parameter import DHIS2Widget
from openhexa.toolbox.dhis2 import DHIS2, dataframe
from utils import check_server_health, last_analytics_update, parse_cutoff_date, validate_dataset


@pipeline("snis_to_dedop_sync", timeout=43200)
@parameter(
    "snis_connection",
    type=DHIS2Connection,  # type: ignore
    name="DHIS2 Connection for SNIS",
    help="DHIS2 connection to fetch SNIS data from.",
    default="snis-dhis2",
    required=True,
)
@parameter(
    "dedop_connection",
    type=DHIS2Connection,  # type: ignore
    name="DHIS2 Connection for Dedop",
    help="DHIS2 connection to fetch Dedop data from.",
    default="dhis2-nmdr-temp",
    required=True,
)
@parameter(
    "dataset_id",
    type=str,
    widget=DHIS2Widget.DATASETS,
    connection="dedop_connection",
    name="Dataset ID in Dedop",
    required=False,
    multiple=True,
)
@parameter(
    "org_unit_id",
    type=str,
    widget=DHIS2Widget.ORG_UNITS,
    connection="dedop_connection",
    name="Organisation Unit ID in Dedop",
    required=False,
    multiple=True,
)
@parameter(
    code="start_date",
    type=str,  # type: ignore
    name="Start date (YYYY-MM-DD)",
    help="Start date for DHIS2 extraction (default today)",
    required=False,
)
@parameter(
    code="end_date",
    type=str,  # type: ignore
    name="End date (YYYY-MM-DD)",
    help=("End date for the extraction (default last day of start date)."),
    required=False,
)
@parameter(
    "months_back",
    type=int,  # type: ignore
    name="Historical period in months to refresh",
    help="Number of months to look back from current month to refresh",
    default=24,
    required=False,
)
@parameter(
    "last_updated",
    type=str,  # type: ignore
    name="Last updated date (YYYY-MM-DD)",
    help="Only fetch records updated since this date from SNIS",
    required=False,
)
@parameter(
    "output_directory",
    type=str,  # type: ignore
    name="Output directory",
    help="Directory to save the output files",
    default="sync SNIS DEDOP/data/output",
    required=True,
)
@parameter(
    "dhis2_aoc",
    type=str,  # type: ignore
    name="DHIS2 attribute option combo",
    help="DHIS2 attribute option combo",
    default="HllvX50cXC0",
    required=True,
)
@parameter(
    "use_cache",
    type=bool,  # type: ignore
    name="Use API SNIS cache",
    help="Whether to use cached API responses where possible",
    default=False,
    required=False,
)
def snis_to_dedop_sync(
    snis_connection: DHIS2Connection,
    dedop_connection: DHIS2Connection,
    dataset_id: str,
    org_unit_id: str,
    start_date: str | None,
    end_date: str | None,
    months_back: int,
    last_updated: str | None,
    output_directory: str,
    dhis2_aoc: str,
    dry_run: bool = False,
    use_cache: bool = True,
    import_mode: str = "CREATE_AND_UPDATE",
    post_batch_size: int = 5000,
):
    """Pipeline to synchronize data from SNIS DHIS2 to Dedop DHIS2.

    Parameters.
    ----------
    snis_connection : DHIS2Connection
        DHIS2 connection to fetch SNIS data from.
    dedop_connection : DHIS2Connection
        DHIS2 connection to fetch Dedop data from.
    dataset_id : str
        Dataset ID in Dedop to synchronize.
    org_unit_id : str
    """
    snis = (
        DHIS2(connection=snis_connection, cache_dir=Path(workspace.files_path, "snis", ".cache"))
        if use_cache
        else DHIS2(connection=snis_connection)
    )
    dedop = DHIS2(connection=dedop_connection)

    check_server_health(snis)
    check_server_health(dedop)

    if last_update_snis := last_analytics_update(snis):
        current_run.log_info(
            "Dernière mise à jour des tables analytiques SNIS: "
            f"{last_update_snis.strftime('%Y-%m-%d %H:%M:%S')}"
        )

    if last_update_dedop := last_analytics_update(dedop):
        current_run.log_info(
            "Dernière mise à jour des tables analytiques DEDOP: "
            f"{last_update_dedop.strftime('%Y-%m-%d %H:%M:%S')}"
        )

    periods_range = process_periods(
        start_date=start_date, end_date=end_date, months_back=months_back
    )
    last_updated = parse_cutoff_date(last_updated) if last_updated else None  # type: ignore

    dataset_ids = list(dataset_id) if dataset_id else DATASET_IDS
    org_unit_ids = list(org_unit_id) if org_unit_id else None

    for dataset_id in dataset_ids:
        current_run.log_info(f"Traitement du dataset `{dataset_id}`")

        is_valid = validate_dataset(snis, dataset_id)
        if not is_valid:
            continue

        if not org_unit_ids:
            org_unit_ids = sync_missing_orgunits(
                snis=snis, dedop=dedop, dataset_id=dataset_id, org_unit_ids=org_unit_ids
            )

        data_snis = fetch_dhis2_data(
            snis=snis,
            dedop=dedop,
            dataset_id=dataset_id,
            org_unit_ids=org_unit_ids,
            periods_range=periods_range,
            last_updated=last_updated,
        )

        payload = prepare_data_for_dhis2(df=data_snis, dhis2_aoc=dhis2_aoc)

        summary = push_data_to_dhis2(
            dhis2=dedop,
            payload=payload,
            dataset_id=dataset_id,
            dry_run=dry_run,
            import_mode=import_mode,
            post_batch_size=post_batch_size,
        )

        write = write_import_report((Path(output_directory) / dataset_id), payload, summary)

        cleanup_old_directory_files((Path(output_directory) / dataset_id), write)


@snis_to_dedop_sync.task
def process_periods(
    start_date: str | None,
    end_date: str | None,
    months_back: int,
) -> list[datetime]:
    """Traite les périodes selon les dates et le décalage temporel.

    Parameters
    ----------
    start_date : str | None
        Date de début (format YYYY-MM-DD)
    end_date : str | None
        Date de fin (format YYYY-MM-DD)
    months_back : int
        Nombre de mois à reculer depuis la date de début

    Returns
    -------
    list[str]
        Liste contenant [date_début, date_fin] formatées

    Raises
    ------
    ValueError
        Si format date invalide ou incohérence temporelle
    """
    current_run.log_info("Traitement des périodes d'extraction")

    # Conversion et gestion des dates
    start_dt = parse_cutoff_date(start_date) if start_date else datetime.now()
    if not start_date:
        current_run.log_info(f"Date de début absente, utilisation: {start_dt.strftime('%Y-%m-%d')}")

    end_dt = parse_cutoff_date(end_date) if end_date else start_dt
    end_dt = end_dt + relativedelta(day=31)
    if not end_date:
        current_run.log_info(
            f"Date de fin absente, utilisation fin de mois: {end_dt.strftime('%Y-%m-%d')}"
        )

    if months_back:
        start_dt = start_dt - relativedelta(months=months_back)
        current_run.log_info(
            f"Recul de {months_back} mois appliqué: nouvelle date début {start_dt}"
        )

    if start_dt == end_dt:
        return [start_dt]

    if start_dt > end_dt:
        current_run.log_error(
            f"Incohérence temporelle: date de début {start_dt.strftime('%Y-%m-%d')} "
            f"postérieure à date de fin {end_dt.strftime('%Y-%m-%d')}"
        )
        raise ValueError("La date de début doit être antérieure ou égale à la date de fin.")

    dates = list(rrule.rrule(freq=rrule.DAILY, dtstart=start_dt, until=end_dt))
    return sorted({dt for dt in dates})


@snis_to_dedop_sync.task
def sync_missing_orgunits(
    snis: DHIS2, dedop: DHIS2, dataset_id: str, org_unit_ids: list[str] | None
) -> list[str]:
    """Synchronize missing organisation units from SNIS to Dedop for a given dataset.

    Parameters.
    ----------
    snis : DHIS2
        DHIS2 client used to perform API calls to SNIS.
    dedop : DHIS2
        DHIS2 client used to perform API calls to Dedop.
    dataset_id : str
        Identifier of the dataset to synchronize organisation units for.
    org_unit_ids : list[str] | None
        Specific organisation unit IDs to consider (None to include all).

    Returns
    -------
    list[str]
        List of organisation unit IDs that are present in the Dedop dataset after synchronization.
    """
    # Extract organisation units from SNIS dataset
    dataset_units_snis = snis.api.get(
        endpoint=f"dataSets/{dataset_id}?fields=organisationUnits[id]", use_cache=False
    )
    existing_ids_snis = {ou["id"] for ou in dataset_units_snis.get("organisationUnits", [])}

    # Extract organisation units from Dedop dataset
    dataset_units_dedop = dedop.api.get(
        endpoint=f"dataSets/{dataset_id}?fields=organisationUnits[id]", use_cache=False
    )
    existing_ids_dedop = {ou["id"] for ou in dataset_units_dedop.get("organisationUnits", [])}

    to_add = (
        existing_ids_snis - existing_ids_dedop
        if org_unit_ids is None
        else existing_ids_snis.intersection(set(org_unit_ids)) - existing_ids_dedop
    )
    if not to_add:
        current_run.log_info(f"Aucun orgunit manquant à synchroniser pour le dataset {dataset_id}")
        return list(existing_ids_dedop)

    # Add each missing org unit to Dataset
    output_orgunits = []
    for ou in sorted(to_add):
        for endpoint in (f"dataSets/{dataset_id}/organisationUnits/{ou}",):
            try:
                res = dedop.api.post(endpoint=endpoint)
                status = getattr(res, "status_code", None)
                if status in (200, 201):
                    current_run.log_info(
                        f"Ajout de l'orgUnit {ou} au DataSet {dataset_id} (status={status})."
                    )
                    output_orgunits.append(ou)
                else:
                    # Some DHIS2 instances return 409 if already present (idempotency)
                    body = getattr(res, "text", "")
                    if status == 409:
                        current_run.log_info(
                            f"OrgUnit {ou} already present for '{endpoint}' (409)."
                        )
                    else:
                        current_run.log_error(
                            f"Failed to add orgUnit {ou} to '{endpoint}': "
                            f"status={status}, body={body}"
                        )
            except Exception as e:
                current_run.log_error(f"Exception while adding orgUnit {ou} to '{endpoint}': {e!s}")
    return list(existing_ids_dedop.union(output_orgunits))


@snis_to_dedop_sync.task
def fetch_dhis2_data(
    snis: DHIS2,
    dedop: DHIS2,
    dataset_id: str,
    org_unit_ids: list[str] | None,
    periods_range: list[datetime],
    last_updated: str | None,
) -> pl.DataFrame:
    """Fetch data from DHIS2 for given dataset, org unit, periods, and last updated filter.

    Parameters
    ----------
    snis : DHIS2
        DHIS2 client used to perform API calls to SNIS.
    dedop : DHIS2
        DHIS2 client for Dedop system.
    dataset_id : str
        Identifier of the dataset to query.
    org_unit_ids : list[str] | None
        Organisation unit IDs to filter on (None to include all).
    periods_range : list[datetime]
        List of periods (as datetime objects) to fetch data for.
    last_updated : str | None
        Only return records updated since this ISO date (YYYY-MM-DD), if provided.

    Returns
    -------
    pl.DataFrame
        A Polars DataFrame containing the fetched data.
    """
    try:
        # Recover dataElement from dataset
        data_element = dedop.api.get(
            endpoint=f"dataSets/{dataset_id}?fields=dataSetElements[dataElement[id]]",
            use_cache=False,
        )
        existing_de_id_ddp = {
            de["dataElement"]["id"] for de in data_element.get("dataSetElements", [])
        }
        data_element = snis.api.get(
            endpoint=f"dataSets/{dataset_id}?fields=dataSetElements[dataElement[id]]",
            use_cache=False,
        )
        existing_de_id_snis = {
            de["dataElement"]["id"] for de in data_element.get("dataSetElements", [])
        }
        data_element_ids = list(existing_de_id_ddp.intersection(existing_de_id_snis))

        # Filters data elements where categoryCombo are same
        filters = f"id:in:[{','.join(data_element_ids)}]"

        df_de = pl.DataFrame(
            dedop.meta.data_elements(fields="id,categoryCombo", filters=filters)
        ).join(
            pl.DataFrame(snis.meta.data_elements(fields="id,categoryCombo", filters=filters)),
            on="id",
        )
        df_de = df_de.with_columns(
            pl.struct(["categoryCombo", "categoryCombo_right"])
            .map_elements(
                lambda row: len(set(row["categoryCombo"]) - set(row["categoryCombo_right"]))
            )
            .alias("coc_equiv")
        )
        selected_de = df_de.filter(pl.col("coc_equiv") == 0)["id"].unique().to_list()
        ignored_de = df_de.filter(pl.col("coc_equiv") != 0)["id"].unique().to_list()

        # Get period type of target dataset
        datasets = snis.meta.datasets(
            fields="periodType",
            filters=f"identifiable:token:{dataset_id}",
        )
        period_type_source = datasets[0].get("periodType", "Monthly") if datasets else "Monthly"

        datasets = dedop.meta.datasets(
            fields="periodType",
            filters=f"identifiable:token:{dataset_id}",
        )
        period_type_target = datasets[0].get("periodType", "Monthly") if datasets else "Monthly"

        # Fetch data for each period and aggregate
        current_run.log_info(
            f"Récupération des données depuis le SNIS pour le dataset id `{dataset_id}` "
            f"pour les périodes `{periods_range[0].strftime('%Y-%m-%d')}`"
            f" - `{periods_range[-1].strftime('%Y-%m-%d')}`"
        )
        data = dataframe.extract_dataset(
            snis,
            dataset=[dataset_id],
            org_units=["ZD44Asc0bAk"],
            start_date=periods_range[0],
            end_date=periods_range[-1],
            last_updated=last_updated,
            include_children=True,
        )
        data = (
            data.filter(pl.col("organisation_unit_id").is_in(org_unit_ids))
            if org_unit_ids
            else data
        )
        if ignored_de:
            current_run.log_critical(
                f"Ces éléments de données {', '.join(ignored_de)} ont été ignorés compte tenu "
                "du fait que les categoryCombos ne sont pas identiques"
            )

        data = data.filter(pl.col("data_element_id").is_in(selected_de))
        current_run.log_info(
            f"Extraction de {len(data)} enregistrements pour le dataset `{dataset_id}` "
            f"effectuée avec succès."
        )
        if period_type_source == period_type_target:
            return data

        current_run.log_info(
            f"Une conversion est requise pour le dataset id {dataset_id} entre "
            f"les types de période du SNIS: {period_type_source} et DEDOP: {period_type_target}"
            "revoir la configuration du dataset dans DEDOP."
        )
        return pl.DataFrame()

    except Exception as err:
        current_run.log_error(
            f"Erreur lors de la récupération des données DHIS2 "
            f"sur le dataset_id {dataset_id}: {err!s}"
        )
        return pl.DataFrame()


@snis_to_dedop_sync.task
def prepare_data_for_dhis2(df: pl.DataFrame, dhis2_aoc: str) -> list[dict]:
    """Prepare data for DHIS2 by adjusting organisation unit codes.

    Parameters
    ----------
    df : pl.DataFrame
        Input DataFrame containing DHIS2 data.
    dhis2_aoc : str
        DHIS2 attribute option combo ID to set for all records.

    Returns
    -------
    list[dict]
        Flat list of data values ready for DHIS2 ingestion.
    """
    if df.is_empty():
        return []

    # Required columns sanity check
    required_cols = {
        "data_element_id",
        "organisation_unit_id",
        "category_option_combo_id",
        "period",
        "value",
    }
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        current_run.log_error(
            f"Colonnes manquantes pour la préparation du payload: {', '.join(missing)}"
        )
        return []

    # Filter out null/empty values early to reduce payload size
    df = df.filter(pl.col("value").is_not_null())

    # Always set the attribute option combo provided by parameter
    df = df.with_columns(pl.lit(dhis2_aoc).alias("attributeOptionCombo"))

    df = df.select(
        [
            pl.col("data_element_id").alias("dataElement"),
            pl.col("attributeOptionCombo"),
            pl.col("organisation_unit_id").alias("orgUnit"),
            pl.col("category_option_combo_id").alias("categoryOptionCombo"),
            pl.col("period"),
            pl.col("value").cast(pl.String).alias("value"),
        ]
    ).drop_nulls(["dataElement", "orgUnit", "period"])

    return df.to_dicts()


@snis_to_dedop_sync.task
def push_data_to_dhis2(
    dhis2: DHIS2,
    payload: list[dict],
    dataset_id: str,
    dry_run: bool,
    import_mode: str = "CREATE_AND_UPDATE",
    post_batch_size: int = 5000,
) -> dict:
    """Envoi des données à DHIS2 avec découpage en chunks et retry.

    Args:
        dhis2: Client DHIS2 configuré
        payload: Données à importer
        dataset_id: Identifiant du dataset DHIS2
        dry_run: Mode test sans écriture
        import_mode: Stratégie d'import DHIS2 (CREATE, UPDATE, CREATE_AND_UPDATE)
        post_batch_size: Taille des lots pour les requêtes POST DHIS2

    Returns:
        Dict de résumé d'import agrégé.
    """
    total = len(payload)
    if total == 0:
        return {"status": "skipped", "imported": 0}

    def _chunks(seq: list[dict], size: int):
        for i in range(0, len(seq), size):
            yield seq[i : i + size]

    aggregated: dict = {
        "status": "completed",
        "import_strategy": import_mode,
        "dry_run": dry_run,
        "total": total,
        "chunks": [],
        "totals": {"imported": 0, "updated": 0, "ignored": 0, "deleted": 0},
    }

    request_params = {"dryRun": dry_run, "importStrategy": import_mode}
    max_retries = 3
    backoff_base = 1.0
    url = dhis2.api.url + "/dataValueSets"

    for idx, chunk in enumerate(_chunks(payload, post_batch_size), start=1):
        import_counts = {"imported": 0, "updated": 0, "ignored": 0, "deleted": 0}
        issues: list = []

        response = None
        for attempt in range(1, max_retries + 1):
            response = dhis2.api.session.post(
                url=url, json={"dataSet": dataset_id, "dataValues": chunk}, params=request_params
            )
            status = response.status_code
            if status == 200:
                break
            if status == 429 or 500 <= status < 600:
                sleep_s = backoff_base * (2 ** (attempt - 1))
                current_run.log_warning(
                    f"Chunk {idx} attempt {attempt}/{max_retries} failed (status={status}). "
                    f"Retrying in {sleep_s:.1f}s..."
                )
                time.sleep(sleep_s)
                continue
            # Non-retryable error
            break

        if response is None:
            aggregated["chunks"].append(
                {"index": idx, "size": len(chunk), "summary": {}, "status": "failed"}
            )
            current_run.log_error(
                f"Error importing chunk {idx}: no response from DHIS2 (strategy={import_mode})"
            )
            continue

        try:
            resp_data = response.json()
        except Exception:
            resp_data = {}

        if response.status_code != 200:
            aggregated["chunks"].append(
                {"index": idx, "size": len(chunk), "summary": resp_data, "status": "failed"}
            )
            current_run.log_error(
                f"Error importing chunk {idx}: {response.text} (strategy={import_mode})"
            )
            continue

        chunk_summary = resp_data.get("response", resp_data)
        if "importCount" in chunk_summary:
            ic = chunk_summary.get("importCount", {})
            import_counts["ignored"] = ic.get("ignored", 0)
            import_counts["imported"] = ic.get("imported", 0)
            import_counts["updated"] = ic.get("updated", 0)
            import_counts["deleted"] = ic.get("deleted", 0)

        for conflict in chunk_summary.get("conflicts", []) or []:
            current_run.log_warning(
                "Conflict in chunk {i}: {obj} - {val}".format(
                    i=idx, obj=conflict.get("object", ""), val=conflict.get("value", "")
                )
            )
            issues.append(conflict)

        aggregated["chunks"].append(
            {
                "index": idx,
                "size": len(chunk),
                "importCount": import_counts,
                "issues": issues,
                "status": "success",
            }
        )

        aggregated["totals"]["imported"] += import_counts["imported"]
        aggregated["totals"]["updated"] += import_counts["updated"]
        aggregated["totals"]["ignored"] += import_counts["ignored"]
        aggregated["totals"]["deleted"] += import_counts["deleted"]

    total_success = aggregated["totals"]["imported"] + aggregated["totals"]["updated"]
    aggregated["imported"] = total_success
    current_run.log_info(
        f"Imported {total_success}/{total} data values to DHIS2 (strategy={import_mode})"
    )
    return aggregated  # type: ignore


@snis_to_dedop_sync.task
def write_import_report(output_dir: Path, payload: list[dict], summary: dict) -> None:
    """Génère les rapports d'import.

    Args:
        output_dir: Répertoire de sortie
        payload: Données envoyées
        summary: Résumé DHIS2
    """
    if len(payload) == 0:
        current_run.log_info("Aucun enregistrement à écrire dans le rapport d'import.")
        return

    output_dir = Path(
        workspace.files_path,
        output_dir,
        datetime.now(tz=UTC).strftime("%Y-%m-%d_%H-%M-%S"),
    )
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    payload_fp = output_dir / "payload.json"
    with payload_fp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    report_fp = output_dir / "report.json"
    with report_fp.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    current_run.log_info(f"Import report written to {output_dir.as_posix()}")
    current_run.add_file_output(payload_fp.as_posix())
    current_run.add_file_output(report_fp.as_posix())

    return


@snis_to_dedop_sync.task
def cleanup_old_directory_files(output_dir: Path, _write: None, retention_days: int = 2) -> None:
    """Supprime les anciens fichiers de rapport.

    Pour avoir une chronologie des exécutions des tâches les deux paramètres
    ont été rajoutés mais ils ne sont pas utilisés dans la tâche.

    Args:
        output_dir: Répertoire de sortie
        _write: Paramètre factice pour la chronologie
        retention_days: Nombre de jours à conserver
    """
    output_dir = Path(workspace.files_path, output_dir)
    now = datetime.now()
    for item in output_dir.iterdir():
        if item.is_dir():
            try:
                folder_time = datetime.strptime(item.name, "%Y-%m-%d_%H-%M-%S")
                if (now - folder_time).days >= retention_days:
                    for sub_item in item.iterdir():
                        sub_item.unlink()
                    item.rmdir()
                    current_run.log_info(f"Deleted old report directory: {item.as_posix()}")
            except Exception:
                continue


if __name__ == "__main__":
    snis_to_dedop_sync()
