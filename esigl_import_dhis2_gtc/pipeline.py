"""Template for newly generated pipelines."""

import json
import time
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
from dateutil.relativedelta import relativedelta
from metabase import Metabase
from openhexa.sdk import (
    CustomConnection,
    DHIS2Connection,
    File,  # type: ignore
    current_run,
    parameter,
    pipeline,
    workspace,
)
from openhexa.toolbox.dhis2 import DHIS2
from queries import QUERY_ETAT_STOCK_GTC
from utils import check_metabase_server_health, parse_cutoff_date

EXTENDED_PRODUCT_CODE = {
    "3050345": "AM02170",
    "3050058": "AM02185",
    "3050346": "AM02180",
    "3050055": "AM02145",
    "3050349": "AM02068",
}


@pipeline("esigl_import_dhis2")
@parameter(
    "dhis2_connection",
    type=DHIS2Connection,  # type: ignore
    name="Target DHIS2 instance",
    help="Target DHIS2 instance",
    default="dhis2-nmdr",
    required=True,
)
@parameter(
    "metabase_connection",
    type=CustomConnection,  # type: ignore
    default="metabase-esigl",
    name="Metabase instance",
    help="Metabase instance",
    required=True,
)
@parameter(
    "fp_coc_mapping",
    type=File,
    name="File path for coc mapping",
    help="File path for coc mapping cols located in directory `metabase eSIGL/data/ressources`",
    default="metabase eSIGL/data/ressources/mapping_coc.json",
    required=True,
)
@parameter(
    "fp_ou_de_mapping",
    type=File,
    name="File path OrgUnit and dataElement mapping eSIGL to DHIS2",
    help=(
        "File path OrgUnit and dataElement mapping eSIGL to DHIS2 "
        "located in directory `metabase eSIGL/data/ressources`"
    ),
    default="metabase eSIGL/data/ressources/Fichier mapping OrgUnit eSIGL DHIS2.xlsx",
    required=True,
)
@parameter(
    "output_directory",
    type=str,  # type: ignore
    name="Output directory",
    help="Directory to save the output files",
    default="metabase eSIGL/data/output/gtc",
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
    code="start_date",
    type=str,  # type: ignore
    name="Start date (YYYY-MM-DD)",
    help="Start date for eSIGL data extraction",
    required=False,
    default=None,
)
@parameter(
    code="end_date",
    type=str,  # type: ignore
    name="End date (YYYY-MM-DD)",
    help="End date for eSIGL data extraction",
    required=False,
    default=None,
)
@parameter(
    "facilities_code",
    type=str,  # type: ignore
    name="eSIGL facilities code",
    help="eSIGL facilities code to filter data",
    required=False,
    multiple=True,
)
@parameter(
    "product_code",
    type=str,  # type: ignore
    name="eSIGL products code",
    help="eSIGL products code to filter data",
    required=False,
    multiple=True,
)
@parameter(
    "months_back",
    type=int,  # type: ignore
    name="Historical period in months",
    help="Number of months to look back from current month",
    default=3,
    choices=[0, 1, 3, 6, 12],
    required=True,
)
@parameter(
    "import_mode",
    type=str,  # type: ignore
    name="Import mode",
    help="DHIS2 import mode",
    choices=["CREATE", "CREATE_AND_UPDATE", "UPDATE"],
    default="CREATE_AND_UPDATE",
    required=True,
)
@parameter(
    "dry_run",
    type=bool,  # type: ignore
    default=False,
    name="Dry run",
    help="Simulate DHIS2 import",
    required=False,
)
def esigl_import_dhis2(
    dhis2_connection: DHIS2Connection,
    metabase_connection: CustomConnection,
    fp_coc_mapping: str,
    fp_ou_de_mapping: str,
    output_directory: str,
    dhis2_aoc: str = "HllvX50cXC0",
    start_date: str | None = None,
    end_date: str | None = None,
    months_back: int = 3,
    facilities_code: list[str] | None = None,
    product_code: list[str] | None = None,
    dry_run: bool = False,
    import_mode: str = "CREATE_AND_UPDATE",
    post_batch_size: int = 5000,
):
    """Orchestre le processus complet d'import des données eSIGL -> DHIS2.

    Args:
        dhis2_connection: Connexion DHIS2 cible
        metabase_connection: Connexion Metabase source
        fp_coc_mapping: Chemin du mapping COC
        fp_ou_de_mapping: Chemin du mapping orgUnit et dataElement
        output_directory: Répertoire de sortie
        dhis2_aoc: COC attribut DHIS2
        start_date: Date de début pour l'extraction des données
        end_date: Date de fin pour l'extraction des données
        months_back: Nombre de mois à rafraîchir
        facilities_code: Code des établissements eSIGL pour filtrer les données
        product_code: Code des produits eSIGL pour filtrer les données
        dry_run: Mode test sans écriture
        import_mode: Mode d'import DHIS2
        post_batch_size: Taille des lots pour les requêtes POST DHIS2
    """
    df_ou_mapping = read_ressources_files(file_path=fp_ou_de_mapping, sheet_name="OrgUnit")
    df_coc_mapping = read_ressources_files(fp_coc_mapping, schema=["coc", "col"])

    dhis2 = DHIS2(connection=dhis2_connection, cache_dir=Path(workspace.files_path, ".cache"))

    df_de = fetch_gtc_dataelement(dhis2=dhis2)

    df_etat_stock = extract_data_from_esigl(
        metabase_connection,
        df_ou_mapping,
        df_de,
        start_date,
        end_date,
        months_back,
        facilities_code,
        product_code,
    )

    add_missing_ou = add_missing_orgunits(dhis2, df_etat_stock)

    payload = prepare_data_for_dhis2(df_etat_stock, df_coc_mapping, dhis2_aoc)

    summary = push_data_to_dhis2(
        dhis2=dhis2,
        payload=payload,
        dry_run=dry_run,
        import_mode=import_mode,
        post_batch_size=post_batch_size,
        add_missing_ou=add_missing_ou,
    )

    write_import_report(output_directory, payload, summary)

    cleanup_old_directory_files(Path(output_directory), payload, summary)


@esigl_import_dhis2.task
def read_ressources_files(
    file_path: File, sheet_name: str | None = None, schema: list | None = None
) -> pl.DataFrame:
    """Charge un fichier JSON de mapping en DataFrame.

    Args:
        file_path: Chemin relatif depuis le répertoire de travail
        sheet_name: Nom de la feuille à lire (si applicable)
        schema: Liste des noms de colonnes

    Returns:
        DataFrame Polars structuré

    Raises:
        FileNotFoundError: Si le fichier n'existe pas
    """
    full_path = Path(workspace.files_path) / Path(file_path.path)
    if not full_path.exists():
        error_msg = f"File not found : {full_path.as_posix()}"
        current_run.log_error(error_msg)
        raise FileNotFoundError(error_msg)

    if full_path.suffix == ".json":
        with Path.open(full_path, "r", encoding="utf-8") as file:
            dico_map = json.load(file)

        return pl.DataFrame(
            data=list(dico_map.items()),
            schema=schema,
            orient="row",
        )
    if full_path.suffix in [".xlsx", ".xls"]:
        if sheet_name:
            return pl.read_excel(full_path, sheet_name=sheet_name)

        return pl.read_excel(full_path)

    return pl.DataFrame()


@esigl_import_dhis2.task
def fetch_gtc_dataelement(dhis2: DHIS2) -> pl.DataFrame:
    """Récupère les dataElements GTC depuis DHIS2.

    Args:
        dhis2: Client DHIS2 configuré

    Returns:
        DataFrame des dataElements GTC
    """
    df_de = pl.DataFrame(
        dhis2.api.get(
            endpoint="dataElements/?filter=identifiable:token:gtc",
            params={"fields": "id,code,"},
        )["dataElements"]
    ).filter(pl.col("code").is_not_null())

    current_run.log_info(f"Retrieved {df_de.shape[0]} GTC dataElements from DHIS2")
    return df_de


@esigl_import_dhis2.task
def add_missing_orgunits(
    dhis2: DHIS2,
    df_etat_stock: pl.DataFrame,
    dataset_uid: str = "FxlkZtA0VbX",
) -> None:
    """Ensure all mapped org units are members of the target dataset.

    This reads the org unit IDs from the provided mapping file and compares them
    to the current members of the organisation unit dateset. Any missing org units
    are added to both the dataset and the org unit dataset.

    Args:
        dhis2: Client DHIS2 configuré
        df_etat_stock: DataFrame contenant les données extraites d'eSIGL
        dataset_uid: UID de l'ensemble de données
    """
    # Collect existing members of the org unit group
    dataset_units = dhis2.api.get(
        endpoint=f"dataSets/{dataset_uid}?fields=organisationUnits[id]", use_cache=False
    )
    existing_ids = {ou["id"] for ou in dataset_units.get("organisationUnits", [])}

    # Collect target org units from mapping (non-null, unique)
    mapping_ids = set(df_etat_stock.select("orgUnit").unique().to_series().to_list())

    to_add = mapping_ids - existing_ids

    current_run.log_info(
        f"OrgUnit membership sync: {len(existing_ids)} existing in dataset, "
        f"{len(mapping_ids)} in mapping, {len(to_add)} to add."
    )

    if not to_add:
        current_run.log_info("No new org units to add to dataset.")
        return

    # Add each missing org unit to Dataset
    for ou in sorted(to_add):
        current_run.log_info(f"Adding orgUnit {ou} to DataSet per mapping.")

        for endpoint in (f"dataSets/{dataset_uid}/organisationUnits/{ou}",):
            try:
                res = dhis2.api.post(endpoint=endpoint)
                status = getattr(res, "status_code", None)
                if status in (200, 201):
                    current_run.log_info(f"Added orgUnit {ou} to '{endpoint}' (status={status}).")
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


@esigl_import_dhis2.task
def extract_data_from_esigl(
    metabase: CustomConnection,
    df_ou_mapping: pl.DataFrame,
    df_de: pl.DataFrame,
    start_date: str,
    end_date: str,
    months_back: int,
    facilities_code: list[str] | None = None,
    products_code: list[str] | None = None,
) -> pl.DataFrame:
    """Extrait et transforme les données depuis Metabase.

    Args:
        metabase: Connexion Metabase
        df_ou_mapping: DataFrame mapping des unités organisationnelles
        df_de: DataFrame mapping des dataElements
        start_date: Date de début pour l'extraction des données
        end_date: Date de fin pour l'extraction des données
        months_back: Historique en mois à rafraîchir
        facilities_code: Code des établissements eSIGL pour filtrer les données
        products_code: Code des produits eSIGL pour filtrer les données

    Returns:
        DataFrame combinant les données métier et les métadonnées
    """
    mb_client = Metabase(metabase)

    check_metabase_server_health(mb_client)

    # Conversion et gestion des dates
    start_dt = parse_cutoff_date(start_date) if start_date else datetime.now()
    if not start_date:
        current_run.log_info(
            f"No start date provided, defaulting to current date: {start_dt.strftime('%Y-%m-%d')}"
        )

    end_dt = parse_cutoff_date(end_date) if end_date else start_dt + relativedelta(day=31)
    if not end_date:
        current_run.log_info(
            f"No end date provided, defaulting to end of month: {end_dt.strftime('%Y-%m-%d')}"
        )

    if months_back:
        start_dt = start_dt - relativedelta(months=months_back)
        current_run.log_info(
            f"Adjusting start date to {start_dt.strftime('%Y-%m-%d')} "
            f"based on months_back parameter"
        )

    current_run.log_info(
        f"Extracting data from eSIGL from period: "
        f"`{start_dt.strftime('%Y-%m-%d')}` to `{end_dt.strftime('%Y-%m-%d')}`"
    )

    processing_periods = f""" pp.startdate BETWEEN '{start_dt.strftime("%Y-%m-%d")}'::date AND '{end_dt.strftime("%Y-%m-%d")}'::date"""  # noqa: E501

    query_etat_stock = QUERY_ETAT_STOCK_GTC

    if facilities_code:
        df_site = pl.DataFrame(mb_client.get_data_from_sql_query("SELECT code FROM facilities"))

        wrong_facilites_code = [
            code for code in facilities_code if code not in df_site["code"].to_list()
        ]
        if wrong_facilites_code:
            error_msg = (
                f"Invalid facilities code: {', '.join(wrong_facilites_code)}. "
                "Please check the facilities code in eSIGL."
            )
            current_run.log_critical(error_msg)

        facilities_code = [code for code in facilities_code if code not in wrong_facilites_code]

        query_etat_stock += f" AND facilities.code IN {tuple(facilities_code) if len(facilities_code) > 1 else f'({facilities_code[0]!r})'}"  # noqa: E501

        current_run.log_info(f"Filtering data for facilities: {', '.join(facilities_code)}")

    if products_code:
        wrong_product_code = [
            code for code in products_code if code not in df_de["code"].unique().to_list()
        ]
        if wrong_product_code:
            error_msg = (
                f"Invalid product code: {', '.join(wrong_product_code)}. "
                "Please check the product code in eSIGL or DEDOP."
            )
            current_run.log_critical(error_msg)

        products_code = [code for code in products_code if code not in wrong_product_code]
        if not products_code:
            error_msg = "No valid product codes provided after validation."
            current_run.log_critical(error_msg)

        current_run.log_info(f"Filtering data for products: {', '.join(products_code)}")
    else:
        products_code = df_de["code"].unique().to_list()

    extended_product_code = [
        EXTENDED_PRODUCT_CODE[code_produit]
        for code_produit in products_code
        if code_produit in EXTENDED_PRODUCT_CODE
    ]  # type: ignore
    if extended_product_code:
        products_code.extend(extended_product_code)  # type: ignore

    products_code = f" rli.productcode IN {tuple(products_code) if len(products_code) > 1 else f'({products_code[0]!r})'}"  # type: ignore # noqa: E501

    query_etat_stock = query_etat_stock.format(
        products_code=products_code, processing_periods=processing_periods
    )
    current_run.log_debug(query_etat_stock)

    df_etat_stock = pl.DataFrame(mb_client.get_data_from_sql_query(query_etat_stock))

    # Jointure des métadonnées
    mapping = {v: k for k, v in EXTENDED_PRODUCT_CODE.items()}
    df_etat_stock = df_etat_stock.with_columns(pl.col("code_produit").cast(pl.String)).with_columns(
        pl.col("code_produit").replace(mapping).alias("code_produit")
    )

    df_etat_stock = df_etat_stock.join(
        df_de.select(["code", "id"]).rename({"code": "code_produit", "id": "dataElement"}),
        on="code_produit",
        how="inner",
    )

    df_ou_mapping = (
        df_ou_mapping.filter(pl.col("ID_Dhis2").is_not_null())
        .select(pl.col("New_Code").cast(str), pl.col("ID_Dhis2"))
        .rename({"New_Code": "code_site", "ID_Dhis2": "orgUnit"})
    )
    # The first join recover id district from eSIGL and the last one map this id to OrgUnit ID DHIS2
    df_etat_stock = df_etat_stock.join(
        df_ou_mapping,
        on="code_site",
        how="inner",
    )

    # En raison du fait qu'il peut y avoir des unités d'organisation DEDOP mappé à un
    # ou plusieurs site eSIGL, on doit sommer les valeurs par
    # dataElement, attributeOptionCombo, orgUnit et period
    df_etat_stock = (
        df_etat_stock.select(
            pl.col("period"),
            pl.col("orgUnit"),
            pl.col("dataElement"),
            pl.col(pl.NUMERIC_DTYPES),
        )
        .group_by(["period", "orgUnit", "dataElement"])
        .agg(pl.col(pl.NUMERIC_DTYPES).sum())
    )
    df_etat_stock = df_etat_stock.with_columns(pl.col(pl.NUMERIC_DTYPES).round(0).cast(pl.Int64))

    current_run.log_info(f"Extracted {df_etat_stock.shape[0]} records from eSIGL")
    return df_etat_stock


@esigl_import_dhis2.task
def prepare_data_for_dhis2(
    df: pl.DataFrame, df_coc_mapping: pl.DataFrame, dhis2_aoc: str
) -> list[dict]:
    """Prépare le payload DHIS2 à partir des données brutes.

    Args:
        df: DataFrame des données combinées
        df_coc_mapping: Mapping des COC
        dhis2_aoc: Attribute option combo

    Returns:
        Liste de dictionnaires au format DHIS2
    """
    dfs: list[pl.DataFrame] = []
    for row in df_coc_mapping.iter_rows(named=True):
        col_name = row["col"]
        subset = df.filter(pl.col(col_name).is_not_null())
        if subset.is_empty():
            continue
        dfs.append(
            subset.select(
                pl.col("dataElement"),
                pl.lit(row["coc"]).alias("categoryOptionCombo"),
                pl.lit(dhis2_aoc).alias("attributeOptionCombo"),
                pl.col("orgUnit"),
                pl.col("period"),
                pl.col(col_name).cast(pl.String).alias("value"),
            )
        )
    if not dfs:
        return []

    return pl.concat(dfs).to_dicts()


@esigl_import_dhis2.task
def push_data_to_dhis2(
    dhis2: DHIS2,
    payload: list[dict],
    dry_run: bool,
    import_mode: str = "CREATE_AND_UPDATE",
    post_batch_size: int = 5000,
    add_missing_ou: str | None = None,
) -> dict:
    """Envoi des données à DHIS2 avec découpage en chunks et retry.

    Args:
        dhis2: Client DHIS2 configuré
        payload: Données à importer
        dry_run: Mode test sans écriture
        import_mode: Stratégie d'import DHIS2 (CREATE, UPDATE, CREATE_AND_UPDATE)
        post_batch_size: Taille des lots pour les requêtes POST DHIS2
        add_missing_ou: Résultat de l'ajout des org units manquantes

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

    for idx, chunk in enumerate(_chunks(payload, post_batch_size), start=1):
        import_counts = {"imported": 0, "updated": 0, "ignored": 0, "deleted": 0}
        issues: list = []

        response = None
        for attempt in range(1, max_retries + 1):
            response = dhis2.api.post(
                endpoint="dataValueSets", json={"dataValues": chunk}, params=request_params
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


@esigl_import_dhis2.task
def write_import_report(output_dir: Path, payload: list[dict], summary: dict) -> None:
    """Génère les rapports d'import.

    Args:
        output_dir: Répertoire de sortie
        payload: Données envoyées
        summary: Résumé DHIS2
    """
    output_dir = Path(
        workspace.files_path, output_dir, datetime.now(tz=UTC).strftime("%Y-%m-%d_%H-%M-%S")
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


@esigl_import_dhis2.task
def cleanup_old_directory_files(
    output_dir: Path, payload: list[dict], summary: dict, retention_days: int = 20
) -> None:
    """Supprime les anciens fichiers de rapport.

    Pour avoir une chronologie des exécutions des tâches les deux paramètres
    ont été rajoutés mais ils ne sont pas utilisés dans la tâche.

    Args:
        output_dir: Répertoire de sortie
        payload: Données envoyées
        summary: Résumé de l'import DHIS2
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
    esigl_import_dhis2()
