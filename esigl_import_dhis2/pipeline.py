"""Template for newly generated pipelines."""

import json
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
from metabase import Metabase
from openhexa.sdk import (
    CustomConnection,
    DHIS2Connection,
    current_run,
    parameter,
    pipeline,
    workspace,
)
from openhexa.toolbox.dhis2 import DHIS2
from queries import QUERY_ETAT_STOCK


@pipeline("esigl_import_dhis2")
@parameter(
    "dhis2_connection",
    type=DHIS2Connection,
    name="Target DHIS2 instance",
    help="Target DHIS2 instance",
    default="dhis2-nmdr",
    required=True,
)
@parameter(
    "metabase_connection",
    type=CustomConnection,
    default="metabase-esigl",
    name="Metabase instance",
    help="Metabase instance",
    required=True,
)
@parameter(
    "fp_coc_mapping",
    type=str,
    name="File path for coc mapping",
    help="File path for coc mapping cols located in directory `metabase eSIGL/data/ressources`",
    default="metabase eSIGL/data/ressources/mapping_coc.json",
    required=True,
)
@parameter(
    "fp_ou_de_mapping",
    type=str,
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
    type=str,
    name="Output directory",
    help="Directory to save the output files",
    default="metabase eSIGL/data/output",
    required=True,
)
@parameter(
    "dhis2_aoc",
    type=str,
    name="DHIS2 attribute option combo",
    help="DHIS2 attribute option combo",
    default="HllvX50cXC0",
    required=True,
)
@parameter(
    code="start_date",
    type=str,
    name="Start date (YYYY-MM-DD)",
    help="Start date for eSIGL data extraction",
    required=False,
    default=None,
)
@parameter(
    code="end_date",
    type=str,
    name="End date (YYYY-MM-DD)",
    help="End date for eSIGL data extraction",
    required=False,
    default=None,
)
@parameter(
    "months_back",
    type=int,
    name="Historical period in months",
    help="Number of months to look back from current month",
    default=3,
    choices=[1, 3, 6, 12],
    required=True,
)
@parameter(
    "facilities_code",
    type=str,
    name="eSIGL facilities code",
    help="eSIGL facilities code to filter data",
    required=False,
    multiple=True,
)
@parameter(
    "dry_run",
    type=bool,
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
    dry_run: bool = False,
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
        dry_run: Mode test sans écriture
    """
    df_ou_mapping = read_ressources_files(file_path=fp_ou_de_mapping, sheet_name="OrgUnit")
    df_de_mapping = read_ressources_files(file_path=fp_ou_de_mapping, sheet_name="DataElement")
    df_coc_mapping = read_ressources_files(fp_coc_mapping, ["coc", "col"])

    dhis2 = DHIS2(connection=dhis2_connection, cache_dir=Path(workspace.files_path, ".cache"))

    df_etat_stock = extract_data_from_esigl(
        metabase_connection,
        df_ou_mapping,
        df_de_mapping,
        start_date,
        end_date,
        months_back,
        facilities_code,
    )

    payload = prepare_data_for_dhis2(df_etat_stock, df_coc_mapping, dhis2_aoc)

    summary = push_data_to_dhis2(dhis2, payload, dry_run)
    write_import_report(output_directory, payload, summary)


@esigl_import_dhis2.task
def read_ressources_files(
    file_path: str, sheet_name: str | None = None, schema: list | None = None
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
    full_path = Path(workspace.files_path) / file_path
    if not full_path.exists():
        error_msg = f"File not found : {full_path.as_posix()}"
        current_run.log_error(error_msg)
        raise FileNotFoundError(error_msg)

    if full_path.suffix == ".json":
        with Path.open(full_path.as_posix(), encoding="utf-8") as file:
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
def extract_data_from_esigl(
    metabase: CustomConnection,
    df_ou_mapping: pl.DataFrame,
    df_de_mapping: pl.DataFrame,
    start_date: str,
    end_date: str,
    months: int,
    facilities_code: list[str] | None = None,
) -> pl.DataFrame:
    """Extrait et transforme les données depuis Metabase.

    Args:
        metabase: Connexion Metabase
        df_ou_mapping: DataFrame mapping des unités organisationnelles
        df_de_mapping: DataFrame mapping des dataElements
        start_date: Date de début pour l'extraction des données
        end_date: Date de fin pour l'extraction des données
        months: Historique en mois à rafraîchir
        facilities_code: Code des établissements eSIGL pour filtrer les données

    Returns:
        DataFrame combinant les données métier et les métadonnées
    """
    mb_client = Metabase(metabase)

    # Chargement des données stock
    if start_date and end_date:
        try:
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError as err:
            error_msg = "Invalid date format. Please use YYYY-MM-DD."
            current_run.log_error(error_msg)
            raise ValueError(error_msg) from err
        if start_date > end_date:
            error_msg = (
                f"Start date `{start_date.strftime('%Y-%m-%d')}` must be before end date "
                f"`{end_date.strftime('%Y-%m-%d')}`."
            )
            current_run.log_error(error_msg)
            raise ValueError(error_msg)

        start_date = start_date.strftime("%Y-%m-%d")
        end_date = end_date.strftime("%Y-%m-%d")
        current_run.log_info(
            f"Extracting data from eSIGL from period: `{start_date}` to `{end_date}`"
        )
        processing_periods = (
            f"""processing_periods.enddate BETWEEN '{start_date}'::date AND '{end_date}'::date"""
        )
    else:
        current_run.log_info(f"Extracting data from eSIGL from last {months} months from today")
        processing_periods = f"""processing_periods.enddate >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '{months} months'"""  # noqa: E501

    query_etat_stock = QUERY_ETAT_STOCK

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

    df_etat_stock = pl.DataFrame(
        mb_client.get_data_from_sql_query(
            query_etat_stock.format(
                products_code=tuple(df_de_mapping["code_produit"].unique().to_list()),
                processing_periods=processing_periods,
            )
        )
    )

    # Jointure des métadonnées
    df_etat_stock = df_etat_stock.join(
        df_de_mapping.select(["code_produit", "dataElement"]),
        on="code_produit",
        how="left",
    )

    # The first join recover id district from eSIGL and the last one map this id to OrgUnit ID DHIS2
    return df_etat_stock.join(
        df_ou_mapping.filter(pl.col("ID_Dhis2").is_not_null())
        .select(["New_Code", "ID_Dhis2"])
        .rename({"New_Code": "code_site", "ID_Dhis2": "orgUnit"}),
        on="code_site",
        how="left",
    ).with_columns(
        pl.col("enddate")
        .map_elements(lambda x: x[:7].replace("-", ""), return_dtype=pl.String)
        .alias("period")
    )


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
    payload = []
    for row in df_coc_mapping.iter_rows(named=True):
        new_df = df.filter(pl.col(row["col"]).is_not_null())
        payload.extend(
            new_df.select(
                pl.col("dataElement"),
                pl.lit(row["coc"]).alias("categoryOptionCombo"),
                pl.lit("HllvX50cXC0").alias("attributeOptionCombo"),
                pl.col("orgUnit"),
                pl.col("period"),
                pl.col(row["col"]).cast(int).cast(str).alias("value"),
            ).to_dicts()
        )

    return payload


@esigl_import_dhis2.task
def push_data_to_dhis2(
    dhis2: DHIS2,
    payload: list[dict],
    dry_run: bool,
) -> dict:
    """Envoi des données à DHIS2.

    Args:
        dhis2: Client DHIS2 configuré
        payload: Données à importer
        dry_run: Mode test

    Returns:
        Résumé de l'import DHIS2
    """
    dhis2.data_value_sets.MAX_POST_DATA_VALUES = 1000

    summary = dhis2.data_value_sets.post(
        data_values=payload,
        import_strategy="CREATE_AND_UPDATE",
        dry_run=dry_run,
        skip_validation=True,
    )
    msg = f"Imported {len(payload)} data values to DHIS2"
    current_run.log_info(msg)

    return summary


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
    output_dir.mkdir(parents=True, exist_ok=True)

    fp = output_dir / "payload.json"
    with Path.open(fp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    fp = output_dir / "report.json"
    with Path.open(fp, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    msg = f"Import report written to {output_dir.as_posix()}"
    current_run.log_info(msg)

    current_run.add_file_output((output_dir / "payload.json").as_posix())
    current_run.add_file_output((output_dir / "report.json").as_posix())


if __name__ == "__main__":
    esigl_import_dhis2()
