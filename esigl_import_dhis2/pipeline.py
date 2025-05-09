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
from queries import QUERY_DISTRICT, QUERY_ETAT_STOCK


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
    "file_path_coc_mapping",
    type=str,
    name="File path for coc mapping",
    help="File path for coc mapping cols located in directory `metabase eSIGL/data/ressources`",
    default="metabase eSIGL/data/ressources/mapping_coc.json",
    required=True,
)
@parameter(
    "file_path_district_mapping",
    type=str,
    name="File path district mapping",
    help="File path for district mapping cols located in directory `metabase eSIGL/data/ressources`",  # noqa: E501
    default="metabase eSIGL/data/ressources/mapping_district.json",
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
    "months_back",
    type=int,
    name="Historical period in months",
    help="Number of months to look back from current month",
    default=3,
    choices=[1, 3, 6, 12],
    required=True,
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
    file_path_coc_mapping: str,
    file_path_district_mapping: str,
    output_directory: str,
    dhis2_aoc: str = "HllvX50cXC0",
    months_back: int = 3,
    dry_run: bool = False,
):
    """Orchestre le processus complet d'import des données eSIGL -> DHIS2.

    Args:
        dhis2_connection: Connexion DHIS2 cible
        metabase_connection: Connexion Metabase source
        file_path_coc_mapping: Chemin du mapping COC
        file_path_district_mapping: Chemin du mapping districts
        output_directory: Répertoire de sortie
        dhis2_aoc: COC attribut DHIS2
        months_back: Nombre de mois à rafraîchir
        dry_run: Mode test sans écriture
    """
    df_district_mapping = read_ressources_files(
        file_path_district_mapping, ["orgUnit", "id_district"]
    )
    df_coc_mapping = read_ressources_files(file_path_coc_mapping, ["coc", "col"])

    dhis2 = DHIS2(connection=dhis2_connection, cache_dir=Path(workspace.files_path, ".cache"))

    df_etat_stock = extract_data_from_esigl(
        dhis2, metabase_connection, df_district_mapping, months_back
    )

    payload = prepare_data_for_dhis2(df_etat_stock, df_coc_mapping, dhis2_aoc)

    summary = push_data_to_dhis2(dhis2, payload, dry_run)
    write_import_report(output_directory, payload, summary)


@esigl_import_dhis2.task
def read_ressources_files(file_path: str, schema: list) -> pl.DataFrame:
    """Charge un fichier JSON de mapping en DataFrame.

    Args:
        file_path: Chemin relatif depuis le répertoire de travail
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

    with Path.open(full_path.as_posix(), encoding="utf-8") as file:
        dico_map = json.load(file)

    return pl.DataFrame(
        data=list(dico_map.items()),
        schema=schema,
        orient="row",
    )


@esigl_import_dhis2.task
def extract_data_from_esigl(
    dhis2: DHIS2,
    metabase: CustomConnection,
    df_district_mapping: pl.DataFrame,
    months: int,
) -> pl.DataFrame:
    """Extrait et transforme les données depuis Metabase.

    Args:
        dhis2: Client DHIS2 configuré
        metabase: Connexion Metabase
        df_district_mapping: Mapping des districts
        months: Historique en mois à rafraîchir

    Returns:
        DataFrame combinant les données métier et les métadonnées
    """
    mb_client = Metabase(metabase)

    # Récupération des métadonnées produits
    df_products = pl.DataFrame(dhis2.meta.data_elements(fields="code,id,name")).filter(
        pl.col("name").str.contains("PNLP") & pl.col("code").is_not_null()
    )

    # Chargement des données stock
    df_etat_stock = pl.DataFrame(
        mb_client.get_data_from_sql_query(
            QUERY_ETAT_STOCK.format(
                products_code=tuple(df_products["code"].unique().to_list()), lookback_months=months
            )
        )
    )

    # Jointure des métadonnées
    df_etat_stock = df_etat_stock.join(
        df_products.select(["code", "id"]).rename({"id": "dataElement"}),
        left_on="code_produit",
        right_on="code",
        how="left",
    )

    # Mapping des districts
    df_district_esigl = (
        pl.DataFrame(mb_client.get_data_from_sql_query(QUERY_DISTRICT))
        .select(["district", "id_district"])
        .unique()
    )

    # The first join recover id district from eSIGL and the last one map this id to OrgUnit ID DHIS2
    return (
        df_etat_stock.join(df_district_esigl, on="district", how="left")
        .join(df_district_mapping, on="id_district", how="left")
        .with_columns(
            pl.col("startdate")
            .map_elements(lambda x: x[:7].replace("-", ""), return_dtype=pl.String)
            .alias("period")
        )
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
        payload.extend(
            df.select(
                pl.col("dataElement"),
                pl.lit(row["coc"]).alias("categoryOptionCombo"),
                pl.lit("HllvX50cXC0").alias("attributeOptionCombo"),
                pl.col("orgUnit"),
                pl.col("period"),
                pl.col(row["col"]).fill_null(0).round(2).cast(str).alias("value"),
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
