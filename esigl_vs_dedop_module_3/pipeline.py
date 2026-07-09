"""Pipeline for eSIGL vs Dedop Module 3 data comparison and analysis."""

import json
import locale
from datetime import datetime
from pathlib import Path
from typing import Literal

import polars as pl
import psycopg2
from constants import COC_MAPPING, EXTENDED_PRODUCT_CODE
from dateutil import rrule
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
from openhexa.toolbox.dhis2.dataframe import extract_data_elements, get_organisation_units
from queries import NEW_QUERY_ETAT_STOCK_GTC, QUERY_ETAT_STOCK
from utils import (
    check_metabase_server_health,
    check_server_health,
    get_date_report,
    last_analytics_update,
    parse_cutoff_date,
    process_dates_from_df,
)


@pipeline("esigl_vs_dedop_module_3")
@parameter(
    "dedop_connection",
    type=DHIS2Connection,  # type: ignore
    name="Target DHIS2 instance",
    help="Target DHIS2 instance",
    default="dhis2-nmdr-temp",
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
    "fp_ou_de_mapping",
    type=File,
    name="File path OrgUnit and dataElement eSIGL to DHIS2",
    help=(
        "File path OrgUnit and dataElement mapping eSIGL to DHIS2 "
        "located in directory `metabase eSIGL/data/ressources`"
    ),
    default="metabase eSIGL/data/ressources/Fichier mapping OrgUnit eSIGL DHIS2.xlsx",
    required=True,
)
def esigl_vs_dedop_module_3(
    dedop_connection: DHIS2Connection,
    metabase_connection: CustomConnection,
    fp_ou_de_mapping: str,
    start_date: str | None,
    end_date: str | None,
    months_back: int,
):
    """
    Orchestrates the eSIGL vs Dedop Module 3 data comparison and analysis pipeline.

    Parameters.
    ----------
        dedop_connection : DHIS2Connection
            Connection parameters for the Dedop DHIS2 instance.
        metabase_connection : CustomConnection
            Connection parameters for the Metabase instance.
        fp_ou_de_mapping : str
            File path for OrgUnit and dataElement mapping eSIGL to DHIS2.
        start_date : str | None
            Start date for DHIS2 extraction (format: YYYY-MM-DD)
        end_date : str | None
            End date for DHIS2 extraction (format: YYYY-MM-DD)
        months_back : int
            Number of months to look back from the current month to refresh data.
    """
    # Chargement des fichiers de ressources
    df_ou_mapping = read_ressources_files(file_path=fp_ou_de_mapping, sheet_name="OrgUnit")
    df_de_mapping = read_ressources_files(
        file_path=fp_ou_de_mapping, sheet_name="DataElementOldCode"
    )

    dedop = DHIS2(connection=dedop_connection)
    metabase = Metabase(metabase_connection)

    check_server_health(dedop)
    check_metabase_server_health(metabase)

    if last_update_dedop := last_analytics_update(dedop):
        current_run.log_info(
            "Dernière mise à jour des tables analytiques DEDOP: "
            f"{last_update_dedop.strftime('%Y-%m-%d %H:%M:%S')}"
        )
    # Extraction des unités d'organisation
    df_org_units = fetch_organisation_units(dedop)

    # Extraction des éléments de données
    data_elements_routine = fetch_routine_data_elements(dhis2=dedop)
    data_elements_gtc = fetch_gtc_data_elements(dhis2=dedop)

    # Traitement des périodes
    periods_range = process_periods(
        start_date=start_date, end_date=end_date, months_back=months_back
    )

    # Extraction des données DHIS2
    df_data_ddp = fetch_dhis2_data(
        dhis2=dedop,
        data_elements_routine=data_elements_routine,
        data_elements_gtc=data_elements_gtc,
        periods_range=periods_range,
    )

    # Extraction des données Metabase
    df_metabase_routine = fetch_metabase_routine_data(
        metabase=metabase,
        df_ou_mapping=df_ou_mapping,
        df_de_mapping=df_de_mapping,
        data_elements_routine=data_elements_routine,
        periods_range=periods_range,
    )

    df_metabase_gtc = fetch_metabase_gtc_data(
        metabase=metabase,
        df_ou_mapping=df_ou_mapping,
        data_elements_gtc=data_elements_gtc,
        periods_range=periods_range,
    )

    # Evaluation globale de la cohérence des données entre esigl et dedop
    df_compare = compare_esigl_dedop(
        dhis2=dedop,
        df_data_ddp=df_data_ddp,
        df_metabase_routine=df_metabase_routine,
        df_metabase_gtc=df_metabase_gtc,
    )

    # Evaluation de la cohérence des données
    df_coherence = evaluate_data_coherence(dhis2=dedop, df_compare=df_compare)

    # Evaluation de la complétude des donnée
    df_completude = evaluate_data_completeness(
        dhis2=dedop,
        df_data_ddp=df_data_ddp,
        df_metabase_routine=df_metabase_routine,
        df_metabase_gtc=df_metabase_gtc,
    )

    # Standardisation des données ajout des informations d'unité d'organisation
    df_compare = process_data_with_org_units(
        df_data=df_compare, df_org_units=df_org_units, table_name="esigl_vs_dedop_data_module_3"
    )

    # Cohérence avec tracabilité
    df_coherence_tracabilite = process_data_with_org_units(
        df_data=df_coherence,
        df_org_units=df_org_units,
        table_name="esigl_vs_dedop_data_module_3_coherence_tracabilite",
    )

    df_coherence = process_data_with_org_units(
        df_data=df_coherence,
        df_org_units=df_org_units,
        table_name="esigl_vs_dedop_data_module_3_coherence",
    )

    # Complétude par district
    df_completude_district = process_data_with_org_units(
        df_data=df_completude,
        df_org_units=df_org_units,
        table_name="esigl_vs_dedop_data_module_3_completude_district",
    )

    df_completude = process_data_with_org_units(
        df_data=df_completude,
        df_org_units=df_org_units,
        table_name="esigl_vs_dedop_data_module_3_completude",
    )

    df_dim_date = get_dates_from_df(df_compare, df_completude)

    # Exportation des données vers la BD
    export_to_database(df_data=df_compare, table_name="esigl_vs_dedop_data_module_3")
    export_to_database(df_data=df_coherence, table_name="esigl_vs_dedop_data_module_3_coherence")
    export_to_database(
        df_data=df_coherence_tracabilite,
        table_name="esigl_vs_dedop_data_module_3_coherence_tracabilite",
    )
    export_to_database(df_data=df_completude, table_name="esigl_vs_dedop_data_module_3_completude")
    export_to_database(
        df_data=df_completude_district,
        table_name="esigl_vs_dedop_data_module_3_completude_district",
    )

    export_to_database(df_data=df_dim_date, table_name="dim_dedop_dim_report_two")


@esigl_vs_dedop_module_3.task
def fetch_organisation_units(dedop: DHIS2) -> pl.DataFrame:
    """
    Fetch organisation units from the Dedop DHIS2 instance.

    Parameters
    ----------
    dedop : DHIS2
        The DHIS2 instance for Dedop.

    Returns
    -------
    pl.DataFrame
        A DataFrame containing organisation units from both instances.
    """
    current_run.log_info("⏳ Extraction des unités d'organisation depuis les instances DHIS2")

    df_org_units = get_organisation_units(dedop)

    df_org_units = df_org_units.unique(subset="id", keep="first")
    df_org_units = df_org_units.with_columns(
        [
            # Extraire directement les coordonnées du MultiPolygon
            pl.col("geometry")
            .map_elements(
                lambda geom: (
                    eval(geom)["coordinates"][0][0] if isinstance(eval(geom), dict) else None
                ),
                return_dtype=pl.Object,
            )
            .alias("coordinates")
        ]
    )

    df_org_units = df_org_units.with_columns(
        [pl.col("coordinates").map_elements(json.dumps, return_dtype=pl.Utf8).alias("coordinates")]
    )

    current_run.log_info("✅ Extraction des unités d'organisation terminée.")
    return df_org_units


@esigl_vs_dedop_module_3.task
def process_periods(
    start_date: str | None, end_date: str | None, months_back: int = 2
) -> list[str]:
    """
    Traite les périodes selon les dates et le décalage temporel.

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

    end_dt = parse_cutoff_date(end_date) if end_date else start_dt + relativedelta(day=31)
    if not end_date:
        current_run.log_info(
            f"Date de fin absente, utilisation fin de mois: {end_dt.strftime('%Y-%m-%d')}"
        )

    if months_back:
        start_dt = start_dt - relativedelta(months=months_back)
        current_run.log_info(
            f"Recul de {months_back} mois appliqué: nouvelle date début "
            f"{start_dt.strftime('%Y-%m-%d')}"
        )

    if start_dt == end_dt:
        return [start_dt.strftime("%Y-%m")]

    if start_dt > end_dt:
        current_run.log_error(
            f"Incohérence temporelle: date de début {start_dt.strftime('%Y-%m-%d')} "
            f"postérieure à date de fin {end_dt.strftime('%Y-%m-%d')}"
        )
        raise ValueError("La date de début doit être antérieure ou égale à la date de fin.")

    if start_dt.month == end_dt.month and start_dt.year == end_dt.year:
        return [start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d")]

    dates = list(rrule.rrule(freq=rrule.MONTHLY, dtstart=start_dt, until=end_dt))
    return sorted({dt.strftime("%Y-%m-%d") for dt in dates})


@esigl_vs_dedop_module_3.task
def fetch_routine_data_elements(dhis2: DHIS2) -> pl.DataFrame:
    """
    Récupère les dataElements de routine depuis DHIS2.

    Args:
        dhis2: Client DHIS2 configuré

    Returns:
        DataFrame des dataElements de routine
    """
    df_de = pl.DataFrame(
        dhis2.meta.data_elements(fields="id,name,code,categoryCombo"), infer_schema_length=100000
    )
    df_de = df_de.filter(
        pl.col("code").is_not_null()
        & pl.col("categoryCombo").struct.field("id").str.contains("El9O9wWhg8F")
    )

    current_run.log_info(f"Retrieved {df_de.shape[0]} routine dataElements from DHIS2")
    return df_de


@esigl_vs_dedop_module_3.task
def fetch_gtc_data_elements(dhis2: DHIS2) -> pl.DataFrame:
    """
    Récupère les dataElements GTC depuis DHIS2.

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


@esigl_vs_dedop_module_3.task
def read_ressources_files(
    file_path: File, sheet_name: str | None = None, schema: list | None = None
) -> pl.DataFrame:
    """
    Charge un fichier JSON de mapping en DataFrame.

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


@esigl_vs_dedop_module_3.task
def fetch_dhis2_data(
    dhis2: DHIS2,
    data_elements_routine: pl.DataFrame,
    data_elements_gtc: pl.DataFrame,
    periods_range: list[str],
) -> pl.DataFrame:
    """
    Fetch data from DHIS2 for the specified periods and data elements.

    Parameters
    ----------
    dhis2 : DHIS2
        The DHIS2 instance to fetch data from.
    data_elements_routine : pl.DataFrame
        DataFrame containing the routine data elements to fetch.
    data_elements_gtc : pl.DataFrame
        DataFrame containing the GTC data elements to fetch.
    periods_range : list[str]
        The list of periods to fetch data for (format: YYYY-MM).

    Returns
    -------
    pl.DataFrame
        A DataFrame containing the fetched data.
    """
    periods_range = {
        datetime.strptime(f"{pe}", "%Y-%m-%d").strftime("%Y%m") for pe in periods_range
    }  # type: ignore
    periods_range = sorted(periods_range)
    msg_info = (
        "⏳ Extraction des données DHIS2 depuis le DEDOP "
        f"aux périodes: `{', '.join(periods_range)}`..."
    )
    current_run.log_info(msg_info)

    data_elements = (
        data_elements_routine["id"].unique().to_list() + data_elements_gtc["id"].unique().to_list()
    )

    try:
        df_data_ddp = extract_data_elements(
            dhis2=dhis2,
            data_elements=data_elements,
            org_units=["ZD44Asc0bAk"],
            include_children=True,
            periods=periods_range,  # type: ignore
        )
        # df_data_ddp = dhis2.meta.add_coc_name_column(df_data_ddp, "category_option_combo_id")
        df_data_ddp = df_data_ddp.with_columns(
            pl.col("value").cast(pl.Float64).cast(pl.Int64),
        )  # type: ignore
        current_run.log_info(f"Extracted {df_data_ddp.shape[0]} records from DEDOP")
        current_run.log_debug(f"DHIS2 DataFrame columns: {df_data_ddp.columns}")
        return df_data_ddp
    except Exception as e:
        current_run.log_error(f"Erreur lors de l'extraction depuis {dhis2.api.url}: {e}")
        raise


@esigl_vs_dedop_module_3.task
def fetch_metabase_routine_data(
    metabase: Metabase,
    df_ou_mapping: pl.DataFrame,
    df_de_mapping: pl.DataFrame,
    data_elements_routine: pl.DataFrame,
    periods_range: list[str],
) -> pl.DataFrame:
    """
    Fetch routine data from Metabase for the specified periods and data elements.

    Parameters
    ----------
    metabase : Metabase
        The Metabase instance to fetch data from.
    df_ou_mapping: pl.DataFrame
        DataFrame mapping des unités organisationnelles eSIGL to DHIS2
    df_de_mapping: pl.DataFrame
        DataFrame mapping des dataElements eSIGL to DHIS2
    data_elements_routine : pl.DataFrame
        DataFrame containing the routine data elements to fetch.
    periods_range : list[str]
        The list of periods to fetch data for (format: YYYY-MM).

    Returns
    -------
    pl.DataFrame
        A DataFrame containing the fetched routine data.
    """
    msg_info = (
        "⏳ Extraction des données de routine depuis Metabase "
        f"aux périodes: `{', '.join(periods_range)}`..."
    )
    current_run.log_info(msg_info)

    # Formattage des periodes
    periods_set = {
        period
        for dt in [datetime.strptime(f"{pe}", "%Y-%m-%d") for pe in periods_range]
        for period in get_date_report(dt)
    }
    sorted_periods = sorted(periods_set)

    periods_sql = (
        "(" + ", ".join("'" + p.replace("'", "''").upper() + "'" for p in sorted_periods) + ")"
    )
    processing_periods = f"UPPER(processing_periods.name) IN {periods_sql}"

    # filtre des code produits et extension à partir du mapping
    products_code = data_elements_routine["code"].unique().to_list()

    dico_products = {
        str(row["code_produit"]): row["ancien_code"]
        for row in df_de_mapping.filter(pl.col("code_produit").is_not_null())
        .select(["ancien_code", "code_produit"])
        .iter_rows(named=True)
    }
    extended_product_code = [
        dico_products[code_produit]
        for code_produit in products_code
        if code_produit in dico_products
    ]

    if extended_product_code:
        products_code.extend(extended_product_code)

    current_run.log_debug(f"Product codes: {products_code}")

    query_etat_stock = QUERY_ETAT_STOCK
    query_etat_stock += f" AND requisition_line_items.productcode IN {tuple(products_code) if len(products_code) > 1 else f'({products_code[0]!r})'}"  # noqa: E501
    sql_query = query_etat_stock.format(processing_periods=processing_periods)

    # current_run.log_debug(sql_query)
    try:
        df_metabase_routine = pl.DataFrame(metabase.get_data_from_sql_query(sql_query=sql_query))
    except Exception as e:
        current_run.log_error(f"Erreur lors de l'extraction depuis Metabase: {e}")
        raise

    # Jointure des métadonnées
    mapping = {v: k for k, v in dico_products.items()}
    df_metabase_routine = df_metabase_routine.with_columns(
        pl.col("code_produit").cast(pl.String)
    ).with_columns(pl.col("code_produit").replace(mapping).alias("code_produit"))

    # Code produit eSIGL -> code produit DHIS2
    df_metabase_routine = df_metabase_routine.join(
        data_elements_routine.select(
            pl.col("id").alias("data_element_id"), pl.col("code").alias("code_produit")
        ),
        on="code_produit",
        how="inner",
    )

    # Code site eSIGL -> orgUnit DHIS2
    df_ou_mapping = (
        df_ou_mapping.filter(pl.col("ID_Dhis2").is_not_null())
        .select(pl.col("New_Code").cast(str), pl.col("ID_Dhis2"))
        .rename({"New_Code": "code_site", "ID_Dhis2": "organisation_unit_id"})
    )
    df_metabase_routine = df_metabase_routine.join(
        df_ou_mapping,
        on="code_site",
        how="inner",
    )
    # Renommage des colonnes d'intérêt
    df_metabase_routine = df_metabase_routine.rename(COC_MAPPING)

    current_run.log_info(f"Extracted {df_metabase_routine.shape[0]} records from eSIGL")
    df_metabase_routine = df_metabase_routine.unpivot(
        index=["data_element_id", "period", "organisation_unit_id"],
        on=list(COC_MAPPING.values()),
        variable_name="category_option_combo_id",
        value_name="value",
    ).with_columns(pl.col("value").cast(pl.Int64))

    df_metabase_routine = df_metabase_routine.group_by(
        ["data_element_id", "period", "organisation_unit_id", "category_option_combo_id"]
    ).agg(pl.col("value").sum().alias("value"))
    current_run.log_debug(f"Routine DataFrame columns: {df_metabase_routine.columns}")
    return df_metabase_routine


@esigl_vs_dedop_module_3.task
def fetch_metabase_gtc_data(
    metabase: Metabase,
    df_ou_mapping: pl.DataFrame,
    data_elements_gtc: pl.DataFrame,
    periods_range: list[str],
) -> pl.DataFrame:
    """
    Fetch GTC data from Metabase for the specified periods and data elements.

    Parameters
    ----------
    metabase : Metabase
        The Metabase instance to fetch data from.
    df_ou_mapping: pl.DataFrame
        DataFrame mapping des unités organisationnelles eSIGL to DHIS2
    data_elements_gtc : pl.DataFrame
        DataFrame containing the GTC data elements to fetch.
    periods_range : list[str]
        The list of periods to fetch data for (format: YYYY-MM).

    Returns
    -------
    pl.DataFrame
        A DataFrame containing the fetched GTC data.
    """
    periods_range = [datetime.strptime(f"{pe}", "%Y-%m-%d") for pe in periods_range]  # type: ignore
    start_dt = min(periods_range)
    end_dt = max(periods_range)
    # Adjust to first and last day of month
    start_dt = start_dt.replace(day=1) - relativedelta(months=3)  # type: ignore
    end_dt = end_dt + relativedelta(day=31)  # type: ignore

    msg_info = (
        "⏳ Extraction des données GTC depuis Metabase "
        # type: ignore
        f"aux périodes: `{start_dt.strftime('%Y-%m-%d')} - {end_dt.strftime('%Y-%m-%d')}`"
        "recul de 3 mois appliqué pour le calcul des CMM..."
    )
    current_run.log_info(msg_info)

    processing_periods = f""" pp.startdate BETWEEN '{start_dt.strftime("%Y-%m-%d")}'::date AND '{end_dt.strftime("%Y-%m-%d")}'::date"""  # type: ignore # noqa: E501

    # filtre des code produits et extension à partir du mapping
    products_code = data_elements_gtc["code"].unique().to_list()

    extended_product_code = [
        EXTENDED_PRODUCT_CODE[code_produit]
        for code_produit in products_code
        if code_produit in EXTENDED_PRODUCT_CODE
    ]

    extended_product_code = [
        EXTENDED_PRODUCT_CODE[code_produit]
        for code_produit in products_code
        if code_produit in EXTENDED_PRODUCT_CODE
    ]

    if extended_product_code:
        products_code.extend(extended_product_code)

    products_code = f" rli.productcode IN {tuple(products_code) if len(products_code) > 1 else f'({products_code[0]!r})'}"  # type: ignore # noqa: E501

    sql_query = NEW_QUERY_ETAT_STOCK_GTC.format(
        products_code=products_code, processing_periods=processing_periods
    )
    current_run.log_debug(sql_query)
    try:
        df_metabase_gtc = pl.DataFrame(metabase.get_data_from_sql_query(sql_query=sql_query))
    except Exception as e:
        current_run.log_error(f"Erreur lors de l'extraction depuis Metabase: {e}")
        raise

    # Jointure des métadonnées
    mapping = {v: k for k, v in EXTENDED_PRODUCT_CODE.items()}
    df_metabase_gtc = df_metabase_gtc.with_columns(
        pl.col("code_produit").cast(pl.String)
    ).with_columns(pl.col("code_produit").replace(mapping).alias("code_produit"))

    # Code produit eSIGL -> code produit DHIS2
    df_metabase_gtc = df_metabase_gtc.join(
        data_elements_gtc.select(
            pl.col("id").alias("data_element_id"), pl.col("code").alias("code_produit")
        ),
        on="code_produit",
        how="inner",
    )

    # Code site eSIGL -> orgUnit DHIS2
    df_ou_mapping = (
        df_ou_mapping.filter(pl.col("ID_Dhis2").is_not_null())
        .select(pl.col("New_Code").cast(str), pl.col("ID_Dhis2"))
        .rename({"New_Code": "code_site", "ID_Dhis2": "organisation_unit_id"})
    )
    df_metabase_gtc = df_metabase_gtc.join(
        df_ou_mapping,
        on="code_site",
        how="inner",
    )
    current_run.log_info(f"Extracted {df_metabase_gtc.shape[0]} GTC records from eSIGL")

    # Normalisation des données pour le calcul des cmm
    df_cmm = (
        df_metabase_gtc.select(
            pl.col("period"),
            pl.col("organisation_unit_id"),
            pl.col("data_element_id"),
            pl.col("quantite_distribuee"),
        )
        .group_by(["period", "organisation_unit_id", "data_element_id"])
        .agg(pl.col("quantite_distribuee").sum().alias("quantite_distribuee"))
    )
    df_cmm = df_cmm.with_columns(
        pl.col("period").str.strptime(pl.Date, format="%Y%m").alias("period_date")
    ).sort(["organisation_unit_id", "data_element_id", "period_date"])

    df_cmm = (
        df_cmm.group_by(["organisation_unit_id", "data_element_id"])
        .agg(
            [
                pl.col("period_date"),
                pl.col("quantite_distribuee")
                .rolling_mean(window_size=3, min_periods=1)  # type: ignore
                .alias("cmm"),
            ]
        )
        .explode(["period_date", "cmm"])
        .join(df_cmm, on=["organisation_unit_id", "data_element_id", "period_date"], how="left")
    )
    df_metabase_gtc = (
        df_metabase_gtc.with_columns(pl.col("enddate").cast(pl.Datetime))
        .filter(pl.col("enddate") >= start_dt + relativedelta(months=3))
        .select(
            pl.col("period"),
            pl.col("organisation_unit_id"),
            pl.col("data_element_id"),
            pl.col("quantite_recue"),
            pl.col("quantite_distribuee"),
            pl.col("nbrejrsrupture"),
            pl.col("perte_ajustement"),
            pl.col("stock_initial"),
            pl.col("sdu"),
        )
        .group_by(["period", "organisation_unit_id", "data_element_id"])
        .agg(
            pl.col("quantite_recue").sum().alias("quantite_recue"),
            pl.col("quantite_distribuee").sum().alias("quantite_distribuee"),
            pl.col("nbrejrsrupture").sum().alias("nbrejrsrupture"),
            pl.col("perte_ajustement").sum().alias("perte_ajustement"),
            pl.col("stock_initial").first().alias("stock_initial"),
            pl.col("sdu").last().alias("sdu"),
        )
    )
    df_metabase_gtc = df_metabase_gtc.join(
        df_cmm.select(
            pl.col("period"),
            pl.col("organisation_unit_id"),
            pl.col("data_element_id"),
            pl.col("cmm"),
        ),
        on=["period", "organisation_unit_id", "data_element_id"],
        how="left",
    )

    df_metabase_gtc = df_metabase_gtc.with_columns(
        pl.col(pl.NUMERIC_DTYPES).round(0).cast(pl.Int64)
    )
    # df_metabase_gtc = df_metabase_gtc.with_columns(
    #     pl.lit(None).cast(pl.Int64).alias("quantite_proposee"),
    #     pl.lit(None).cast(pl.Int64).alias("quantite_commandee"),
    #     pl.lit(None).cast(pl.Int64).alias("quantite_approuvee"),
    # )

    # Renommage des colonnes d'intérêt
    dico_rename = {k: v for k, v in COC_MAPPING.items() if k in df_metabase_gtc.columns}
    df_metabase_gtc = df_metabase_gtc.rename(dico_rename)
    df_metabase_gtc = df_metabase_gtc.unpivot(
        index=["data_element_id", "period", "organisation_unit_id"],
        on=list(dico_rename.values()),
        variable_name="category_option_combo_id",
        value_name="value",
    ).with_columns(pl.col("value").cast(pl.Int64))

    df_metabase_gtc = df_metabase_gtc.group_by(
        ["data_element_id", "period", "organisation_unit_id", "category_option_combo_id"]
    ).agg(pl.col("value").sum().alias("value"))

    current_run.log_debug(f"GTC DataFrame columns: {df_metabase_gtc.columns}")
    return df_metabase_gtc


@esigl_vs_dedop_module_3.task
def compare_esigl_dedop(
    dhis2: DHIS2,
    df_data_ddp: pl.DataFrame,
    df_metabase_routine: pl.DataFrame,
    df_metabase_gtc: pl.DataFrame,
) -> pl.DataFrame:
    """
    Compare eSIGL data from Metabase with Dedop data from DHIS2.

    Parameters
    ----------
    dhis2 : DHIS2
        The DHIS2 instance to fetch data from.
    df_data_ddp : pl.DataFrame
        DataFrame containing Dedop data from DHIS2.
    df_metabase_routine : pl.DataFrame
        DataFrame containing routine data from Metabase.
    df_metabase_gtc : pl.DataFrame
        DataFrame containing GTC data from Metabase.

    Returns
    -------
    pl.DataFrame
        A DataFrame containing the comparison results.
    """
    current_run.log_info("⏳ Comparing eSIGL data from Metabase with Dedop data from DHIS2...")

    df_metabase = pl.concat([df_metabase_routine, df_metabase_gtc], how="diagonal_relaxed")

    df_merged = df_metabase.join(
        df_data_ddp,
        on=[
            "data_element_id",
            "period",
            "organisation_unit_id",
            "category_option_combo_id",
        ],
        how="full",
        suffix="_ddp",
    )
    df_merged = dhis2.meta.add_dx_name_column(df_merged, "data_element_id")
    df_merged = dhis2.meta.add_org_unit_name_column(df_merged, "organisation_unit_id")
    df_merged = dhis2.meta.add_coc_name_column(df_merged, "category_option_combo_id")

    df_merged = df_merged.rename({"value": "value_esigl"})

    df_merged = df_merged.with_columns(
        pl.when(pl.col("value_esigl").is_null() & pl.col("value_ddp").is_null())
        .then(None)
        .otherwise((pl.col("value_esigl").fill_null(0) - pl.col("value_ddp").fill_null(0)).abs())
        .alias("ecart"),
        pl.when((pl.col("value_esigl").is_null()) | (pl.col("value_esigl") == 0))
        .then(None)
        .when(pl.col("value_ddp").is_null())
        .then(pl.col("value_esigl").abs() / pl.col("value_esigl"))
        .otherwise((pl.col("value_esigl") - pl.col("value_ddp")).abs() / pl.col("value_esigl"))
        .alias("ecart_relatif"),
    )  # type: ignore

    df_merged = df_merged.with_columns(
        pl.when(pl.col("value_esigl") == 0)
        .then(None)
        .otherwise(pl.col("ecart_relatif"))
        .alias("ecart_relatif")
    )

    locale.setlocale(locale.LC_TIME, "fr_FR.UTF-8")
    return df_merged.with_columns(
        pl.col("period")
        .cast(pl.Utf8)
        .str.strptime(pl.Datetime("ns"), "%Y%m")
        .cast(pl.Date)
        .alias("date_report")
    ).with_columns(
        pl.col("period").map_elements(
            lambda col: datetime.strptime(f"{col}01", "%Y%m%d").strftime("%B %Y").capitalize(),
            return_dtype=pl.String,
        ),
        # pl.col("dx_name").str.replace(r"PNLP-|GTC-", "").str.strip_chars().alias("dx_name"),
    )


@esigl_vs_dedop_module_3.task
def evaluate_data_coherence(
    dhis2: DHIS2,
    df_compare: pl.DataFrame,
) -> pl.DataFrame:
    """
    Evaluate overall data coherence between eSIGL and Dedop.

    Parameters
    ----------
    dhis2 : DHIS2
        The DHIS2 instance to fetch data from.
    df_compare : pl.DataFrame
        DataFrame containing the comparison results.

    Returns
    -------
    pl.DataFrame
        A DataFrame containing the coherence evaluation results.
    """
    current_run.log_info("⏳ Evaluating overall data coherence between eSIGL and Dedop...")

    # Placeholder for coherence evaluation logic
    df_coherence = (
        df_compare.with_columns(
            [
                # Cohérence = écart = 0 OU les deux valeurs nulles
                (
                    (pl.col("ecart") == 0)
                    | (pl.col("value_esigl").is_null() & pl.col("value_ddp").is_null())
                ).alias("est_coherent"),
                # Incohérence = écart non nul et au moins une valeur non null
                (pl.col("ecart").is_not_null() & pl.col("ecart") != 0).alias("est_incoherent"),
                # Toutes les lignes comptent comme comparables
                pl.lit(True).alias("est_comparable"),
            ]
        )
        .group_by(
            [
                "date_report",
                "period",
                "organisation_unit_id",
                "data_element_id",
            ]
        )
        .agg(
            [
                pl.sum("est_coherent").alias("nb_coherents"),
                pl.sum("est_incoherent").alias("nb_incoherents"),
                pl.sum("est_comparable").alias("nb_comparables"),
                (pl.sum("est_coherent") / pl.sum("est_comparable")).alias("taux_coherence"),
                (pl.sum("est_incoherent") / pl.sum("est_comparable")).alias("taux_incoherence"),
            ]
        )
        .with_columns(
            [
                (pl.col("taux_coherence") * 100).round(1).alias("taux_coherence"),
                (pl.col("taux_incoherence") * 100).round(1).alias("taux_incoherence"),
            ]
        )
        .select(
            [
                "period",
                "date_report",
                "organisation_unit_id",
                "data_element_id",
                "nb_comparables",
                "nb_coherents",
                "nb_incoherents",
                "taux_coherence",
                "taux_incoherence",
            ]
        )
        .sort("taux_incoherence", descending=True)
    )
    df_coherence = dhis2.meta.add_org_unit_name_column(df_coherence, "organisation_unit_id")
    # type: ignore
    return dhis2.meta.add_dx_name_column(df_coherence, "data_element_id")  # type: ignore


# .with_columns(
#        pl.col("dx_name").str.replace(r"PNLP-|GTC-", "").str.strip_chars().alias("dx_name"),
#   )  # type: ignore


@esigl_vs_dedop_module_3.task
def evaluate_data_completeness(
    dhis2: DHIS2,
    df_data_ddp: pl.DataFrame,
    df_metabase_routine: pl.DataFrame,
    df_metabase_gtc: pl.DataFrame,
) -> pl.DataFrame:
    """
    Evaluate overall data completeness between eSIGL and Dedop.

    Parameters
    ----------
    dhis2 : DHIS2
        The DHIS2 instance to fetch data from.
    df_data_ddp : pl.DataFrame
        DataFrame containing Dedop data from DHIS2.
    df_metabase_routine : pl.DataFrame
        DataFrame containing routine data from Metabase.
    df_metabase_gtc : pl.DataFrame
        DataFrame containing GTC data from Metabase.

    Returns
    -------
    pl.DataFrame
        A DataFrame containing the completeness evaluation results.
    """
    current_run.log_info("⏳ Evaluating overall data completeness between eSIGL and Dedop...")
    selected_cols = [
        "period",
        "organisation_unit_id",
        "data_element_id",
        "category_option_combo_id",
    ]
    df_metabase = pl.concat([df_metabase_routine, df_metabase_gtc], how="diagonal_relaxed")

    df_completude = (
        df_metabase.select(selected_cols)
        .unique()
        .join(
            df_data_ddp.select(selected_cols)
            .unique()
            .with_columns([pl.lit(True).alias("ddp_present")]),
            on=selected_cols,
            how="left",
        )
        .with_columns(
            [pl.when(pl.col("ddp_present").is_null()).then(True).otherwise(False).alias("manquant")]
        )
    )

    df_completude = df_completude.with_columns(
        [pl.when(~pl.col("manquant")).then(1).otherwise(0).alias("present")]
    )
    df_completude = (
        df_completude.group_by(["period", "organisation_unit_id", "data_element_id"])
        .agg(
            [
                pl.len().alias("nb_total_valeurs_esigl"),
                pl.sum("present").alias("nb_valeurs_importe_ddp"),
                (pl.sum("present") / pl.len()).alias("taux_completude"),
            ]
        )
        .with_columns([(pl.col("taux_completude") * 100).round(1)])
        .sort("taux_completude", descending=True)
    )
    locale.setlocale(locale.LC_TIME, "fr_FR.UTF-8")
    df_completude = df_completude.with_columns(
        pl.col("period")
        .cast(pl.Utf8)
        .str.strptime(pl.Datetime("ns"), "%Y%m")
        .cast(pl.Date)
        .alias("date_report")
    ).with_columns(
        pl.col("period").map_elements(
            lambda col: datetime.strptime(f"{col}01", "%Y%m%d").strftime("%B %Y").capitalize(),
            return_dtype=pl.String,
        )
    )

    df_completude = dhis2.meta.add_org_unit_name_column(df_completude, "organisation_unit_id")
    # type: ignore
    return dhis2.meta.add_dx_name_column(df_completude, "data_element_id")  # type: ignore


# .with_columns(
#         pl.col("dx_name").str.replace(r"PNLP-|GTC-", "").str.strip_chars().alias("dx_name"),
#     )  # type: ignore


@esigl_vs_dedop_module_3.task
def process_data_with_org_units(
    df_data: pl.DataFrame, df_org_units: pl.DataFrame, table_name: str
) -> pl.DataFrame:
    """
    Process data by joining with organisation units and saving to a table.

    Parameters
    ----------
    df_data : pl.DataFrame
        The DataFrame to process.
    df_org_units : pl.DataFrame
        The DataFrame containing organisation units.
    table_name : str
        The name of the table to save the processed data to.

    Returns
    -------
    pl.DataFrame
        The processed DataFrame.
    """
    df_processed = df_data.join(
        df_org_units.select(
            [
                pl.col("id"),
                pl.col("level_2_name").alias("region"),
                pl.col("level_3_name").alias("district"),
                pl.col("level_3_id").alias("level_3_id"),
                pl.col("level").alias("type_ou"),
            ]
        ),
        left_on="organisation_unit_id",
        right_on="id",
    ).with_columns(
        pl.when(pl.col("type_ou") == 4)
        .then(pl.lit("Établissement sanitaire"))
        .when(pl.col("type_ou") == 3)
        .then(pl.lit("District sanitaire"))
        .when(pl.col("type_ou") == 2)
        .then(pl.lit("Région sanitaire"))
        .otherwise(pl.lit("level_5"))
        .alias("type_ou"),
    )
    if "ou_name" in df_processed.columns:
        df_processed = df_processed.with_columns(
            pl.when((pl.col("ou_name") == "SAN PEDRO") & pl.col("district").is_null())
            .then(pl.lit("SAN-PEDRO"))
            .otherwise(pl.col("district"))
            .alias("district")
        )

    if table_name == "esigl_vs_dedop_data_module_3":
        df_processed = (
            df_processed.select(
                [
                    "period",
                    "date_report",
                    "region",
                    "district",
                    "ou_name",
                    "type_ou",
                    "dx_name",
                    "co_name",
                    "value_esigl",
                    "value_ddp",
                    "ecart",
                    "ecart_relatif",
                    "created",
                ],
            )
            .filter(pl.col("ecart") != 0)
            .with_columns(
                pl.col("ecart_relatif").fill_null(0).round(2),
                pl.col("co_name").str.replace("SIGL-", "").str.strip_chars().alias("co_name"),
            )
        )

    if table_name == "esigl_vs_dedop_data_module_3_coherence":
        df_processed = df_processed.select(
            [
                "period",
                "date_report",
                "region",
                "district",
                "ou_name",
                "type_ou",
                "dx_name",
                "nb_comparables",
                "nb_coherents",
                "nb_incoherents",
                "taux_coherence",
                "taux_incoherence",
            ],
        )

    if table_name == "esigl_vs_dedop_data_module_3_coherence_tracabilite":
        df_processed = (
            df_processed.with_columns(
                pl.lit(datetime.now()).cast(pl.Date).alias("date_controle"),
            )
            .select(
                [
                    "period",
                    "date_report",
                    "region",
                    "district",
                    "date_controle",
                    # "ou_name",
                    # "dx_name",
                    "nb_comparables",
                    "nb_coherents",
                    "nb_incoherents",
                ],
            )
            .group_by(
                [
                    "period",
                    "date_report",
                    "region",
                    "district",
                    "date_controle",
                    # "ou_name",
                    # "dx_name",
                ]
            )
            .agg(
                [
                    pl.sum("nb_comparables").alias("nb_comparables"),
                    pl.sum("nb_coherents").alias("nb_coherents"),
                    pl.sum("nb_incoherents").alias("nb_incoherents"),
                ]
            )
        )

    if table_name == "esigl_vs_dedop_data_module_3_completude":
        df_processed = df_processed.select(
            [
                "period",
                "date_report",
                "region",
                "district",
                "ou_name",
                "type_ou",
                "dx_name",
                "nb_total_valeurs_esigl",
                "nb_valeurs_importe_ddp",
                "taux_completude",
            ],
        )
    if table_name == "esigl_vs_dedop_data_module_3_completude_district":
        df_processed = (
            df_processed.join(
                df_org_units.select(
                    [
                        pl.col("id"),
                        pl.col("coordinates").alias("coordinates"),
                    ]
                ),
                left_on="level_3_id",
                right_on="id",
            )
            .select(
                [
                    "period",
                    "date_report",
                    "region",
                    "district",
                    "dx_name",
                    "nb_total_valeurs_esigl",
                    "nb_valeurs_importe_ddp",
                    "coordinates",
                ],
            )
            .group_by(
                [
                    "period",
                    "date_report",
                    "region",
                    "district",
                    "dx_name",
                    "coordinates",
                ]
            )
            .agg(
                [
                    pl.sum("nb_total_valeurs_esigl").alias("nb_total_valeurs_esigl"),
                    pl.sum("nb_valeurs_importe_ddp").alias("nb_valeurs_importe_ddp"),
                ]
            )
        )

    return df_processed


@esigl_vs_dedop_module_3.task
def export_to_database(
    df_data: pl.DataFrame,
    table_name: str,
    mode: Literal["append", "replace", "fail"] = "append",
) -> None:
    """
    Export the DataFrame to the specified database table.

    Parameters
    ----------
    df_data : pl.DataFrame
        The DataFrame to export.
    table_name : str
        The name of the database table to export to.
    mode: Literal["append", "replace", "fail"]
        Export mode, one of "append", "replace", or "fail".
    """
    if df_data.is_empty():
        current_run.log_info(f"Aucune donnée à exporter vers la table `{table_name}`.")
        return

    current_run.log_info(f"Export des données vers la table `{table_name}` de la base de données.")

    periode_range = df_data["period"].unique().to_list()
    periode_range = tuple(periode_range) if len(periode_range) > 1 else f"('{periode_range[0]}')"

    manager_conn = psycopg2.connect(workspace.database_url)
    manager_conn.autocommit = False
    manager_cursor = manager_conn.cursor()

    query = (
        f"""
        DELETE FROM "{table_name}"
        WHERE period in {periode_range}
    """
        if table_name != "esigl_vs_dedop_data_module_3_coherence_tracabilite"
        else f"""
        DELETE FROM "{table_name}"
        WHERE date_controle = '{datetime.now().date().strftime("%Y-%m-%d")}'
    """
    )
    manager_cursor.execute(query)
    deleted_rows = manager_cursor.rowcount

    if deleted_rows > 0:
        current_run.log_info(
            f"{deleted_rows} enregistrements supprimés de la table `{table_name}` pour mise à jour."
        )
        manager_conn.commit()

    selected_columns = pl.read_database_uri(
        query=f"""SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        """,
        uri=workspace.database_url,
    )["column_name"].to_list()

    df_data[selected_columns].write_database(
        table_name=table_name,
        connection=workspace.database_url,
        if_table_exists=mode,
    )  # type: ignore
    current_run.add_database_output(table_name)


@esigl_vs_dedop_module_3.task
def get_dates_from_df(df_compare: pl.DataFrame, df_completude: pl.DataFrame) -> pl.DataFrame:
    """
    Récupère les dates de rapport uniques à partir des DataFrames.

    Parameters
    ----------
    df_compare : pl.DataFrame
        DataFrame contenant les résultats de la comparaison.
    df_completude : pl.DataFrame
        DataFrame contenant les résultats de la complétude.

    Returns
    -------
    pl.DataFrame
        DataFrame contenant les dates de rapport uniques.
    """
    return pl.concat(
        [
            process_dates_from_df(df_compare),
            process_dates_from_df(df_completude),
        ],
        how="diagonal_relaxed",
    ).unique()


if __name__ == "__main__":
    esigl_vs_dedop_module_3()
