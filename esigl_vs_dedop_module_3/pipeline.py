"""Pipeline for eSIGL vs Dedop Module 3 data comparison and analysis."""

import json
from datetime import datetime
from pathlib import Path

import polars as pl
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
from queries import QUERY_ETAT_STOCK, QUERY_ETAT_STOCK_GTC
from utils import (
    check_metabase_server_health,
    check_server_health,
    get_date_report,
    last_analytics_update,
    parse_cutoff_date,
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
def esigl_vs_dedop_module_3(
    dedop_connection: DHIS2Connection,
    metabase_connection: CustomConnection,
    fp_ou_de_mapping: str,
    start_date: str | None,
    end_date: str | None,
    months_back: int,
):
    """Write your pipeline orchestration here.

    Pipeline functions should only call tasks and should never perform IO operations or expensive computations.
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
    df_etat_stock = fetch_metabase_routine_data(
        metabase=metabase,
        df_ou_mapping=df_ou_mapping,
        df_de_mapping=df_de_mapping,
        data_elements_routine=data_elements_routine,
        periods_range=periods_range,
    )

    df_etat_stock_gtc = fetch_metabase_gtc_data(
        metabase=metabase,
        df_ou_mapping=df_ou_mapping,
        data_elements_gtc=data_elements_gtc,
        periods_range=periods_range,
    )


@esigl_vs_dedop_module_3.task
def fetch_organisation_units(dedop: DHIS2) -> pl.DataFrame:
    """Fetch organisation units from the Dedop DHIS2 instance.

    Parameters
    ----------
    snis : DHIS2
        The DHIS2 instance for SNIS.
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
                lambda geom: eval(geom)["coordinates"][0][0]
                if isinstance(eval(geom), dict)
                else None,
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

    dates = list(rrule.rrule(freq=rrule.MONTHLY, dtstart=start_dt, until=end_dt))
    return sorted({dt.strftime("%Y-%m-%d") for dt in dates})


@esigl_vs_dedop_module_3.task
def fetch_routine_data_elements(dhis2: DHIS2) -> pl.DataFrame:
    """Récupère les dataElements de routine depuis DHIS2.

    Args:
        dhis2: Client DHIS2 configuré

    Returns:
        DataFrame des dataElements de routine
    """
    df_de = pl.DataFrame(dhis2.meta.data_elements(fields="id,name,code,categoryCombo"))
    df_de = df_de.filter(
        pl.col("code").is_not_null()
        & pl.col("categoryCombo").struct.field("id").str.contains("El9O9wWhg8F")
    )

    current_run.log_info(f"Retrieved {df_de.shape[0]} routine dataElements from DHIS2")
    return df_de


@esigl_vs_dedop_module_3.task
def fetch_gtc_data_elements(dhis2: DHIS2) -> pl.DataFrame:
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


@esigl_vs_dedop_module_3.task
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


@esigl_vs_dedop_module_3.task
def fetch_dhis2_data(
    dhis2: DHIS2,
    data_elements_routine: pl.DataFrame,
    data_elements_gtc: pl.DataFrame,
    periods_range: list[str],
) -> pl.DataFrame:
    """Fetch data from DHIS2 for the specified periods and data elements.

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
    periods_range = [
        datetime.strptime(f"{pe}", "%Y-%m-%d").strftime("%Y%m") for pe in periods_range
    ]  # type: ignore
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
        df_data_ddp = dhis2.meta.add_coc_name_column(df_data_ddp, "category_option_combo_id")  # type: ignore
        df_data_ddp = df_data_ddp.with_columns(
            pl.col("value").cast(pl.Int64),
            pl.col("dx_name").str.replace(r"PNLP-|GTC-", "").str.strip_chars().alias("dx_name"),
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
    """Fetch routine data from Metabase for the specified periods and data elements.

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
        row["code_produit"]: row["ancien_code"]
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

    query_etat_stock = QUERY_ETAT_STOCK
    query_etat_stock += f" AND requisition_line_items.productcode IN {tuple(products_code) if len(products_code) > 1 else f'({products_code[0]!r})'}"  # noqa: E501
    sql_query = query_etat_stock.format(processing_periods=processing_periods)

    current_run.log_debug(sql_query)
    try:
        df_etat_stock = pl.DataFrame(metabase.get_data_from_sql_query(sql_query=sql_query))
    except Exception as e:
        current_run.log_error(f"Erreur lors de l'extraction depuis Metabase: {e}")
        raise

    # Jointure des métadonnées
    mapping = {v: k for k, v in dico_products.items()}
    df_etat_stock = df_etat_stock.with_columns(pl.col("code_produit").cast(pl.String)).with_columns(
        pl.col("code_produit").replace(mapping).alias("code_produit")
    )

    # Code produit eSIGL -> code produit DHIS2
    df_etat_stock = df_etat_stock.join(
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
    df_etat_stock = df_etat_stock.join(
        df_ou_mapping,
        on="code_site",
        how="inner",
    )
    # Renommage des colonnes d'intérêt
    df_etat_stock = df_etat_stock.rename(COC_MAPPING)

    current_run.log_info(f"Extracted {df_etat_stock.shape[0]} records from eSIGL")
    df_etat_stock = df_etat_stock.unpivot(
        index=["data_element_id", "period", "organisation_unit_id"],
        on=list(COC_MAPPING.values()),
        variable_name="category_option_combo_id",
        value_name="value",
    ).with_columns(pl.col("value").cast(pl.Int64))

    df_etat_stock = df_etat_stock.group_by(
        ["data_element_id", "period", "organisation_unit_id", "category_option_combo_id"]
    ).agg(pl.col("value").sum().alias("value"))
    current_run.log_debug(f"Routine DataFrame columns: {df_etat_stock.columns}")
    return df_etat_stock


@esigl_vs_dedop_module_3.task
def fetch_metabase_gtc_data(
    metabase: Metabase,
    df_ou_mapping: pl.DataFrame,
    data_elements_gtc: pl.DataFrame,
    periods_range: list[str],
) -> pl.DataFrame:
    """Fetch GTC data from Metabase for the specified periods and data elements.

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

    msg_info = (
        "⏳ Extraction des données GTC depuis Metabase "
        f"aux périodes: `{start_dt.strftime('%Y-%m-%d')} - {end_dt.strftime('%Y-%m-%d')}`..."  # type: ignore
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

    sql_query = QUERY_ETAT_STOCK_GTC.format(
        products_code=products_code, processing_periods=processing_periods
    )
    current_run.log_debug(sql_query)
    try:
        df_etat_stock_gtc = pl.DataFrame(metabase.get_data_from_sql_query(sql_query=sql_query))
    except Exception as e:
        current_run.log_error(f"Erreur lors de l'extraction depuis Metabase: {e}")
        raise

    # Jointure des métadonnées
    mapping = {v: k for k, v in EXTENDED_PRODUCT_CODE.items()}
    df_etat_stock_gtc = df_etat_stock_gtc.with_columns(
        pl.col("code_produit").cast(pl.String)
    ).with_columns(pl.col("code_produit").replace(mapping).alias("code_produit"))

    # Code produit eSIGL -> code produit DHIS2
    df_etat_stock_gtc = df_etat_stock_gtc.join(
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
    df_etat_stock_gtc = df_etat_stock_gtc.join(
        df_ou_mapping,
        on="code_site",
        how="inner",
    )
    # Renommage des colonnes d'intérêt
    df_etat_stock_gtc = df_etat_stock_gtc.rename(COC_MAPPING)

    current_run.log_info(f"Extracted {df_etat_stock_gtc.shape[0]} GTC records from eSIGL")
    df_etat_stock_gtc = df_etat_stock_gtc.unpivot(
        index=["data_element_id", "period", "organisation_unit_id"],
        on=list(COC_MAPPING.values()),
        variable_name="category_option_combo_id",
        value_name="value",
    ).with_columns(pl.col("value").cast(pl.Int64))

    df_etat_stock_gtc = df_etat_stock_gtc.group_by(
        ["data_element_id", "period", "organisation_unit_id", "category_option_combo_id"]
    ).agg(pl.col("value").sum().alias("value"))

    current_run.log_debug(f"GTC DataFrame columns: {df_etat_stock_gtc.columns}")
    return df_etat_stock_gtc


if __name__ == "__main__":
    esigl_vs_dedop_module_3()
