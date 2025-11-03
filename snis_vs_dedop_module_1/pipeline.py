"""Pipeline for SNIS vs Dedop Module 1 data comparison and analysis."""

import json
import locale
from datetime import datetime
from typing import Literal

import polars as pl
import psycopg2
from constants import DATA_ELEMENTS
from dateutil import rrule
from dateutil.relativedelta import relativedelta
from openhexa.sdk import (
    DHIS2Connection,
    current_run,
    parameter,
    pipeline,
    workspace,
)
from openhexa.toolbox.dhis2 import DHIS2
from openhexa.toolbox.dhis2.dataframe import extract_data_elements, get_organisation_units
from utils import check_server_health, last_analytics_update, parse_cutoff_date


@pipeline("snis_vs_dedop_module_1")
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
def snis_vs_dedop_module_1(
    snis_connection: DHIS2Connection,
    dedop_connection: DHIS2Connection,
    start_date: str | None,
    end_date: str | None,
    months_back: int,
) -> None:
    """Main pipeline function for SNIS vs Dedop Module 1 data comparison and analysis."""
    snis = DHIS2(connection=snis_connection)
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

    # Extraction des unités d'organisation
    df_org_units = fetch_organisation_units(snis, dedop)

    periods_range = process_periods(
        start_date=start_date, end_date=end_date, months_back=months_back
    )

    # Extraction des données DHIS2
    data_snis = fetch_dhis2_data(
        dhis2=snis, periods_range=periods_range, data_elements=DATA_ELEMENTS, instance="SNIS"
    )
    data_dedop = fetch_dhis2_data(
        dhis2=dedop, periods_range=periods_range, data_elements=DATA_ELEMENTS, instance="DEDOP"
    )
    current_run.log_info("✅ Extraction des données DHIS2 terminée.")

    # Evaluation de la cohérence des données entre SNIS et Dedop
    df_compare = compare_snis_dedop(dhis2=snis, data_snis=data_snis, data_dedop=data_dedop)

    # Evaluation de la cohérence des données
    df_coherence = evaluate_data_coherence(dhis2=snis, df_compare=df_compare)

    # Evaluation de la complétude des données
    df_completude = evaluate_data_completude(dhis2=snis, data_snis=data_snis, data_dedop=data_dedop)

    # Standardisation des données ajout des informations d'unité d'organisation
    df_compare = process_data_with_org_units(
        df_data=df_compare, df_org_units=df_org_units, table_name="snis_vs_dedop_data_module_1"
    )

    # Cohérence avec tracabilité
    df_coherence_tracabilite = process_data_with_org_units(
        df_data=df_coherence,
        df_org_units=df_org_units,
        table_name="snis_vs_dedop_data_module_1_coherence_tracabilite",
    )

    df_coherence = process_data_with_org_units(
        df_data=df_coherence,
        df_org_units=df_org_units,
        table_name="snis_vs_dedop_data_module_1_coherence",
    )

    # Complétude par district
    df_completude_district = process_data_with_org_units(
        df_data=df_completude,
        df_org_units=df_org_units,
        table_name="snis_vs_dedop_data_module_1_completude_district",
    )

    df_completude = process_data_with_org_units(
        df_data=df_completude,
        df_org_units=df_org_units,
        table_name="snis_vs_dedop_data_module_1_completude",
    )

    # Exportation des données vers la BD
    export_to_database(df_data=df_compare, table_name="snis_vs_dedop_data_module_1")
    export_to_database(df_data=df_coherence, table_name="snis_vs_dedop_data_module_1_coherence")
    export_to_database(
        df_data=df_coherence_tracabilite,
        table_name="snis_vs_dedop_data_module_1_coherence_tracabilite",
    )
    export_to_database(df_data=df_completude, table_name="snis_vs_dedop_data_module_1_completude")
    export_to_database(
        df_data=df_completude_district, table_name="snis_vs_dedop_data_module_1_completude_district"
    )


@snis_vs_dedop_module_1.task
def fetch_organisation_units(snis: DHIS2, dedop: DHIS2) -> pl.DataFrame:
    """Fetch organisation units from both DHIS2 instances.

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

    df_snis_org_units = get_organisation_units(snis)
    df_dedop_org_units = get_organisation_units(dedop)

    df_org_units = pl.concat([df_snis_org_units, df_dedop_org_units], how="vertical_relaxed").sort(
        ["id", "geometry"]
    )

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


@snis_vs_dedop_module_1.task
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
    return sorted({dt.strftime("%Y-%m") for dt in dates})


@snis_vs_dedop_module_1.task
def fetch_dhis2_data(
    dhis2: DHIS2,
    periods_range: list[str],
    data_elements: list[str],
    instance: Literal["SNIS", "DEDOP"] = "SNIS",
) -> pl.DataFrame:
    """Fetch data from DHIS2 for the specified periods and data elements.

    Parameters
    ----------
    dhis2 : DHIS2
        The DHIS2 instance to fetch data from.
    periods_range : list[str]
        The list of periods to fetch data for (format: YYYY-MM).
    data_elements : list[str]
        The list of data element IDs to fetch.
    instance : Literal["SNIS", "DEDOP"]
        The DHIS2 instance type for logging purposes.

    Returns
    -------
    pl.DataFrame
        A DataFrame containing the fetched data.
    """
    msg_info = (
        f"⏳ Extraction des données DHIS2 depuis le {instance} "
        f"aux périodes: `{', '.join(periods_range)}`..."
    )
    current_run.log_info(msg_info)
    try:
        return extract_data_elements(
            dhis2=dhis2,
            data_elements=data_elements,
            org_units=["ZD44Asc0bAk"],
            include_children=True,
            periods=periods_range,  # type: ignore
        )
    except Exception as e:
        current_run.log_error(f"Erreur lors de l'extraction depuis {dhis2.api.url}: {e}")
        raise


@snis_vs_dedop_module_1.task
def compare_snis_dedop(
    dhis2: DHIS2, data_snis: pl.DataFrame, data_dedop: pl.DataFrame
) -> pl.DataFrame:
    """Compare SNIS and Dedop data for consistency.

    Parameters
    ----------
    dhis2 : DHIS2
        The DHIS2 instance to use for metadata enrichment.
    data_snis : pl.DataFrame
        The DataFrame containing SNIS data.
    data_dedop : pl.DataFrame
        The DataFrame containing Dedop data.

    Returns
    -------
    pl.DataFrame
        A DataFrame highlighting discrepancies between SNIS and Dedop data.
    """
    current_run.log_info("Comparaison des données SNIS et DEDOP pour cohérence")

    df_merged = data_snis.join(
        data_dedop,
        on=[
            "data_element_id",
            "period",
            "organisation_unit_id",
            "category_option_combo_id",
        ],
        suffix="_ddp",
    ).drop(["attribute_option_combo_id", "attribute_option_combo_id_ddp"])

    # Add meta data columns
    df_merged = dhis2.meta.add_coc_name_column(df_merged, "category_option_combo_id")
    df_merged = dhis2.meta.add_org_unit_name_column(df_merged, "organisation_unit_id")
    df_merged = dhis2.meta.add_dx_name_column(df_merged, "data_element_id")

    df_merged = df_merged.with_columns(
        pl.col("value").cast(pl.Int64), pl.col("value_ddp").cast(pl.Int64)
    ).rename({"value": "value_snis"})  # type: ignore

    df_merged = df_merged.with_columns(
        (pl.col("value_snis") - pl.col("value_ddp")).abs().alias("ecart"),
        ((pl.col("value_snis") - pl.col("value_ddp")).abs() / pl.col("value_snis")).alias(
            "ecart_relatif"
        ),
    )

    df_merged = df_merged.with_columns(
        pl.when(pl.col("value_snis") == 0)
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
        )
    )


@snis_vs_dedop_module_1.task
def evaluate_data_coherence(dhis2: DHIS2, df_compare: pl.DataFrame) -> pl.DataFrame:
    """Evaluate data coherence based on comparison DataFrame.

    Parameters
    ----------
    dhis2 : DHIS2
        The DHIS2 instance to use for metadata enrichment.
    df_compare : pl.DataFrame
        The DataFrame resulting from the comparison of SNIS and Dedop data.

    Returns
    -------
    pl.DataFrame
        A DataFrame summarizing the coherence evaluation.
    """
    current_run.log_info("Évaluation de la cohérence des données")

    df_coherence = (
        df_compare.with_columns(
            [
                # Cohérence = écart = 0 OU les deux valeurs nulles
                (
                    (pl.col("ecart") == 0)
                    | (pl.col("value_snis").is_null() & pl.col("value_ddp").is_null())
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
    return dhis2.meta.add_dx_name_column(df_coherence, "data_element_id")  # type: ignore


@snis_vs_dedop_module_1.task
def evaluate_data_completude(
    dhis2: DHIS2, data_snis: pl.DataFrame, data_dedop: pl.DataFrame
) -> pl.DataFrame:
    """Evaluate data completeness based on comparison DataFrame.

    Parameters
    ----------
    dhis2 : DHIS2
        The DHIS2 instance to use for metadata enrichment.
    data_snis : pl.DataFrame
        The DataFrame containing SNIS data.
    data_dedop : pl.DataFrame
        The DataFrame containing Dedop data.

    Returns
    -------
    pl.DataFrame
        A DataFrame summarizing the completeness evaluation.
    """
    current_run.log_info("Évaluation de la complétude des données")
    selected_cols = [
        "period",
        # "date_report",
        "organisation_unit_id",
        "data_element_id",
        "category_option_combo_id",
    ]
    df_completude = (
        data_snis.select(selected_cols)
        .unique()
        .join(
            data_dedop.select(selected_cols)
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
                pl.len().alias("nb_total_valeurs_snis"),
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
    return dhis2.meta.add_dx_name_column(df_completude, "data_element_id")  # type: ignore


@snis_vs_dedop_module_1.task
def process_data_with_org_units(
    df_data: pl.DataFrame, df_org_units: pl.DataFrame, table_name: str
) -> pl.DataFrame:
    """Process data by joining with organisation units and saving to a table.

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
        pl.col("dx_name").str.replace("SIG -", "").str.strip_chars().alias("dx_name"),
    )
    if table_name == "snis_vs_dedop_data_module_1":
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
                    "value_snis",
                    "value_ddp",
                    "ecart",
                    "ecart_relatif",
                    "created",
                ],
            )
            .filter(pl.col("ecart") != 0)
            .with_columns(pl.col("ecart_relatif").fill_null(0).round(2))
        )

    if table_name == "snis_vs_dedop_data_module_1_coherence":
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

    if table_name == "snis_vs_dedop_data_module_1_coherence_tracabilite":
        df_processed = (
            df_processed.with_columns(
                pl.lit(datetime.now()).cast(pl.Date).alias("date_controle"),
            )
            .select(
                [
                    "period",
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

    if table_name == "snis_vs_dedop_data_module_1_completude":
        df_processed = df_processed.select(
            [
                "period",
                "date_report",
                "region",
                "district",
                "ou_name",
                "type_ou",
                "dx_name",
                "nb_total_valeurs_snis",
                "nb_valeurs_importe_ddp",
                "taux_completude",
            ],
        )
    if table_name == "snis_vs_dedop_data_module_1_completude_district":
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
                    "nb_total_valeurs_snis",
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
                    pl.sum("nb_total_valeurs_snis").alias("nb_total_valeurs_snis"),
                    pl.sum("nb_valeurs_importe_ddp").alias("nb_valeurs_importe_ddp"),
                ]
            )
        )

    return df_processed


@snis_vs_dedop_module_1.task
def export_to_database(
    df_data: pl.DataFrame,
    table_name: str,
    mode: Literal["append", "replace", "fail"] = "append",
) -> None:
    """Export the DataFrame to the specified database table.

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

    if table_name != "snis_vs_dedop_data_module_1_coherence_tracabilite":
        manager_cursor.execute(f"""
        DELETE FROM "{table_name}"
        WHERE period in {periode_range}
        """)
        deleted_rows = manager_cursor.rowcount

        if deleted_rows > 0:
            current_run.log_info(
                f"{deleted_rows} enregistrements supprimés de la table "
                f"`{table_name}` pour mise à jour."
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


if __name__ == "__main__":
    snis_vs_dedop_module_1()
