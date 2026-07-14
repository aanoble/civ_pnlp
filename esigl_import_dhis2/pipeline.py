"""Pipeline unifié d'import eSIGL → DHIS2 (module Gestion de Stock).

Modèle de données cible : le **produit** est porté par le ``categoryOptionCombo`` et la
**métrique** par le ``dataElement`` (voir ``constants.py``). Ce pipeline remplace les deux
anciens pipelines routine / GTC ; la promptitude des rapports y est intégrée comme branche.

Flux : lecture des mappings → validation des métadonnées cibles → extraction eSIGL
(stock + promptitude) → calcul des indicateurs dérivés → construction du payload →
push ``dataValueSets`` (retry/backoff, seuil d'``ignored``) → alignement des suppressions
(optionnel) → rapports d'audit → nettoyage.
"""

import json
import time
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import transforms as T  # noqa: N812
from coc_mapping import COC_MAPPING
from constants import (
    DE_MAPPING,
    DEFAULT_COC_NAME,
    FALLBACK_GTC_PRODUCT_CODES,
    GTC_PRODUCT_TYPE,
    PRODUCT_CATEGORY_COMBO,
    PROMPTITUDE_DE_MAPPING,
    PROMPTITUDE_DEADLINE_DAYS,
    PROMPTITUDE_DEFAULT_DEADLINE_DAY,
    PROMPTITUDE_PROGRAM_ID,
    REPORT_CATEGORY_COMBO,
    TARGET_DATASET_ID,
    ZERO_SIGNIFICANT_METRICS,
)
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
from openhexa.sdk.pipelines.parameter import DHIS2Widget
from openhexa.toolbox.dhis2 import DHIS2
from product_aliases import PRODUCT_CODE_ALIASES
from queries import QUERY_ETAT_STOCK, QUERY_FACILITIES, QUERY_PROMPTITUDE
from utils import check_metabase_server_health, compute_extraction_window

ORGUNIT_SHEET = "OrgUnit"
PRODUCT_TYPE_SHEET = "R ou G"
OU_GROUP_UID = "nJ1jXZxufek"


@pipeline("esigl_import_dhis2", timeout=43200)
@parameter(
    "dhis2_connection",
    type=DHIS2Connection,  # type: ignore
    name="Target DHIS2 instance",
    help="Instance DHIS2 cible (définitive)",
    default="dedop-nouvelle-instance",
    required=True,
)
@parameter(
    "metabase_connection",
    type=CustomConnection,  # type: ignore
    default="metabase-esigl",
    name="Metabase instance",
    help="Instance Metabase source (eSIGL)",
    required=True,
)
@parameter(
    "dataset_id",
    type=str,  # type: ignore
    widget=DHIS2Widget.DATASETS,
    connection="dhis2_connection",
    name="Dataset DHIS2 (Gestion de Stock)",
    default="KyO2eSxVW4q",
    required=False,
)
@parameter(
    "fp_ou_mapping",
    type=File,
    name="Mapping OrgUnit eSIGL → DHIS2 (XLSX)",
    help="XLSX contenant les feuilles OrgUnit / Traceurs / R ou G",
    default="metabase eSIGL/data/ressources/Fichier mapping OrgUnit eSIGL DHIS2.xlsx",
    required=True,
    directory="metabase eSIGL/data/ressources/mapping_orgunit_esigl_dhis2/",
)
@parameter(
    "fp_traceurs",
    type=File,
    name="Liste des produits traceurs",
    help="Fichier `annee,code_produit` (CSV/XLSX) des produits traceurs par année",
    default="metabase eSIGL/data/ressources/produits_traceurs/produits_traceurs.xlsx",
    required=True,
    directory="metabase eSIGL/data/ressources/produits_traceurs/",
)
@parameter(
    "fp_site_attendus",
    type=File,
    name="Sites attendus consolidés",
    help="Fichier `annee,code_site,rapport_attendu` (CSV/XLSX) pour la promptitude",
    default="metabase eSIGL/data/ressources/site_attendus/SITES ATTENDUS 2026.xlsx",
    required=False,
    directory="metabase eSIGL/data/ressources/site_attendus/",
)
@parameter(
    code="start_date",
    type=str,  # type: ignore
    name="Start date (YYYY-MM-DD)",
    help="Date de début d'extraction (défaut : mois courant)",
    required=False,
)
@parameter(
    code="end_date",
    type=str,  # type: ignore
    name="End date (YYYY-MM-DD)",
    help="Date de fin d'extraction (défaut : fin du mois de start_date)",
    required=False,
)
@parameter(
    "months_back",
    type=int,  # type: ignore
    name="Historique (mois)",
    help="Nombre de mois d'historique republiés avant start_date",
    default=3,
    choices=[0, 1, 3, 6, 12],
    required=True,
)
@parameter(
    "facilities_code",
    type=str,  # type: ignore
    name="Codes établissements eSIGL",
    help="Filtre optionnel sur les sites",
    required=False,
    multiple=True,
)
@parameter(
    "product_code",
    type=str,  # type: ignore
    name="Codes produits eSIGL",
    help="Filtre optionnel sur les produits",
    required=False,
    multiple=True,
)
@parameter(
    "output_directory",
    type=str,  # type: ignore
    name="Output directory",
    help="Répertoire de sortie des rapports",
    default="metabase eSIGL/data/output",
    required=False,
)
@parameter(
    "max_conflict_ratio",
    type=float,  # type: ignore
    name="Seuil d'échec conflits",
    help="Ratio conflits/total au-delà duquel le run échoue (0-1) ; conflit = métadonnée manquante",
    default=0.05,
    required=True,
)
@parameter(
    "import_mode",
    type=str,  # type: ignore
    name="Import mode",
    choices=["CREATE", "CREATE_AND_UPDATE", "UPDATE"],
    default="CREATE_AND_UPDATE",
    required=True,
)
@parameter(
    "enable_promptitude",
    type=bool,  # type: ignore
    name="Activer la promptitude",
    help="Calculer et pousser rapport_attendu / rapport_prompt",
    default=True,
    required=False,
)
@parameter(
    "dry_run",
    type=bool,  # type: ignore
    default=False,
    name="Dry run",
    help="Simule l'import DHIS2",
    required=False,
)
def esigl_import_dhis2(
    dhis2_connection: DHIS2Connection,
    metabase_connection: CustomConnection,
    fp_ou_mapping: File,
    fp_traceurs: File,
    output_directory: str,
    dataset_id: str = TARGET_DATASET_ID,
    fp_site_attendus: File | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    months_back: int = 3,
    facilities_code: list[str] | None = None,
    product_code: list[str] | None = None,
    dhis2_aoc: str | None = None,
    enable_promptitude: bool = True,
    enable_bien_stocke: bool = False,
    delete_stale_values: bool = False,
    max_conflict_ratio: float = 0.05,
    import_mode: str = "CREATE_AND_UPDATE",
    dry_run: bool = False,
    post_batch_size: int = 5000,
) -> None:
    """Orchestre l'import unifié eSIGL → DHIS2 du module Gestion de Stock.

    Parameters
    ----------
    dhis2_connection : DHIS2Connection
        Connexion DHIS2 cible (définitive).
    metabase_connection : CustomConnection
        Connexion Metabase source (eSIGL).
    fp_ou_mapping : File
        Mapping OrgUnit + feuille R ou G (XLSX).
    fp_traceurs : File
        Liste des produits traceurs par année (``annee, code_produit``).
    output_directory : str
        Répertoire de sortie des rapports.
    dataset_id : str
        Dataset DHIS2 cible.
    dhis2_aoc : str | None
        AOC DHIS2 (et COC promptitude). Si ``None``, résolu depuis l'AOC par défaut de l'instance.
    fp_site_attendus : File | None
        Fichier consolidé des sites attendus (promptitude).
    start_date, end_date : str | None
        Bornes d'extraction (``YYYY-MM-DD``).
    months_back : int
        Historique en mois republié avant ``start_date``.
    facilities_code, product_code : list[str] | None
        Filtres optionnels sites / produits.
    enable_promptitude : bool
        Active la branche promptitude.
    enable_bien_stocke : bool
        Active le calcul de ``bien_stocke``.
    delete_stale_values : bool
        Aligne les suppressions (DELETE ciblé des flags obsolètes).
    max_conflict_ratio : float
        Seuil d'échec sur le ratio ``conflits/total`` (métadonnées cibles manquantes).
    import_mode : str
        Stratégie d'import DHIS2.
    dry_run : bool
        Simule l'import.
    post_batch_size : int
        Taille des lots POST DHIS2.
    """
    mappings = read_mappings(fp_ou_mapping, fp_traceurs)

    dhis2 = DHIS2(connection=dhis2_connection, cache_dir=Path(workspace.files_path, ".cache"))

    aoc = resolve_aoc(dhis2, dhis2_aoc)

    valid_cocs = validate_target_metadata(dhis2, dataset_id, aoc, mappings)

    ou_synced = add_missing_orgunits(dhis2, mappings, dataset_id, OU_GROUP_UID)

    df_stock = extract_stock(
        metabase_connection,
        mappings,
        valid_cocs,
        start_date,
        end_date,
        months_back,
        facilities_code,
        product_code,
        _ou_synced=ou_synced,
    )

    df_products = compute_derived(
        df_stock, mappings, start_date, end_date, months_back, enable_bien_stocke
    )

    df_prompt = extract_promptitude(
        metabase_connection,
        mappings,
        fp_site_attendus,
        start_date,
        end_date,
        months_back,
        facilities_code,
        enabled=enable_promptitude,
    )

    payload = build_payload(df_products, df_prompt, aoc)

    summary = push_data_to_dhis2(
        dhis2, payload, dataset_id, dry_run, import_mode, post_batch_size, max_conflict_ratio
    )

    align = align_stale_values(
        dhis2,
        payload,
        dataset_id,
        aoc,
        start_date,
        end_date,
        months_back,
        dry_run,
        enabled=delete_stale_values,
        _summary=summary,
    )

    report_dir = write_import_report(output_directory, payload, summary, align)
    cleanup_old_directory_files(Path(output_directory), _report_dir=report_dir)


@esigl_import_dhis2.task
def read_mappings(fp_ou_mapping: File, fp_traceurs: File) -> dict[str, pl.DataFrame]:
    """Charge les tables de mapping (COC depuis la config versionnée, reste depuis le workspace).

    Parameters
    ----------
    fp_ou_mapping : File
        XLSX (feuilles OrgUnit / R ou G).
    fp_traceurs : File
        Fichier dédié des produits traceurs (``annee, code_produit``), CSV ou XLSX.

    Returns
    -------
    dict[str, pl.DataFrame]
        Clés ``coc``, ``ou``, ``traceurs``, ``product_type``.
    """
    ou_path = Path(workspace.files_path) / fp_ou_mapping.path
    traceurs_path = Path(workspace.files_path) / fp_traceurs.path
    for p in (ou_path, traceurs_path):
        if not p.exists():
            current_run.log_error(f"Fichier introuvable : {p.as_posix()}")
            raise FileNotFoundError(p.as_posix())

    df_coc = _load_coc_mapping()

    df_ou = (
        pl.read_excel(ou_path, sheet_name=ORGUNIT_SHEET)
        .filter(pl.col("ID_Dhis2").is_not_null())
        .select(pl.col("New_Code").cast(pl.String), pl.col("ID_Dhis2"))
        .rename({"New_Code": "code_site", "ID_Dhis2": "orgUnit"})
        .unique()
    )

    df_traceurs = _read_traceurs(traceurs_path)

    df_product_type = _read_product_type(ou_path)

    current_run.log_info(
        f"Mappings chargés : {df_coc.height} COC, {df_ou.height} OU, "
        f"{df_traceurs.height} traceurs, {df_product_type.height} classifications produit"
    )
    return {"coc": df_coc, "ou": df_ou, "traceurs": df_traceurs, "product_type": df_product_type}


def _read_traceurs(path: Path) -> pl.DataFrame:
    """Lit la liste des produits traceurs par année (fichier dédié, CSV ou XLSX).

    En-tête attendu en ligne 1, avec au moins les colonnes ``annee`` et ``code_produit``
    (les éventuelles colonnes ``Code`` / ``Désignation`` sont ignorées).

    Parameters
    ----------
    path : Path
        Chemin du fichier des produits traceurs.

    Returns
    -------
    pl.DataFrame
        Colonnes ``annee`` (int) et ``code_produit`` (str).
    """
    df = pl.read_csv(path) if path.suffix.lower() == ".csv" else pl.read_excel(path)
    return df.select(
        pl.col("annee").cast(pl.Int64),
        pl.col("code_produit").cast(pl.String),
    )


def _read_product_type(ou_path: Path) -> pl.DataFrame:
    """Lit la classification ROUTINE/GTC (feuille « R ou G »), avec repli sur constante.

    Parameters
    ----------
    ou_path : Path
        Chemin du classeur de mapping.

    Returns
    -------
    pl.DataFrame
        Colonnes ``code_produit`` (str) et ``is_gtc`` (bool).
    """
    try:
        df = pl.read_excel(ou_path, sheet_name=PRODUCT_TYPE_SHEET)
        return df.select(
            pl.col("code_produit").cast(pl.String),
            (pl.col("Type produit").str.to_uppercase() == GTC_PRODUCT_TYPE).alias("is_gtc"),
        ).unique()
    except Exception as err:
        current_run.log_warning(
            f"Feuille « {PRODUCT_TYPE_SHEET} » illisible ({err!s}). "
            f"Repli sur la liste GTC en dur ({len(FALLBACK_GTC_PRODUCT_CODES)} codes)."
        )
        return pl.DataFrame({"code_produit": sorted(FALLBACK_GTC_PRODUCT_CODES)}).with_columns(
            pl.lit(True).alias("is_gtc")
        )


def _load_coc_mapping() -> pl.DataFrame:
    """Charge le mapping produit → COC depuis le module versionné ``coc_mapping.py``.

    Aucun fichier externe : ``COC_MAPPING`` est un ``.py`` embarqué au push OpenHEXA,
    donc pas d'``@parameter File`` à importer ni d'upload workspace.

    Returns
    -------
    pl.DataFrame
        Colonnes ``code_produit`` (str) et ``coc`` (str).
    """
    rows = [{"code_produit": str(code), "coc": coc} for code, coc in COC_MAPPING.items()]
    return pl.DataFrame(rows, schema={"code_produit": pl.String, "coc": pl.String})


@esigl_import_dhis2.task
def resolve_aoc(dhis2: DHIS2, dhis2_aoc: str | None) -> str:
    """Résout l'attribute option combo à utiliser.

    Si ``dhis2_aoc`` est fourni, il est utilisé tel quel. Sinon, l'``categoryOptionCombo``
    par défaut de l'instance (nom « default ») est récupéré et sert d'AOC (et de COC promptitude).

    Parameters
    ----------
    dhis2 : DHIS2
        Client DHIS2 cible.
    dhis2_aoc : str | None
        AOC fourni par l'utilisateur (ou ``None``).

    Returns
    -------
    str
        UID de l'AOC à utiliser.

    Raises
    ------
    ValueError
        Si l'AOC par défaut de l'instance est introuvable.
    """
    if dhis2_aoc:
        current_run.log_info(f"AOC fourni : {dhis2_aoc}")
        return dhis2_aoc

    resp = dhis2.api.get(
        endpoint="categoryOptionCombos",
        params={"filter": f"name:eq:{DEFAULT_COC_NAME}", "fields": "id"},
        use_cache=False,
    )
    combos = resp.get("categoryOptionCombos", [])
    if not combos:
        current_run.log_critical("AOC par défaut (« default ») introuvable dans l'instance.")
        raise ValueError("AOC par défaut introuvable")
    aoc = combos[0]["id"]
    current_run.log_info(f"AOC par défaut résolu depuis l'instance : {aoc}")
    return aoc


@esigl_import_dhis2.task
def validate_target_metadata(
    dhis2: DHIS2, dataset_id: str, dhis2_aoc: str, mappings: dict[str, pl.DataFrame]
) -> list[str]:
    """Valide les métadonnées cibles avant tout push (risque n°1 du modèle produit=COC).

    Vérifie l'existence et le ``categoryCombo`` des 21 dataElements, la présence de l'AOC,
    du dataset, et surtout que **chaque COC produit appartient au categoryCombo produit**
    (sinon les valeurs seraient silencieusement ``ignored``).

    Parameters
    ----------
    dhis2 : DHIS2
        Client DHIS2 cible.
    dataset_id : str
        Dataset cible.
    dhis2_aoc : str
        AOC attendu.
    mappings : dict[str, pl.DataFrame]
        Tables de mapping (utilise ``coc``).

    Returns
    -------
    list[str]
        Liste des COC produits valides (∈ categoryCombo produit).

    Raises
    ------
    ValueError
        Si un dataElement, le categoryCombo, l'AOC ou le dataset est absent/incohérent.
    """
    # 1. dataElements existants + categoryCombo
    all_de = pl.DataFrame(
        dhis2.meta.data_elements(fields="id,code,categoryCombo"), infer_schema_length=10000
    )
    de_combo = {
        row["id"]: (row["categoryCombo"] or {}).get("id")
        for row in all_de.to_dicts()
        if row.get("id")
    }

    expected = {uid: PRODUCT_CATEGORY_COMBO for uid in DE_MAPPING.values()}
    expected.update({uid: REPORT_CATEGORY_COMBO for uid in PROMPTITUDE_DE_MAPPING.values()})

    missing_de = [uid for uid in expected if uid not in de_combo]
    if missing_de:
        current_run.log_critical(f"dataElements absents en cible : {', '.join(missing_de)}")
        raise ValueError("dataElements cibles manquants")

    bad_combo = {uid: de_combo[uid] for uid, cc in expected.items() if de_combo.get(uid) != cc}
    if bad_combo:
        current_run.log_critical(
            f"categoryCombo inattendu pour {len(bad_combo)} dataElements : {bad_combo}"
        )
        raise ValueError("categoryCombo cible incohérent")

    # 2. AOC + dataset
    try:
        dhis2.api.get(endpoint=f"categoryOptionCombos/{dhis2_aoc}", use_cache=False)
        dhis2.api.get(endpoint=f"dataSets/{dataset_id}", use_cache=False)
    except Exception as err:
        current_run.log_critical(
            f"AOC `{dhis2_aoc}` ou dataset `{dataset_id}` introuvable : {err!s}"
        )
        raise ValueError("AOC ou dataset cible manquant") from err

    # 3. COC produit ∈ categoryCombo produit
    combo = dhis2.api.get(
        endpoint=f"categoryCombos/{PRODUCT_CATEGORY_COMBO}",
        params={"fields": "categoryOptionCombos[id]"},
        use_cache=False,
    )
    combo_cocs = {c["id"] for c in combo.get("categoryOptionCombos", [])}

    mapped_cocs = set(mappings["coc"]["coc"].unique().to_list())
    valid = sorted(mapped_cocs & combo_cocs)
    invalid = sorted(mapped_cocs - combo_cocs)
    if invalid:
        current_run.log_warning(
            f"{len(invalid)} COC du mapping absents du categoryCombo produit "
            f"(produits exclus pour éviter des `ignored`) : {', '.join(invalid[:20])}"
            + (" …" if len(invalid) > 20 else "")
        )
    current_run.log_info(
        f"Validation métadonnées OK : {len(expected)} DE, "
        f"{len(valid)}/{len(mapped_cocs)} COC valides"
    )
    return valid


@esigl_import_dhis2.task
def add_missing_orgunits(
    dhis2: DHIS2, mappings: dict[str, pl.DataFrame], dataset_id: str, group_uid: str
) -> bool:
    """Assure que les OU mappées sont membres du dataset et du groupe d'OU.

    Parameters
    ----------
    dhis2 : DHIS2
        Client DHIS2 cible.
    mappings : dict[str, pl.DataFrame]
        Tables de mapping (utilise ``ou`` : ``code_site -> orgUnit``).
    dataset_id : str
        Dataset cible.
    group_uid : str
        Groupe d'unités d'organisation cible.

    Returns
    -------
    bool
        ``True`` une fois la synchro effectuée (jeton d'ordonnancement du DAG).
    """
    df_ou = mappings["ou"]
    group_resp = dhis2.api.get(
        endpoint=f"organisationUnitGroups/{group_uid}?fields=organisationUnits[id]",
        use_cache=False,
    )
    existing = {ou["id"] for ou in group_resp.get("organisationUnits", [])}
    mapping_ids = set(df_ou.select("orgUnit").unique().to_series().to_list())
    to_add = mapping_ids - existing

    current_run.log_info(
        f"OrgUnits : {len(existing)} dans le groupe, {len(mapping_ids)} mappées, "
        f"{len(to_add)} à ajouter."
    )
    for ou in sorted(to_add):
        for endpoint in (
            f"dataSets/{dataset_id}/organisationUnits/{ou}",
            f"organisationUnitGroups/{group_uid}/organisationUnits/{ou}",
        ):
            try:
                res = dhis2.api.post(endpoint=endpoint)
                status = getattr(res, "status_code", None)
                if status in (200, 201):
                    current_run.log_info(f"OrgUnit {ou} ajoutée à '{endpoint}'.")
                elif status == 409:
                    current_run.log_info(f"OrgUnit {ou} déjà présente ('{endpoint}').")
                else:
                    body = getattr(res, "text", "")
                    current_run.log_error(
                        f"Échec ajout OrgUnit {ou} à '{endpoint}' : {status} {body}"
                    )
            except Exception as e:
                current_run.log_error(f"Exception ajout OrgUnit {ou} à '{endpoint}' : {e!s}")
    return True


@esigl_import_dhis2.task
def extract_stock(
    metabase: CustomConnection,
    mappings: dict[str, pl.DataFrame],
    valid_cocs: list[str],
    start_date: str | None,
    end_date: str | None,
    months_back: int,
    facilities_code: list[str] | None,
    product_code: list[str] | None,
    _ou_synced: bool = True,
) -> pl.DataFrame:
    """Extrait les états de stock eSIGL et les enrichit (COC, OrgUnit, type produit).

    Parameters
    ----------
    metabase : CustomConnection
        Connexion Metabase.
    mappings : dict[str, pl.DataFrame]
        Tables de mapping.
    valid_cocs : list[str]
        COC produits valides (issus de la validation métadonnées).
    start_date, end_date : str | None
        Bornes d'extraction.
    months_back : int
        Historique en mois.
    facilities_code, product_code : list[str] | None
        Filtres optionnels.
    _ou_synced : bool
        Jeton d'ordonnancement du DAG (ignoré).

    Returns
    -------
    pl.DataFrame
        Lignes enrichies : ``period, enddate, orgUnit, coc, is_gtc`` + métriques brutes.
    """
    mb = Metabase(metabase)
    check_metabase_server_health(mb)

    cmm_start, _pub_start, end_dt = compute_extraction_window(start_date, end_date, months_back)
    current_run.log_info(
        f"Extraction stock du {cmm_start:%Y-%m-%d} au {end_dt:%Y-%m-%d} "
        f"(3 mois amont inclus pour la CMM des GTC)."
    )
    processing_periods = (
        f"pp.enddate BETWEEN '{cmm_start:%Y-%m-%d}'::date AND '{end_dt:%Y-%m-%d}'::date"
    )
    products_clause = _build_products_clause(mappings, product_code)
    facilities_clause = _build_facilities_clause(mb, facilities_code)

    query = QUERY_ETAT_STOCK.format(
        processing_periods=processing_periods,
        products_code=products_clause,
        facilities=facilities_clause,
    )

    df = pl.DataFrame(mb.get_data_from_sql_query(query))
    if df.is_empty():
        current_run.log_warning("Aucune donnée de stock extraite d'eSIGL.")
        return df

    df = (
        df.with_columns(pl.col("code_produit").cast(pl.String), pl.col("code_site").cast(pl.String))
        # Normalise les anciens codes (< 2020) vers le code produit actuel avant jointure.
        .with_columns(pl.col("code_produit").replace(PRODUCT_CODE_ALIASES))
        .join(mappings["coc"].select("code_produit", "coc"), on="code_produit", how="inner")
        .join(mappings["ou"], on="code_site", how="inner")
        .join(mappings["product_type"], on="code_produit", how="left")
        .with_columns(pl.col("is_gtc").fill_null(False))
        .filter(pl.col("coc").is_in(valid_cocs))
    )
    current_run.log_info(
        f"Stock extrait : {df.height} lignes ({df.filter(pl.col('is_gtc')).height} GTC)."
    )
    return df


def _build_products_clause(
    mappings: dict[str, pl.DataFrame], product_code: list[str] | None
) -> str:
    """Construit la clause SQL de filtrage produit.

    Sans filtre explicite, on ne restreint pas en SQL : la jointure interne sur le mapping
    COC réalise déjà le filtrage produit (évite un ``IN (...)`` de plusieurs centaines de codes).

    Quand des produits sont demandés, on **étend** la liste avec leurs anciens codes (< 2020)
    pour ne pas manquer les périodes antérieures ; la normalisation vers le code actuel se fait
    à l'extraction.

    Parameters
    ----------
    mappings : dict[str, pl.DataFrame]
        Tables de mapping (``coc``).
    product_code : list[str] | None
        Produits explicitement demandés.

    Returns
    -------
    str
        Clause booléenne SQL (``TRUE`` ou ``rli.productcode IN (...)``).

    Raises
    ------
    ValueError
        Si des produits sont demandés mais aucun n'est reconnu.
    """
    if not product_code:
        return "TRUE"
    known = set(mappings["coc"]["code_produit"].unique().to_list())
    wrong = [c for c in product_code if c not in known]
    if wrong:
        current_run.log_warning(f"Codes produits absents du mapping COC : {', '.join(wrong)}")
    codes = [c for c in product_code if c in known]
    if not codes:
        current_run.log_critical("Aucun code produit valide après validation.")
        raise ValueError("Aucun code produit valide")

    requested = set(codes)
    old_codes = [old for old, new in PRODUCT_CODE_ALIASES.items() if new in requested]
    if old_codes:
        current_run.log_info(f"Extension aux anciens codes (< 2020) : {len(old_codes)} code(s).")
    current_run.log_info(f"Filtrage sur {len(codes)} produit(s).")
    return f"rli.productcode IN {_sql_in(codes + old_codes)}"


def _build_facilities_clause(mb: Metabase, facilities_code: list[str] | None) -> str:
    """Construit la clause SQL de filtrage établissement (avec validation distante).

    Parameters
    ----------
    mb : Metabase
        Client Metabase.
    facilities_code : list[str] | None
        Établissements demandés.

    Returns
    -------
    str
        Clause booléenne SQL (``TRUE`` ou ``facilities.code IN (...)``).
    """
    if not facilities_code:
        return "TRUE"
    df_site = pl.DataFrame(mb.get_data_from_sql_query(QUERY_FACILITIES))
    known = set(df_site["code"].to_list())
    wrong = [c for c in facilities_code if c not in known]
    if wrong:
        current_run.log_warning(f"Codes établissements inconnus : {', '.join(wrong)}")
    codes = [c for c in facilities_code if c in known]
    if not codes:
        return "TRUE"
    current_run.log_info(f"Filtrage sur {len(codes)} établissement(s).")
    return f"f.code IN {_sql_in(codes)}"


def _sql_in(values: list[str]) -> str:
    """Rend une liste de valeurs en littéral SQL ``IN`` (échappement des quotes).

    Parameters
    ----------
    values : list[str]
        Valeurs à inclure.

    Returns
    -------
    str
        Fragment ``('a', 'b')`` (parenthèses incluses).
    """
    escaped = ["'" + str(v).replace("'", "''") + "'" for v in values]
    return "(" + ", ".join(escaped) + ")"


@esigl_import_dhis2.task
def compute_derived(
    df_stock: pl.DataFrame,
    mappings: dict[str, pl.DataFrame],
    start_date: str | None,
    end_date: str | None,
    months_back: int,
    enable_bien_stocke: bool,
) -> pl.DataFrame:
    """Agrège routine/GTC et calcule tous les indicateurs dérivés.

    Parameters
    ----------
    df_stock : pl.DataFrame
        Lignes enrichies (sortie de :func:`extract_stock`).
    mappings : dict[str, pl.DataFrame]
        Tables de mapping (``coc``, ``traceurs``).
    start_date, end_date : str | None
        Bornes d'extraction.
    months_back : int
        Historique en mois.
    enable_bien_stocke : bool
        Active le calcul de ``bien_stocke``.

    Returns
    -------
    pl.DataFrame
        Agrégat produit au grain ``(period, orgUnit, coc)`` avec indicateurs dérivés.
    """
    if df_stock.is_empty():
        return df_stock

    _cmm_start, pub_start, end_dt = compute_extraction_window(start_date, end_date, months_back)

    df_routine = df_stock.filter(~pl.col("is_gtc"))
    df_gtc = df_stock.filter(pl.col("is_gtc"))

    parts: list[pl.DataFrame] = []
    if not df_routine.is_empty():
        parts.append(T.aggregate_routine(df_routine, pub_start, end_dt))
    if not df_gtc.is_empty():
        parts.append(T.aggregate_gtc(df_gtc, pub_start, end_dt))
    if not parts:
        return pl.DataFrame()

    df = pl.concat(parts, how="diagonal_relaxed")

    traceur_periods = T.build_traceur_periods(
        mappings["traceurs"], mappings["coc"], end_dt.year, end_dt.month
    )
    df = T.compute_derived_indicators(df, traceur_periods, enable_bien_stocke)
    current_run.log_info(f"Indicateurs dérivés calculés : {df.height} lignes produit.")
    return df


@esigl_import_dhis2.task
def extract_promptitude(
    metabase: CustomConnection,
    mappings: dict[str, pl.DataFrame],
    fp_site_attendus: File | None,
    start_date: str | None,
    end_date: str | None,
    months_back: int,
    facilities_code: list[str] | None,
    enabled: bool,
) -> pl.DataFrame | None:
    """Calcule la promptitude des rapports (attendus vs transmis dans le délai).

    Parameters
    ----------
    metabase : CustomConnection
        Connexion Metabase.
    mappings : dict[str, pl.DataFrame]
        Tables de mapping (``ou``).
    fp_site_attendus : File | None
        Fichier consolidé des sites attendus.
    start_date, end_date : str | None
        Bornes d'extraction.
    months_back : int
        Historique en mois.
    facilities_code : list[str] | None
        Filtre optionnel sur les établissements eSIGL (mêmes codes que pour le stock).
    enabled : bool
        Active la branche.

    Returns
    -------
    pl.DataFrame | None
        Grain ``(period, orgUnit, rapport_attendu, rapport_prompt)`` ou ``None`` si désactivé.
    """
    if not enabled:
        current_run.log_info("Promptitude désactivée (enable_promptitude=False).")
        return None
    if fp_site_attendus is None:
        current_run.log_warning("Promptitude activée mais aucun fichier sites attendus fourni.")
        return None

    attendus_path = Path(workspace.files_path) / fp_site_attendus.path
    if not attendus_path.exists():
        current_run.log_warning(f"Fichier sites attendus introuvable : {attendus_path.as_posix()}")
        return None

    df_attendus = _read_site_attendus(attendus_path)

    mb = Metabase(metabase)
    _cmm_start, pub_start, end_dt = compute_extraction_window(start_date, end_date, months_back)
    processing_periods = (
        f"pp.enddate BETWEEN '{pub_start:%Y-%m-%d}'::date AND '{end_dt:%Y-%m-%d}'::date"
    )
    facilities_clause = _build_facilities_clause(mb, facilities_code)
    query = QUERY_PROMPTITUDE.format(
        processing_periods=processing_periods,
        promptitude_program_id=PROMPTITUDE_PROGRAM_ID,
        facilities=facilities_clause,
    )
    df_prompt = pl.DataFrame(mb.get_data_from_sql_query(query))
    if df_prompt.is_empty():
        current_run.log_warning("Aucune donnée de promptitude extraite.")
        return None

    df_prompt = df_prompt.with_columns(
        pl.col("date_soumission").cast(pl.Datetime, strict=False),
        pl.col("date_autorisation").cast(pl.Datetime, strict=False),
    )
    df_prompt = T.add_promptitude_flag(
        df_prompt, PROMPTITUDE_DEADLINE_DAYS, PROMPTITUDE_DEFAULT_DEADLINE_DAY
    )

    attendus_periods = T.build_site_attendus_periods(df_attendus, end_dt.year, end_dt.month)
    exclude = [datetime.now().strftime("%Y%m")]
    result = T.aggregate_promptitude(attendus_periods, df_prompt, mappings["ou"], exclude)
    current_run.log_info(f"Promptitude calculée : {result.height} lignes (period, orgUnit).")
    return result


def _read_site_attendus(path: Path) -> pl.DataFrame:
    """Lit le fichier consolidé des sites attendus (``annee, code_site, rapport_attendu``).

    Parameters
    ----------
    path : Path
        Chemin du fichier (CSV ou XLSX).

    Returns
    -------
    pl.DataFrame
        Colonnes ``annee`` (int), ``code_site`` (str), ``rapport_attendu`` (int).
    """
    df = pl.read_csv(path) if path.suffix == ".csv" else pl.read_excel(path)
    return df.select(
        pl.col("annee").cast(pl.Int64),
        pl.col("code_site").cast(pl.String),
        pl.col("rapport_attendu").cast(pl.Int64),
    )


@esigl_import_dhis2.task
def build_payload(
    df_products: pl.DataFrame, df_prompt: pl.DataFrame | None, dhis2_aoc: str
) -> list[dict]:
    """Construit le payload ``dataValueSets``.

    Le COC est routé par famille : produit (colonne ``coc``) pour les métriques/dérivés,
    AOC par défaut pour la promptitude. Les zéros sont filtrés sauf pour les DE dont
    ``zeroIsSignificant=true`` (promptitude), où ils sont significatifs.

    Parameters
    ----------
    df_products : pl.DataFrame
        Agrégat produit avec indicateurs dérivés.
    df_prompt : pl.DataFrame | None
        Agrégat promptitude (ou ``None``).
    dhis2_aoc : str
        AOC (et COC promptitude).

    Returns
    -------
    list[dict]
        dataValues au format DHIS2.
    """
    dfs: list[pl.DataFrame] = []

    if df_products is not None and not df_products.is_empty():
        for metric, uid in DE_MAPPING.items():
            if metric not in df_products.columns:
                continue
            subset = df_products.filter(pl.col(metric).is_not_null())
            if metric not in ZERO_SIGNIFICANT_METRICS:
                subset = subset.filter(pl.col(metric) != 0)
            if subset.is_empty():
                continue
            dfs.append(
                subset.select(
                    pl.lit(uid).alias("dataElement"),
                    pl.col("coc").alias("categoryOptionCombo"),
                    pl.lit(dhis2_aoc).alias("attributeOptionCombo"),
                    pl.col("orgUnit"),
                    pl.col("period"),
                    pl.col(metric).cast(pl.String).alias("value"),
                )
            )

    if df_prompt is not None and not df_prompt.is_empty():
        for metric, uid in PROMPTITUDE_DE_MAPPING.items():
            if metric not in df_prompt.columns:
                continue
            subset = df_prompt.filter(pl.col(metric).is_not_null())
            if subset.is_empty():
                continue
            dfs.append(
                subset.select(
                    pl.lit(uid).alias("dataElement"),
                    pl.lit(dhis2_aoc).alias("categoryOptionCombo"),
                    pl.lit(dhis2_aoc).alias("attributeOptionCombo"),
                    pl.col("orgUnit"),
                    pl.col("period"),
                    pl.col(metric).cast(pl.String).alias("value"),
                )
            )

    if not dfs:
        current_run.log_warning("Payload vide.")
        return []
    payload = pl.concat(dfs).to_dicts()
    current_run.log_info(f"Payload construit : {len(payload)} dataValues.")
    return payload


@esigl_import_dhis2.task
def push_data_to_dhis2(
    dhis2: DHIS2,
    payload: list[dict],
    dataset_id: str,
    dry_run: bool,
    import_mode: str,
    post_batch_size: int,
    max_conflict_ratio: float,
) -> dict:
    """Pousse le payload vers ``dataValueSets`` (chunks + retry/backoff + seuil de conflits).

    Parameters
    ----------
    dhis2 : DHIS2
        Client DHIS2 cible.
    payload : list[dict]
        dataValues à importer.
    dataset_id : str
        Dataset cible.
    dry_run : bool
        Simule l'import.
    import_mode : str
        Stratégie d'import.
    post_batch_size : int
        Taille des lots.
    max_conflict_ratio : float
        Seuil d'échec sur ``conflits/total`` (les conflits signalent une métadonnée manquante ;
        le brut ``ignored`` inclut les valeurs inchangées et n'est pas un critère fiable).

    Returns
    -------
    dict
        Résumé d'import agrégé.

    Raises
    ------
    ValueError
        Si le ratio de conflits dépasse ``max_conflict_ratio``.
    """
    total = len(payload)
    if total == 0:
        return {"status": "skipped", "imported": 0}

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

    for idx, start in enumerate(range(0, total, post_batch_size), start=1):
        chunk = payload[start : start + post_batch_size]
        counts = {"imported": 0, "updated": 0, "ignored": 0, "deleted": 0}
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
                    f"Chunk {idx} tentative {attempt}/{max_retries} échouée (status={status}). "
                    f"Nouvel essai dans {sleep_s:.1f}s..."
                )
                time.sleep(sleep_s)
                continue
            break

        if response is None or response.status_code != 200:
            body = response.text if response is not None else "no response"
            aggregated["chunks"].append({"index": idx, "size": len(chunk), "status": "failed"})
            current_run.log_error(f"Échec import chunk {idx} : {body} (strategy={import_mode})")
            continue

        try:
            resp_data = response.json()
        except Exception:
            resp_data = {}
        chunk_summary = resp_data.get("response", resp_data)
        ic = chunk_summary.get("importCount", {})
        for key in counts:
            counts[key] = ic.get(key, 0)

        for conflict in chunk_summary.get("conflicts", []) or []:
            current_run.log_warning(
                f"Conflit chunk {idx} : {conflict.get('object', '')} - {conflict.get('value', '')}"
            )
            issues.append(conflict)

        aggregated["chunks"].append(
            {
                "index": idx,
                "size": len(chunk),
                "importCount": counts,
                "issues": issues,
                "status": "success",
            }
        )
        for key, value in counts.items():
            aggregated["totals"][key] += value

    totals = aggregated["totals"]
    success = totals["imported"] + totals["updated"]
    aggregated["imported"] = success

    # Les conflits (et non le brut `ignored`) signalent une métadonnée cible manquante :
    # en CREATE_AND_UPDATE, les valeurs inchangées d'un run à l'autre sont comptées `ignored`
    # sans conflit, ce qui rendrait un seuil sur `ignored` ininterprétable en régime permanent.
    total_conflicts = sum(len(c.get("issues", [])) for c in aggregated["chunks"])
    conflict_ratio = total_conflicts / total if total else 0.0
    aggregated["ignored_ratio"] = totals["ignored"] / total if total else 0.0
    aggregated["conflicts"] = total_conflicts
    aggregated["conflict_ratio"] = conflict_ratio

    current_run.log_info(
        f"Importé {success}/{total} dataValues (ignored={totals['ignored']}, "
        f"deleted={totals['deleted']}, conflits={total_conflicts}, strategy={import_mode})."
    )

    if not dry_run and conflict_ratio > max_conflict_ratio:
        current_run.log_critical(
            f"Ratio de conflits={conflict_ratio:.1%} > seuil {max_conflict_ratio:.1%} : "
            "métadonnées cibles probablement manquantes (COC/AOC/OU)."
        )
        raise ValueError(f"Trop de conflits à l'import ({conflict_ratio:.1%})")
    return aggregated


@esigl_import_dhis2.task
def align_stale_values(
    dhis2: DHIS2,
    payload: list[dict],
    dataset_id: str,
    dhis2_aoc: str,
    start_date: str | None,
    end_date: str | None,
    months_back: int,
    dry_run: bool,
    enabled: bool,
    _summary: dict | None = None,
) -> dict:
    """Supprime en cible les valeurs obsolètes absentes du payload (opt-in).

    Nécessaire car DHIS2 ignore les zéros pour les DE ``zeroIsSignificant=false`` : un flag
    dérivé passé de 1 à 0 (correction rétroactive) ne peut être corrigé que par un DELETE.
    Restreint aux DE gérés par ce pipeline et à l'AOC courant (aucun autre flux impacté).

    Parameters
    ----------
    dhis2 : DHIS2
        Client DHIS2 cible.
    payload : list[dict]
        Payload courant (upserts).
    dataset_id : str
        Dataset cible.
    dhis2_aoc : str
        AOC courant.
    start_date, end_date : str | None
        Bornes d'extraction.
    months_back : int
        Historique en mois.
    dry_run : bool
        Simule le DELETE.
    enabled : bool
        Active l'alignement.
    _summary : dict | None
        Jeton d'ordonnancement du DAG (ignoré).

    Returns
    -------
    dict
        Résumé de l'alignement (nb supprimés, statut).
    """
    if not enabled:
        return {"status": "disabled", "deleted": 0}
    if not payload:
        return {"status": "skipped", "deleted": 0}

    _cmm_start, pub_start, end_dt = compute_extraction_window(start_date, end_date, months_back)
    periods = _monthly_period_codes(pub_start, end_dt)
    managed_de = list(DE_MAPPING.values()) + list(PROMPTITUDE_DE_MAPPING.values())

    try:
        existing = dhis2.data_value_sets.get(
            datasets=[dataset_id],
            data_elements=managed_de,
            periods=periods,
            attribute_option_combos=[dhis2_aoc],
        )
    except Exception as err:
        current_run.log_error(f"Alignement suppressions : échec lecture cible : {err!s}")
        return {"status": "failed", "deleted": 0}

    def _key(dv: dict) -> tuple:
        return (
            dv["dataElement"],
            dv.get("categoryOptionCombo"),
            dv["orgUnit"],
            dv["period"],
        )

    payload_keys = {_key(dv) for dv in payload}
    stale = [dv for dv in existing if _key(dv) not in payload_keys]

    if not stale:
        current_run.log_info("Alignement suppressions : aucune valeur obsolète.")
        return {"status": "completed", "deleted": 0}

    current_run.log_info(
        f"Alignement suppressions : {len(stale)} valeurs obsolètes à supprimer (dry_run={dry_run})."
    )
    try:
        dhis2.data_value_sets.post(data_values=stale, import_strategy="DELETE", dry_run=dry_run)
    except Exception as err:
        current_run.log_error(f"Alignement suppressions : échec DELETE : {err!s}")
        return {"status": "failed", "deleted": 0, "candidates": len(stale)}
    return {"status": "completed", "deleted": len(stale), "dry_run": dry_run}


def _monthly_period_codes(start: datetime, end: datetime) -> list[str]:
    """Liste les codes période mensuels ``YYYYMM`` de ``start`` à ``end`` (inclus).

    Parameters
    ----------
    start, end : datetime
        Bornes (incluses).

    Returns
    -------
    list[str]
        Codes période au format DHIS2 mensuel.
    """
    periods: list[str] = []
    cursor = start.replace(day=1)
    while cursor <= end:
        periods.append(cursor.strftime("%Y%m"))
        cursor += relativedelta(months=1)
    return periods


@esigl_import_dhis2.task
def write_import_report(output_dir: str, payload: list[dict], summary: dict, align: dict) -> Path:
    """Écrit les rapports d'audit (payload + résumé d'import + alignement).

    Parameters
    ----------
    output_dir : str
        Répertoire de sortie (relatif au workspace).
    payload : list[dict]
        Payload envoyé.
    summary : dict
        Résumé du push.
    align : dict
        Résumé de l'alignement des suppressions.

    Returns
    -------
    Path
        Répertoire du rapport créé.
    """
    run_dir = Path(
        workspace.files_path, output_dir, datetime.now(tz=UTC).strftime("%Y-%m-%d_%H-%M-%S")
    )
    run_dir.mkdir(parents=True, exist_ok=True)

    payload_fp = run_dir / "payload.json"
    with payload_fp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    report_fp = run_dir / "report.json"
    with report_fp.open("w", encoding="utf-8") as f:
        json.dump({"import": summary, "align": align}, f, indent=2, ensure_ascii=False)

    current_run.log_info(f"Rapports écrits dans {run_dir.as_posix()}")
    current_run.add_file_output(payload_fp.as_posix())
    current_run.add_file_output(report_fp.as_posix())
    return run_dir


@esigl_import_dhis2.task
def cleanup_old_directory_files(
    output_dir: Path, _report_dir: Path, retention_days: int = 60
) -> None:
    """Supprime les répertoires de rapport plus vieux que ``retention_days``.

    Parameters
    ----------
    output_dir : Path
        Répertoire de sortie (relatif au workspace).
    _report_dir : Path
        Jeton d'ordonnancement du DAG (ignoré).
    retention_days : int
        Rétention en jours.
    """
    base = Path(workspace.files_path, output_dir)
    if not base.exists():
        return
    now = datetime.now()
    for item in base.iterdir():
        if not item.is_dir():
            continue
        try:
            folder_time = datetime.strptime(item.name, "%Y-%m-%d_%H-%M-%S")
        except ValueError:
            continue
        if (now - folder_time).days >= retention_days:
            for sub in item.iterdir():
                sub.unlink()
            item.rmdir()
            current_run.log_info(f"Répertoire de rapport supprimé : {item.as_posix()}")


if __name__ == "__main__":
    esigl_import_dhis2()
