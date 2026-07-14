"""Transformations métier pures du module Gestion de Stock (refonte eSIGL → DHIS2).

Toutes les fonctions de ce module sont **pures** (``pl.DataFrame -> pl.DataFrame``) et
**sans dépendance OpenHEXA/DHIS2** : elles sont testables unitairement (voir ``tests/``).
Le pipeline (`pipeline.py`) se contente d'orchestrer les I/O (Metabase, DHIS2) autour d'elles.

Grain de sortie systématique : ``(period, orgUnit, coc)`` pour les métriques produit,
``(period, orgUnit)`` pour la promptitude.
"""

from datetime import date, datetime

import polars as pl
import polars.selectors as cs

# Métriques sommables (agrégation entre sites eSIGL mappés à une même orgUnit DHIS2).
_SUM_METRICS = (
    "quantite_recue",
    "quantite_distribuee",
    "nbrejrsrupture",
    "perte_ajustement",
)


def monthly_periods(min_year: int, max_year: int, max_month: int) -> pl.DataFrame:
    """Génère la grille mensuelle des périodes DHIS2 (format ``YYYYMM``).

    Parameters
    ----------
    min_year : int
        Première année (incluse), au 1er janvier.
    max_year : int
        Dernière année.
    max_month : int
        Dernier mois (inclus) de ``max_year``.

    Returns
    -------
    pl.DataFrame
        Colonnes ``period`` (str ``YYYYMM``) et ``annee`` (int).
    """
    return (
        pl.DataFrame(
            {
                "period_date": pl.date_range(
                    start=date(min_year, 1, 1),
                    end=date(max_year, max_month, 1),
                    interval="1mo",
                    eager=True,
                )
            }
        )
        .with_columns(
            pl.col("period_date").dt.strftime("%Y%m").alias("period"),
            pl.col("period_date").dt.year().alias("annee"),
        )
        .select("period", "annee")
    )


def add_nbrejrsdumois(df: pl.DataFrame) -> pl.DataFrame:
    """Ajoute le nombre de jours du mois de la période (à partir de ``enddate``).

    Parameters
    ----------
    df : pl.DataFrame
        Doit contenir une colonne ``enddate``.

    Returns
    -------
    pl.DataFrame
        ``df`` enrichi de la colonne ``nbrejrsdumois`` (int).
    """
    return df.with_columns(
        pl.col("enddate").cast(pl.Datetime).dt.month_end().dt.day().alias("nbrejrsdumois")
    )


def add_report_duration(df: pl.DataFrame) -> pl.DataFrame:
    """Ajoute la durée couverte par le rapport, en jours (``enddate - startdate + 1``).

    Utilisé pour les produits GTC, rapportés en hebdomadaire : le dénominateur de la rupture
    n'est pas le nombre de jours du mois calendaire mais la durée réellement couverte par le
    rapport. La somme de ces durées sur les rapports d'un mois donne le total de jours couverts.

    Parameters
    ----------
    df : pl.DataFrame
        Doit contenir ``enddate`` et ``startdate``.

    Returns
    -------
    pl.DataFrame
        ``df`` enrichi de ``nbrejrsdumois`` (durée du rapport en jours).
    """
    duree = (
        pl.col("enddate").cast(pl.Datetime) - pl.col("startdate").cast(pl.Datetime)
    ).dt.total_days() + 1
    return df.with_columns(duree.alias("nbrejrsdumois"))


def _round_int(df: pl.DataFrame) -> pl.DataFrame:
    """Arrondit toutes les colonnes numériques et les convertit en entiers 64 bits."""  # noqa: DOC201
    return df.with_columns(cs.numeric().round(0).cast(pl.Int64))


def aggregate_routine(df: pl.DataFrame, pub_start: datetime, pub_end: datetime) -> pl.DataFrame:
    """Agrège les produits de routine au grain ``(period, orgUnit, coc)``.

    Les CMM et quantités de commande proviennent directement d'eSIGL (contrairement au GTC).
    ``nbrejrsdumois`` et ``nbrejrsrupture`` sont sommés sur les sites eSIGL regroupés sur une
    même orgUnit (agrégation par produit), pour que la comparaison de rupture reste cohérente.

    Parameters
    ----------
    df : pl.DataFrame
        Lignes enrichies (``period, orgUnit, coc, enddate`` + métriques brutes).
    pub_start, pub_end : datetime
        Bornes (incluses) de la fenêtre de publication.

    Returns
    -------
    pl.DataFrame
        Agrégat routine avec ``produit_gere = 1``.
    """
    df = add_nbrejrsdumois(df.with_columns(pl.col("enddate").cast(pl.Datetime))).filter(
        (pl.col("enddate") >= pub_start) & (pl.col("enddate") <= pub_end)
    )
    return (
        df.group_by(["period", "orgUnit", "coc"])
        .agg(
            *[pl.col(c).sum() for c in _SUM_METRICS],
            pl.col("quantite_proposee").sum(),
            pl.col("quantite_commandee").sum(),
            pl.col("quantite_approuvee").sum(),
            pl.col("cmm").sum(),
            pl.col("stock_initial").sum(),
            pl.col("sdu").sum(),
            pl.col("nbrejrsdumois").sum(),
        )
        .pipe(_round_int)
        .with_columns(pl.lit(1).cast(pl.Int64).alias("produit_gere"))
    )


def compute_cmm_glissante(df: pl.DataFrame) -> pl.DataFrame:
    """Calcule la CMM comme moyenne glissante (3 mois) des quantités distribuées.

    Utilisé pour les produits GTC dont la CMM eSIGL n'est pas exploitable. Le calcul
    exploite l'historique complet extrait (fenêtre étendue de 3 mois avant publication).

    Parameters
    ----------
    df : pl.DataFrame
        Lignes GTC (``period, orgUnit, coc, quantite_distribuee``), toutes périodes extraites.

    Returns
    -------
    pl.DataFrame
        Colonnes ``period, orgUnit, coc, cmm``.
    """
    base = (
        df.select("period", "orgUnit", "coc", "quantite_distribuee")
        .group_by(["period", "orgUnit", "coc"])
        .agg(pl.col("quantite_distribuee").sum())
        .with_columns(pl.col("period").str.strptime(pl.Date, format="%Y%m").alias("period_date"))
        .sort(["orgUnit", "coc", "period_date"])
    )
    return (
        base.group_by(["orgUnit", "coc"])
        .agg(
            pl.col("period_date"),
            pl.col("quantite_distribuee").rolling_mean(window_size=3, min_samples=1).alias("cmm"),
        )
        .explode(["period_date", "cmm"])
        .join(
            base.select("orgUnit", "coc", "period_date", "period"),
            on=["orgUnit", "coc", "period_date"],
        )
        .select("period", "orgUnit", "coc", "cmm")
    )


def aggregate_gtc(df: pl.DataFrame, pub_start: datetime, pub_end: datetime) -> pl.DataFrame:
    """Agrège les produits GTC au grain ``(period, orgUnit, coc)``.

    ``stock_initial`` = premier de la période, ``sdu`` = dernier, CMM recalculée en moyenne
    glissante sur l'historique complet. Les quantités de commande sont nulles (non suivies GTC).
    Rapportage **hebdomadaire** : ``nbrejrsdumois`` = somme des durées des rapports du mois
    (``enddate - startdate + 1``), et non le nombre de jours du mois calendaire.

    Parameters
    ----------
    df : pl.DataFrame
        Lignes GTC enrichies (``startdate`` inclus), toutes périodes extraites (fenêtre CMM).
    pub_start, pub_end : datetime
        Bornes (incluses) de la fenêtre de publication.

    Returns
    -------
    pl.DataFrame
        Agrégat GTC avec ``produit_gere = 1`` et quantités de commande à ``null``.
    """
    df = df.with_columns(pl.col("enddate").cast(pl.Datetime), pl.col("startdate").cast(pl.Datetime))
    df_cmm = compute_cmm_glissante(df)

    return (
        add_report_duration(df)
        .filter((pl.col("enddate") >= pub_start) & (pl.col("enddate") <= pub_end))
        .group_by(["period", "orgUnit", "coc"])
        .agg(
            *[pl.col(c).sum() for c in _SUM_METRICS],
            pl.col("stock_initial").first(),
            pl.col("sdu").last(),
            pl.col("nbrejrsdumois").sum(),
        )
        .join(df_cmm, on=["period", "orgUnit", "coc"], how="left")
        .pipe(_round_int)
        .with_columns(
            pl.lit(1).cast(pl.Int64).alias("produit_gere"),
            pl.lit(None).cast(pl.Int64).alias("quantite_proposee"),
            pl.lit(None).cast(pl.Int64).alias("quantite_commandee"),
            pl.lit(None).cast(pl.Int64).alias("quantite_approuvee"),
        )
    )


def build_traceur_periods(
    df_traceurs: pl.DataFrame, df_coc_mapping: pl.DataFrame, max_year: int, max_month: int
) -> pl.DataFrame:
    """Construit la table ``(period, coc)`` des produits traceurs.

    Le statut « traceur » est annuel : chaque produit traceur d'une année est traceur pour
    tous les mois de cette année. Le code produit est traduit en COC via le mapping produit.

    Parameters
    ----------
    df_traceurs : pl.DataFrame
        Feuille « Traceurs » (colonnes ``annee``, ``code_produit`` après normalisation).
    df_coc_mapping : pl.DataFrame
        Mapping ``code_produit -> coc``.
    max_year, max_month : int
        Dernière année / dernier mois de la grille de périodes.

    Returns
    -------
    pl.DataFrame
        Colonnes ``period, coc, categorie_produit_traceur`` (=1), dédupliquées.
    """
    min_year = int(df_traceurs["annee"].min())  # type: ignore[arg-type]
    grid = monthly_periods(min_year, max_year, max_month)
    return (
        grid.join(df_traceurs.select("annee", "code_produit"), on="annee", how="left")
        .join(df_coc_mapping.select("code_produit", "coc"), on="code_produit", how="left")
        .filter(pl.col("coc").is_not_null())
        .select("period", "coc")
        .unique()
        .with_columns(pl.lit(1).cast(pl.Int64).alias("categorie_produit_traceur"))
    )


def add_traceur_flags(df: pl.DataFrame, df_traceur_periods: pl.DataFrame) -> pl.DataFrame:
    """Ajoute les flags traceur / non-traceur par jointure ``(period, coc)``.

    Parameters
    ----------
    df : pl.DataFrame
        Agrégat produit (grain ``period, orgUnit, coc``).
    df_traceur_periods : pl.DataFrame
        Sortie de :func:`build_traceur_periods`.

    Returns
    -------
    pl.DataFrame
        ``df`` enrichi de ``categorie_produit_traceur`` (1/null) et ``produit_non_traceur`` (0/1).
    """
    return df.join(df_traceur_periods, on=["period", "coc"], how="left").with_columns(
        pl.col("categorie_produit_traceur").is_null().cast(pl.Int64).alias("produit_non_traceur")
    )


def add_rupture_flags(df: pl.DataFrame) -> pl.DataFrame:
    """Calcule les indicateurs de rupture (global, traceur, non-traceur).

    Un produit est en rupture si ``nbrejrsrupture >= nbrejrsdumois``.

    Parameters
    ----------
    df : pl.DataFrame
        Doit contenir ``nbrejrsrupture``, ``nbrejrsdumois``, ``produit_non_traceur``.

    Returns
    -------
    pl.DataFrame
        ``df`` enrichi de ``rupture_stock`` et de sa ventilation traceur / non-traceur.
    """
    rupture = pl.col("nbrejrsrupture") >= pl.col("nbrejrsdumois")
    non_traceur = pl.col("produit_non_traceur") == 1
    return df.with_columns(
        rupture.cast(pl.Int64).alias("rupture_stock"),
        (rupture & ~non_traceur).cast(pl.Int64).alias("rupture_traceur_stock"),
        (rupture & non_traceur).cast(pl.Int64).alias("rupture_non_traceur_stock"),
    )


def add_cmm_gestionnaire(df: pl.DataFrame) -> pl.DataFrame:
    """Calcule la CMM gestionnaire.

    Règle métier (à confirmer) : ``(quantite_commandee + sdu) / 4`` si une commande a été
    passée (``quantite_commandee > 0``), sinon la CMM standard. Le diviseur ``4`` est une
    constante métier héritée du notebook de refonte (justification à documenter).

    Parameters
    ----------
    df : pl.DataFrame
        Doit contenir ``quantite_commandee``, ``sdu``, ``cmm``.

    Returns
    -------
    pl.DataFrame
        ``df`` enrichi de ``cmm_gestionnaire``.
    """
    commandee = pl.col("quantite_commandee").fill_null(0)
    return df.with_columns(
        pl.when(commandee > 0)
        .then((pl.col("quantite_commandee") + pl.col("sdu")) / 4)
        .otherwise(pl.col("cmm"))
        .alias("cmm_gestionnaire")
    )


def add_bien_stocke(df: pl.DataFrame, enabled: bool) -> pl.DataFrame:
    """Ajoute l'indicateur « produit bien stocké ».

    ⚠️ Règle métier **provisoire** tant que la définition officielle n'est pas fournie :
    bien stocké = non en rupture, CMM connue, et SDU compris entre 1 et 3 mois de CMM.
    Désactivé par défaut (colonne à ``null`` → non poussée) pour ne pas publier de valeurs
    non validées.

    Parameters
    ----------
    df : pl.DataFrame
        Doit contenir ``rupture_stock``, ``cmm``, ``sdu``.
    enabled : bool
        Active le calcul ; sinon la colonne est ``null``.

    Returns
    -------
    pl.DataFrame
        ``df`` enrichi de ``bien_stocke`` (0/1 ou null).
    """
    if not enabled:
        return df.with_columns(pl.lit(None).cast(pl.Int64).alias("bien_stocke"))
    bien = (
        (pl.col("rupture_stock") == 0)
        & (pl.col("cmm") > 0)
        & (pl.col("sdu") >= pl.col("cmm"))
        & (pl.col("sdu") <= 3 * pl.col("cmm"))
    )
    return df.with_columns(bien.cast(pl.Int64).alias("bien_stocke"))


def compute_derived_indicators(
    df: pl.DataFrame,
    df_traceur_periods: pl.DataFrame,
    enable_bien_stocke: bool,
) -> pl.DataFrame:
    """Enchaîne tous les indicateurs dérivés sur un agrégat produit.

    Parameters
    ----------
    df : pl.DataFrame
        Agrégat produit (routine + GTC concaténés).
    df_traceur_periods : pl.DataFrame
        Table ``(period, coc)`` des traceurs.
    enable_bien_stocke : bool
        Active le calcul de ``bien_stocke``.

    Returns
    -------
    pl.DataFrame
        Agrégat enrichi de tous les indicateurs dérivés.
    """
    return (
        df.pipe(add_traceur_flags, df_traceur_periods)
        .pipe(add_rupture_flags)
        .pipe(add_cmm_gestionnaire)
        .pipe(add_bien_stocke, enable_bien_stocke)
    )


# --- Promptitude des rapports -------------------------------------------------


def add_promptitude_flag(
    df: pl.DataFrame, deadline_days: dict[str, int], default_day: int
) -> pl.DataFrame:
    """Détermine si le rapport a été transmis dans le délai (``rapport_prompt``).

    La date-limite est le jour ``N`` du mois de soumission, ``N`` dépendant du type de
    structure. Un rapport non soumis (``date_soumission`` nulle) est non-prompt (0).

    Parameters
    ----------
    df : pl.DataFrame
        Doit contenir ``type_structure`` et ``date_soumission`` (Datetime).
    deadline_days : dict[str, int]
        Jour-limite par type de structure.
    default_day : int
        Jour-limite par défaut.

    Returns
    -------
    pl.DataFrame
        ``df`` enrichi de ``date_limite_promptitude`` et ``rapport_prompt`` (0/1).
    """
    day_expr = pl.lit(default_day)
    for structure, day in deadline_days.items():
        day_expr = (
            pl.when(pl.col("type_structure") == structure).then(pl.lit(day)).otherwise(day_expr)
        )

    df = df.with_columns(
        pl.datetime(
            pl.col("date_soumission").dt.year(),
            pl.col("date_soumission").dt.month(),
            day_expr,
        ).alias("date_limite_promptitude")
    )
    return df.with_columns(
        (pl.col("date_soumission") <= pl.col("date_limite_promptitude"))
        .cast(pl.Int64)
        .fill_null(0)
        .alias("rapport_prompt")
    )


def build_site_attendus_periods(
    df_attendus: pl.DataFrame, max_year: int, max_month: int
) -> pl.DataFrame:
    """Étend la liste annuelle des sites attendus en grille mensuelle.

    Parameters
    ----------
    df_attendus : pl.DataFrame
        Colonnes ``annee``, ``code_site``, ``rapport_attendu``.
    max_year, max_month : int
        Dernière année / dernier mois de la grille.

    Returns
    -------
    pl.DataFrame
        Colonnes ``period, code_site, rapport_attendu``.
    """
    min_year = int(df_attendus["annee"].min())  # type: ignore[arg-type]
    grid = monthly_periods(min_year, max_year, max_month)
    return grid.join(df_attendus, on="annee", how="left").select(
        "period", "code_site", "rapport_attendu"
    )


def aggregate_promptitude(
    df_attendus_periods: pl.DataFrame,
    df_prompt: pl.DataFrame,
    df_ou_mapping: pl.DataFrame,
    exclude_periods: list[str],
) -> pl.DataFrame:
    """Croise sites attendus et soumissions, agrège au grain ``(period, orgUnit)``.

    Parameters
    ----------
    df_attendus_periods : pl.DataFrame
        Sortie de :func:`build_site_attendus_periods`.
    df_prompt : pl.DataFrame
        Soumissions avec ``rapport_prompt`` (sortie de :func:`add_promptitude_flag`).
    df_ou_mapping : pl.DataFrame
        Mapping ``code_site -> orgUnit``.
    exclude_periods : list[str]
        Périodes à exclure (ex. mois courant incomplet).

    Returns
    -------
    pl.DataFrame
        Colonnes ``period, orgUnit, rapport_attendu, rapport_prompt``.
    """
    return (
        df_attendus_periods.with_columns(pl.col("code_site").cast(pl.String))
        .join(
            df_prompt.select("period", "code_site", "rapport_prompt"),
            on=["period", "code_site"],
            how="left",
        )
        .filter(~pl.col("period").is_in(exclude_periods))
        .with_columns(pl.col("rapport_prompt").fill_null(0))
        .join(df_ou_mapping, on="code_site", how="inner")
        .group_by(["period", "orgUnit"])
        .agg(pl.col("rapport_attendu").sum(), pl.col("rapport_prompt").sum())
    )
