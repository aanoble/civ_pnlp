"""Tests unitaires des transformations métier pures (``transforms.py``)."""

from datetime import datetime

import polars as pl
import pytest
import transforms as T  # noqa: N812


def test_monthly_periods_spans_years() -> None:
    """La grille couvre chaque mois de min_year jusqu'à (max_year, max_month)."""
    grid = T.monthly_periods(2024, 2025, 3)
    assert grid["period"].to_list()[0] == "202401"
    assert grid["period"].to_list()[-1] == "202503"
    assert grid.height == 12 + 3


def test_add_nbrejrsdumois_days_in_month() -> None:
    """Nbrejrsdumois = nombre de jours du mois de enddate."""
    df = pl.DataFrame({"enddate": [datetime(2025, 2, 7), datetime(2024, 2, 28)]})
    out = T.add_nbrejrsdumois(df)
    assert out["nbrejrsdumois"].to_list() == [28, 29]


def _routine_row(**kw: object) -> dict:
    base = {
        "period": "202501",
        "enddate": datetime(2025, 1, 31),
        "orgUnit": "OU1",
        "coc": "C1",
        "stock_initial": 10,
        "quantite_recue": 5,
        "quantite_distribuee": 4,
        "nbrejrsrupture": 0,
        "perte_ajustement": 0,
        "quantite_proposee": 1,
        "quantite_commandee": 2,
        "quantite_approuvee": 3,
        "cmm": 4,
        "sdu": 11,
    }
    base.update(kw)
    return base


def test_aggregate_routine_sums_and_flags() -> None:
    """La routine somme les métriques (dont nbrejrsdumois) et pose produit_gere=1."""
    df = pl.DataFrame([_routine_row(), _routine_row(quantite_recue=5, orgUnit="OU1")])
    out = T.aggregate_routine(df, datetime(2025, 1, 1), datetime(2025, 1, 31, 23, 59))
    assert out.height == 1
    row = out.row(0, named=True)
    assert row["quantite_recue"] == 10  # 5 + 5
    assert row["nbrejrsdumois"] == 62  # 31 + 31 (sommé sur les sites regroupés)
    assert row["produit_gere"] == 1


def test_aggregate_routine_filters_window() -> None:
    """Les lignes hors fenêtre de publication sont écartées."""
    df = pl.DataFrame([_routine_row(period="202412", enddate=datetime(2024, 12, 31))])
    out = T.aggregate_routine(df, datetime(2025, 1, 1), datetime(2025, 1, 31, 23, 59))
    assert out.is_empty()


def test_compute_cmm_glissante_rolling_mean() -> None:
    """La CMM est la moyenne glissante (3 mois) des quantités distribuées."""
    df = pl.DataFrame(
        {
            "period": ["202501", "202502", "202503"],
            "orgUnit": ["OU1"] * 3,
            "coc": ["C1"] * 3,
            "quantite_distribuee": [3, 6, 9],
        }
    )
    out = T.compute_cmm_glissante(df).sort("period")
    # moyennes glissantes: 3, (3+6)/2=4.5, (3+6+9)/3=6
    assert out["cmm"].to_list() == pytest.approx([3.0, 4.5, 6.0])


def _gtc_row(**kw: object) -> dict:
    base = {
        "period": "202501",
        "enddate": datetime(2025, 1, 31),
        "orgUnit": "OU1",
        "coc": "C1",
        "stock_initial": 100,
        "quantite_recue": 5,
        "quantite_distribuee": 4,
        "nbrejrsrupture": 0,
        "perte_ajustement": 0,
        "sdu": 90,
    }
    base.update(kw)
    return base


def test_aggregate_gtc_nulls_order_quantities() -> None:
    """Le GTC laisse les quantités de commande nulles et calcule une CMM."""
    df = pl.DataFrame([_gtc_row()])
    out = T.aggregate_gtc(df, datetime(2025, 1, 1), datetime(2025, 1, 31, 23, 59))
    row = out.row(0, named=True)
    assert row["quantite_commandee"] is None
    assert row["produit_gere"] == 1
    assert row["cmm"] == 4  # une seule période


def test_traceur_flags() -> None:
    """Un COC traceur sur la période est marqué ; sinon produit_non_traceur=1."""
    df_traceurs = pl.DataFrame({"annee": [2025], "code_produit": ["P1"]})
    df_coc = pl.DataFrame({"code_produit": ["P1"], "coc": ["C1"]})
    periods = T.build_traceur_periods(df_traceurs, df_coc, 2025, 3)
    assert set(periods["period"].to_list()) == {"202501", "202502", "202503"}

    df = pl.DataFrame({"period": ["202501", "202501"], "coc": ["C1", "C2"], "orgUnit": ["O", "O"]})
    out = T.add_traceur_flags(df, periods).sort("coc")
    assert out["categorie_produit_traceur"].to_list() == [1, None]
    assert out["produit_non_traceur"].to_list() == [0, 1]


def test_rupture_flags() -> None:
    """rupture_stock si nbrejrsrupture >= nbrejrsdumois, ventilé traceur/non-traceur."""
    df = pl.DataFrame(
        {
            "nbrejrsrupture": [31, 5],
            "nbrejrsdumois": [31, 31],
            "produit_non_traceur": [0, 1],
        }
    )
    out = T.add_rupture_flags(df)
    assert out["rupture_stock"].to_list() == [1, 0]
    assert out["rupture_traceur_stock"].to_list() == [1, 0]
    assert out["rupture_non_traceur_stock"].to_list() == [0, 0]


def test_cmm_gestionnaire_branches() -> None:
    """CMM gestionnaire = (commandee+sdu)/4 si commande, sinon cmm ; robuste au null."""
    df = pl.DataFrame(
        {
            "quantite_commandee": [4, 0, None],
            "sdu": [8, 8, 8],
            "cmm": [10, 10, 10],
        }
    )
    out = T.add_cmm_gestionnaire(df)
    assert out["cmm_gestionnaire"].to_list() == pytest.approx([3.0, 10.0, 10.0])


def test_bien_stocke_toggle() -> None:
    """bien_stocke est null si désactivé, calculé sinon."""
    df = pl.DataFrame({"rupture_stock": [0], "cmm": [10], "sdu": [15]})
    assert T.add_bien_stocke(df, enabled=False)["bien_stocke"].to_list() == [None]
    assert T.add_bien_stocke(df, enabled=True)["bien_stocke"].to_list() == [1]


def test_promptitude_flag() -> None:
    """rapport_prompt=1 si soumis dans le délai, 0 si en retard ou non soumis."""
    df = pl.DataFrame(
        {
            "type_structure": ["DISTRICT SANITAIRE", "HOPITAL GENERAL", "CHR"],
            "date_soumission": [
                datetime(2025, 2, 9),  # <= 10 → prompt
                datetime(2025, 2, 9),  # > 7 → retard
                None,  # non soumis → 0
            ],
        }
    )
    out = T.add_promptitude_flag(df, {"DISTRICT SANITAIRE": 10, "CHU": 10}, 7)
    assert out["rapport_prompt"].to_list() == [1, 0, 0]


def test_aggregate_promptitude() -> None:
    """Croisement sites attendus vs soumissions, agrégé par (period, orgUnit)."""
    attendus = pl.DataFrame(
        {"period": ["202501", "202501"], "code_site": ["S1", "S2"], "rapport_attendu": [1, 1]}
    )
    prompt = pl.DataFrame({"period": ["202501"], "code_site": ["S1"], "rapport_prompt": [1]})
    ou = pl.DataFrame({"code_site": ["S1", "S2"], "orgUnit": ["O1", "O1"]})
    out = T.aggregate_promptitude(attendus, prompt, ou, exclude_periods=[])
    row = out.row(0, named=True)
    assert row["rapport_attendu"] == 2
    assert row["rapport_prompt"] == 1  # S2 non soumis → 0
