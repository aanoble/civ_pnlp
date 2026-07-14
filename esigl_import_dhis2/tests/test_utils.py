"""Tests unitaires du calcul de la fenêtre d'extraction/publication."""

from datetime import datetime

from utils import compute_extraction_window

NOW = datetime(2026, 7, 14)  # mois courant = juillet (non ouvert à la saisie)


def test_window_default_caps_at_previous_month() -> None:
    """Par défaut, la fin est plafonnée à la dernière période close (juin), pas juillet."""
    cmm_start, pub_start, end = compute_extraction_window(None, None, months_back=3, now=NOW)
    assert (end.year, end.month, end.day) == (2026, 6, 30)  # pas juillet
    assert (pub_start.year, pub_start.month) == (2026, 3)  # juin - 3 mois
    assert (cmm_start.year, cmm_start.month) == (2025, 12)  # pub_start - 3 mois (CMM)


def test_window_future_end_date_is_capped() -> None:
    """Une end_date au-delà de la dernière période close est ramenée à ce plafond."""
    _cmm, _pub, end = compute_extraction_window("2026-01-01", "2026-09-30", months_back=0, now=NOW)
    assert (end.year, end.month, end.day) == (2026, 6, 30)


def test_window_past_backfill_not_capped() -> None:
    """Un backfill entièrement passé n'est pas plafonné."""
    _cmm, pub_start, end = compute_extraction_window(
        "2025-01-01", "2025-03-31", months_back=0, now=NOW
    )
    assert (pub_start.year, pub_start.month, pub_start.day) == (2025, 1, 1)
    assert (end.year, end.month, end.day) == (2025, 3, 31)
