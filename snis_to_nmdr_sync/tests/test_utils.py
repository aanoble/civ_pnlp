"""Unit tests for the pure helpers of the snis_to_dedop_sync pipeline."""

import pytest
from utils import PERIOD_GRANULARITY, PERIOD_TYPE_TO_CLASS, convert_period_id


@pytest.mark.parametrize(
    ("period", "source", "target", "expected"),
    [
        ("202401", "Monthly", "Monthly", "202401"),
        ("202401", "Monthly", "Quarterly", "2024Q1"),
        ("202403", "Monthly", "Quarterly", "2024Q1"),
        ("202404", "Monthly", "Quarterly", "2024Q2"),
        ("202401", "Monthly", "Yearly", "2024"),
        ("2024Q1", "Quarterly", "Yearly", "2024"),
    ],
)
def test_convert_period_id_supported(period: str, source: str, target: str, expected: str) -> None:
    """Aggregation from a finer to a coarser period type is supported."""
    assert convert_period_id(period, source, target) == expected


@pytest.mark.parametrize(
    ("period", "source", "target"),
    [
        ("2024Q1", "Quarterly", "Monthly"),  # coarse -> fine not possible
        ("2024", "Yearly", "Quarterly"),
        ("202401", "Monthly", "Unknown"),  # unknown target
        ("202401", "Unknown", "Monthly"),  # unknown source
    ],
)
def test_convert_period_id_unsupported(period: str, source: str, target: str) -> None:
    """Unsupported conversions return None."""
    assert convert_period_id(period, source, target) is None


def test_period_type_tables_are_consistent() -> None:
    """Every period class has a granularity ranking."""
    assert set(PERIOD_TYPE_TO_CLASS) == set(PERIOD_GRANULARITY)
