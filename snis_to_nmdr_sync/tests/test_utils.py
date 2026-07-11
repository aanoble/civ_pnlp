"""Unit tests for the pure helpers of the snis_to_dedop_sync pipeline."""

import itertools
import sys
from datetime import datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils import (
    PERIOD_GRANULARITY,
    PERIOD_TYPE_TO_CLASS,
    compute_incremental_cutoff,
    convert_period_id,
    parse_cutoff_date,
)


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
    "cutoff_str",
    [
        "2024-01-31",
        "1999-12-01",
        "2020-02-29",
    ],
)
def test_parse_cutoff_date_valid_iso_dates(cutoff_str: str) -> None:
    """Parse valid ISO date strings."""
    cutoff = parse_cutoff_date(cutoff_str)
    assert isinstance(cutoff, datetime)
    expected = datetime.strptime(cutoff_str, "%Y-%m-%d")
    assert cutoff.year == expected.year
    assert cutoff.month == expected.month
    assert cutoff.day == expected.day


@pytest.mark.parametrize(
    "cutoff_str",
    [
        "2024/01/31",  # wrong separator
        "2024-13-01",  # invalid month
        "2024-00-10",  # invalid month zero
        "2024-01-32",  # invalid day
        "",  # empty string
        "not-a-date",  # arbitrary text
    ],
)
def test_parse_cutoff_date_invalid_string_inputs(cutoff_str: str) -> None:
    """Invalid ISO date strings should raise a ValueError."""
    with pytest.raises(ValueError):  # noqa: PT011
        parse_cutoff_date(cutoff_str)


@pytest.mark.parametrize(
    "value",
    [
        None,
        123,
        12.34,
        object(),
        True,
    ],
)
def test_parse_cutoff_date_non_string_inputs(value: str) -> None:
    """Non-string inputs should raise a ValueError."""
    with pytest.raises(ValueError):  # noqa: PT011
        parse_cutoff_date(value)


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


def test_compute_incremental_cutoff_explicit_last_updated_wins() -> None:
    """An explicit last_updated is always returned unchanged, even on a backfill tick."""
    now = datetime(2024, 3, 15, 6, 0, 0)  # would otherwise be a backfill tick
    explicit = datetime(2023, 1, 1)
    assert compute_incremental_cutoff(now, explicit) == explicit


@pytest.mark.parametrize(
    ("now", "expected"),
    [
        # Backfill ticks: 1st / 15th / last day of month at 06h or 18h -> previous month start.
        (datetime(2024, 3, 1, 6, 0), datetime(2024, 2, 1)),
        (datetime(2024, 3, 15, 18, 0), datetime(2024, 2, 1)),
        (datetime(2024, 3, 31, 6, 0), datetime(2024, 2, 1)),
        (datetime(2024, 2, 29, 18, 0), datetime(2024, 1, 1)),  # leap-year last day
        (datetime(2024, 1, 1, 6, 0), datetime(2023, 12, 1)),  # crosses year boundary
    ],
)
def test_compute_incremental_cutoff_backfill_ticks(now: datetime, expected: datetime) -> None:
    """On backfill ticks the cutoff widens to the first day of the previous month."""
    assert compute_incremental_cutoff(now, None) == expected


@pytest.mark.parametrize(
    "now",
    [
        datetime(2024, 3, 2, 6, 0),  # not a trigger day
        datetime(2024, 3, 15, 7, 0),  # trigger day but wrong hour
        datetime(2024, 3, 1, 0, 0),  # trigger day but wrong hour
        datetime(2024, 3, 10, 18, 0),  # trigger hour but not a trigger day
    ],
)
def test_compute_incremental_cutoff_regular_ticks(now: datetime) -> None:
    """Outside backfill ticks the cutoff is the current day."""
    assert compute_incremental_cutoff(now, None) == now


def test_period_type_tables_are_consistent() -> None:
    """Every period class has a granularity ranking."""
    assert set(PERIOD_TYPE_TO_CLASS) == set(PERIOD_GRANULARITY)


def test_period_granularity_ordering() -> None:
    """Granularity values are ordered from finer to coarser periods.

    This guards against accidental changes to the relative ranking, which would
    break conversions in convert_period_id.
    """
    expected_order = [
        "Daily",
        "Weekly",
        "Monthly",
        "Quarterly",
        "Yearly",
    ]

    ordered_existing = [p for p in expected_order if p in PERIOD_GRANULARITY]

    assert len(ordered_existing) >= 2

    for lower, higher in itertools.pairwise(ordered_existing):
        assert PERIOD_GRANULARITY[lower] < PERIOD_GRANULARITY[higher], (
            f"Expected {lower} to be finer than {higher}"
        )
