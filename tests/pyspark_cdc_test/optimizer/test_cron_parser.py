import pytest

from pyspark_cdc.optimizer.cron_parser import parse_day_of_month


def test_wildcard_returns_all_days() -> None:
    """Test that '*' returns all days 1-31."""
    result = parse_day_of_month("*")
    expected = set(range(1, 32))
    assert result == expected


def test_single_day() -> None:
    """Test parsing single day numbers."""
    assert parse_day_of_month("1") == {1}
    assert parse_day_of_month("15") == {15}
    assert parse_day_of_month("31") == {31}


def test_multiple_days_comma_separated() -> None:
    """Test parsing multiple comma-separated days."""
    assert parse_day_of_month("1,15,31") == {1, 15, 31}
    assert parse_day_of_month("5,10,15,20,25") == {5, 10, 15, 20, 25}


def test_range_with_dash() -> None:
    """Test parsing day ranges with dash."""
    assert parse_day_of_month("1-5") == {1, 2, 3, 4, 5}
    assert parse_day_of_month("15-20") == {15, 16, 17, 18, 19, 20}
    assert parse_day_of_month("28-31") == {28, 29, 30, 31}


def test_step_values_with_wildcard() -> None:
    """Test parsing step values with wildcard."""
    assert parse_day_of_month("*/2") == {
        1,
        3,
        5,
        7,
        9,
        11,
        13,
        15,
        17,
        19,
        21,
        23,
        25,
        27,
        29,
        31,
    }
    assert parse_day_of_month("*/5") == {1, 6, 11, 16, 21, 26, 31}
    assert parse_day_of_month("*/10") == {1, 11, 21, 31}


def test_step_values_with_range() -> None:
    """Test parsing step values with ranges."""
    assert parse_day_of_month("1-10/2") == {1, 3, 5, 7, 9}
    assert parse_day_of_month("5-15/3") == {5, 8, 11, 14}
    assert parse_day_of_month("20-30/5") == {20, 25, 30}


def test_complex_combinations() -> None:
    """Test complex combinations of different formats."""
    assert parse_day_of_month("1,5-10,15,*/7") == {1, 5, 6, 7, 8, 9, 10, 15, 22, 29}
    assert parse_day_of_month("1-5,10-15,20,25-27") == {
        1,
        2,
        3,
        4,
        5,
        10,
        11,
        12,
        13,
        14,
        15,
        20,
        25,
        26,
        27,
    }


def test_handles_whitespace() -> None:
    """Test that function handles whitespace correctly."""
    assert parse_day_of_month(" 1 , 5 , 10 ") == {1, 5, 10}
    assert parse_day_of_month("1-5 , 10-15") == {1, 2, 3, 4, 5, 10, 11, 12, 13, 14, 15}
    assert parse_day_of_month(" */2 ") == {
        1,
        3,
        5,
        7,
        9,
        11,
        13,
        15,
        17,
        19,
        21,
        23,
        25,
        27,
        29,
        31,
    }


def test_boundary_values() -> None:
    """Test boundary values (1 and 31)."""
    assert parse_day_of_month("1") == {1}
    assert parse_day_of_month("31") == {31}
    assert parse_day_of_month("1-31") == set(range(1, 32))


def test_empty_expression_raises_error() -> None:
    with pytest.raises(ValueError):
        parse_day_of_month("")

    with pytest.raises(ValueError):
        parse_day_of_month(None)  # type: ignore


def test_empty_segment_raises_error() -> None:
    """Test that empty segments raise ValueError."""
    with pytest.raises(ValueError):
        parse_day_of_month("1,,5")

    with pytest.raises(ValueError):
        parse_day_of_month("1, ,5")


def test_invalid_step_raises_error() -> None:
    """Test that invalid step values raise ValueError."""
    with pytest.raises(ValueError):
        parse_day_of_month("*/0")

    with pytest.raises(ValueError):
        parse_day_of_month("*/-1")

    with pytest.raises(ValueError):
        parse_day_of_month("1-10/0")


def test_invalid_range_format_raises_error() -> None:
    """Test that invalid range formats raise ValueError."""
    with pytest.raises(ValueError):
        parse_day_of_month("5/2")  # Step without proper range

    with pytest.raises(ValueError):
        parse_day_of_month("abc")  # Non-numeric values

    with pytest.raises(ValueError):
        parse_day_of_month("1-")  # Incomplete range


def test_invalid_numeric_values_raise_error() -> None:
    """Test that invalid numeric values raise ValueError."""
    with pytest.raises(ValueError):
        parse_day_of_month("0")  # Day 0 doesn't exist

    with pytest.raises(ValueError):
        parse_day_of_month("32")  # Day 32 doesn't exist

    with pytest.raises(ValueError):
        parse_day_of_month("1-35")  # Range exceeds valid days


def test_edge_cases_with_steps() -> None:
    """Test edge cases with step values."""
    # Large step that results in single value
    assert parse_day_of_month("*/50") == {1}

    # Step equal to range size
    assert parse_day_of_month("1-5/5") == {1}

    # Step larger than range
    assert parse_day_of_month("1-3/10") == {1}


def test_duplicate_values_are_handled() -> None:
    """Test that duplicate values result in unique set."""
    assert parse_day_of_month("1,1,1") == {1}
    assert parse_day_of_month("1-3,2-4") == {1, 2, 3, 4}


def test_reverse_range_raises_error() -> None:
    """Test that reverse ranges raise appropriate errors."""
    with pytest.raises(ValueError):
        parse_day_of_month("10-5")  # End before start


def test_real_world_cron_expressions() -> None:
    """Test real-world cron-like expressions."""
    # Every Monday (assuming day 1 = Monday in some contexts)
    assert parse_day_of_month("1") == {1}

    # Weekdays (if 1-5 represents Mon-Fri)
    assert parse_day_of_month("1-5") == {1, 2, 3, 4, 5}

    # Every other day starting from 1st
    assert parse_day_of_month("1-31/2") == {
        1,
        3,
        5,
        7,
        9,
        11,
        13,
        15,
        17,
        19,
        21,
        23,
        25,
        27,
        29,
        31,
    }

    # First and last day of month
    assert parse_day_of_month("1,31") == {1, 31}
