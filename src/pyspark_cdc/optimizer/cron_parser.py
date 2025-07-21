from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable


def parse_day_step(expr: str) -> Iterable[int]:
    """
    Parses stepped day expressions like '*/5' or '10-20/2'.
    """
    try:
        range_part, step_str = expr.split("/", 1)
        step = int(step_str)
        if step <= 0:
            raise ValueError(f"Step must be greater than 0: {step}")
    except ValueError as e:
        raise ValueError(f"Invalid step expression: '{expr}'") from e

    if range_part == "*":
        return range(1, 32, step)
    elif "-" in range_part:
        return parse_day_range(range_part, step)
    else:
        raise ValueError(f"Unsupported range format in: '{range_part}'")


def parse_day_range(expr: str, step: int = 1) -> Iterable[int]:
    """
    Parses ranges like '10-20'.
    """
    try:
        start, end = map(int, expr.split("-", 1))
    except ValueError as e:
        raise ValueError(f"Invalid range format: '{expr}'") from e

    if not (1 <= start <= 31):
        raise ValueError(f"Start must be between 1 and 31: {start}")
    if not (1 <= end <= 31):
        raise ValueError(f"End must be between 1 and 31: {end}")
    if start > end:
        raise ValueError(f"Start cannot be greater than end: {start}-{end}")

    return range(start, end + 1, step)


def parse_day(expr: str) -> int:
    """
    Parses a single day value.
    """
    try:
        day = int(expr)
    except ValueError as e:
        raise ValueError(f"Invalid day value: '{expr}'") from e

    if not (1 <= day <= 31):
        raise ValueError(f"Day must be between 1 and 31: {day}")

    return day


def parse_day_of_month(expr: str) -> set[int]:
    """
    Parses a cron day-of-month expression and returns a set of valid days (1-31).

    Supports:
    - "*" → all days
    - "1,3,15" → single days
    - "10-20" → ranges
    - "*/5", "10-20/2" → stepped ranges
    - Mixed: "1,3,10-20/2,22-28/4"
    """
    if not expr or not expr.strip():
        raise ValueError("Expression cannot be empty.")

    result: set[int] = set()

    for raw_part in expr.split(","):
        part = raw_part.strip()

        if part == "*":
            result.update(range(1, 32))
        elif "/" in part:
            result.update(parse_day_step(part))
        elif "-" in part:
            result.update(parse_day_range(part))
        else:
            result.add(parse_day(part))

    return result
