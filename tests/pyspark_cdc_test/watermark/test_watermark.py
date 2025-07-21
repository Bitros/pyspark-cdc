from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from pyspark_cdc.watermark import Watermark


def test_watermark_dst() -> None:
    tz = ZoneInfo("Europe/Berlin")
    first_2am = datetime(2025, 10, 26, 2, 30, 0, fold=0, tzinfo=tz)
    second_2am = datetime(2025, 10, 26, 2, 30, 0, fold=1, tzinfo=tz)
    # true in Python
    assert first_2am == second_2am
    # but false in Unix timestamp
    assert first_2am.timestamp() < second_2am.timestamp()
    wm_date_1 = Watermark(
        "test_table", "test_column", first_2am.isoformat(), "timestamp"
    )
    wm_date_2 = Watermark(
        "test_table", "test_column", second_2am.isoformat(), "timestamp"
    )
    assert wm_date_1 < wm_date_2


def test_watermark_offset() -> None:
    wm_date_1 = Watermark(
        "test_table", "test_column", "2025-10-26T03:30:00+02:00", "timestamp"
    )
    wm_date_2 = Watermark(
        "test_table", "test_column", "2025-10-26T02:30:00+01:00", "timestamp"
    )
    assert wm_date_1 == wm_date_2

    wm_date_1 = Watermark(
        "test_table", "test_column", "2025-10-26T03:30:00+02:00", "timestamp"
    )
    wm_date_2 = Watermark(
        "test_table", "test_column", "2025-10-26T04:30:00+08:00", "timestamp"
    )

    assert wm_date_1 > wm_date_2


def test_watermark() -> None:
    wm_int_1 = Watermark("test_table", "test_column", 12345, "int")
    assert wm_int_1.table_name == "test_table"
    assert wm_int_1.column_name == "test_column"
    assert wm_int_1.value == 12345
    assert wm_int_1.type == "int"
    assert (
        str(wm_int_1)
        == '{"table_name": "test_table", "column_name": "test_column", "value": 12345, "type": "int"}'
    )

    assert wm_int_1.dict() == {
        "table_name": "test_table",
        "column_name": "test_column",
        "value": 12345,
        "type": "int",
    }

    wm_int_2 = Watermark(**wm_int_1.dict())
    assert wm_int_1 == wm_int_2

    wm_int_1 = Watermark("test_table", "test_column", 12345, "int")
    wm_int_2 = Watermark("test_table", "test_column", 23456, "int")
    assert wm_int_1 <= wm_int_2
    assert wm_int_2 >= wm_int_1

    wm_date_1 = Watermark(
        "test_table", "test_column", "2025-07-01T11:11:11.111111Z", "timestamp"
    )
    assert wm_date_1.table_name == "test_table"
    assert wm_date_1.column_name == "test_column"
    assert wm_date_1.value == "2025-07-01T11:11:11.111111Z"
    assert wm_date_1.type == "timestamp"

    wm_date_2 = Watermark(
        "test_table", "test_column", "2025-07-01T11:11:11.111111Z", "timestamp"
    )
    assert wm_date_1 == wm_date_2
    assert wm_date_1 <= wm_date_2
    assert wm_date_1 >= wm_date_2

    wm_date_2 = Watermark(
        "test_table", "test_column", "2025-07-01T22:22:22.222222Z", "timestamp"
    )
    assert wm_date_1 < wm_date_2
    assert wm_date_2 > wm_date_1

    with pytest.raises(TypeError, match="Can't compare"):
        assert wm_int_1 <= wm_date_2
