import pytest

from pyspark_cdc.watermark import Watermark


def test_watermark() -> None:
    wm_int_1 = Watermark(
        table_name="test_table", column_name="test_column", value=12345, type="int"
    )
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

    wm_int_1 = Watermark(
        table_name="test_table", column_name="test_column", value=12345, type="int"
    )
    wm_int_2 = Watermark(
        table_name="test_table", column_name="test_column", value=23456, type="int"
    )
    assert wm_int_1 <= wm_int_2
    assert wm_int_2 >= wm_int_1

    wm_date_1 = Watermark(
        table_name="test_table",
        column_name="test_column",
        value="2025-07-01 11:11:11.111111",
        type="timestamp",
    )
    wm_date_2 = Watermark(
        table_name="test_table",
        column_name="test_column",
        value="2025-07-01 11:11:11.111111",
        type="timestamp",
    )
    assert wm_date_1 == wm_date_2

    wm_date_2 = Watermark(
        table_name="test_table",
        column_name="test_column",
        value="2025-07-01 11:11:11.222222",
        type="timestamp",
    )
    assert wm_date_1 <= wm_date_2

    wm_date_2 = Watermark(
        table_name="test_table",
        column_name="test_column",
        value="2023-07-01 11:11:11.222222",
        type="timestamp",
    )
    assert wm_date_1 >= wm_date_2

    with pytest.raises(
        TypeError, match="not supported between instances of 'int' and 'str'"
    ):
        wm_int_1 <= wm_date_2
