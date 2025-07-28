from __future__ import annotations

from pathlib import Path

import pytest

from pyspark_cdc_test.utils.employee_generator import EmployeeGenerator
from pyspark_cdc_test.utils.postgres_operations import (
    add_column,
    delete,
    insert,
    run_script,
    table_exists,
    update,
)


def test_employee_generator() -> None:
    generator = EmployeeGenerator()

    data, schema = generator.generate(
        count=20, watermark_start="-2d", watermark_end="-1d"
    )
    assert len(data) == 20  # Should generate 20 records
    assert len(data[0]) == len(schema)  # Should match schema length
    assert data[0][0] == 1  # First ID should be 1
    assert data[-1][0] == 20  # Last ID should be 20

    data, _ = generator.generate(
        id_start=10, id_end=100, watermark_start="-1d", watermark_end="now"
    )

    assert len(data) == 91  # From ID 10 to 100 inclusive
    assert data[0][0] == 10  # First ID should be 10
    assert data[-1][0] == 100  # Last ID should be 100


def test_postgres_operations() -> None:
    assert table_exists("public.employee")
    assert table_exists("employee")
    assert not table_exists("not_exist.employee")

    employee_ddl_file = (
        Path(__file__).resolve().parent.parent / "capture/postgres/employee_ddl.sql"
    )
    run_script(employee_ddl_file)

    assert table_exists("public.employee")

    generator = EmployeeGenerator()
    data, schema = generator.generate(100, watermark_start="-10d", watermark_end="-5d")

    insert("public.employee", data, schema)

    update("public.employee", {"surname": "Doe", "status": "inactive"}, "id = 1")

    delete("public.employee", "id = 2")

    add_column("public.employee", "full_name", "VARCHAR(50)")
    with pytest.raises(ValueError, match="set NOT NULL without a default value"):
        add_column("public.employee", "full_name1", "VARCHAR(50)", not_null=True)
    add_column("public.employee", "full_name2", "VARCHAR(50)")
    add_column(
        "public.employee", "full_name3", "VARCHAR(50)", "John Doe", not_null=True
    )
    add_column(
        "public.employee", "full_name4", "VARCHAR(50)", "John Doe", not_null=False
    )

    update("public.employee", {"full_name": "first_name || ' ' || surname"})

    with pytest.raises(ValueError, match="set NOT NULL without a default value"):
        add_column("public.employee", "id1", "INTEGER", not_null=True)
    add_column("public.employee", "id2", "INTEGER")
    add_column("public.employee", "id3", "INTEGER", 3, not_null=True)
    add_column("public.employee", "id4", "INTEGER", 4, not_null=False)
