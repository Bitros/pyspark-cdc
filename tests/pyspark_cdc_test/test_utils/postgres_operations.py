from __future__ import annotations

import atexit
from pathlib import Path
from typing import Any

import psycopg
import pytest
from tests.test_utils.employee_generator import EmployeeGenerator

connection_string = "postgresql://postgres:postgres@postgres:5432/postgres"
conn = psycopg.connect(connection_string)
atexit.register(lambda: conn.close())


def table_exists(table_identifier: str) -> bool:
    sql = """
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = %s {}
        LIMIT 1
    """

    with conn.cursor() as cur:
        if "." in table_identifier:
            schema, table = table_identifier.split(".")
            sql = sql.format("AND table_schema = %s")
            cur.execute(sql, (table, schema))
        else:
            sql = sql.format("")
            cur.execute(sql, (table_identifier,))

        return cur.fetchone() is not None


def run_script(script: str | Path) -> None:
    script_path = Path(script)
    with script_path.open("r", encoding="utf-8") as f:
        conn.execute(f.read())
        conn.commit()


def insert(table: str, data: list[tuple[Any, ...]], schema: tuple[str, ...]) -> int:
    if not data:
        return 0

    columns = ", ".join(schema)
    placeholders = ", ".join(["%s"] * len(schema))
    insert_sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

    with conn.cursor() as cur:
        cur.executemany(insert_sql, data)
        conn.commit()
        return int(cur.rowcount)


def update(table: str, updates: dict[str, Any], condition: str = "TRUE") -> int:
    if not updates:
        return 0

    set_clauses = []
    values = []

    for col, value in updates.items():
        if isinstance(value, str) and any(
            op in value for op in ["||", "+", "-", "*", "/", "CASE"]
        ):
            # Treat as SQL expression
            set_clauses.append(f"{col} = {value}")
        else:
            # Treat as parameterized value
            set_clauses.append(f"{col} = %s")
            values.append(value)

    set_clause = ", ".join(set_clauses)

    update_sql = f"UPDATE {table} SET {set_clause} WHERE {condition}"

    with conn.cursor() as cur:
        cur.execute(update_sql, values)
        conn.commit()
        return int(cur.rowcount)


def delete(table: str, condition: str) -> int:
    delete_sql = f"DELETE FROM {table} WHERE {condition}"

    with conn.cursor() as cur:
        cur.execute(delete_sql)
        conn.commit()
        return int(cur.rowcount)


def add_column(
    table: str,
    column_name: str,
    column_type: str,
    default_value: Any = None,
    not_null: bool = False,
) -> None:
    if not_null and not default_value:
        raise ValueError("Cannot set NOT NULL without a default value")

    alter_sql = f"ALTER TABLE {table} ADD COLUMN {column_name} {column_type}"

    if default_value:
        alter_sql += f" DEFAULT '{default_value}'"
    if not_null:
        alter_sql += " NOT NULL"

    with conn.cursor() as cur:
        cur.execute(alter_sql)
        conn.commit()


def create_with_ddl_file(
    table: str, ddl_file: str | Path, recreate: bool = False
) -> None:
    if recreate or not table_exists(table):
        run_script(ddl_file)


def test() -> None:
    assert table_exists("public.employee") == True
    assert table_exists("employee") == True
    assert table_exists("not_exist.employee") == False

    employee_ddl_file = (
        Path(__file__).resolve().parent.parent / "capture/postgres/employee_ddl.sql"
    )
    run_script(employee_ddl_file)

    assert table_exists("public.employee") == True

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
