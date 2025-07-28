from __future__ import annotations

import atexit
from pathlib import Path
from typing import Any

import psycopg

connection_string = "postgresql://postgres:postgres@postgres:5432/postgres"
connection = None


def conn() -> psycopg.Connection:
    global connection
    if not connection:
        connection = psycopg.connect(connection_string)
        atexit.register(lambda: connection.close())
    return connection


def table_exists(table_identifier: str) -> bool:
    sql = """
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = %s {}
        LIMIT 1
    """

    with conn().cursor() as cur:
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
        conn().execute(f.read())
        conn().commit()


def insert(table: str, data: list[tuple[Any, ...]], schema: tuple[str, ...]) -> int:
    if not data:
        return 0

    columns = ", ".join(schema)
    placeholders = ", ".join(["%s"] * len(schema))
    insert_sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"

    with conn().cursor() as cur:
        cur.executemany(insert_sql, data)
        conn().commit()
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

    with conn().cursor() as cur:
        cur.execute(update_sql, values)
        conn().commit()
        return int(cur.rowcount)


def delete(table: str, condition: str) -> int:
    delete_sql = f"DELETE FROM {table} WHERE {condition}"

    with conn().cursor() as cur:
        cur.execute(delete_sql)
        conn().commit()
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

    with conn().cursor() as cur:
        cur.execute(alter_sql)
        conn().commit()


def create_with_ddl_file(
    table: str, ddl_file: str | Path, recreate: bool = False
) -> None:
    if recreate or not table_exists(table):
        run_script(ddl_file)
