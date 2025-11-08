import csv
import os
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Set

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection as PgConnection
from supabase import Client, create_client
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[1]
CSV_DIR = BASE_DIR / "supabase_update"
RESTAURANTS_CSV = CSV_DIR / "restaurants.csv"
FOOD_ITEMS_CSV = CSV_DIR / "food_items.csv"
DEFAULT_BATCH_SIZE = 500


def _db_schema() -> str:
    return os.getenv("SUPABASE_DB_SCHEMA", "public")


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing environment variable: {name}")
    return value


def get_client() -> Client:
    url = _require_env("SUPABASE_URL")
    key = _require_env("SUPABASE_KEY")
    return create_client(url, key)


def get_database_connection() -> PgConnection:
    dsn = os.getenv("SUPABASE_DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if not dsn:
        raise RuntimeError("Missing SUPABASE_DATABASE_URL (or SUPABASE_DB_URL) for direct Postgres access")
    return psycopg2.connect(dsn)


def truncate_supabase_tables(tables: Sequence[str]) -> None:
    if not tables:
        return
    with get_database_connection() as conn:
        with conn.cursor() as cursor:
            identifiers = [sql.Identifier(table) for table in tables]
            query = sql.SQL("TRUNCATE TABLE {} RESTART IDENTITY CASCADE;").format(
                sql.SQL(", ").join(identifiers)
            )
            cursor.execute(query)


def _normalize_row(
    row: Dict[str, str],
    int_fields: Set[str],
    float_fields: Set[str],
    allowed_fields: Optional[Set[str]] = None,
) -> Dict[str, Any]:
    normalized: Dict[str, Any] = {}
    for key, raw_value in row.items():
        if allowed_fields is not None and key not in allowed_fields:
            continue
        if raw_value is None:
            normalized[key] = None
            continue
        value = raw_value.strip()
        if value == "":
            normalized[key] = None
        elif key in int_fields:
            normalized[key] = int(value)
        elif key in float_fields:
            normalized[key] = float(value)
        else:
            normalized[key] = value
    return normalized


def _stream_csv_batches(
    csv_path: Path,
    *,
    int_fields: Set[str],
    float_fields: Set[str],
    batch_size: int,
    allowed_fields: Optional[Set[str]] = None,
) -> Iterator[List[Dict[str, Any]]]:
    if batch_size <= 0:
        raise ValueError("Batch size must be positive")
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    with csv_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        batch: List[Dict[str, Any]] = []
        for row in reader:
            batch.append(
                _normalize_row(
                    row,
                    int_fields,
                    float_fields,
                    allowed_fields=allowed_fields,
                )
            )
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch


def _upload_batches(client: Client, table_name: str, batches: Iterable[List[Dict[str, Any]]]) -> int:
    total = 0
    for batch in batches:
        if not batch:
            continue
        client.table(table_name).insert(batch).execute()
        total += len(batch)
    return total


def get_table_columns(table_name: str) -> Set[str]:
    schema = _db_schema()
    with get_database_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (schema, table_name),
            )
            return {row[0] for row in cursor.fetchall()}


def upload_from_csv() -> None:
    restaurants_table = _require_env("SUPABASE_TABLE_RESTAURANTS")
    food_items_table = _require_env("SUPABASE_TABLE_FOOD_ITEMS")
    batch_size = int(os.getenv("SUPABASE_BATCH_SIZE", str(DEFAULT_BATCH_SIZE)))

    truncate_supabase_tables([food_items_table, restaurants_table])

    client = get_client()

    restaurant_columns = get_table_columns(restaurants_table)
    food_columns = get_table_columns(food_items_table)

    restaurant_batches = _stream_csv_batches(
        RESTAURANTS_CSV,
        int_fields={"id"},
        float_fields={"latitude", "longitude"},
        batch_size=batch_size,
        allowed_fields=restaurant_columns,
    )
    restaurants_uploaded = _upload_batches(client, restaurants_table, restaurant_batches)
    print(f"Uploaded {restaurants_uploaded} restaurants to {restaurants_table}.")

    food_batches = _stream_csv_batches(
        FOOD_ITEMS_CSV,
        int_fields={"id", "restaurant_id"},
        float_fields=set(),
        batch_size=batch_size,
        allowed_fields=food_columns,
    )
    food_uploaded = _upload_batches(client, food_items_table, food_batches)
    print(f"Uploaded {food_uploaded} food items to {food_items_table}.")


if __name__ == "__main__":
    upload_from_csv()
