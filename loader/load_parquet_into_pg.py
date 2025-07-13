#!/usr/bin/env python3
"""
load_parquet_into_pg.py

Scan a directory of Parquet files (statcast, statsapi_playlog, roster, lineup),
CREATE TABLE IF NOT EXISTS for each, and bulk-load via COPY with an explicit
column list to avoid schema mismatches.
"""

import os
import io
import argparse
import pandas as pd
import psycopg2
from psycopg2 import sql
from datetime import datetime


def sanitize(col: str) -> str:
    """Sanitize DataFrame column names into valid Postgres identifiers."""
    return (
        col.strip()
           .replace(".", "_")
           .replace("-", "_")
           .replace(" ", "_")
           .lower()
    )


def connect(dsn: str):
    """Establish and return a new Postgres connection."""
    return psycopg2.connect(dsn)


def ensure_table(conn, table: str, df: pd.DataFrame):
    """
    CREATE TABLE IF NOT EXISTS with every column as TEXT.
    Does *not* alter existing tables—only creates if missing.
    """
    cols = [
        sql.SQL("{} TEXT").format(sql.Identifier(c))
        for c in df.columns
    ]
    create = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
        sql.Identifier(table),
        sql.SQL(", ").join(cols)
    )
    with conn.cursor() as cur:
        cur.execute(create)
    conn.commit()


def load_table(conn, table: str, df: pd.DataFrame):
    """
    Given an open connection, a table name, and a DataFrame:
     1) Sanitize its columns,
     2) CREATE TABLE IF NOT EXISTS,
     3) COPY INTO that table using an explicit column list.
    """
    # 1) Sanitize & rename columns
    sanitized = [sanitize(c) for c in df.columns]
    df.columns = sanitized

    # 2) Ensure table exists with those columns
    ensure_table(conn, table, df)

    # 3) Bulk-load via COPY with explicit column list
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)

    col_list = sql.SQL(", ").join(sql.Identifier(c) for c in sanitized)
    copy_sql = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT CSV)").format(
        sql.Identifier(table),
        col_list
    )
    with conn.cursor() as cur:
        cur.copy_expert(copy_sql, buf)
    conn.commit()
    print(f"✅ Loaded {len(df)} rows into `{table}`")


def scan_and_load(dsn: str, stage: str):
    """
    Walk the `stage/` directory, find all .parquet files,
    infer the table name from the filename prefix, and load.
    """
    conn = connect(dsn)

    for fn in sorted(os.listdir(stage)):
        if not fn.endswith(".parquet"):
            continue

        # filename like: statcast_2021-04-01.parquet
        table, _ = os.path.splitext(fn)
        path = os.path.join(stage, fn)

        print(f"⏳ Loading {table}…")
        df = pd.read_parquet(path)
        if df.empty:
            print(f"⚠️  Skipping {table}: empty DataFrame")
            continue

        load_table(conn, table, df)

    conn.close()


def main():
    p = argparse.ArgumentParser(
        description="Load all Parquet files in a folder into Postgres."
    )
    p.add_argument(
        "--stage",
        default=os.getenv("INPUT_DIR", "stage"),
        help="Directory of Parquet files (default: stage/)"
    )
    p.add_argument(
        "--dsn",
        default=os.getenv("PG_DSN"),
        required=True,
        help="Postgres DSN (e.g. postgresql://user:pass@host:5432/db)"
    )
    args = p.parse_args()
    scan_and_load(args.dsn, args.stage)
    print("🎉 All done.")


if __name__ == "__main__":
    main()
