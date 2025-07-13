#!/usr/bin/env python3

"""
load_parquet_into_pg.py – Bulk-loads all Parquet files under a folder into Postgres.

Usage:
    python load_parquet_into_pg.py [--input-dir DIR] [--tables T1 T2 ...]

Flags:
  --input-dir  Directory containing .parquet files (default="stage")
  --tables     Optional list of tables to load (default=all found)
Env:
  PG_DSN       Postgres DSN, e.g. postgresql://user:pw@host:5432/db
"""

import argparse
import io
from pathlib import Path

import pandas as pd
import psycopg2


def connect():
    dsn = os.getenv("PG_DSN")
    if not dsn:
        raise ValueError("PG_DSN env var must be set")
    return psycopg2.connect(dsn)


def load_table(conn, table: str, df: pd.DataFrame):
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)
    cols = ",".join(df.columns)
    copy_sql = f"COPY public.{table} ({cols}) FROM STDIN WITH (FORMAT CSV)"
    cur = conn.cursor()
    cur.copy_expert(copy_sql, buf)
    conn.commit()
    print(f"✅ Loaded {len(df)} rows into mlb.{table}")


def main():
    import os

    p = argparse.ArgumentParser()
    p.add_argument(
        "--input-dir",
        default="stage",
        help="Directory of parquet files to load"
    )
    p.add_argument(
        "--tables",
        nargs="*",
        help="List of table basenames to load (e.g. statcast, statsapi_playlog, roster, lineup)"
    )
    args = p.parse_args()

    in_dir = Path(args.input_dir)
    files = sorted(in_dir.glob("*.parquet"))
    if not files:
        print(f"⚠️  No Parquet files in {in_dir}")
        return

    # optionally filter by `--tables` list
    if args.tables:
        keep = set(args.tables)
        files = [
            f for f in files
            if (
                (t := f.stem.split("_", 1)[0]) in keep
                or (
                    t == "statsapi" and "statsapi_playlog" in keep
                )
            )
        ]
        if not files:
            print(f"⚠️  Nothing matches tables {keep}")
            return

    conn = connect()
    for pq in files:
        base = pq.stem.split("_", 1)[0]
        table = "statsapi_playlog" if base == "statsapi" else base
        table = table.rstrip("s")
        print(f"⏳ Loading {pq.name} into `{table}`…", end=" ")
        df = pd.read_parquet(pq)
        load_table(conn, table, df)
    conn.close()
    print("🎉 All loads complete.")


if __name__ == "__main__":
    main()
