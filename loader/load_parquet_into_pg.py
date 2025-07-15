#!/usr/bin/env python3
"""
load_parquet_into_pg.py – Load everything in a folder of Parquets into Postgres,  
automatically dropping any DataFrame columns the target table doesn't have.

Usage:
    python load_parquet_into_pg.py [--input-dir DIR] [--tables T1 T2 ...]

Env:
    PG_DSN     Postgres DSN, e.g.: postgresql://user:pw@host:5432/db
"""
import os
import argparse
import io
import time
from pathlib import Path

import pandas as pd
import psycopg2


def connect():
    dsn = os.getenv("PG_DSN")
    if not dsn:
        raise ValueError("PG_DSN must be set to your PostgreSQL DSN")
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    return conn


def get_table_columns(conn, table: str):
    """
    Return a Python list of column names existing in public.<table>.
    """
    sql = """
      SELECT column_name
        FROM information_schema.columns
       WHERE table_schema = 'public'
         AND table_name   = %s
       ORDER BY ordinal_position
    """
    cur = conn.cursor()
    cur.execute(sql, (table,))
    return [row[0] for row in cur.fetchall()]


def load_table(conn, table: str, df: pd.DataFrame):
    # figure out which columns the table actually has
    existing = set(get_table_columns(conn, table))
    # prune any extra keys from the DataFrame
    to_load = [c for c in df.columns if c in existing]
    if not to_load:
        print(f"⚠️  After pruning, no columns remain for table '{table}', skipping")
        return

    buf = io.StringIO()
    df[to_load].to_csv(buf, index=False, header=False)
    buf.seek(0)

    cols_csv = ", ".join(to_load)
    
    # Create a temporary table first
    temp_table = f"temp_{table}_{int(time.time())}"
    copy_sql = f"COPY {temp_table} ({cols_csv}) FROM STDIN WITH (FORMAT CSV)"
    
    cur = conn.cursor()
    
    try:
        # Drop temp table if it exists, then create it
        cur.execute(f"DROP TABLE IF EXISTS {temp_table}")
        cur.execute(f"CREATE TEMP TABLE {temp_table} (LIKE public.{table})")
        
        # Load data into temp table
        cur.copy_expert(copy_sql, buf)
        
        # Insert only new records using ON CONFLICT DO NOTHING
        all_cols = ", ".join(to_load)
        cur.execute(f"""
            INSERT INTO public.{table} ({all_cols})
            SELECT {all_cols} FROM {temp_table}
            ON CONFLICT DO NOTHING
        """)
        
        rows_inserted = cur.rowcount
        print(f"✅ Loaded {rows_inserted} new rows → public.{table} ({len(to_load)} cols)")
        
        # Clean up temp table
        cur.execute(f"DROP TABLE IF EXISTS {temp_table}")
        
    except Exception as e:
        print(f"❌ Error loading {table}: {e}")
        # Clean up on error too
        try:
            cur.execute(f"DROP TABLE IF EXISTS {temp_table}")
        except:
            pass
        conn.rollback()
        raise


def main():
    p = argparse.ArgumentParser()
    p.add_argument(
        "--input-dir",
        default="stage",
        help="Folder containing your *.parquet files"
    )
    p.add_argument(
        "--tables",
        nargs="*",
        help="Optional subset of basenames to load: e.g. statcast statsapi_playlog roster lineup"
    )
    args = p.parse_args()

    in_dir = Path(args.input_dir)
    files = sorted(in_dir.glob("*.parquet"))
    if not files:
        print(f"❌ No parquet files found in {in_dir}")
        return

    # filter by --tables if provided:
    if args.tables:
        keep = set(args.tables)
        def matches(f):
            stem = f.stem
            base = stem.split("_", 1)[0]
            # statsapi files are named statsapi_YYYY-MM-DD.parquet but table is statsapi_playlog
            tbl = "statsapi_playlog" if base == "statsapi" else base.rstrip("s")
            return tbl in keep
        files = [f for f in files if matches(f)]
        if not files:
            print(f"❌ None of the files match --tables {keep}")
            return

    conn = connect()
    for pq in files:
        stem = pq.stem
        base = stem.split("_", 1)[0]
        table = "statsapi_playlog" if base == "statsapi" else base.rstrip("s")
        print(f"⏳ {pq.name} → public.{table} …", end=" ")
        df = pd.read_parquet(pq)
        load_table(conn, table, df)

    conn.close()
    print("🎉 All done!")


if __name__ == "__main__":
    main()