#!/usr/bin/env python

import os
import sys
import glob
import pandas as pd
import psycopg2
from psycopg2 import sql
from io import StringIO

def connect():
    dsn = os.getenv("PG_DSN")
    if not dsn:
        print("❌ ERROR: PG_DSN environment variable is not set")
        sys.exit(1)
    return psycopg2.connect(dsn)

def load_table(conn, parquet_pattern, table):
    files = sorted(glob.glob(parquet_pattern))
    if not files:
        print(f"⚠️  No files found for pattern {parquet_pattern}")
        return

    for path in files:
        df = pd.read_parquet(path)
        if df.empty:
            continue

        # reorder columns to match table
        cols = list(df.columns)
        buf = StringIO()
        df.to_csv(buf, index=False, header=False)
        buf.seek(0)

        with conn.cursor() as cur:
            cur.copy_expert(
                sql.SQL("COPY mlb.{} ({}) FROM STDIN WITH (FORMAT CSV)").format(
                    sql.Identifier(table),
                    sql.SQL(',').join(map(sql.Identifier, cols))
                ), buf
            )
            conn.commit()
        print(f"✅ Loaded {len(df)} rows into mlb.{table}")

def main():
    conn = None
    try:
        conn = connect()
        # load dimensions first
        load_table(conn, "stage/roster_*.parquet", "roster")
        load_table(conn, "stage/statsapi_*.parquet", "statsapi_playlog")
        load_table(conn, "stage/statcast_*.parquet", "statcast_eventlog")
        load_table(conn, "stage/statcast_*.parquet", "statcast_pitchlog")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
