#!/usr/bin/env python3
import os
import io
import argparse
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2 import sql

def connect():
    """
    Connect to Postgres using the PG_DSN environment variable.
    Example: export PG_DSN="postgresql://user:pass@host:5432/dbname"
    """
    dsn = os.getenv("PG_DSN")
    if not dsn:
        raise RuntimeError("PG_DSN environment variable is not set")
    return psycopg2.connect(dsn)

def load_table(conn, table: str, df: pd.DataFrame):
    """
    COPY only the intersection of df.columns and actual table columns.
    """
    if df.empty:
        print(f"→ No rows to load into `{table}`; skipping.")
        return

    # 1) Dump DataFrame to in-memory CSV
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)

    # 2) Fetch actual columns for this table from Postgres
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name   = %s
            ORDER BY ordinal_position
            """,
            (table,),
        )
        db_cols = {row[0] for row in cur.fetchall()}

    # 3) Compute the common columns (in order)
    common_cols = [c for c in df.columns if c in db_cols]
    if not common_cols:
        print(f"⚠️  No matching columns for table `{table}`; skipping.")
        return

    # 4) Build and execute the COPY statement
    cols_sql = ",".join(common_cols)
    copy_sql = f"COPY public.{table} ({cols_sql}) FROM STDIN WITH (FORMAT CSV)"
    with conn.cursor() as cur:
        cur.copy_expert(copy_sql, buf)
    conn.commit()

    print(f"✅ Loaded {len(df)} rows into public.{table} ({len(common_cols)} columns)")

def main():
    p = argparse.ArgumentParser(
        description="Load all Parquet files in a folder into Postgres"
    )
    p.add_argument(
        "--stage",
        help="Input directory containing .parquet files (default=stage)",
        default="stage",
    )
    args = p.parse_args()

    stage_dir = Path(args.stage)
    if not stage_dir.is_dir():
        raise RuntimeError(f"Stage directory not found: {stage_dir}")

    # find all parquet files named like statcast_YYYY-MM-DD.parquet
    parquet_files = sorted(stage_dir.glob("*.parquet"))
    if not parquet_files:
        print("⚠️  No Parquet files found in", stage_dir)
        return

    conn = connect()
    print(f"🔌 Connected to Postgres via {os.getenv('PG_DSN')}")

    for pq in parquet_files:
        table = pq.name.split("_", 1)[0]  # statcast_... → statcast
        print(f"⏳ Loading {pq.name} into `{table}`…", end=" ")
        df = pd.read_parquet(pq)
        try:
            load_table(conn, table, df)
        except Exception as e:
            print(f"❌ Error loading {pq.name} -> {table}: {e}")

    conn.close()
    print("🎉 All done!")

if __name__ == "__main__":
    main()
