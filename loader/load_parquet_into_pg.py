#!/usr/bin/env python3
import os
import io
import argparse
from pathlib import Path

import pandas as pd
import psycopg2

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

    # 1) Dump DataFrame to in-memory CSV (no header, no index)
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

    # 3) Compute the common columns (in original DataFrame order)
    common = [c for c in df.columns if c in db_cols]
    if not common:
        print(f"⚠️  No matching columns for table `{table}`; skipping.")
        return

    # 4) Run COPY … STDIN
    cols_sql = ", ".join(common)
    copy_sql = f"COPY public.{table} ({cols_sql}) FROM STDIN WITH (FORMAT CSV)"
    with conn.cursor() as cur:
        cur.copy_expert(copy_sql, buf)
    conn.commit()

    print(f"✅ Loaded {len(df)} rows into public.{table} ({len(common)} cols)")

def main():
    parser = argparse.ArgumentParser(
        description="Load all Parquet files in a folder into Postgres"
    )
    parser.add_argument(
        "--stage",
        help="Input directory containing .parquet files (default=stage)",
        default="stage",
    )
    args = parser.parse_args()

    stage_dir = Path(args.stage)
    if not stage_dir.is_dir():
        raise RuntimeError(f"Stage directory not found: {stage_dir}")

    parquet_files = sorted(stage_dir.glob("*.parquet"))
    if not parquet_files:
        print(f"⚠️  No Parquet files found in {stage_dir}")
        return

    conn = connect()
    print(f"🔌 Connected to Postgres via PG_DSN={os.getenv('PG_DSN')}")

    for pq in parquet_files:
        table = pq.stem.split("_", 1)[0]  # e.g. statcast_YYYY-MM-DD → statcast
        print(f"⏳ Loading {pq.name} → table `{table}`…", end=" ")
        df = pd.read_parquet(pq)
        try:
            load_table(conn, table, df)
        except Exception as e:
            print(f"❌ Error loading {pq.name}: {e}")

    conn.close()
    print("🎉 All done!")

if __name__ == "__main__":
    main()
