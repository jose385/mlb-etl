#!/usr/bin/env python3
import os
import io
import glob
import argparse
import psycopg2
from psycopg2 import sql
import pandas as pd

def sanitize(col: str) -> str:
    """Turn any input column into a safe SQL identifier."""
    return col.replace(".", "_").replace("-", "_").lower()

def connect():
    dsn = os.getenv("PG_DSN")
    if not dsn:
        raise RuntimeError("PG_DSN environment variable is not set")
    return psycopg2.connect(dsn)

def ensure_table(conn, table: str, df: pd.DataFrame):
    """CREATE TABLE IF NOT EXISTS with one text column per sanitized df column."""
    cols = [sql.Identifier(sanitize(c)) for c in df.columns]
    types = [sql.SQL("text")] * len(cols)
    ddl = sql.SQL(", ").join(
        sql.Composed([c, sql.SQL(" "), t])
        for c, t in zip(cols, types)
    )
    stmt = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
        sql.Identifier(table),
        ddl
    )
    with conn.cursor() as cur:
        cur.execute(stmt)
    conn.commit()

def load_table(conn, table: str, df: pd.DataFrame):
    """Sanitize columns, ensure table exists, then COPY data in."""
    # 1) rename df columns to safe identifiers
    df.columns = [sanitize(c) for c in df.columns]

    # 2) create the table if it's not already there
    ensure_table(conn, table, df)

    # 3) bulk-load via COPY
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)

    with conn.cursor() as cur:
        copy_sql = sql.SQL("COPY {} FROM STDIN WITH (FORMAT CSV)").format(
            sql.Identifier(table)
        )
        cur.copy_expert(copy_sql, buf)
    conn.commit()
    print(f"✅ Loaded {len(df)} rows into `{table}`")

def main():
    p = argparse.ArgumentParser(
        description="Load staged Parquet files into Postgres"
    )
    p.add_argument(
        "--input-dir", "-i",
        default="stage",
        help="Directory containing `<table>_YYYY-MM-DD.parquet` files"
    )
    p.add_argument(
        "--tables", "-t",
        nargs="+",
        help=(
            "List of table names to load (no dates). "
            "E.g. `statcast statsapi_playlog roster lineup`. "
            "If omitted, will load every distinct `<table>` found in input-dir."
        )
    )
    args = p.parse_args()

    # figure out which tables to load
    parquet_paths = glob.glob(os.path.join(args.input_dir, "*.parquet"))
    if not parquet_paths:
        print(f"⚠️ No parquet files found in `{args.input_dir}` – nothing to do.")
        return

    if args.tables:
        tables = args.tables
    else:
        # auto-discover table prefixes
        tables = sorted({
            os.path.basename(fp).split("_")[0]
            for fp in parquet_paths
        })

    conn = connect()

    for table in tables:
        pattern = os.path.join(args.input_dir, f"{table}_*.parquet")
        files = sorted(glob.glob(pattern))
        if not files:
            print(f"⚠️  No files for table `{table}` (looking at `{pattern}`)")
            continue

        for path in files:
            fname = os.path.basename(path)
            print(f"► Loading {fname} into `{table}`…")
            df = pd.read_parquet(path)
            if df.empty:
                print(f"⏭️ Skipping {fname} (empty DataFrame)")
                continue
            load_table(conn, table, df)

    conn.close()
    print("🎉 All done.")

if __name__ == "__main__":
    main()
