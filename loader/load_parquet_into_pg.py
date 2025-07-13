# loader/load_parquet_into_pg.py
import os
import io
import argparse
from datetime import datetime
import psycopg2
import pandas as pd

def connect():
    """
    Connect to Postgres using the PG_DSN environment variable.
    PG_DSN should look like: postgresql://user:pass@host:port/dbname
    """
    dsn = os.getenv("PG_DSN")
    if not dsn:
        raise RuntimeError("PG_DSN environment variable is required")
    return psycopg2.connect(dsn)

def load_table(conn, table: str, df: pd.DataFrame):
    """
    COPY a pandas DataFrame into a Postgres table via COPY FROM STDIN.
    - Sanitizes column names (no dots, hyphens, uppercase).
    """
    # 1) sanitize column names
    safe_cols = [
        c.replace(".", "_")
         .replace("-", "_")
         .lower()
        for c in df.columns
    ]
    df.columns = safe_cols

    # 2) write DataFrame to a CSV buffer
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)

    # 3) perform COPY
    cols_sql = ",".join(safe_cols)
    print(f"⏳ Loading {table}: {len(df)} rows into public.{table} ({cols_sql})")
    with conn.cursor() as cur:
        cur.copy_expert(
            f"COPY public.{table} ({cols_sql}) FROM STDIN WITH (FORMAT CSV)",
            buf
        )
    conn.commit()
    print(f"✅ Loaded {len(df)} rows into public.{table}")

def main():
    parser = argparse.ArgumentParser(
        description="Load all Parquet files in a directory into Postgres tables"
    )
    parser.add_argument(
        "--input-dir",
        help="Directory containing <table>_YYYY-MM-DD.parquet files",
        default="stage"
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        required=True,
        help="List of tables to load, e.g.: statcast statsapi_playlog roster lineup"
    )
    args = parser.parse_args()

    conn = connect()

    for table in args.tables:
        prefix = f"{table}_"
        print(f"\n▶ Scanning for {table} files in {args.input_dir}…")
        for fn in sorted(os.listdir(args.input_dir)):
            if not fn.startswith(prefix) or not fn.endswith(".parquet"):
                continue

            path = os.path.join(args.input_dir, fn)
            date_str = fn[len(prefix):-8]  # strip off prefix_ and .parquet
            print(f"  • {date_str}: reading {fn}…", end=" ")

            df = pd.read_parquet(path)
            if df.empty:
                print("empty, skipping")
            else:
                print(f"{len(df)} rows → loading")
                load_table(conn, table, df)

    conn.close()
    print("\n🎉 Done.")

if __name__ == "__main__":
    main()
