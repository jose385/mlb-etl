import os
import io
import argparse
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd


def connect():
    dsn = os.getenv("PG_DSN")
    if not dsn:
        raise RuntimeError("PG_DSN environment variable is not set")
    return psycopg2.connect(dsn)


def load_table(conn, table: str, df: pd.DataFrame):
    # Prepare columns and CSV buffer
    cols = ", ".join(df.columns)
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)

    with conn.cursor() as cur:
        cur.copy_expert(f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT CSV)", buf)
    conn.commit()
    print(f"✅ Loaded {len(df)} rows into mlb.{table}")


def main():
    parser = argparse.ArgumentParser(description="Load all stage/*.parquet files into Postgres")
    parser.add_argument('--dir', default='stage', help='Directory containing parquet files')
    args = parser.parse_args()

    conn = connect()
    files = sorted(os.listdir(args.dir))
    for fname in files:
        if not fname.endswith('.parquet'):
            continue
        path = os.path.join(args.dir, fname)
        table = fname.split('_')[0]  # statcast, statsapi, roster, lineup, etc.
        print(f"▶ Loading {fname} into table '{table}'...")
        df = pd.read_parquet(path)
        load_table(conn, table, df)

if __name__ == '__main__':
    main()
