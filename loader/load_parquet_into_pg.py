#!/usr/bin/env python3
import os
import io
import sys
import glob
import argparse
import pandas as pd
import psycopg2

# columns to keep per table (rest are dropped)
EXPECTED_COLUMNS = {
    "statcast": [
      "game_date","game_pk","at_bat_number","pitcher","batter",
      "balls","strikes","plate_x","plate_z","release_speed",
      "release_pos_x","release_pos_z","spin_rate","plate_time","pitch_number"
    ],
    "statsapi": None,
    "roster":   None,
    "lineup":   None
}

def connect():
    dsn = os.getenv("PG_DSN")
    if not dsn:
        print("❌ PG_DSN not set"); sys.exit(1)
    return psycopg2.connect(dsn)

def load_table(conn, table, df):
    keys = EXPECTED_COLUMNS.get(table)
    if keys:
        df = df[[c for c in keys if c in df.columns]]
    cols = ",".join(df.columns)
    buf = io.StringIO(df.to_csv(index=False, header=False))
    buf.seek(0)
    with conn.cursor() as cur:
        print(f"→ Loading {table}: {len(df)} rows")
        cur.copy_expert(f"COPY public.{table} ({cols}) FROM STDIN WITH (FORMAT CSV)", buf)
    conn.commit()

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dir", default="stage")
    args = p.parse_args()
    conn = connect()
    for path in sorted(glob.glob(os.path.join(args.dir, "*.parquet"))):
        table = os.path.basename(path).split("_")[0]
        df = pd.read_parquet(path)
        load_table(conn, table, df)
    conn.close()

if __name__ == "__main__":
    main()
