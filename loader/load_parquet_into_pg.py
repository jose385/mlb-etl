#!/usr/bin/env python3
import os
import io
import glob
import argparse
import psycopg2
from psycopg2 import sql
import pandas as pd

def sanitize(col: str) -> str:
    return col.replace(".", "_").replace("-", "_").lower()

def connect():
    dsn = os.getenv("PG_DSN")
    return psycopg2.connect(dsn)

def ensure_table(conn, table: str, df: pd.DataFrame):
    cols = [sql.Identifier(sanitize(c)) for c in df.columns]
    types = [sql.SQL("text")] * len(cols)
    cols_ddl = sql.SQL(", ").join(
        sql.Composed([c, sql.SQL(" "), t])
        for c, t in zip(cols, types)
    )
    create_sql = sql.SQL("CREATE TABLE IF NOT EXISTS {} ({})").format(
        sql.Identifier(table),
        cols_ddl
    )
    with conn.cursor() as cur:
        cur.execute(create_sql)
    conn.commit()

def load_table(conn, table: str, df: pd.DataFrame):
    # sanitize DataFrame columns
    df.columns = [sanitize(c) for c in df.columns]

    # ensure table exists with matching columns
    ensure_table(conn, table, df)

    # write to buffer, no header
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)

    # COPY
    with conn.cursor() as cur:
        copy_sql = sql.SQL("COPY {} FROM STDIN WITH (FORMAT CSV)").format(
            sql.Identifier(table)
        )
        cur.copy_expert(copy_sql, buf)
    conn.commit()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", help="Staging dir", default="stage")
    args = parser.parse_args()

    conn = connect()
    stage = args.stage

    # find all parquet files by pattern
    for path in sorted(glob.glob(os.path.join(stage, "*.parquet"))):
        table = os.path.splitext(os.path.basename(path))[0]  # e.g. statcast_2021-04-01
        # derive table name (e.g. statcast)
        table = table.split("_")[0]

        print(f"► Loading {os.path.basename(path)} into table '{table}'…")
        df = pd.read_parquet(path)
        load_table(conn, table, df)

    conn.close()

if __name__ == "__main__":
    main()
