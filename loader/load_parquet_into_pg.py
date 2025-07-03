#!/usr/bin/env python
"""
Read every *.parquet in ./stage with DuckDB and stream it into
Postgres using psycopg COPY -- fastest path:contentReference[oaicite:9]{index=9}:contentReference[oaicite:10]{index=10}.
"""
import os, glob, duckdb, psycopg2, datetime, textwrap

PG = psycopg2.connect(os.environ["PG_DSN"])
duck = duckdb.connect()
cur = PG.cursor()
for fp in glob.glob("stage/*.parquet"):
    df = duck.execute(f"SELECT * FROM read_parquet('{fp}')").fetch_df()  # DuckDB read_parquet:contentReference[oaicite:11]{index=11}
    if df.empty: continue
    cols = ','.join(df.columns)
    with cur.copy(f"COPY mlb.statcast_pitchlog ({cols}) FROM STDIN") as copy:
        for row in df.itertuples(index=False, name=None):
            copy.write_row(row)
    PG.commit()
    print(f"→ loaded {len(df)} rows from {fp}")
cur.close(); PG.close()

