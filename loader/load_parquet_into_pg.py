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

    df.columns = [c.replace('.', '_').replace('-', '_') for c in df.columns]

    cols = ','.join(df.columns)  

    # ⬇⬇⬇ paste the pathlib-based loader here ⬇⬇⬇

    from pathlib import Path


    output_dir = Path(os.getenv("OUTPUT_DIR", "stage"))

    file_path  = Path(fp)


    if not file_path.exists():

        raise FileNotFoundError(f"No Parquet file found at {file_path.resolve()}")
    

    with file_path.open("rb") as f, PG.cursor() as cur:

        cur.execute("CREATE SCHEMA IF NOT EXISTS mlb")

        cur.copy_expert(

            f"COPY mlb.statcast_pitchlog ({cols}) FROM STDIN",

            f

        )

    # ⬆⬆⬆ end of pasted block ⬆⬆⬆


    PG.commit()  

    print(f"→ loaded {len(df)} rows from {fp}")  

cur.close(); PG.close()  


