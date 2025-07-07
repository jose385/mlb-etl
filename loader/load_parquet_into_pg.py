#!/usr/bin/env python

import os, glob

from io import StringIO

from pathlib import Path


import duckdb



import psycopg2


# ——— 1) Connect to Postgres with URL parsing ——————————————————————

def connect():

    raw = os.environ["PG_DSN"]

    if raw.startswith("postgres://"):

        from urllib.parse import urlparse

        u = urlparse(raw.replace("postgres://", "postgresql://", 1))

        return psycopg2.connect(

            host=u.hostname, port=u.port,

            user=u.username, password=u.password,

            dbname=u.path.lstrip("/")

        )
    
    return psycopg2.connect(raw)


# ——— 2) Initialize schemas & tables —————————————————————————

def init_schema(conn):

    ddl = """

    CREATE SCHEMA IF NOT EXISTS mlb;


    CREATE TABLE IF NOT EXISTS mlb.statcast_pitchlog (

      game_date DATE, game_pk INT, pitcher INT, batter INT,

      pitch_type TEXT, release_speed REAL,

      release_pos_x REAL, release_pos_z REAL,

      plate_x REAL, plate_z REAL

      /* add any static Statcast columns here */

    );


    CREATE TABLE IF NOT EXISTS mlb.statsapi_playlog (

      game_pk INT, atbat_index INT, pitch_index INT,

      result_event TEXT, result_description TEXT

      /* add core PBP columns, extras will be added dynamically */

    );


    CREATE TABLE IF NOT EXISTS mlb.team (

      team_id INT PRIMARY KEY, team_name TEXT, abbreviation TEXT

    );


    CREATE TABLE IF NOT EXISTS mlb.player (

      player_id INT PRIMARY KEY, full_name TEXT,

      position TEXT, bats TEXT, throws TEXT

    );


    CREATE TABLE IF NOT EXISTS mlb.roster (

      game_date DATE, team_id INT REFERENCES mlb.team,

      player_id INT REFERENCES mlb.player,

      side TEXT, status TEXT,

      PRIMARY KEY(game_date, team_id, player_id)

    );

    """

    with conn.cursor() as cur:

        cur.execute(ddl)

    conn.commit()


# ——— 3) Dynamic ADD COLUMN helper ———————————————————————————

def sync_columns(conn, schema, table, cols):

    with conn.cursor() as cur:

        cur.execute("""
                    
            SELECT column_name
                    
              FROM information_schema.columns
                    
             WHERE table_schema = %s
                    
               AND table_name   = %s
                    
        """, (schema, table))

        existing = {r[0] for r in cur.fetchall()}

        for c in cols:

            if c not in existing:

                cur.execute(

                    f"ALTER TABLE {schema}.{table} "

                    f"ADD COLUMN IF NOT EXISTS {c} TEXT;"

                )

    conn.commit()


# ——— 4) Main loader loop —————————————————————————————————————

def main():

    conn = connect()

    init_schema(conn)

    du   = duckdb.connect()

    OUT  = os.getenv("OUTPUT_DIR", "stage")


    for fp in glob.glob(f"{OUT}/*.parquet"):

        df = du.execute(f"SELECT * FROM read_parquet('{fp}')").fetch_df()

        if df.empty:

            continue


        # sanitize column names

        df.columns = [c.replace('.', '_').replace('-', '_').lower()
                      
                      for c in df.columns]
        
        cols = ','.join(df.columns)


        name = Path(fp).name

        if name.startswith("roster_"):

            table = "roster"

        elif name.startswith("statsapi_"):

            table = "statsapi_playlog"

        else:

            table = "statcast_pitchlog"


        sync_columns(conn, "mlb", table, df.columns.tolist())


        # stream CSV into Postgres

        buf = StringIO()

        df.to_csv(buf, index=False, header=False)

        buf.seek(0)

        with conn.cursor() as cur:

            cur.copy_expert(

                f"COPY mlb.{table} ({cols}) FROM STDIN WITH (FORMAT CSV)",

                buf

            )

        conn.commit()

        print(f"→ Loaded {len(df)} rows into mlb.{table}")


    conn.close()


if __name__ == "__main__":

    main()
    
