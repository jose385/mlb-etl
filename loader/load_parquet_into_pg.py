#!/usr/bin/env python

import os

import glob

from io import StringIO

from pathlib import Path



import duckdb

import psycopg2


# 1) Connect

PG = psycopg2.connect(os.environ["PG_DSN"])

duck = duckdb.connect()


def sync_columns_psycopg2(conn, schema: str, table: str, df_cols: list):

    """

    Add any df_cols missing from the Postgres table, using

    information_schema and psycopg2 only.

    """

    with conn.cursor() as cur:

        # Fetch existing column names

        cur.execute("""
                    
            SELECT column_name
                    
            FROM information_schema.columns
                    
            WHERE table_schema = %s
                    
              AND table_name   = %s
                    
        """, (schema, table))

        existing = {row[0] for row in cur.fetchall()}


        # For each missing column, ALTER TABLE ADD COLUMN TEXT

        for col in df_cols:

            if col not in existing:

                cur.execute(

                    f"ALTER TABLE {schema}.{table} "

                    f"ADD COLUMN IF NOT EXISTS {col} TEXT;"

                )

                print(f"[DDL SYNC] Added missing column: {col}")

    conn.commit()


# 2) Ensure schemas & static tables (pitchlog and playlog) exist up front

with PG.cursor() as cur:

    cur.execute("CREATE SCHEMA IF NOT EXISTS mlb;")

    cur.execute("""
                
        CREATE TABLE IF NOT EXISTS mlb.statcast_pitchlog (
                
            game_date      DATE,
                
            game_pk        INTEGER,
                
            pitcher        INTEGER,
                
            batter         INTEGER,
                
            pitch_type     TEXT,
                
            release_speed  REAL,
                
            release_pos_x  REAL,
                
            release_pos_z  REAL,
                
            plate_x        REAL,
                
            plate_z        REAL
                
            -- add other known Statcast columns here if desired
                
        );
                
    """)

    cur.execute("""
                
        CREATE TABLE IF NOT EXISTS mlb.statsapi_playlog (
                
            game_pk            INTEGER,
                
            about_inning       INTEGER,
                
            about_halfinning   TEXT,
                
            about_iscomplete   BOOLEAN,
                
            result_event       TEXT,
                
            result_description TEXT,
                
            result_rbi         INTEGER
                
            -- add a few core StatsAPI columns; the rest will get ALTERed in
                
        );
                
    """)

    cur.execute("""
                
        CREATE TABLE IF NOT EXISTS mlb.team (
                
  team_id      INTEGER PRIMARY KEY,
                
  team_name    TEXT,
                
  abbreviation TEXT
                
);
                

-- Players dimension
                
CREATE TABLE IF NOT EXISTS mlb.player (
                
  player_id    INTEGER PRIMARY KEY,
                
  full_name    TEXT,
                
  position     TEXT,
                
  bats         TEXT,
                
  throws       TEXT
                
);
                

-- Roster fact table
                
CREATE TABLE IF NOT EXISTS mlb.roster (
                
  game_date    DATE,
                
  team_id      INTEGER REFERENCES mlb.team(team_id),
                
  player_id    INTEGER REFERENCES mlb.player(pClayer_id),
                
  side         TEXT,
                
  status       TEXT,
                
  PRIMARY KEY (game_date, team_id, player_id)
                
);
                        
                
    """)


    PG.commit()


# 3) Load loop

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "stage")

for fp in glob.glob(f"{OUTPUT_DIR}/*.parquet"):

    file_path = Path(fp)

    df        = duck.execute(f"SELECT * FROM read_parquet('{fp}')").fetch_df()

    if df.empty:

        print(f"→ skipping empty {fp}")

        continue



    # 3a) sanitize & lowercase columns

    df.columns = [c.replace('.', '_').replace('-', '_').lower()
                  
                  for c in df.columns]
    
    cols = ','.join(df.columns)

# Detect roster file

+    if fp.lower().startswith(f"{OUTPUT_DIR}/roster_"):

+        table = 'roster'

+        # auto-sync roster columns

+        sync_columns_psycopg2(PG, 'mlb', table, df.columns.tolist())

+    else:

+        # existing pitch/play logic

+        if 'statsapi' in fp.lower():

+            table = 'statsapi_playlog'

+        else:

+            table = 'statcast_pitchlog'

+        sync_columns_psycopg2(PG, 'mlb', table, df.columns.tolist())



    # 3b) auto‐sync missing columns into the right table

    df_cols    = df.columns.tolist()

    if 'statsapi' in fp.lower():

        sync_columns_psycopg2(PG, 'mlb', 'statsapi_playlog', df_cols)

        table = 'statsapi_playlog'

    else:

        sync_columns_psycopg2(PG, 'mlb', 'statcast_pitchlog', df_cols)

        table = 'statcast_pitchlog'


    # 3c) stream CSV into Postgres

    buf = StringIO()

    df.to_csv(buf, index=False, header=False)

    buf.seek(0)


    with PG.cursor() as cur:

        cur.copy_expert(

            f"COPY mlb.{table} ({cols}) FROM STDIN WITH (FORMAT CSV)",

            buf

        )

    PG.commit()

    print(f"→ loaded {len(df)} rows into mlb.{table} from {fp}")


PG.close()

