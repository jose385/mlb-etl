#!/usr/bin/env python

import os, glob

from io import StringIO

from pathlib import Path


import duckdb

import psycopg2


# 1) Connect & prepare

PG = psycopg2.connect(os.environ["PG_DSN"])

duck = duckdb.connect()


# 2) Ensure schemas & tables exist

with PG, PG.cursor() as cur:

    # Statcast table
    
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
                
            plate_z        REAL,
                
            events         TEXT,

            description    TEXT,
                
            zone           INTEGER,
                
            stand          TEXT,
                
            p_throws       TEXT,

            home_team      TEXT,
                
            away_team      TEXT,
                
            home_score     INTEGER,
                
            away_score     INTEGER,
                
            at_bat_number  INTEGER,

            pitch_number   INTEGER,
                
            plate_time     REAL,
                
            spin_rate      REAL,

            launch_speed   REAL,
                
            launch_angle   REAL,
                
            distance       REAL,
                
            result_type    TEXT
                
        );
                
    """)

    # StatsAPI table

    cur.execute("""
                
        CREATE TABLE IF NOT EXISTS mlb.statsapi_playlog (
                
            game_pk            INTEGER,
                
            about_inning       INTEGER,
                
            about_halfInning   TEXT,
                
            about_isComplete   BOOLEAN,
                
            result_event       TEXT,

            result_description TEXT,
                
            result_rbi         INTEGER,
                
            result_homeScore   INTEGER,

            result_awayScore   INTEGER,
                
            matchup_batter_id  INTEGER,
                
            matchup_batter_name TEXT,

            matchup_pitcher_id INTEGER,
                
            count_balls        INTEGER,
                
            count_strikes      INTEGER
                
            pitchindex         INTEGER,
                
                
            -- add more columns as needed
                
        );

    """)


# 3) Load each Parquet

OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "stage"))

for fp in glob.glob(f"{OUTPUT_DIR}/*.parquet"):

    file_path = Path(fp)

    # Read via DuckDB

    df = duck.execute(f"SELECT * FROM read_parquet('{fp}')").fetch_df()

    if df.empty:

        print(f"→ Skipping empty file: {fp}")

        continue


    # sanitize names

    df.columns = [c.replace('.', '_').replace('-', '_').lower() for c in df.columns]

    cols = ','.join(df.columns)


    # convert to CSV in-memory

    buf = StringIO()

    df.to_csv(buf, index=False, header=False)

    buf.seek(0)


    # determine target table

    if "statsapi" in fp.lower():

        table = "mlb.statsapi_playlog"

    else:

        table = "mlb.statcast_pitchlog"


    # COPY into Postgres

    with PG, PG.cursor() as cur:

        cur.copy_expert(

            f"COPY {table} ({cols}) FROM STDIN WITH (FORMAT CSV)",

            buf

        )

    print(f"→ Loaded {len(df)} rows into {table} from {fp}")

