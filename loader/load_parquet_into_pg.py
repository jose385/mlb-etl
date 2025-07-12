#!/usr/bin/env python

import os, glob

from io import StringIO

from pathlib import Path


import duckdb



import psycopg2


# loader/load_parquet_into_pg.py (add at the top with other imports)

import statsapi

from datetime import datetime, timedelta




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

    -- Game metadata table

      CREATE TABLE IF NOT EXISTS mlb.game (

       game_pk      INTEGER PRIMARY KEY,

       game_date    DATE      NOT NULL,

        home_team_id INTEGER   NOT NULL,

        away_team_id INTEGER   NOT NULL,

        home_score   INTEGER   NOT NULL,

        away_score   INTEGER   NOT NULL

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
    
    du = duckdb.connect()

    OUT = os.getenv("OUTPUT_DIR", "stage")


    # Determine the target date for game schedule (YYYY-MM-DD)

    files = glob.glob(f"{OUT}/*.parquet")

    date_str = None

    for f in files:

        name = Path(f).name

        if name.startswith("statcast_") or name.startswith("statsapi_"):

            date_str = name.split("_", 1)[1].split(".")[0]

            break

    if not date_str and files:

        name = Path(files[0]).name

        if "_" in name:

            date_str = name.split("_", 1)[1].split(".")[0]

    if not date_str:

        date_str = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")


    # Loop through each Parquet file and load into the corresponding table

    for fp in glob.glob(f"{OUT}/*.parquet"):

        df = du.execute(f"SELECT * FROM read_parquet('{fp}')").fetch_df()

        if df.empty:

            continue


        # Sanitize column names

        df.columns = [c.replace('.', '_').replace('-', '_').lower() for c in df.columns]

            # ─── FIX: ensure roster parquet has a player_id column ────────────────────

        if name.startswith("roster_"):

        # rename the imported "person_id" to "player_id" so the COPY matches the table

           if "person_id" in df.columns:

               df = df.rename(columns={"person_id": "player_id"})




        cols = ','.join(df.columns)

        name = Path(fp).name


        if name.startswith("roster_"):

            table = "roster"

        elif name.startswith("statsapi_"):

            table = "statsapi_playlog"

        else:

            table = "statcast_pitchlog"


        # Ensure the target table has all necessary columns

        sync_columns(conn, "mlb", table, df.columns.tolist())


        # Enforce correct data types for numeric fields

        with conn.cursor() as cur:

            if table == "statcast_pitchlog":

                cur.execute("""
                            
                    ALTER TABLE mlb.statcast_pitchlog
                            
                      ALTER COLUMN game_date TYPE DATE    USING game_date::DATE,
                            
                      ALTER COLUMN game_pk   TYPE INTEGER USING game_pk::INTEGER,
                            
                      ALTER COLUMN pitcher   TYPE INTEGER USING pitcher::INTEGER,
                            
                      ALTER COLUMN batter    TYPE INTEGER USING batter::INTEGER,
                            
                      ALTER COLUMN release_speed TYPE REAL USING release_speed::REAL,
                            
                      ALTER COLUMN release_pos_x TYPE REAL USING release_pos_x::REAL,
                            
                      ALTER COLUMN release_pos_z TYPE REAL USING release_pos_z::REAL,
                            
                      ALTER COLUMN plate_x   TYPE REAL    USING plate_x::REAL,
                            
                      ALTER COLUMN plate_z   TYPE REAL    USING plate_z::REAL;
                            
                """)

            elif table == "statsapi_playlog":

                cur.execute("""
                            
                    ALTER TABLE mlb.statsapi_playlog
                            
                      ALTER COLUMN game_pk     TYPE INTEGER USING game_pk::INTEGER,
                            
                      ALTER COLUMN atbat_index TYPE INTEGER USING atbat_index::INTEGER,
                            
                      ALTER COLUMN pitch_index TYPE INTEGER USING pitch_index::INTEGER;
                            
                """)

        conn.commit()


        # Stream the DataFrame into Postgres using COPY

        buf = StringIO()

        df.to_csv(buf, index=False, header=False)

        buf.seek(0)

        with conn.cursor() as cur:

            cur.copy_expert(f"COPY mlb.{table} ({cols}) FROM STDIN WITH (FORMAT CSV)", buf)

        conn.commit()


        # If this file is a roster, upsert new teams and players for that date

        if table == "roster":

            roster_date = name.split("_", 1)[1].split(".")[0]

            with conn.cursor() as cur:

                cur.execute(

                    "INSERT INTO mlb.team (team_id) "

                    "SELECT DISTINCT team_id FROM mlb.roster WHERE game_date = %s "

                    "ON CONFLICT (team_id) DO NOTHING;",

                    (roster_date,)

                )

                cur.execute(

                    "INSERT INTO mlb.player (player_id, full_name, position, bats, throws) "

                    "SELECT DISTINCT person_id, person_fullname, primaryposition_name, batside_code, pitchhand_code "

                    "FROM mlb.roster WHERE game_date = %s "

                    "ON CONFLICT (player_id) DO NOTHING;",

                    (roster_date,)

                )

            conn.commit()

        # Log the load

        print(f"→ Loaded {len(df)} rows into mlb.{table}")


    # Finally, fetch today's game schedule and upsert into mlb.game table

    with conn.cursor() as cur:

        try:

            games = statsapi.schedule(start_date=date_str, end_date=date_str) or []

        except Exception as e:

            print(f"❌ Error fetching schedule for {date_str}: {e}")

            games = []

        for g in games:

            game_pk = g.get("game_id") or g.get("game_pk")

            if not game_pk:
                
                continue

            home_id  = g.get("home_id")

            away_id  = g.get("away_id")

            home_score = g.get("home_score", 0)

            away_score = g.get("away_score", 0)

            cur.execute(

                "INSERT INTO mlb.game (game_pk, game_date, home_team_id, away_team_id, home_score, away_score) "

                "VALUES (%s, %s, %s, %s, %s, %s) "

                "ON CONFLICT (game_pk) DO UPDATE "

                "SET home_score = EXCLUDED.home_score, away_score = EXCLUDED.away_score;",

                (game_pk, date_str, home_id, away_id, home_score, away_score)

            )

    conn.commit()

    conn.close()
    


if __name__ == "__main__":

    main()
    
