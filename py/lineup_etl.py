#!/usr/bin/env python


import os

import argparse


import pandas as pd

from datetime import datetime, timedelta

from sqlalchemy import create_engine, text

import statsapi


def main(start_date: str, end_date: str):

    # Connect to Postgres using PG_DSN environment variable

    dsn = os.environ.get("PG_DSN")

    if not dsn:

        raise RuntimeError("PG_DSN environment variable not set")
    
    # Adjust for deprecated postgres:// URI scheme if present

    if dsn.startswith("postgres://"):

        dsn = dsn.replace("postgres://", "postgresql://", 1)

    engine = create_engine(dsn)

 # Ensure team & player dims exist

    with engine.begin() as conn:

        conn.execute(text("""
                          
        CREATE SCHEMA IF NOT EXISTS mlb;
                          
        CREATE TABLE IF NOT EXISTS mlb.team (
                          
          team_id   INT PRIMARY KEY,
                          
          team_name TEXT,
                          
          abbreviation TEXT
                          
        );
                          
        """))

        conn.execute(text("""
                          
        CREATE TABLE IF NOT EXISTS mlb.player (
                          
          player_id INT PRIMARY KEY,
                          
          full_name TEXT,
                          
          position  TEXT,
                          
          bats      TEXT,
                          
          throws    TEXT
                          
        );
                          
        """))
        

    # Ensure target schema and table exist

    create_table_sql = """

    CREATE SCHEMA IF NOT EXISTS mlb;

    CREATE TABLE IF NOT EXISTS mlb.lineup (

        game_pk INT,

        team_id INT REFERENCES mlb.team(team_id),

        player_id INT REFERENCES mlb.player(player_id),

        batting_order INT,

        field_position TEXT,

        PRIMARY KEY (game_pk, team_id, player_id)

    );

    """

    # Execute DDL (may execute multiple statements, split them)

    with engine.begin() as conn:

        for stmt in create_table_sql.strip().split(";"):

            if stmt.strip():

                conn.execute(text(stmt))


    # Parse input dates

    start_dt = datetime.fromisoformat(start_date)

    end_dt = datetime.fromisoformat(end_date)

    if end_dt < start_dt:

        raise ValueError("End date must be on or after start date")
    

    records = []  # to collect lineup records

    current = start_dt

    while current <= end_dt:

        date_str = current.strftime("%Y-%m-%d")

        try:

            games = statsapi.schedule(start_date=date_str, end_date=date_str) or []

        except Exception as e:

            print(f"❌ Error fetching schedule for {date_str}: {e}")

            current += timedelta(days=1)

            continue


        if not games:

            # No games on this date

            current += timedelta(days=1)

            continue


        for g in games:

            game_pk = g.get("game_id") or g.get("game_pk")

            if not game_pk:

                continue

            # Ensure game_pk is an integer

            try:

                game_pk = int(game_pk)

            except Exception:

                continue


            home_team_id = g.get("home_id")

            away_team_id = g.get("away_id")


            # Fetch detailed game data (lineups are in the boxscore section)

            try:

                game_data = statsapi.get("game", {"gamePk": game_pk})

            except Exception as e:

                print(f"❌ Error fetching game data for game_pk {game_pk}: {e}")

                continue


            boxscore = game_data.get("liveData", {}).get("boxscore", {}).get("teams", {})

            if not boxscore:

                # No boxscore data (lineup info) available for this game

                print(f"⚠️ Lineup not available for game {game_pk} (no boxscore data)")

                continue


            away_section = boxscore.get("away", {})

            home_section = boxscore.get("home", {})

            away_batters = away_section.get("batters", []) or []

            home_batters = home_section.get("batters", []) or []

            if not away_batters or not home_batters:

                # Lineups not posted or game not played

                print(f"⚠️ Lineup not available for game {game_pk} (batters list empty)")

                continue


            # Process away team lineup

            away_players = away_section.get("players", {})

            for pid in away_batters:

                player_key = f"ID{pid}"

                player_info = away_players.get(player_key, {})

                bo_str = player_info.get("battingOrder")

                if not bo_str:

                    continue

                try:

                    bo_val = int(bo_str)

                except ValueError:

                    continue

                # Only keep starters (battingOrder values 100, 200, ..., 900)

                if bo_val % 100 != 0:

                    continue

                batting_order_num = bo_val // 100

                # Get field position (use abbreviation if available)

                pos = player_info.get("position", {})

                field_pos = pos.get("abbreviation") or pos.get("code") or pos.get("name") or ""

                records.append({

                    "game_pk": game_pk,

                    "team_id": away_team_id,

                    "player_id": int(pid),

                    "batting_order": batting_order_num,

                    "field_position": field_pos

                })


            # Process home team lineup

            home_players = home_section.get("players", {})

            for pid in home_batters:

                player_key = f"ID{pid}"

                player_info = home_players.get(player_key, {})

                bo_str = player_info.get("battingOrder")

                if not bo_str:

                    continue

                try:

                    bo_val = int(bo_str)

                except ValueError:

                    continue

                if bo_val % 100 != 0:

                    continue

                batting_order_num = bo_val // 100

                pos = player_info.get("position", {})

                field_pos = pos.get("abbreviation") or pos.get("code") or pos.get("name") or ""

                records.append({

                    "game_pk": game_pk,

                    "team_id": home_team_id,

                    "player_id": int(pid),

                    "batting_order": batting_order_num,

                    "field_position": field_pos

                })

        # move to next date

        current += timedelta(days=1)


    # If no lineup records were found, exit gracefully

    if not records:

        print("✅ No lineup data found for the given date range.")

        return
    

    # Insert or update records in the database (upsert)

    insert_stmt = text("""
                       
        INSERT INTO mlb.lineup (game_pk, team_id, player_id, batting_order, field_position)
                       
        VALUES (:game_pk, :team_id, :player_id, :batting_order, :field_position)
                       
        ON CONFLICT (game_pk, team_id, player_id)
                       
        DO UPDATE SET batting_order = EXCLUDED.batting_order,
                       
                      field_position = EXCLUDED.field_position;
                       
    """)

    # Use a database transaction for batch upsert

    with engine.begin() as conn:

        for rec in records:

            conn.execute(insert_stmt, rec)


    print(f"✅ Upserted {len(records)} records into mlb.lineup")


if __name__ == "__main__":


    import pandas as pd

    from datetime import datetime


    parser = argparse.ArgumentParser(

        description="ETL script for MLB starting lineups."

    )

    parser.add_argument(

        "--start_date", "--start",

        dest="start_date",

        help="Season backfill start (YYYY-MM-DD). Defaults to Opening Day of last season.",

        required=False

    )

    parser.add_argument(

        "--end_date", "--end",

        dest="end_date",

        help="Backfill end date (YYYY-MM-DD). Defaults to today.",

        required=False

    )

    args = parser.parse_args()


    yesterday_dt = datetime.today() - timedelta(days=1)

    default_end = yesterday_dt.strftime("%Y-%m-%d")


    # Determine current season year (if today is April+ it's this year, else it's last year)

    today = datetime.today()

    if today.month >= 4:

        current_season = today.year

    else:

        current_season = today.year - 1


    # Opening Day for the current season is April 1 of current_season

    default_start = f"{current_season}-04-01"


    # Apply overrides or defaults

    start_date = args.start_date or default_start

    end_date   = args.end_date   or default_end


    print(f"🔄 Backfilling lineups from {start_date} through {end_date} (Season {current_season})")

    main(start_date, end_date)
    