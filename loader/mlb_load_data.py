#!/usr/bin/env python3
"""
MLB BallDontLie PostgreSQL Loader
Loads collected CSV files into the mlb_* PostgreSQL tables.

Usage:
    python loader/mlb_load_data.py --all
    python loader/mlb_load_data.py --teams --players --games --stats
    python loader/mlb_load_data.py --stats --season 2025
"""

import os
import sys
import argparse
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = Path("data/mlb")

# Postgres connection from env
DB_CONFIG = {
    "host":     os.environ.get("DB_HOST",     "localhost"),
    "port":     int(os.environ.get("DB_PORT", "5432")),
    "dbname":   os.environ.get("DB_NAME",     "sports"),
    "user":     os.environ.get("DB_USER",     "postgres"),
    "password": os.environ.get("DB_PASSWORD", ""),
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def upsert(conn, table: str, df: pd.DataFrame, conflict_cols: list,
           update_cols: list = None) -> int:
    if df.empty:
        return 0
    cols = list(df.columns)
    values = [tuple(row) for row in df.itertuples(index=False, name=None)]
    col_str = ", ".join(cols)
    placeholders = "(" + ", ".join(["%s"] * len(cols)) + ")"

    if update_cols:
        update_str = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
        conflict_str = ", ".join(conflict_cols)
        sql = (
            f"INSERT INTO {table} ({col_str}) VALUES %s "
            f"ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str}"
        )
    else:
        conflict_str = ", ".join(conflict_cols)
        sql = (
            f"INSERT INTO {table} ({col_str}) VALUES %s "
            f"ON CONFLICT ({conflict_str}) DO NOTHING"
        )

    with conn.cursor() as cur:
        execute_values(cur, sql, values, template=placeholders)
    conn.commit()
    return len(values)


def load_teams(conn):
    path = DATA_DIR / "teams.csv"
    if not path.exists():
        print(f"⚠️  {path} not found — run backfill first"); return
    df = pd.read_csv(path)
    n = upsert(conn, "mlb_teams", df, ["team_id"],
               ["name", "full_name", "abbreviation", "city", "league", "division"])
    print(f"✅ mlb_teams: {n} rows upserted")


def load_players(conn):
    path = DATA_DIR / "players.csv"
    if not path.exists():
        print(f"⚠️  {path} not found"); return
    df = pd.read_csv(path)
    n = upsert(conn, "mlb_players", df, ["player_id"],
               ["first_name", "last_name", "full_name", "position", "bats_throws",
                "jersey", "college", "birth_place", "dob", "age", "height", "weight",
                "draft", "debut_year", "active",
                "team_id", "team_name", "team_short_name", "team_abbr",
                "team_location", "team_league", "team_division"])
    print(f"✅ mlb_players: {n} rows upserted")


def load_games(conn, season: int):
    path = DATA_DIR / f"games_{season}.csv"
    if not path.exists():
        print(f"⚠️  {path} not found"); return
    df = pd.read_csv(path)
    n = upsert(conn, "mlb_games", df, ["game_id"],
               ["status", "home_score", "away_score", "home_innings",
                "away_innings", "winning_pitcher", "losing_pitcher", "save_pitcher"])
    print(f"✅ mlb_games: {n} rows upserted")


def load_stats(conn, season: int):
    path = DATA_DIR / f"stats_{season}.csv"
    if not path.exists():
        print(f"⚠️  {path} not found"); return
    df = pd.read_csv(path)
    n = upsert(conn, "mlb_stats", df, ["stat_id"],
               ["avg", "obp", "slg", "ops", "p_era", "p_whip"])
    print(f"✅ mlb_stats: {n} rows upserted")


def load_standings(conn, season: int):
    path = DATA_DIR / f"standings_{season}.csv"
    if not path.exists():
        print(f"⚠️  {path} not found"); return
    df = pd.read_csv(path)
    n = upsert(conn, "mlb_standings", df, ["team_id", "season"],
               ["rank", "division_rank", "wins", "losses", "win_pct",
                "games_back", "home_record", "road_record", "last_ten",
                "streak", "runs_scored", "runs_allowed", "run_differential"])
    print(f"✅ mlb_standings: {n} rows upserted")


def load_season_stats(conn, season: int):
    path = DATA_DIR / f"season_stats_{season}.csv"
    if not path.exists():
        print(f"⚠️  {path} not found"); return
    df = pd.read_csv(path)
    n = upsert(conn, "mlb_season_stats", df, ["player_id", "season"],
               ["games", "ab", "h", "hr", "rbi", "avg", "obp", "slg", "ops",
                "era", "whip", "ip", "p_so", "p_bb", "wins", "losses", "saves"])
    print(f"✅ mlb_season_stats: {n} rows upserted")


def load_team_season_stats(conn, season: int):
    path = DATA_DIR / f"team_season_stats_{season}.csv"
    if not path.exists():
        print(f"⚠️  {path} not found"); return
    df = pd.read_csv(path)
    n = upsert(conn, "mlb_team_season_stats", df, ["team_id", "season"],
               ["avg", "obp", "slg", "ops", "r", "h", "hr", "rbi", "era", "whip"])
    print(f"✅ mlb_team_season_stats: {n} rows upserted")


def load_splits(conn, season: int):
    path = DATA_DIR / f"splits_{season}.csv"
    if not path.exists():
        print(f"⚠️  {path} not found"); return
    df = pd.read_csv(path)
    n = upsert(conn, "mlb_splits", df, ["player_id", "season", "split_type", "split_value"],
               ["ab", "h", "hr", "rbi", "avg", "obp", "slg", "ops"])
    print(f"✅ mlb_splits: {n} rows upserted")


def load_versus(conn, season: int):
    path = DATA_DIR / f"versus_{season}.csv"
    if not path.exists():
        print(f"⚠️  {path} not found"); return
    df = pd.read_csv(path)
    n = upsert(conn, "mlb_versus", df, ["batter_id", "pitcher_id", "season"],
               ["ab", "h", "hr", "avg", "obp", "slg", "ops"])
    print(f"✅ mlb_versus: {n} rows upserted")


def load_plays(conn):
    path = DATA_DIR / "plays_2025.csv"
    if not path.exists():
        print(f"⚠️  {path} not found"); return
    df = pd.read_csv(path)
    n = upsert(conn, "mlb_plays", df, ["play_id"])
    print(f"✅ mlb_plays: {n} rows upserted")


def load_plate_appearances(conn):
    path = DATA_DIR / "plate_appearances_2025.csv"
    if not path.exists():
        print(f"⚠️  {path} not found"); return
    df = pd.read_csv(path)
    n = upsert(conn, "mlb_plate_appearances", df, ["pa_id"])
    print(f"✅ mlb_plate_appearances: {n} rows upserted")


def load_injuries(conn):
    path = DATA_DIR / "injuries.csv"
    if not path.exists():
        print(f"⚠️  {path} not found"); return
    df = pd.read_csv(path)
    # injuries uses SERIAL PK — drop injury_id col if present so DB auto-assigns
    if "injury_id" in df.columns:
        df = df.drop(columns=["injury_id"])
    # Insert all; skip duplicates by checking player_id + date combination
    cols = list(df.columns)
    values = [tuple(row) for row in df.itertuples(index=False, name=None)]
    col_str = ", ".join(cols)
    placeholders = "(" + ", ".join(["%s"] * len(cols)) + ")"
    sql = (f"INSERT INTO mlb_injuries ({col_str}) VALUES %s "
           f"ON CONFLICT DO NOTHING")
    with conn.cursor() as cur:
        from psycopg2.extras import execute_values
        execute_values(cur, sql, values, template=placeholders)
    conn.commit()
    print(f"✅ mlb_injuries: {len(values)} rows inserted")


def main():
    parser = argparse.ArgumentParser(description="Load MLB CSVs into PostgreSQL")
    parser.add_argument("--season", type=int, default=2025)
    parser.add_argument("--all",               action="store_true")
    parser.add_argument("--teams",             action="store_true")
    parser.add_argument("--players",           action="store_true")
    parser.add_argument("--games",             action="store_true")
    parser.add_argument("--stats",             action="store_true")
    parser.add_argument("--standings",         action="store_true")
    parser.add_argument("--season-stats",      action="store_true")
    parser.add_argument("--team-season-stats", action="store_true")
    parser.add_argument("--splits",            action="store_true")
    parser.add_argument("--versus",            action="store_true")
    parser.add_argument("--plays",             action="store_true")
    parser.add_argument("--plate-appearances", action="store_true")
    parser.add_argument("--injuries",          action="store_true")
    parser.add_argument("--odds",              action="store_true")
    parser.add_argument("--player-props",      action="store_true")
    args = parser.parse_args()

    print(f"🔌 Connecting to PostgreSQL ({DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']})...")
    conn = get_conn()
    print("   Connected.\n")

    s = args.season

    if args.all or args.teams:            load_teams(conn)
    if args.all or args.players:          load_players(conn)
    if args.all or args.games:            load_games(conn, s)
    if args.all or args.stats:            load_stats(conn, s)
    if args.all or args.standings:        load_standings(conn, s)
    if args.all or args.season_stats:     load_season_stats(conn, s)
    if args.all or args.team_season_stats: load_team_season_stats(conn, s)
    if args.all or args.splits:           load_splits(conn, s)
    if args.all or args.versus:           load_versus(conn, s)
    if args.all or args.plays:            load_plays(conn)
    if args.all or args.plate_appearances: load_plate_appearances(conn)
    if args.all or args.injuries:         load_injuries(conn)
    if args.all or args.odds:             load_odds(conn)
    if args.all or args.player_props:     load_player_props(conn)

    conn.close()
    print("\n✅ Load complete.")


if __name__ == "__main__":
    main()


def load_odds(conn):
    path = DATA_DIR / "odds.csv"
    if not path.exists():
        print(f"⚠️  {path} not found"); return
    df = pd.read_csv(path)
    n = upsert(conn, "mlb_odds", df, ["odd_id"],
               ["spread_home_value", "spread_home_odds", "spread_away_value",
                "spread_away_odds", "moneyline_home_odds", "moneyline_away_odds",
                "total_value", "total_over_odds", "total_under_odds", "updated_at"])
    print(f"✅ mlb_odds: {n} rows upserted")


def load_player_props(conn):
    path = DATA_DIR / "player_props.csv"
    if not path.exists():
        print(f"⚠️  {path} not found"); return
    df = pd.read_csv(path)
    n = upsert(conn, "mlb_player_props", df, ["prop_id"],
               ["line_value", "market_type", "over_odds", "under_odds", "odds", "updated_at"])
    print(f"✅ mlb_player_props: {n} rows upserted")
