# backfill.py - Script to backfill MLB data from 2021 season to current date

"""

This script backfills MLB Statcast data, StatsAPI play-by-play data, and roster data from the start of the 2021 season up to a specified end date (default is today). 

It uses the existing modules in the mlb-etl project (statcast_csv_pull, statsapi_etl, rosters_etl) to fetch data for each day in the range, while avoiding duplicate loads.


**Placement:** Save this file as `backfill.py` (for example, in the repository root or the `py/` directory of the mlb-etl project).


**Usage:** 

Run the script with optional `--start` and `--end` date arguments. For example:

If not provided, `--start` defaults to 2021-04-01 (Opening Day 2021) and `--end` defaults to today.


The script will iterate day-by-day through the date range and backfill data for each date:

- **Statcast** (pitch-level data) via pybaseball

- **StatsAPI play-by-play** (game events) via MLB-StatsAPI

- **Rosters** (team rosters for games played that day) via MLB-StatsAPI


It checks the output "stage" directory for existing parquet files to avoid re-fetching data that has already been loaded. The OUTPUT_DIR can be configured via the environment (defaults to "stage"). 


The script is idempotent and safe to run repeatedly: on subsequent runs it will skip any dates that have already been processed (files exist), and only fetch new or missing data. This makes it suitable for one-time historical backfill as well as scheduled periodic runs (e.g., via cron or GitHub Actions) to keep data up-to-date.


After running this backfill, you can use the existing loader (e.g., `loader/load_parquet_into_pg.py`) to load the new parquet files into the database.


"""

import os

import argparse

from datetime import datetime, timedelta


import pandas as pd

from pybaseball import statcast

import statsapi


def fetch_statcast_for_date(date_str: str, output_dir: str):

    """Fetches Statcast data for a single date and writes to parquet if data is found."""

    file_path = os.path.join(output_dir, f"statcast_{date_str}.parquet")

    if os.path.exists(file_path):

        print(f"⏭️ Skipping Statcast for {date_str} (already loaded)")

        return False  # No new data fetched
    
    try:

        # Use pybaseball's statcast function for the single day

        df = statcast(start_dt=date_str, end_dt=date_str)

    except Exception as e:

        print(f"❌ Statcast API error on {date_str}: {e}")

        return False
    
    if df is None or df.empty:

        # If no data (e.g., no games or no pitches on that day)

        print(f"✅ No Statcast data for {date_str}")

        return False
    
    # Write the data to parquet

    try:

        df.to_parquet(file_path, index=False)

    except Exception as e:

        print(f"❌ Error writing Statcast data for {date_str} to parquet: {e}")

        return False
    
    print(f"✅ Statcast: Wrote {len(df)} rows → {file_path}")

    return True


def fetch_statsapi_for_date(date_str: str, output_dir: str):

    """Fetches play-by-play data via StatsAPI for a single date and writes to parquet if data is found."""

    file_path = os.path.join(output_dir, f"statsapi_{date_str}.parquet")

    if os.path.exists(file_path):

        print(f"⏭️ Skipping StatsAPI PBP for {date_str} (already loaded)")

        return False
    
    # Get schedule for the date (list of games)

    try:

        games = statsapi.schedule(start_date=date_str, end_date=date_str)

    except Exception as e:

        print(f"❌ StatsAPI schedule error on {date_str}: {e}")

        return False
    
    if not games:

        print(f"✅ No games on {date_str} (no play-by-play data)")

        return False
    
    rows = []

    for game in games:

        # Each game in the schedule has a game_id or game_pk

        game_pk = game.get("game_id") or game.get("game_pk")

        if not game_pk:

            continue

        try:

            pbp_data = statsapi.get("game_playByPlay", {"gamePk": game_pk})

        except Exception as e:

            print(f"❌ StatsAPI play-by-play error for game {game_pk} on {date_str}: {e}")

            continue  # skip this game, move to next

        # Extract play-by-play events

        plays = pbp_data.get("allPlays") or pbp_data.get("liveData", {}).get("plays", {}).get("allPlays", [])

        for play in plays:

            rows.append(play)

    if not rows:

        print(f"✅ No play-by-play data for {date_str}")

        return False
    
    # Normalize JSON plays to dataframe

    df = pd.json_normalize(rows)

    try:

        df.to_parquet(file_path, index=False)

    except Exception as e:

        print(f"❌ Error writing PBP data for {date_str} to parquet: {e}")

        return False
    
    print(f"✅ StatsAPI: Wrote {len(df)} rows → {file_path}")

    return True


def fetch_roster_for_date(date_str: str, output_dir: str):

    """Fetches team rosters for all games on a given date and writes to parquet if data is found."""

    file_path = os.path.join(output_dir, f"roster_{date_str}.parquet")

    if os.path.exists(file_path):

        print(f"⏭️ Skipping Rosters for {date_str} (already loaded)")

        return False
    
    # Determine season year from date

    try:

        date_obj = datetime.fromisoformat(date_str)

    except Exception as e:

        print(f"❌ Invalid date format {date_str}: {e}")

        return False
    
    season_year = date_obj.year

    # Get games scheduled on that date

    try:

        games = statsapi.schedule(start_date=date_str, end_date=date_str)

    except Exception as e:

        print(f"❌ StatsAPI schedule error on {date_str}: {e}")

        return False
    
    if not games:

        print(f"✅ No games on {date_str} (no rosters to fetch)")

        return False
    
    roster_frames = []

    for game in games:

        # Get home and away team IDs from schedule

        home_team_id = game.get("home_id") or game.get("home_team_id") or game.get("home_team", {}).get("id")

        away_team_id = game.get("away_id") or game.get("away_team_id") or game.get("away_team", {}).get("id")

        teams = []

        if home_team_id:

            teams.append((home_team_id, "home"))

        if away_team_id:

            teams.append((away_team_id, "away"))

        for team_id, side in teams:

            try:

                data = statsapi.roster(team_id, season=season_year)

            except Exception as e:

                print(f"❌ StatsAPI roster error for team {team_id} on {date_str}: {e}")

                continue

            # The statsapi.roster could return a dict with 'roster' key or a list directly

            if isinstance(data, dict):

                records = data.get("roster", [])

            elif isinstance(data, list):

                records = data

            else:

                records = []

            if not records:

                continue

            df_team = pd.DataFrame(records)

            # Add team and date context

            df_team["team_id"] = team_id

            df_team["game_date"] = date_str

            df_team["side"] = side

            roster_frames.append(df_team)

    if not roster_frames:

        print(f"✅ No roster data for {date_str}")

        return False
    
    # Concatenate all teams' rosters for the day

    df_rosters = pd.concat(roster_frames, ignore_index=True)

    # Clean column names (replace dots and hyphens, lowercasing) to match loader expectations

    df_rosters.columns = [c.replace('.', '_').replace('-', '_').lower() for c in df_rosters.columns]

    try:

        df_rosters.to_parquet(file_path, index=False)

    except Exception as e:

        print(f"❌ Error writing roster data for {date_str} to parquet: {e}")

        return False
    
    print(f"✅ Rosters: Wrote {len(df_rosters)} rows → {file_path}")

    return True


def main():

    parser = argparse.ArgumentParser(description="Backfill MLB Statcast, StatsAPI play-by-play, and roster data.")

    parser.add_argument("--start", help="Start date (YYYY-MM-DD). Defaults to 2021-04-01.", required=False)

    parser.add_argument("--end", help="End date (YYYY-MM-DD). Defaults to today.", required=False)

    args = parser.parse_args()

    # Determine date range

    if args.start:

        start_date_str = args.start

    else:

        start_date_str = "2021-04-01"  # default start of 2021 season

    if args.end:

        end_date_str = args.end

    else:

        # default end is today

        end_date_str = datetime.today().strftime("%Y-%m-%d")
        
    try:

        start_date = datetime.fromisoformat(start_date_str)

    except Exception as e:


        raise ValueError(f"Invalid start date '{start_date_str}': {e}")
    
    try:

        end_date = datetime.fromisoformat(end_date_str)

    except Exception as e:

        raise ValueError(f"Invalid end date '{end_date_str}': {e}")
    
    if end_date < start_date:

        raise ValueError(f"End date {end_date_str} is earlier than start date {start_date_str}.")
    
    # Output directory for data files (stage by default)

    output_dir = os.getenv("OUTPUT_DIR", "stage")

    os.makedirs(output_dir, exist_ok=True)

    print(f"Starting backfill from {start_date_str} to {end_date_str}...")

    current = start_date



    while current <= end_date:

        date_str = current.strftime("%Y-%m-%d")

        # Process each data source for the date

        fetch_statcast_for_date(date_str, output_dir)

        fetch_statsapi_for_date(date_str, output_dir)

        fetch_roster_for_date(date_str, output_dir)

        # Move to next day

        current += timedelta(days=1)

    print("✅ Backfill complete.")

    
if __name__ == "__main__":

    main()
    

