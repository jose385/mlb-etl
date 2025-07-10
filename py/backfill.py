#!/usr/bin/env python

"""
backfill.py - Historical backfill for MLB Statcast, StatsAPI PBP, and Rosters.

Pulls data from Opening Day 2021 (or a custom start) through today (or a custom end),

skips any dates already present in the `stage/` folder, and writes new Parquet files

for statcast_YYYY-MM-DD.parquet, statsapi_YYYY-MM-DD.parquet, and roster_YYYY-MM-DD.parquet.

"""


import os

import argparse

from datetime import datetime, timedelta


import pandas as pd

from pybaseball import statcast

import statsapi


def fetch_statcast_for_date(date_str: str, output_dir: str):

    """Fetch one day's Statcast data and write to Parquet if new."""

    out_file = os.path.join(output_dir, f"statcast_{date_str}.parquet")

    if os.path.exists(out_file):

        print(f"⏭️ Skipping Statcast for {date_str} (already exists)")

        return
    
    df = statcast(start_dt=date_str, end_dt=date_str)

    if df is None or df.empty:

        print(f"✅ No Statcast data for {date_str}")

        return
    
    df.to_parquet(out_file, index=False)

    print(f"✅ Statcast: Wrote {len(df)} rows → {out_file}")


def fetch_statsapi_for_date(date_str: str, output_dir: str):

    """Fetch one day's play-by-play via StatsAPI and write to Parquet if new."""

    out_file = os.path.join(output_dir, f"statsapi_{date_str}.parquet")

    if os.path.exists(out_file):

        print(f"⏭️ Skipping StatsAPI PBP for {date_str} (already exists)")

        return
    
    # get today's games

    games = statsapi.schedule(start_date=date_str, end_date=date_str) or []

    rows = []

    for g in games:

        game_pk = g.get("game_id") or g.get("game_pk")

        if not game_pk:

            continue

        try:

            resp = statsapi.get("game_playByPlay", {"gamePk": game_pk})

        except Exception as e:

            print(f"❌ PBP error for game {game_pk} on {date_str}: {e}")

            continue

        plays = (

            resp.get("allPlays")

            or resp.get("liveData", {}).get("plays", {}).get("allPlays", [])

        )

        rows.extend(plays)

    if not rows:

        print(f"✅ No PBP data for {date_str}")

        return
    
    df = pd.json_normalize(rows)

    df.to_parquet(out_file, index=False)

    print(f"✅ StatsAPI: Wrote {len(df)} rows → {out_file}")


def fetch_roster_for_date(date_str: str, output_dir: str):

    """Fetch one day's rosters (home & away) and write to Parquet if new."""

    out_file = os.path.join(output_dir, f"roster_{date_str}.parquet")

    if os.path.exists(out_file):

        print(f"⏭️ Skipping Rosters for {date_str} (already exists)")

        return
    
    games = statsapi.schedule(start_date=date_str, end_date=date_str) or []

    rows = []

    season = datetime.fromisoformat(date_str).year

    for g in games:

        for team_id, side in ((g["home_id"], "home"), (g["away_id"], "away")):

            data = statsapi.roster(team_id, date=date_str)

            if isinstance(data, dict) and "roster" in data:

               records = data["roster"]

            elif isinstance(data, list):

                 records = data

            else:

                 records = []

            for r in records:

                if isinstance(r, dict):

                    row = dict(r)

                    row["team_id"] = team_id

                    row["game_date"] = yday   # update if your game date var is different

                    row["side"] = side

                    rows.append(row)



                else:

                    pass


                   #print(f"Warning: Skipping non-dict row: {r}")



    if not rows:

        print(f"✅ No Rosters for {date_str}")

        return
    
    df = pd.DataFrame(rows)

    df.columns = [c.replace(".", "_").replace("-", "_").lower() for c in df.columns]

    df.to_parquet(out_file, index=False)

    print(f"✅ Rosters: Wrote {len(df)} rows → {out_file}")


def main():

    p = argparse.ArgumentParser()

    p.add_argument("--start", help="YYYY-MM-DD (default=2021-04-01)", required=False)

    p.add_argument("--end",   help="YYYY-MM-DD (default=today)", required=False)

    args = p.parse_args()


    start = args.start or "2021-04-01"

    end   = args.end or datetime.today().strftime("%Y-%m-%d")

    sd = datetime.fromisoformat(start)

    ed = datetime.fromisoformat(end)

    if ed < sd:

        raise ValueError("End date must be on or after start date")
    

    out_dir = os.getenv("OUTPUT_DIR", "stage")

    os.makedirs(out_dir, exist_ok=True)

    print(f"🔄 Backfilling from {sd.date()} to {ed.date()}…")


    cur = sd

    while cur <= ed:

        ds = cur.strftime("%Y-%m-%d")

        fetch_statcast_for_date(ds, out_dir)

        fetch_statsapi_for_date(ds, out_dir)

        fetch_roster_for_date(ds, out_dir)

        cur += timedelta(days=1)


    print("🎉 Backfill complete.")


if __name__ == "__main__":

    main()
    

