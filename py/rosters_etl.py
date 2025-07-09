#!/usr/bin/env python

import os

import pandas as pd

import statsapi

from datetime import datetime

import argparse


parser = argparse.ArgumentParser(

    description="Pull yesterday’s (or --date) roster via StatsAPI"

)

parser.add_argument(

    "--date",

    help="YYYY-MM-DD; defaults to yesterday",

    required=False

)

args = parser.parse_args()


if args.date:

    yday = args.date

    # ensure season matches

    season = int(yday.split("-")[0])
    


# 1. Setup output directory

OUT = os.getenv("OUTPUT_DIR", "stage")

os.makedirs(OUT, exist_ok=True)


# 2. Today's date

today  = pd.Timestamp.today()

yesterday = today - pd.Timedelta(days=1)

yday   = today.strftime("%Y-%m-%d")

season = today.year


# 3. Fetch schedule for today (list of dicts)

schedule = statsapi.schedule(start_date=yday, end_date=yday)


rows = []


for game in schedule:

    yday = game['game_date']


    for team_id, side in ((game["home_id"], "home"), (game["away_id"], "away")):

        if (yday, team_id) in seen:

            continue

        seen.add((yday, team_id))


        data = statsapi.roster(team_id=team_id, season=season)

        if isinstance(data, dict):

            records = data.get("roster", [])

        elif isinstance(data, list):

            records = data

        else:

            records = []


        if not records:

            continue


        # Add metadata to each roster record:

        enhanced = []

        for r in records:

            r["team_id"] = team_id

            r["game_date"] = yday

            r["side"] = side

            enhanced.append(r)


        roster_df = pd.DataFrame(enhanced)

        rows.append(roster_df)


# After loop, write everything at once

if rows:

    final_df = pd.concat(rows, ignore_index=True)

    final_df.to_parquet(f"{out_dir}/roster_{season}.parquet", index=False)

    print("✅ Roster Pull complete")

else:

    print("⚠️ No roster data found")
    
