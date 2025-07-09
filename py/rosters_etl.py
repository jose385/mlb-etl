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

seen = set()   # track (date, team_id)

for game in schedule:

    for team_id, side in ((game["home_id"], "home"),
                          
                          (game["away_id"], "away")):
        
        if (yday, team_id) in seen:

            continue

        seen.add((yday, team_id))


        data = statsapi.roster(team_id, season=season)

        if isinstance(data, dict):

            records = data.get("roster", [])

        elif isinstance(data, list):

            records = data

        else:

            records = []

        if not records:

            continue


        roster_df = pd.DataFrame(records)

        roster_df["team_id"]   = team_id

        roster_df["game_date"] = yday

        roster_df["side"]      = side

        rows.append(roster_df)



# 5. Write out if any data

if not rows:

    print(f"✅ No rosters for {yday}")

    exit(0)


df = pd.concat(rows, ignore_index=True)


# 6. Sanitize column names

df.columns = [c.replace(".", "_").replace("-", "_").lower() for c in df.columns]


# 7. Save to Parquet

out_path = f"{OUT}/roster_{yday}.parquet"

df.to_parquet(out_path, index=False)

print(f"✅ Wrote {len(df)} roster rows → {out_path}")

