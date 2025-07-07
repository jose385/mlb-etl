#!/usr/bin/env python

import os

import pandas as pd

import statsapi

from datetime import datetime


# 1. Setup output directory

OUT = os.getenv("OUTPUT_DIR", "stage")

os.makedirs(OUT, exist_ok=True)


# 2. Today's date

today = pd.Timestamp.today()

yday  = today.strftime("%Y-%m-%d")

season = today.year


# 3. Fetch schedule for today (list of dicts)

schedule = statsapi.schedule(start_date=yday, end_date=yday)


rows = []

for game in schedule:

    for team_id, side in ((game["home_id"], "home"), (game["away_id"], "away")):

        # CALL roster(team_id, season), not game_date

        roster_list = statsapi.roster(team_id, season=season)

        if not roster_list:

            continue


        # Convert list of dicts to DataFrame

        roster_df = pd.DataFrame(roster_list)

        roster_df["team_id"]   = team_id

        roster_df["game_date"] = yday      # as string YYYY-MM-DD

        roster_df["side"]      = side      # 'home' or 'away'

        rows.append(roster_df)


# 4. Write out if any data

if not rows:

    print(f"✅ No rosters for {yday}")

    exit(0)


df = pd.concat(rows, ignore_index=True)

# sanitize column names

df.columns = [c.replace(".", "_").replace("-", "_").lower() for c in df.columns]


out_path = f"{OUT}/roster_{yday}.parquet"

df.to_parquet(out_path, index=False)

print(f"✅ Wrote {len(df)} roster rows → {out_path}")

