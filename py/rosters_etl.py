#!/usr/bin/env python

import os

import pandas as pd

import statsapi


# 1. Setup output directory

OUT = os.getenv("OUTPUT_DIR", "stage")

os.makedirs(OUT, exist_ok=True)


# 2. Today's date

today = pd.Timestamp.today().strftime("%Y-%m-%d")


# 3. Fetch schedule for today

schedule = statsapi.schedule(start_date=today, end_date=today)


rows = []

for g in schedule.itertuples():

    for team_id, side in ((g.home_id, 'home'), (g.away_id, 'away')):

        roster_df = statsapi.roster(team_id, game_date=today)

        if roster_df.empty:

            continue

        roster_df['team_id']   = team_id

        roster_df['game_date'] = today  # YYYY-MM-DD string

        roster_df['side']      = side    # 'home' or 'away'

        rows.append(roster_df)


# 4. Concatenate & sanitize

if not rows:

    print(f"✅ No rosters for {today}")

    exit(0)


df = pd.concat(rows, ignore_index=True)

df.columns = [c.replace('.', '_').replace('-', '_').lower() for c in df.columns]


# 5. Write Parquet

out_path = f"{OUT}/roster_{today}.parquet"

df.to_parquet(out_path, index=False)

print(f"✅ Wrote {len(df)} roster rows → {out_path}")
