#!/usr/bin/env python

import os

import pandas as pd

import statsapi


# 1. Setup output directory

OUT = os.getenv("OUTPUT_DIR", "stage")

os.makedirs(OUT, exist_ok=True)


# 2. Today's date

today = pd.Timestamp.today().strftime("%Y-%m-%d")


# 3. Fetch schedule for today (returns a list of dicts)

schedule = statsapi.schedule(start_date=today, end_date=today)


# 4. Collect rosters

rows = []

for g in schedule:

    # g is a dict, not a DataFrame row

    for team_id, side in ((g['home_id'], 'home'), (g['away_id'], 'away')):

        roster_df = statsapi.roster(team_id, game_date=today)

        # Ensure this block is indented under the for-loop

        if roster_df.empty:

            continue

        roster_df['team_id']   = team_id

        roster_df['game_date'] = today

        roster_df['side']      = side

        rows.append(roster_df)


# 5. Write out if any data

if not rows:

    print(f"✅ No rosters for {today}")

else:

    df = pd.concat(rows, ignore_index=True)

    # sanitize column names

    df.columns = [c.replace('.', '_').replace('-', '_').lower() for c in df.columns]

    out_path = f"{OUT}/roster_{today}.parquet"

    df.to_parquet(out_path, index=False)

    print(f"✅ Wrote {len(df)} roster rows → {out_path}")
    