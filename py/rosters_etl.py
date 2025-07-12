#!/usr/bin/env python


import os

import pandas as pd

import statsapi

from datetime import datetime, timedelta

import argparse


# 0. Parse arguments

parser = argparse.ArgumentParser(

    description="Pull a single day's roster via StatsAPI"

)

parser.add_argument(

    "--date",

    help="YYYY-MM-DD; defaults to yesterday",

    required=False

)

args = parser.parse_args()


# 1. Determine the date to fetch

if args.date:

    yday = args.date

else:

    yday_dt = datetime.utcnow().date() - timedelta(days=1)

    yday = yday_dt.isoformat()


# 2. Setup output directory

OUT = os.getenv("OUTPUT_DIR", "stage")

os.makedirs(OUT, exist_ok=True)


# 3. Initialize "seen" set to avoid duplicate team pulls and container for rows\seen = set()

rows = []


# 4. Fetch schedule for the target date\schedule = statsapi.schedule(start_date=yday, end_date=yday) or []


for game in schedule:

    for team_id, side in ((game.get("home_id"), "home"), (game.get("away_id"), "away")):

        if (yday, team_id) in seen:

            continue

        seen.add((yday, team_id))


        # 5. Fetch roster for the team on this date

        try:

            data = statsapi.roster(team_id=team_id, date=yday)

        except Exception as e:

            print(f"❌ Error fetching roster for team {team_id} on {yday}: {e}")

            continue


        # 6. Normalize payload

        if isinstance(data, dict) and "roster" in data:

            records = data["roster"]

        elif isinstance(data, list):

            records = data

        else:

            records = []


        if not records:

            continue


        # 7. Build enriched DataFrame rows

        enriched = []

        for r in records:

            if not isinstance(r, dict):

                continue

            row = r.copy()

            row["team_id"] = team_id

            row["game_date"] = yday

            row["side"] = side

            enriched.append(row)


        if enriched:

            df = pd.DataFrame(enriched)

            rows.append(df)


# 8. Write output

if rows:

    final_df = pd.concat(rows, ignore_index=True)

    # normalize column names

    final_df.columns = [c.replace('.', '_').replace('-', '_').lower() for c in final_df.columns]

    out_file = os.path.join(OUT, f"roster_{yday}.parquet")

    final_df.to_parquet(out_file, index=False)

    print(f"✅ Rosters: Wrote {len(final_df)} rows → {out_file}")

else:

    print(f"⚠️ No Rosters for {yday}")

    
