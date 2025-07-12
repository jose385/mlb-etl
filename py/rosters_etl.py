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

    # default to yesterday

    yday_dt = datetime.utcnow().date() - timedelta(days=1)

    yday = yday_dt.isoformat()


# derive season from date

season = int(yday.split("-")[0])


# 2. Setup output directory

OUT = os.getenv("OUTPUT_DIR", "stage")

os.makedirs(OUT, exist_ok=True)


# 3. Initialize seen set to avoid duplicate team pulls

seen = set()

rows = []


# 4. Fetch schedule for the target date

schedule = statsapi.schedule(start_date=yday, end_date=yday) or []


for game in schedule:

    # game_date may come as string or timestamp; use our yday

    for team_id, side in ((game.get("home_id"), "home"), (game.get("away_id"), "away")):

        # skip if this team-date already processed

        if (yday, team_id) in seen:

            continue

        seen.add((yday, team_id))


        # 5. Fetch roster for the team on this date

        try:

            data = statsapi.roster(team_id=team_id, date=yday)

        except Exception as e:

            print(f"❌ Error fetching roster for team {team_id} on {yday}: {e}")

            continue


        # normalize payload

        if isinstance(data, dict) and "roster" in data:

            records = data["roster"]

        elif isinstance(data, list):

            records = data

        else:

            records = []


        if not records:

            continue


        # 6. Build DataFrame

        enriched = []

        for r in records:

            if not isinstance(r, dict):

                continue

            r_copy = r.copy()

            r_copy["team_id"] = team_id

            r_copy["game_date"] = yday

            r_copy["side"] = side

            enriched.append(r_copy)


        if enriched:

            df = pd.DataFrame(enriched)

            rows.append(df)


# 7. Write output

if rows:

    final_df = pd.concat(rows, ignore_index=True)

    # normalize column names

    final_df.columns = [c.replace('.', '_').replace('-', '_').lower() for c in final_df.columns]

    out_file = os.path.join(OUT, f"roster_{yday}.parquet")

    final_df.to_parquet(out_file, index=False)

    print(f"✅ Rosters: Wrote {len(final_df)} rows → {out_file}")

else:

    print(f"⚠️ No Rosters for {yday}")
    
