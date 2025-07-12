#!/usr/bin/env python3
import os
import pandas as pd
import statsapi
from datetime import datetime
import argparse

# --------------------------------------
# Arguments
# --------------------------------------
parser = argparse.ArgumentParser(description="Pull roster for a given date via StatsAPI")
parser.add_argument("--date", help="YYYY-MM-DD; defaults to yesterday", required=False)
args = parser.parse_args()

# --------------------------------------
# Determine date & season
# --------------------------------------
if args.date:
    yday = args.date
else:
    yday = datetime.today().strftime("%Y-%m-%d")
season = int(yday.split("-")[0])

# --------------------------------------
# Output directory
# --------------------------------------
OUT = os.getenv("OUTPUT_DIR", "stage")
os.makedirs(OUT, exist_ok=True)

# --------------------------------------
# Fetch schedule
# --------------------------------------
schedule = statsapi.schedule(start_date=yday, end_date=yday) or []

rows = []
seen = set()  # <--- initialize seen
for game in schedule:
    game_date = game.get("game_date") or yday
    for team_id, side in ((game["home_id"], "home"), (game["away_id"], "away")):
        if (game_date, team_id) in seen:
            continue
        seen.add((game_date, team_id))
        data = statsapi.roster(team_id=team_id, date=yday)
        # extract roster list
        if isinstance(data, dict) and "roster" in data:
            records = data["roster"]
        elif isinstance(data, list):
            records = data
        else:
            records = []
        if not records:
            continue
        # enrich and collect
        enriched = []
        for r in records:
            if isinstance(r, dict):
                r["team_id"] = team_id
                r["game_date"] = game_date
                r["side"] = side
                enriched.append(r)
        rows.append(pd.DataFrame(enriched))

# write
if rows:
    final_df = pd.concat(rows, ignore_index=True)
    final_df.to_parquet(f"{OUT}/roster_{yday}.parquet", index=False)
    print(f"✅ Wrote {len(final_df)} roster rows → {OUT}/roster_{yday}.parquet")
else:
    print(f"✅ No Rosters for {yday}")