#!/usr/bin/env python

"""Pull yesterday’s games from MLB-StatsAPI, flatten pitches, write Parquet."""

import os, datetime, statsapi, pandas as pd


OUT = os.getenv("OUTPUT_DIR", "stage")

os.makedirs(OUT, exist_ok=True)


yday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

game_ids = [g["game_id"] for g in statsapi.schedule(start_date=yday, end_date=yday)]


rows = []

for gid in game_ids:

    live = statsapi.get("game", {"gamePk": gid})

    rows.extend(live["liveData"]["plays"]["allPlays"])


pd.json_normalize(rows).to_parquet(f"{OUT}/statsapi_{yday}.parquet", index=False)

print(f"✅ wrote {len(rows)} plays for {yday}")

