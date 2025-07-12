#!/usr/bin/env python

import os
import argparse
from datetime import datetime, timedelta
import pandas as pd
import statsapi

def fetch_roster_for_date(date_str: str, output_dir: str):
    out_file = os.path.join(output_dir, f"roster_{date_str}.parquet")
    if os.path.exists(out_file):
        print(f"⏭️ Skipping Rosters for {date_str} (already exists)")
        return

    games = statsapi.schedule(start_date=date_str, end_date=date_str) or []
    rows = []
    seen = set()

    for g in games:
        for team_id, side in ((g["home_id"], "home"), (g["away_id"], "away")):
            if (date_str, team_id) in seen:
                continue
            seen.add((date_str, team_id))

            data = statsapi.roster(team_id, date=date_str)
            records = []
            if isinstance(data, dict) and "roster" in data:
                records = data["roster"]
            elif isinstance(data, list):
                records = data

            for r in records:
                if not isinstance(r, dict):
                    continue
                if r.get("player_id") is None:
                    continue
                row = dict(r)
                row["team_id"] = team_id
                row["game_date"] = date_str
                row["side"] = side
                rows.append(row)

    if not rows:
        print(f"✅ No Rosters for {date_str}")
        return

    df = pd.DataFrame(rows)
    df.columns = [c.replace(".", "_").replace("-", "_").lower() for c in df.columns]
    df.to_parquet(out_file, index=False)
    print(f"✅ Rosters: Wrote {len(df)} rows → {out_file}")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start", required=False)
    p.add_argument("--end", required=False)
    args = p.parse_args()

    start = args.start or datetime.today().strftime("%Y-%m-%d")
    end   = args.end   or start
    sd = datetime.fromisoformat(start)
    ed = datetime.fromisoformat(end)

    os.makedirs("stage", exist_ok=True)
    cur = sd
    while cur <= ed:
        ds = cur.strftime("%Y-%m-%d")
        fetch_roster_for_date(ds, "stage")
        cur += timedelta(days=1)

if __name__ == "__main__":
    main()
