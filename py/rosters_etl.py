#!/usr/bin/env python3
import os
import argparse
from datetime import datetime, timedelta
import pandas as pd
import statsapi

def fetch_roster(date_str, out_dir):
    out_file = f"{out_dir}/roster_{date_str}.parquet"
    if os.path.exists(out_file):
        return
    games = statsapi.schedule(start_date=date_str, end_date=date_str) or []
    rows, seen = [], set()
    for g in games:
        for team, side in ((g["home_id"], "home"), (g["away_id"], "away")):
            if (date_str, team) in seen:
                continue
            seen.add((date_str, team))
            data = statsapi.roster(team, date=date_str)
            recs = data.get("roster") if isinstance(data, dict) else data or []
            for r in recs:
                if not isinstance(r, dict) or r.get("player_id") is None:
                    continue
                r = r.copy()
                r.update({"team_id": team, "game_date": date_str, "side": side})
                rows.append(r)
    if rows:
        df = pd.DataFrame(rows)
        df.columns = [c.replace(".", "_").replace("-", "_").lower() for c in df.columns]
        df.to_parquet(out_file, index=False)
        print(f"✅ Rosters {date_str}: {len(df)} rows")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--date", help="YYYY-MM-DD; default yesterday", required=False)
    args = p.parse_args()
    if args.date:
        ds = args.date
    else:
        ds = (datetime.utcnow().date() - timedelta(days=1)).isoformat()
    os.makedirs("stage", exist_ok=True)
    fetch_roster(ds, "stage")

if __name__ == "__main__":
    main()
