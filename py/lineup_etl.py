#!/usr/bin/env python3
import os
import argparse
import pandas as pd
import statsapi

def fetch_lineup(season, out_dir):
    out_file = f"{out_dir}/lineup_{season}.parquet"
    if os.path.exists(out_file):
        return
    games = statsapi.schedule(season=season, group='Regular Season') or []
    rows = []
    for g in games:
        date_str = g.get("game_date")
        for team, side in ((g["home_id"], "home"), (g["away_id"], "away")):
            data = statsapi.lineup(team, date=date_str)
            recs = data.get("lineup") if isinstance(data, dict) else data or []
            for r in recs:
                if not isinstance(r, dict):
                    continue
                r = r.copy()
                r.update({"team_id": team, "game_date": date_str, "side": side})
                rows.append(r)
    if rows:
        df = pd.DataFrame(rows)
        df.columns = [c.replace(".", "_").replace("-", "_").lower() for c in df.columns]
        df.to_parquet(out_file, index=False)
        print(f"✅ Lineup {season}: {len(df)} rows")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--season", type=int, required=True)
    args = p.parse_args()
    os.makedirs("stage", exist_ok=True)
    fetch_lineup(args.season, "stage")

if __name__ == "__main__":
    main()
