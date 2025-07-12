#!/usr/bin/env python

import os
import argparse
import statsapi
import pandas as pd

def fetch_lineup_for_season(season: int, output_dir: str):
    out_file = os.path.join(output_dir, f"lineup_{season}.parquet")
    games = statsapi.schedule(season=season, group='Regular Season') or []
    rows = []

    for g in games:
        date_str = g.get("game_date")
        for team_id, side in ((g["home_id"], "home"), (g["away_id"], "away")):
            try:
                data = statsapi.lineup(team_id, date=date_str)
            except Exception:
                rows = rows  # skip on error
                continue
            if isinstance(data, dict):
                recs = data.get("lineup", [])
            elif isinstance(data, list):
                recs = data
            else:
                continue
            for r in recs:
                if not isinstance(r, dict):
                    continue
                row = dict(r)
                row.update({"team_id": team_id, "game_date": date_str, "side": side})
                rows.append(row)

    if not rows:
        print(f"✅ No Lineup data for season {season}")
        return

    df = pd.DataFrame(rows)
    df.to_parquet(out_file, index=False)
    print(f"✅ Lineup: Wrote {len(df)} rows → {out_file}")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--season", type=int, required=True)
    args = p.parse_args()

    os.makedirs("stage", exist_ok=True)
    fetch_lineup_for_season(args.season, "stage")

if __name__ == "__main__":
    main()
