#!/usr/bin/env python3
"""
backfill.py – Historical backfill for MLB Statcast, StatsAPI PBP, and Rosters.
Writes daily Parquet files into ./stage for statcast_, statsapi_, and roster_.
"""

import os
import argparse
from datetime import datetime, timedelta
import pandas as pd
from pybaseball import statcast
import statsapi

def fetch_statcast_for_date(date_str, out_dir):
    out_file = f"{out_dir}/statcast_{date_str}.parquet"
    if os.path.exists(out_file):
        print(f"⏭️  Skipping statcast {date_str}")
        return
    df = statcast(start_dt=date_str, end_dt=date_str)
    if df is None or df.empty:
        print(f"✅  No Statcast data for {date_str}")
        return
    df.to_parquet(out_file, index=False)
    print(f"✅  Statcast: wrote {len(df)} rows → {out_file}")

def fetch_statsapi_for_date(date_str, out_dir):
    out_file = f"{out_dir}/statsapi_{date_str}.parquet"
    if os.path.exists(out_file):
        print(f"⏭️  Skipping statsapi {date_str}")
        return
    games = statsapi.schedule(start_date=date_str, end_date=date_str) or []
    rows = []
    for g in games:
        pk = g.get("game_id") or g.get("game_pk")
        if not pk:
            continue
        try:
            resp = statsapi.get("game_playByPlay", {"gamePk": pk})
        except Exception as e:
            print(f"❌  Error PBP {pk} on {date_str}: {e}")
            continue
        plays = resp.get("allPlays") or resp.get("liveData", {}).get("plays", {}).get("allPlays", [])
        rows.extend(plays)
    if not rows:
        print(f"✅  No PBP data for {date_str}")
        return
    df = pd.json_normalize(rows)
    df.to_parquet(out_file, index=False)
    print(f"✅  StatsAPI: wrote {len(df)} rows → {out_file}")

def fetch_roster_for_date(date_str, out_dir):
    out_file = f"{out_dir}/roster_{date_str}.parquet"
    if os.path.exists(out_file):
        print(f"⏭️  Skipping roster {date_str}")
        return
    games = statsapi.schedule(start_date=date_str, end_date=date_str) or []
    rows = []
    seen = set()
    for g in games:
        for team_id, side in ((g["home_id"], "home"), (g["away_id"], "away")):
            if (date_str, team_id) in seen:
                continue
            seen.add((date_str, team_id))
            try:
                data = statsapi.roster(team_id, date=date_str)
            except Exception:
                continue
            records = data.get("roster") if isinstance(data, dict) else data or []
            for r in records:
                if not isinstance(r, dict) or r.get("player_id") is None:
                    continue
                r["team_id"] = team_id
                r["game_date"] = date_str
                r["side"] = side
                rows.append(r)
    if not rows:
        print(f"✅  No roster data for {date_str}")
        return
    df = pd.DataFrame(rows)
    df.columns = [c.replace(".", "_").replace("-", "_").lower() for c in df.columns]
    df.to_parquet(out_file, index=False)
    print(f"✅  Rosters: wrote {len(df)} rows → {out_file}")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start", help="YYYY-MM-DD (default 2021-04-01)", required=False)
    p.add_argument("--end",   help="YYYY-MM-DD (default today)", required=False)
    args = p.parse_args()
    start = args.start or "2021-04-01"
    end   = args.end   or datetime.today().strftime("%Y-%m-%d")
    sd = datetime.fromisoformat(start)
    ed = datetime.fromisoformat(end)
    if ed < sd:
        raise ValueError("End must be ≥ start")
    out_dir = os.getenv("OUTPUT_DIR", "stage")
    os.makedirs(out_dir, exist_ok=True)
    print(f"🔄 Backfilling {sd.date()} → {ed.date()}…")
    cur = sd
    while cur <= ed:
        ds = cur.strftime("%Y-%m-%d")
        fetch_statcast_for_date(ds, out_dir)
        fetch_statsapi_for_date(ds, out_dir)
        fetch_roster_for_date(ds, out_dir)
        cur += timedelta(days=1)
    print("🎉 Backfill complete.")

if __name__ == "__main__":
    main()
