#!/usr/bin/env python3
"""
backfill.py – Historical backfill for MLB Statcast, StatsAPI PBP, Rosters & Lineups.

Usage:
    # Day-by-day (default):
    python py/backfill.py --start 2021-04-01 --end 2021-05-03

    # Monthly chunks:
    python py/backfill.py --start 2021-04-01 --end 2021-12-01 --mode monthly
"""

import os
import argparse
from datetime import datetime, timedelta
import pandas as pd

from pybaseball import statcast
import statsapi

def fetch_statcast_for_date(date_str, output_dir):
    out = os.path.join(output_dir, f"statcast_{date_str}.parquet")
    if os.path.exists(out):
        print(f"⏭️ Skipping Statcast for {date_str}")
        return
    df = statcast(start_dt=date_str, end_dt=date_str)
    if df is None or df.empty:
        print(f"✅ No Statcast for {date_str}")
    else:
        df.to_parquet(out, index=False)
        print(f"✅ Statcast: Wrote {len(df)} → {out}")

def fetch_statsapi_for_date(date_str, output_dir):
    out = os.path.join(output_dir, f"statsapi_{date_str}.parquet")
    if os.path.exists(out):
        print(f"⏭️ Skipping PBP for {date_str}")
        return
    games = statsapi.schedule(start_date=date_str, end_date=date_str) or []
    rows = []
    for g in games:
        pk = g.get("game_pk") or g.get("game_id")
        if not pk:
            continue
        try:
            resp = statsapi.get("game_playByPlay", {"gamePk": pk})
            plays = resp.get("allPlays") or resp.get("liveData", {}) \
                        .get("plays", {}).get("allPlays", [])
            rows.extend(plays)
        except Exception as e:
            print(f"❌ PBP error {pk}@{date_str}: {e}")
    if not rows:
        print(f"✅ No PBP for {date_str}")
    else:
        pd.json_normalize(rows).to_parquet(out, index=False)
        print(f"✅ PBP: Wrote {len(rows)} → {out}")

def fetch_roster_for_date(date_str, output_dir):
    out = os.path.join(output_dir, f"roster_{date_str}.parquet")
    if os.path.exists(out):
        print(f"⏭️ Skipping Rosters for {date_str}")
        return
    games = statsapi.schedule(start_date=date_str, end_date=date_str) or []
    rows = []
    for g in games:
        for team_id, side in ((g["home_id"], "home"), (g["away_id"], "away")):
            data = statsapi.roster(team_id, date=date_str)
            recs = data.get("roster") if isinstance(data, dict) else (data if isinstance(data, list) else [])
            for r in recs:
                if isinstance(r, dict):
                    r2 = dict(r)
                    r2.update(team_id=team_id, game_date=date_str, side=side)
                    rows.append(r2)
    if not rows:
        print(f"✅ No Rosters for {date_str}")
    else:
        df = pd.DataFrame(rows)
        df.columns = [c.replace(".", "_").replace("-", "_").lower() for c in df.columns]
        df.to_parquet(out, index=False)
        print(f"✅ Rosters: Wrote {len(df)} → {out}")

def fetch_lineup_for_date(date_str, output_dir):
    out = os.path.join(output_dir, f"lineup_{date_str}.parquet")
    if os.path.exists(out):
        print(f"⏭️ Skipping Lineup for {date_str}")
        return
    games = statsapi.schedule(start_date=date_str, end_date=date_str) or []
    rows = []
    for g in games:
        pk = g.get("game_pk") or g.get("game_id")
        if not pk:
            continue
        try:
            data = statsapi.get("game_liveFeed", {"gamePk": pk})
            # sometimes liveData missing until after first pitch
            live = data.get("liveData", {})
            home = live.get("boxscore", {}).get("teams", {}).get("home", {}).get("batters", [])
            away = live.get("boxscore", {}).get("teams", {}).get("away", {}).get("batters", [])
            for side, batters in (("home", home), ("away", away)):
                for pos, pid in enumerate(batters, 1):
                    rows.append({"game_pk": pk, "game_date": date_str,
                                 "team_side": side, "bat_order": pos, "player_id": pid})
        except Exception as e:
            print(f"❌ Lineup error for {pk}@{date_str}: {e}")
    if not rows:
        print(f"✅ No Lineups for {date_str}")
    else:
        df = pd.DataFrame(rows)
        df.to_parquet(out, index=False)
        print(f"✅ Lineups: Wrote {len(df)} → {out}")

def iterate_dates(start_dt, end_dt, mode):
    if mode == "daily":
        cur = start_dt
        while cur <= end_dt:
            yield cur, cur
            cur += timedelta(days=1)
    else:  # monthly
        cur = start_dt.replace(day=1)
        while cur <= end_dt:
            next_month = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
            yield cur, min(next_month - timedelta(days=1), end_dt)
            cur = next_month

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start",  default="2021-04-01")
    p.add_argument("--end",    default=datetime.today().strftime("%Y-%m-%d"))
    p.add_argument("--mode", choices=("daily","monthly"), default="daily",
                   help="daily (default) or monthly backfill chunks")
    p.add_argument("--output", default="stage", help="Parquet output dir")
    args = p.parse_args()

    sd = datetime.fromisoformat(args.start)
    ed = datetime.fromisoformat(args.end)
    if ed < sd:
        raise ValueError("end must be ≥ start")

    os.makedirs(args.output, exist_ok=True)
    print(f"🔄 Backfilling {args.mode} from {sd.date()} → {ed.date()}…")

    for chunk_start, chunk_end in iterate_dates(sd, ed, args.mode):
        ds = chunk_start.strftime("%Y-%m-%d")
        de = chunk_end.strftime("%Y-%m-%d")
        print(f"\n▶️  Chunk: {ds} to {de}")
        # if daily mode these are the same; if monthly they span the calendar month
        cur = chunk_start
        while cur <= chunk_end:
            day = cur.strftime("%Y-%m-%d")
            fetch_statcast_for_date(day,  args.output)
            fetch_statsapi_for_date(day,  args.output)
            fetch_roster_for_date(day,    args.output)
            fetch_lineup_for_date(day,    args.output)
            cur += timedelta(days=1)

    print("🎉 Backfill complete.")

if __name__ == "__main__":
    main()
