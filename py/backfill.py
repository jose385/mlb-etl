#!/usr/bin/env python3
"""
backfill.py – Historical backfill for Statcast, PBP & Rosters (and optionally
starting Lineups).  Supports daily or monthly chunks for quick testing.
"""

import os
import argparse
from datetime import datetime, timedelta

import pandas as pd
from pybaseball import statcast
import statsapi   # note: `mlb_statsapi`, not plain statsapi

def fetch_statcast_for_date(date_str, out_dir):
    out = os.path.join(out_dir, f"statcast_{date_str}.parquet")
    if os.path.exists(out):
        print(f"⏭️  Skipping Statcast for {date_str}")
        return
    df = statcast(start_dt=date_str, end_dt=date_str)
    if df is None or df.empty:
        print(f"✅ No Statcast data for {date_str}")
        return
    df.to_parquet(out, index=False)
    print(f"✅ Statcast: Wrote {len(df)} rows → {out}")

def fetch_statsapi_for_date(date_str, out_dir):
    out = os.path.join(out_dir, f"statsapi_{date_str}.parquet")
    if os.path.exists(out):
        print(f"⏭️  Skipping PBP for {date_str}")
        return
    games = statsapi.schedule(start_date=date_str, end_date=date_str) or []
    rows = []
    for g in games:
        pk = g.get("game_pk") or g.get("game_id")
        if not pk:
            continue
        try:
            resp = statsapi.get("game_playByPlay", {"gamePk": pk})
            plays = (resp.get("allPlays")
                     or resp.get("liveData", {}).get("plays", {}).get("allPlays", []))
            rows += plays
        except Exception as e:
            print(f"❌ PBP error {pk}@{date_str}: {e}")
    if not rows:
        print(f"✅ No PBP data for {date_str}")
        return
    df = pd.json_normalize(rows)
    df.to_parquet(out, index=False)
    print(f"✅ StatsAPI: Wrote {len(df)} rows → {out}")

def fetch_roster_for_date(date_str, out_dir):
    out = os.path.join(out_dir, f"roster_{date_str}.parquet")
    if os.path.exists(out):
        print(f"⏭️  Skipping Rosters for {date_str}")
        return
    games = statsapi.schedule(start_date=date_str, end_date=date_str) or []
    recs = []
    for g in games:
        for side, tid in (("home", g["home_id"]), ("away", g["away_id"])):
            try:
                data = statsapi.get("team_roster", {"teamId": tid, "date": date_str})
                players = data.get("roster") or data
                for r in players:
                    row = dict(r)
                    row.update(team_id=tid, game_date=date_str, side=side)
                    recs.append(row)
            except Exception:
                continue
    if not recs:
        print(f"✅ No Rosters for {date_str}")
        return
    df = pd.DataFrame(recs)
    df.columns = [c.replace(".", "_").replace("-", "_").lower() for c in df.columns]
    df.to_parquet(out, index=False)
    print(f"✅ Rosters: Wrote {len(df)} rows → {out}")

def fetch_lineup_for_date(date_str, out_dir):
    out = os.path.join(out_dir, f"lineup_{date_str}.parquet")
    if os.path.exists(out):
        print(f"⏭️  Skipping Lineups for {date_str}")
        return
    games = statsapi.schedule(start_date=date_str, end_date=date_str) or []
    rows = []
    for g in games:
        pk = g.get("game_pk") or g.get("game_id")
        if not pk:
            continue
        try:
            box = statsapi.boxscore_data(pk)
            for side in ("home", "away"):
                for pid in box["liveData"]["boxscore"]["teams"][side]["batters"]:
                    info = box["liveData"]["boxscore"]["teams"][side]["players"][f"ID{pid}"]
                    info.update(game_pk=pk, date=date_str, side=side)
                    rows.append(info)
        except Exception:
            print(f"❌ Lineup error for game {pk}@{date_str}")
    if not rows:
        print(f"✅ No Lineups for {date_str}")
        return
    df = pd.json_normalize(rows)
    df.to_parquet(out, index=False)
    print(f"✅ Lineups: Wrote {len(df)} rows → {out}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end",   required=True, help="YYYY-MM-DD")
    parser.add_argument("--monthly", action="store_true",
                        help="Process in month-long chunks instead of daily")
    args = parser.parse_args()

    out_dir = os.getenv("OUTPUT_DIR", "stage")
    os.makedirs(out_dir, exist_ok=True)

    sd = datetime.fromisoformat(args.start)
    ed = datetime.fromisoformat(args.end)
    cur = sd
    step = timedelta(days=1)

    if args.monthly:
        # jump by calendar months
        def add_month(d):
            m = d.month + 1 if d.month < 12 else 1
            y = d.year + (1 if d.month == 12 else 0)
            return d.replace(year=y, month=m)
        step = None

    while cur <= ed:
        nxt = add_month(cur) if args.monthly else cur + step
        chunk_end = min(nxt - (timedelta(days=1) if args.monthly else timedelta()), ed)
        ds, de = cur.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")
        print(f"▶️  Chunk: {ds} → {de}")
        d = cur
        while d <= chunk_end:
            s = d.strftime("%Y-%m-%d")
            fetch_statcast_for_date(s, out_dir)
            fetch_statsapi_for_date(s, out_dir)
            fetch_roster_for_date(s, out_dir)
            fetch_lineup_for_date(s, out_dir)
            d += timedelta(days=1)
        cur = nxt if args.monthly else cur + step

    print("🎉 Backfill complete.")

if __name__ == "__main__":
    main()
