#!/usr/bin/env python

"""
backfill.py – Historical backfill for Statcast, StatsAPI PBP, Rosters, and Lineups.

Usage:
    python backfill.py --start YYYY-MM-DD --end YYYY-MM-DD [--monthly] [--output DIR]

Flags:
  --start     YYYY-MM-DD to begin (inclusive)
  --end       YYYY-MM-DD to end   (inclusive)
  --monthly   If set, processes month-chunks instead of day by day
  --output    Output directory (parquet files) [default: $OUTPUT_DIR or “stage”]
"""

import os
import argparse
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from pybaseball import statcast
import statsapi

from tqdm import tqdm


def fetch_statcast_for_date(date_str: str, out_dir: Path):
    out_file = out_dir / f"statcast_{date_str}.parquet"
    if out_file.exists():
        print(f"⏭️  Skipping Statcast for {date_str}")
        return
    df = statcast(start_dt=date_str, end_dt=date_str)
    if df is None or df.empty:
        print(f"✅ No Statcast data for {date_str}")
        return
    df.to_parquet(out_file, index=False)
    print(f"✅ Statcast: Wrote {len(df)} rows → {out_file.name}")


def fetch_statsapi_for_date(date_str: str, out_dir: Path):
    out_file = out_dir / f"statsapi_{date_str}.parquet"
    if out_file.exists():
        print(f"⏭️  Skipping StatsAPI for {date_str}")
        return
    games = statsapi.schedule(start_date=date_str, end_date=date_str) or []
    rows = []
    for g in games:
        pk = g.get("game_id") or g.get("game_pk")
        if not pk:
            continue
        try:
            resp = statsapi.get("game_playByPlay", {"gamePk": pk})
            plays = resp.get("allPlays") or resp.get("liveData", {}) \
                         .get("plays", {}).get("allPlays", [])
            rows.extend(plays)
        except Exception as e:
            print(f"❌ PBP error for game {pk}@{date_str}: {e}")
    if not rows:
        print(f"✅ No PBP data for {date_str}")
        return
    df = pd.json_normalize(rows)
    df.to_parquet(out_file, index=False)
    print(f"✅ StatsAPI: Wrote {len(df)} rows → {out_file.name}")


def fetch_roster_for_date(date_str: str, out_dir: Path):
    out_file = out_dir / f"roster_{date_str}.parquet"
    if out_file.exists():
        print(f"⏭️  Skipping Rosters for {date_str}")
        return
    games = statsapi.schedule(start_date=date_str, end_date=date_str) or []
    rows = []
    for g in games:
        for side, tid in (("home", g["home_id"]), ("away", g["away_id"])):
            try:
                data = statsapi.roster(tid, date=date_str)
                recs = data.get("roster") if isinstance(data, dict) else data or []
                for r in recs:
                    row = dict(r)
                    row.update(team_id=tid, game_date=date_str, side=side)
                    rows.append(row)
            except Exception as e:
                print(f"❌ Roster error for team {tid}@{date_str}: {e}")
    if not rows:
        print(f"✅ No Rosters for {date_str}")
        return
    df = pd.DataFrame(rows)
    # normalize column names
    df.columns = [c.replace(".", "_").replace("-", "_").lower() for c in df.columns]
    df.to_parquet(out_file, index=False)
    print(f"✅ Rosters: Wrote {len(df)} rows → {out_file.name}")


def fetch_lineup_for_date(date_str: str, out_dir: Path):
    out_file = out_dir / f"lineup_{date_str}.parquet"
    if out_file.exists():
        print(f"⏭️  Skipping Lineups for {date_str}")
        return
    games = statsapi.schedule(start_date=date_str, end_date=date_str) or []
    rows = []
    for g in games:
        pk = g.get("game_id") or g.get("game_pk")
        if not pk:
            continue
        try:
            data = statsapi.boxscore_data(pk)
            home = data["liveData"]["boxscore"]["teams"]["home"]["batters"]
            away = data["liveData"]["boxscore"]["teams"]["away"]["batters"]
            for side, batters in (("home", home), ("away", away)):
                for pid in batters:
                    rows.append({
                        "game_pk": pk,
                        "game_date": date_str,
                        "side": side,
                        "player_id": pid
                    })
        except Exception as e:
            msg = getattr(e, "message", e)
            print(f"❌ Lineup error for game {pk}@{date_str}: {msg}")
    if not rows:
        print(f"✅ No Lineups for {date_str}")
        return
    df = pd.DataFrame(rows)
    df.to_parquet(out_file, index=False)
    print(f"✅ Lineups: Wrote {len(df)} rows → {out_file.name}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start",    required=True, help="YYYY-MM-DD")
    p.add_argument("--end",      required=True, help="YYYY-MM-DD")
    p.add_argument("--monthly",  action="store_true", help="Month-at-a-time")
    p.add_argument("--output",   help="Output dir (parquet)")
    args = p.parse_args()

    out_dir = Path(args.output or os.getenv("OUTPUT_DIR", "stage"))
    out_dir.mkdir(parents=True, exist_ok=True)

    sd = datetime.fromisoformat(args.start)
    ed = datetime.fromisoformat(args.end)
    if ed < sd:
        raise ValueError("`end` must be ≥ `start`")

    print(f"🔄 Backfilling from {sd.date()} to {ed.date()}…")
    if args.monthly:
        cur = sd
        while cur <= ed:
            # first of month
            month_end = (cur.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
            if month_end > ed:
                month_end = ed
            rng = f"{cur.date()} → {month_end.date()}"
            print(f"▶️ Chunk: {rng}")
            d = cur
            while d <= month_end:
                ds = d.strftime("%Y-%m-%d")
                fetch_statcast_for_date(ds, out_dir)
                fetch_statsapi_for_date(ds, out_dir)
                fetch_roster_for_date(ds, out_dir)
                fetch_lineup_for_date(ds, out_dir)
                d += timedelta(days=1)
            cur = month_end + timedelta(days=1)
    else:
        d = sd
        while d <= ed:
            ds = d.strftime("%Y-%m-%d")
            fetch_statcast_for_date(ds, out_dir)
            fetch_statsapi_for_date(ds, out_dir)
            fetch_roster_for_date(ds, out_dir)
            fetch_lineup_for_date(ds, out_dir)
            d += timedelta(days=1)

    print("🎉 Backfill complete.")


if __name__ == "__main__":
    main()
