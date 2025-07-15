ha#!/usr/bin/env python

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
        print(f"⏭️ Skipping StatsAPI for {date_str}")
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
            
            for play in plays:
                # Flatten the nested structure to match your table schema
                row = {
                    "game_date": date_str,
                    "game_pk": pk,
                    "at_bat_index": play.get("about", {}).get("atBatIndex"),
                    "event_index": play.get("about", {}).get("playIndex"), 
                    "inning": play.get("about", {}).get("inning"),
                    "half_inning": play.get("about", {}).get("halfInning"),
                    "pitcher": play.get("matchup", {}).get("pitcher", {}).get("id"),
                    "batter": play.get("matchup", {}).get("batter", {}).get("id"),
                    "events": play.get("result", {}).get("event"),
                    "description": play.get("result", {}).get("description"),
                    "count_balls": play.get("count", {}).get("balls"),
                    "count_strikes": play.get("count", {}).get("strikes"),
                    # Add other fields as needed to match your schema
                }
                rows.append(row)
                
        except Exception as e:
            print(f"❌ PBP error for game {pk}@{date_str}: {e}")
    
    if not rows:
        print(f"✅ No PBP data for {date_str}")
        return
    
    df = pd.DataFrame(rows)
    df.to_parquet(out_file, index=False)
    print(f"✅ StatsAPI: Wrote {len(df)} rows → {out_file.name}")


def fetch_roster_for_date(date_str: str, output_dir: str):
    """
    Fetch one day's rosters (home & away) and write to Parquet if new.
    """
    out_file = os.path.join(output_dir, f"roster_{date_str}.parquet")
    if os.path.exists(out_file):
        print(f"⏭️ Skipping Rosters for {date_str} (already exists)")
        return

    games = statsapi.schedule(start_date=date_str, end_date=date_str) or []
    rows = []
    for g in games:
        game_pk = g.get("game_id") or g.get("game_pk")
        season = datetime.fromisoformat(date_str).year
        for team_id, side in ((g["home_id"], "home"), (g["away_id"], "away")):
            try:
                data = statsapi.roster(team_id, date=date_str)
            except Exception as e:
                print(f"❌ Roster error for team {team_id}@{date_str}: {e}")
                continue

            # pull out the actual roster list
            if isinstance(data, dict) and "roster" in data:
                records = data["roster"]
            elif isinstance(data, list):
                records = data
            else:
                records = []

            # filter and normalize
            for r in records:
                if not isinstance(r, dict):
                    # skip anything that isn't a dict
                    print(f"⚠️ Skipping malformed roster record for team {team_id}@{date_str}: {r!r}")
                    continue

                row = dict(r)
                row.update({
                    "team_id": team_id,
                    "game_date": date_str,
                    "side": side
                })
                rows.append(row)

    if not rows:
        print(f"✅ No Rosters for {date_str}")
        return

    df = pd.DataFrame(rows)
    # normalize column names: no dots, no hyphens, lowercase
    df.columns = [c.replace(".", "_").replace("-", "_").lower() for c in df.columns]
    df.to_parquet(out_file, index=False)
    print(f"✅ Rosters: Wrote {len(df)} rows → {out_file}")



def fetch_lineup_for_date(date_str: str, output_dir: str):
    out_file = os.path.join(output_dir, f"lineup_{date_str}.parquet")
    if os.path.exists(out_file):
        print(f"⏭️ Skipping Lineups for {date_str} (already exists)")
        return

    games = statsapi.schedule(start_date=date_str, end_date=date_str) or []
    rows = []
    for g in games:
        game_pk = g.get("game_id") or g.get("game_pk")
        if not game_pk:
            continue
        try:
            data = statsapi.get("game_boxscore", {"gamePk": game_pk})
            teams = data.get("teams", {})
            for side in ("home", "away"):
                team_info = teams.get(side, {})
                team_id = g.get("home_id" if side == "home" else "away_id")
                batters = team_info.get("batters", [])
                for batting_order, person_id in enumerate(batters, start=1):
                    rows.append({
                        "game_date": date_str,
                        "game_pk": game_pk,
                        "team_id": team_id,           # ✅ Added
                        "batting_order": batting_order, # ✅ Fixed name
                        "person_id": person_id,        # ✅ Fixed name
                        "position_code": None,         # ✅ Added (you'll need to get this from API)
                        "side": side,                  # ✅ Fixed name
                    })
        except Exception as e:
            print(f"❌ Lineup error for game {game_pk}@{date_str}: {e}")
            continue

    if not rows:
        print(f"✅ No Lineups for {date_str}")
        return

    df = pd.DataFrame(rows)
    df.to_parquet(out_file, index=False)
    print(f"✅ Lineups: Wrote {len(df)} rows → {out_file}")



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
