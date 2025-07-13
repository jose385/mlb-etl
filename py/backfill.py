#!/usr/bin/env python

"""
backfill.py – Historical backfill for MLB Statcast, StatsAPI PBP, and Rosters.

Usage:

  # daily (original behavior):
  python py/backfill.py --start 2021-04-01 --end 2021-10-03

  # monthly (monthly Statcast + daily PBP/roster inside each month):
  python py/backfill.py --start 2021-04-01 --end 2021-10-03 --monthly
"""

import os
import argparse
from datetime import datetime, timedelta

import pandas as pd
from pybaseball import statcast
import statsapi


def fetch_statcast_for_date(date_str: str, output_dir: str):
    out_file = os.path.join(output_dir, f"statcast_{date_str}.parquet")
    if os.path.exists(out_file):
        print(f"⏭️ Skipping Statcast for {date_str} (already exists)")
        return

    df = statcast(start_dt=date_str, end_dt=date_str)
    if df is None or df.empty:
        print(f"✅ No Statcast data for {date_str}")
    else:
        df.to_parquet(out_file, index=False)
        print(f"✅ Statcast: Wrote {len(df)} rows → {out_file}")


def fetch_statsapi_for_date(date_str: str, output_dir: str):
    out_file = os.path.join(output_dir, f"statsapi_{date_str}.parquet")
    if os.path.exists(out_file):
        print(f"⏭️ Skipping StatsAPI PBP for {date_str} (already exists)")
        return

    games = statsapi.schedule(start_date=date_str, end_date=date_str) or []
    rows = []
    for g in games:
        game_pk = g.get("game_id") or g.get("game_pk")
        if not game_pk:
            continue
        try:
            resp = statsapi.get("game_playByPlay", {"gamePk": game_pk})
        except Exception as e:
            print(f"❌ PBP error for game {game_pk} on {date_str}: {e}")
            continue
        plays = (
            resp.get("allPlays")
            or resp.get("liveData", {}).get("plays", {}).get("allPlays", [])
        )
        rows.extend(plays)

    if not rows:
        print(f"✅ No PBP data for {date_str}")
    else:
        df = pd.json_normalize(rows)
        df.to_parquet(out_file, index=False)
        print(f"✅ StatsAPI: Wrote {len(df)} rows → {out_file}")


def fetch_roster_for_date(date_str: str, output_dir: str):
    out_file = os.path.join(output_dir, f"roster_{date_str}.parquet")
    if os.path.exists(out_file):
        print(f"⏭️ Skipping Rosters for {date_str} (already exists)")
        return

    games = statsapi.schedule(start_date=date_str, end_date=date_str) or []
    rows = []
    season = datetime.fromisoformat(date_str).year

    for g in games:
        for team_id, side in ((g["home_id"], "home"), (g["away_id"], "away")):
            data = statsapi.roster(team_id, date=date_str)
            if isinstance(data, dict) and "roster" in data:
                records = data["roster"]
            elif isinstance(data, list):
                records = data
            else:
                records = []

            for r in records:
                if isinstance(r, dict):
                    row = dict(r)
                    row["team_id"]   = team_id
                    row["game_date"] = date_str
                    row["side"]      = side
                    rows.append(row)
                # else: skip non-dicts silently

    if not rows:
        print(f"✅ No Rosters for {date_str}")
    else:
        df = pd.DataFrame(rows)
        df.columns = [c.replace(".", "_").replace("-", "_").lower() for c in df.columns]
        df.to_parquet(out_file, index=False)
        print(f"✅ Rosters: Wrote {len(df)} rows → {out_file}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--start",   help="YYYY-MM-DD (default=2021-04-01)", required=False)
    p.add_argument("--end",     help="YYYY-MM-DD (default=today)",      required=False)
    p.add_argument(
        "--monthly",
        help="Fetch Statcast in monthly batches (one file per month) + daily PBP/roster",
        action="store_true"
    )
    args = p.parse_args()

    start   = args.start or "2021-04-01"
    end     = args.end   or datetime.today().strftime("%Y-%m-%d")
    monthly = args.monthly

    sd = datetime.fromisoformat(start)
    ed = datetime.fromisoformat(end)
    if ed < sd:
        raise ValueError("End date must be on or after start date")

    out_dir = os.getenv("OUTPUT_DIR", "stage")
    os.makedirs(out_dir, exist_ok=True)

    print(f"🔄 Backfilling from {sd.date()} to {ed.date()}",
          "(monthly mode)" if monthly else "(daily mode)")

    if not monthly:
        # daily loop
        cur = sd
        while cur <= ed:
            ds = cur.strftime("%Y-%m-%d")
            fetch_statcast_for_date(ds, out_dir)
            fetch_statsapi_for_date(ds, out_dir)
            fetch_roster_for_date(ds, out_dir)
            cur += timedelta(days=1)

    else:
        # month-by-month loop
        cur = sd.replace(day=1)
        while cur <= ed:
            # end of this calendar month
            nxt = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
            month_end = min(nxt - timedelta(days=1), ed)

            start_str = cur.strftime("%Y-%m-%d")
            end_str   = month_end.strftime("%Y-%m-%d")
            print(f"\n📦 Backfilling month {start_str} → {end_str}")

            # one big Statcast pull
            try:
                df_sc = statcast(start_dt=start_str, end_dt=end_str)
                if df_sc is None or df_sc.empty:
                    print(f"✅ No Statcast data for {start_str}–{end_str}")
                else:
                    out_file = os.path.join(out_dir, f"statcast_{start_str}_to_{end_str}.parquet")
                    df_sc.to_parquet(out_file, index=False)
                    print(f"✅ Statcast: Wrote {len(df_sc)} rows → {out_file}")
            except Exception as e:
                print(f"❌ Statcast error for {start_str}–{end_str}: {e}")

            # inside each month: daily PBP + roster
            md = cur
            while md <= month_end:
                ds = md.strftime("%Y-%m-%d")
                fetch_statsapi_for_date(ds, out_dir)
                fetch_roster_for_date(ds, out_dir)
                md += timedelta(days=1)

            cur = nxt

    print("🎉 Backfill complete.")


if __name__ == "__main__":
    main()
