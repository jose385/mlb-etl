#!/usr/bin/env python

import os

import argparse

import pandas as pd

import statsapi

from datetime import datetime, timedelta


def main(start_date: str, end_date: str):

    OUT = os.getenv("OUTPUT_DIR", "stage")

    os.makedirs(OUT, exist_ok=True)


    start = datetime.fromisoformat(start_date)

    end   = datetime.fromisoformat(end_date)

    current = start


    while current <= end:

        yday = current.strftime("%Y-%m-%d")

        rows = []


        # statsapi.schedule returns a list of dicts

        sched = statsapi.schedule(start_date=yday, end_date=yday)

        if sched:

            df_sched = pd.DataFrame(sched)

            for g in df_sched.itertuples():

                game_pk = g.game_id  # or g.game_pk depending on your statsapi version

                try:

                    pbp = statsapi.get('playByPlay', {'gamePk': game_pk})

                    plays = pbp.get('allPlays', [])

                except Exception as e:

                    print(f"❌ PBP error for game {game_pk} on {yday}: {e}")

                    continue


                for pl in plays:

                    rows.append(pl)


        if not rows:

            print(f"✅ No StatsAPI plays for {yday}")

        else:

            df = pd.json_normalize(rows)

            path = f"{OUT}/statsapi_{yday}.parquet"

            df.to_parquet(path, index=False)

            print(f"✅ Wrote {len(df)} StatsAPI rows → {path}")


        current += timedelta(days=1)


if __name__ == "__main__":

    p = argparse.ArgumentParser()

    p.add_argument("--start", required=False,
                   
                   help="YYYY-MM-DD (default=yesterday)")
    
    p.add_argument("--end",   required=False,
                   
                   help="YYYY-MM-DD (default=start)")
    
    args = p.parse_args()


    if not args.start:

        args.start = (pd.Timestamp.today() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    if not args.end:

        args.end = args.start


    main(args.start, args.end)
    
