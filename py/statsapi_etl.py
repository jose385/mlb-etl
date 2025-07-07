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


        # Fetch schedule for the day

        sched = statsapi.schedule(start_date=yday, end_date=yday)

        if sched:

            df_sched = pd.DataFrame(sched)

            for g in df_sched.itertuples():

                game_pk = g.game_id  # or .game_pk depending on your statsapi version


                # Try the correct endpoint

                try:

                    resp = statsapi.get('game_playByPlay', {'gamePk': game_pk})

                except Exception as e:

                    print(f"❌ PBP error for game {game_pk} on {yday}: {e}")

                    continue


                # Extract plays from either top‐level or nested liveData

                plays = (

                    resp.get('allPlays')

                    or resp.get('liveData', {})

                           .get('plays', {})

                           .get('allPlays', [])

                )


                for pl in plays:

                    rows.append(pl)


        if not rows:

            print(f"✅ No StatsAPI plays for {yday}")

        else:

            df = pd.json_normalize(rows)

            out = f"{OUT}/statsapi_{yday}.parquet"

            df.to_parquet(out, index=False)

            print(f"✅ Wrote {len(df)} StatsAPI rows → {out}")


        current += timedelta(days=1)


if __name__ == "__main__":

    p = argparse.ArgumentParser()

    p.add_argument("--start", required=False,
                   
                   help="YYYY-MM-DD (default=yesterday)")
    
    p.add_argument("--end",   required=False,
                   
                   help="YYYY-MM-DD (default=start)")
    
    args = p.parse_args()


    # default to yesterday if no start provided

    if not args.start:

        args.start = (pd.Timestamp.today() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    if not args.end:

        args.end = args.start


    main(args.start, args.end)
    
