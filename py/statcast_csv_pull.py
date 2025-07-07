#!/usr/bin/env python

import os

import argparse

import pandas as pd


from pybaseball import statcast

from datetime import datetime, timedelta


def main(start_date: str, end_date: str):

    OUT = os.getenv("OUTPUT_DIR", "stage")

    os.makedirs(OUT, exist_ok=True)


    start = datetime.fromisoformat(start_date)

    end   = datetime.fromisoformat(end_date)

    current = start


    while current <= end:

        yday = current.strftime("%Y-%m-%d")

        try:

            df = statcast(start_dt=yday, end_dt=yday)
            
        except Exception as e:

            print(f"❌ Statcast error for {yday}: {e}")

            current += timedelta(days=1)

            continue


        if df.empty:

            print(f"✅ No Statcast data for {yday}")

        else:

            path = f"{OUT}/statcast_{yday}.parquet"

            df.to_parquet(path, index=False)

            print(f"✅ Wrote {len(df)} Statcast rows → {path}")


        current += timedelta(days=1)


if __name__ == "__main__":

    p = argparse.ArgumentParser()

    p.add_argument("--start", required=False,
                   
                   help="YYYY-MM-DD (default=yesterday)")
    
    p.add_argument("--end",   required=False,
                   
                   help="YYYY-MM-DD (default=start)")
    
    args = p.parse_args()


    # defaults

    if not args.start:

        args.start = (pd.Timestamp.today() - pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    if not args.end:

        args.end = args.start


    main(args.start, args.end)
    
