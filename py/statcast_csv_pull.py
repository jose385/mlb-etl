#!/usr/bin/env python

"""Pull yesterday’s full Statcast feed via pybaseball and write Parquet."""

import os, datetime, pyarrow.parquet as pq

from pybaseball import statcast                      # wrapper docs :contentReference[oaicite:2]{index=2}


OUT = os.getenv("OUTPUT_DIR", "stage")

os.makedirs(OUT, exist_ok=True)

yday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")


df = statcast(start_dt=yday, end_dt=yday)            # one call, all pitches :contentReference[oaicite:3]{index=3}

# NEW (works everywhere)

# after you’ve fetched df via pybaseball.statcast(...)

if df.empty:

    print(f"✅ No Statcast data for {yday}")

else:

    out_path = f"{OUT}/statcast_{yday}.parquet"

    df.to_parquet(out_path, index=False)

    print(f"✅ Wrote {len(df)} Statcast rows for {yday} → {out_path}")
    


print(f"✅ wrote {len(df):,} Statcast rows for {yday}")

