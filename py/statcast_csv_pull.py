#!/usr/bin/env python

"""Pull yesterday’s full Statcast feed via pybaseball and write Parquet."""

import os, datetime, pyarrow.parquet as pq

from pybaseball import statcast                      # wrapper docs :contentReference[oaicite:2]{index=2}


OUT = os.getenv("OUTPUT_DIR", "stage")

os.makedirs(OUT, exist_ok=True)

yday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")


df = statcast(start_dt=yday, end_dt=yday)            # one call, all pitches :contentReference[oaicite:3]{index=3}

pq.write_table(df.to_arrow(), f"{OUT}/statcast_{yday}.parquet")

print(f"✅ wrote {len(df):,} Statcast rows for {yday}")

