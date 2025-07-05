#!/usr/bin/env python

"""

Download yesterday’s Statcast CSV with a browser-style header and save Parquet.

"""

import os, io, datetime, requests, pandas as pd, pyarrow.parquet as pq


OUT = os.getenv("OUTPUT_DIR", "stage")

os.makedirs(OUT, exist_ok=True)


yday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

url  = (

    "https://baseballsavant.mlb.com/statcast_search/csv"

    f"?all=true&player_type=pitcher&game_date_gt={yday}&game_date_lt={yday}"

)


headers = {

    # any modern UA string works — we just need *something*

    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64)",

    "Referer":    "https://baseballsavant.mlb.com/",

}


resp = requests.get(url, headers=headers, timeout=180)

resp.raise_for_status()               # will raise if Savant still unhappy


df = pd.read_csv(io.StringIO(resp.text))

pq.write_table(df.to_arrow(), f"{OUT}/statcast_{yday}.parquet")

print(f"✅ wrote {len(df):,} Statcast rows for {yday}")

