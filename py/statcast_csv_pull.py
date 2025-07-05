#!/usr/bin/env python

"""

Reliably download yesterday’s Statcast CSV (pitcher feed) with retries

and save to Parquet.

"""

import os, io, datetime, pandas as pd, pyarrow.parquet as pq

import requests

from urllib3.util.retry import Retry

from requests.adapters import HTTPAdapter


OUT = os.getenv("OUTPUT_DIR", "stage")

os.makedirs(OUT, exist_ok=True)


yday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

URL  = (

    "https://baseballsavant.mlb.com/statcast_search/csv"

    f"?all=true&player_type=pitcher&game_date_gt={yday}&game_date_lt={yday}"

)


# ---------- Session with back-off ----------

retry = Retry(

    total=5,                    # five tries max

    backoff_factor=5,           # 0-5-10-20-40 s

    status_forcelist=[500, 502, 503, 504],

    allowed_methods=["GET"],

)

session = requests.Session()

session.mount("https://", HTTPAdapter(max_retries=retry))


headers = {

    "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64)",

    "Referer": "https://baseballsavant.mlb.com/",

}


resp = session.get(URL, headers=headers, timeout=180)   # 3-min ceiling

resp.raise_for_status()            # still die noisily if 5 retries fail


df = pd.read_csv(io.StringIO(resp.text))

pq.write_table(df.to_arrow(),
               
               f"{OUT}/statcast_{yday}.parquet")

print(f"✅ wrote {len(df):,} Statcast rows for {yday}")

