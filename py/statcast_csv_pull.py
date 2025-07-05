#!/usr/bin/env python  

"""
Download yesterday’s Statcast CSV (pitcher feed has *all* pitches) and write Parquet.  

"""
import os, datetime, pandas as pd, pyarrow as pa, pyarrow.parquet as pq  


out = os.getenv("OUTPUT_DIR", "stage")  

os.makedirs(out, exist_ok=True)  


yday = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")  

url  = (  

    "https://baseballsavant.mlb.com/statcast_search/csv"  

    f"?all=true&player_type=pitcher&game_date_gt={yday}&game_date_lt={yday}"  

)  


df = pd.read_csv(url)                                   # raw Savant feed, any # columns is OK :contentReference[oaicite:0]{index=0}

pq.write_table(pa.Table.from_pandas(df), f"{out}/statcast_{yday}.parquet")  

print(f"✅ wrote {len(df):,} Statcast rows for {yday}")  

