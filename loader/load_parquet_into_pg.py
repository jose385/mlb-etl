#!/usr/bin/env python3
"""
load_parquet_into_pg.py – Load everything in a folder of Parquets into Postgres,  
automatically dropping any DataFrame columns the target table doesn't have.

Usage:
    python load_parquet_into_pg.py [--input-dir DIR] [--tables T1 T2 ...]

Env:
    PG_DSN     Postgres DSN, e.g.: postgresql://user:pw@host:5432/db
"""
import os
import argparse
import io
import time
from pathlib import Path

import pandas as pd
import psycopg2


def connect():
    dsn = os.getenv("PG_DSN")
    if not dsn:
        raise ValueError("PG_DSN must be set to your PostgreSQL DSN")
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    return conn


def get_table_columns(conn, table: str):
    """
    Return a Python list of column names existing in public.<table>.
    """
    sql = """
      SELECT column_name
        FROM information_schema.columns
       WHERE table_schema = 'public'
         AND table_name   = %s
       ORDER BY ordinal_position
    """
    cur = conn.cursor()
    cur.execute(sql, (table,))
    return [row[0] for row in cur.fetchall()]


def load_table(conn, table: str, df: pd.DataFrame):
    # figure out which columns the table actually has
    existing = set(get_table_columns(conn, table))
    # prune any extra keys from the DataFrame
    to_load = [c for c in df.columns if c in existing]
    if not to_load:
        print(f"⚠️  After pruning, no columns remain for table '{table}', skipping")
        return

    buf = io.StringIO()
    df[to_load].to_csv(buf, index=False, header=False)
    buf.seek(0)

    cols_csv = ", ".join(to_load)
    
    # Create a temporary table first
    temp_table = f"temp_{table}_{int(time.time())}"
    copy_sql = f"COPY {temp_table} ({cols_csv}) FROM STDIN WITH (FORMAT CSV)"
    
    cur = conn.cursor()
    
    try:
        # Drop temp table if it exists, then create it
        cur.execute(f"DROP TABLE IF EXISTS {temp_table}")
        cur.execute(f"CREATE TEMP TABLE {temp_table} (LIKE public.{table})")
        
        # Load data into temp table
        cur.copy_expert(copy_sql, buf)
        
        # Insert only new records using ON CONFLICT DO NOTHING
        all_cols = ", ".join(to_load)
        cur.execute(f"""
            INSERT INTO public.{table} ({all_cols})
            SELECT {all_cols} FROM {temp_table}
            ON CONFLICT DO NOTHING
        """)
        
        rows_inserted = cur.rowcount
        print(f"✅ Loaded {rows_inserted} new rows → public.{table} ({len(to_load)} cols)")
        
        # Clean up temp table
        cur.execute(f"DROP TABLE IF EXISTS {temp_table}")
        
    except Exception as e:
        print(f"❌ Error loading {table}: {e}")
        # Clean up on error too
        try:
            cur.execute(f"DROP TABLE IF EXISTS {temp_table}")
        except:
            pass
        conn.rollback()
        raise

#!/usr/bin/env python3
"""
Quick enhanced loader - simple addition to your existing loader
Just add this function to your current load_parquet_into_pg.py
"""

def enhanced_load_table(conn, table: str, df: pd.DataFrame):
    """Enhanced version that handles missing columns automatically"""
    
    # Get existing table columns
    existing = set(get_table_columns(conn, table))
    parquet_cols = set(df.columns)
    
    # Find new columns that don't exist in table
    new_cols = parquet_cols - existing
    
    if new_cols:
        print(f"🆕 Found {len(new_cols)} new columns in {table}")
        
        # Add new columns to table
        cur = conn.cursor()
        for col in sorted(new_cols):
            # Determine type from pandas dtype
            sample_vals = df[col].dropna().head(100)
            
            if df[col].dtype == 'object':
                pg_type = 'TEXT'
            elif 'int' in str(df[col].dtype):
                pg_type = 'BIGINT'
            elif 'float' in str(df[col].dtype):
                pg_type = 'REAL'
            elif 'bool' in str(df[col].dtype):
                pg_type = 'BOOLEAN'
            else:
                pg_type = 'TEXT'
            
            try:
                cur.execute(f"ALTER TABLE public.{table} ADD COLUMN {col} {pg_type}")
                print(f"  ✅ Added column: {col} ({pg_type})")
            except Exception as e:
                print(f"  ⚠️  Could not add column {col}: {e}")

# Add this function to your load_parquet_into_pg.py temporarily
def debug_columns(conn, table: str, df: pd.DataFrame):
    existing = set(get_table_columns(conn, table))
    parquet_cols = set(df.columns)
    
    print(f"\n🔍 DEBUG COLUMN MISMATCH for {table}:")
    print(f"   Table expects: {sorted(existing)}")
    print(f"   Parquet has:   {sorted(parquet_cols)}")
    print(f"   Missing from parquet: {existing - parquet_cols}")
    print(f"   Extra in parquet:     {parquet_cols - existing}")
    
def main():
    p = argparse.ArgumentParser()
    p.add_argument(
        "--input-dir",
        default="stage",
        help="Folder containing your *.parquet files"
    )
    p.add_argument(
        "--tables",
        nargs="*",
        help="Optional subset of basenames to load: e.g. statcast statsapi_playlog roster lineup"
    )
    p.add_argument(
        "--validate",
        action="store_true",
        help="Run data quality validation"
    )
    args = p.parse_args()

    in_dir = Path(args.input_dir)
    files = sorted(in_dir.glob("*.parquet"))
    if not files:
        print(f"❌ No parquet files found in {in_dir}")
        return

    # filter by --tables if provided:
    if args.tables:
        keep = set(args.tables)
        def matches(f):
            stem = f.stem
            base = stem.split("_", 1)[0]
    
            # Map file prefixes to table names
            table_mapping = {
                "weather": "weather",
                "fatigue": "fatigue_metrics", 
                "statsapi": "statsapi_playlog",
                "umpire": "umpires",  # NEW: Handle umpire files
                "umpires": "umpires"  # Handle both singular and plural
            } 
    
            if base in table_mapping:
                tbl = table_mapping[base]
            else:
                tbl = base.rstrip("s")  # Default: remove trailing 's'
    
            return tbl in keep if args.tables else True
            # statsapi files are named statsapi_YYYY-MM-DD.parquet but table is statsapi_playlog
            tbl = "statsapi_playlog" if base == "statsapi" else base.rstrip("s")
            return tbl in keep
        files = [f for f in files if matches(f)]
        if not files:
            print(f"❌ None of the files match --tables {keep}")
            return

    conn = connect()
    for pq in files:
        stem = pq.stem
        base = stem.split("_", 1)[0]
        
        # Enhanced table mapping
        if base == "statsapi":
            table = "statsapi_playlog"
        elif base == "fatigue":
            table = "fatigue_metrics"
        elif base == "weather":
            table = "weather"
        elif base in ["umpire", "umpires"]:  # NEW: Handle umpire files
            table = "umpires"
        else:
            table = base.rstrip("s")
        
        print(f"⏳ {pq.name} → public.{table} …", end=" ")
        df = pd.read_parquet(pq)
        
        # Add debug for new tables
        if table in ["umpires", "statsapi_playlog"]:
            debug_columns(conn, table, df)
        
        enhanced_load_table(conn, table, df)

    conn.close()
    print("🎉 All done!")


if __name__ == "__main__":
    main()