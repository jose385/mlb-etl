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
from functools import wraps

import pandas as pd
import psycopg2


def retry_database_operation(max_retries=3, delay=1):
    """Decorator to retry database operations with exponential backoff"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (psycopg2.Error, psycopg2.OperationalError, psycopg2.DatabaseError) as e:
                    last_exception = e
                    if attempt == max_retries - 1:
                        print(f"❌ Database operation failed after {max_retries} attempts: {e}")
                        raise
                    
                    wait_time = delay * (2 ** attempt)  # Exponential backoff
                    print(f"⚠️ Database error (attempt {attempt + 1}/{max_retries}): {e}")
                    print(f"   Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                except Exception as e:
                    # For non-database errors, don't retry
                    print(f"❌ Non-database error: {e}")
                    raise
            return None
        return wrapper
    return decorator


@retry_database_operation(max_retries=3, delay=2)
def connect():
    """Connect to PostgreSQL with retry logic"""
    dsn = os.getenv("PG_DSN")
    if not dsn:
        raise ValueError("❌ PG_DSN must be set to your PostgreSQL DSN")
    
    print(f"🔄 Connecting to database...")
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    print(f"✅ Database connection successful")
    return conn


def test_database_connection():
    """Test database connection before starting main process"""
    try:
        conn = connect()
        cur = conn.cursor()
        cur.execute("SELECT 1")
        result = cur.fetchone()
        if result and result[0] == 1:
            print("✅ Database connection test successful")
            conn.close()
            return True
        else:
            print("❌ Database connection test failed - unexpected result")
            return False
    except Exception as e:
        print(f"❌ Database connection test failed: {e}")
        return False


@retry_database_operation(max_retries=2, delay=1)
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


@retry_database_operation(max_retries=2, delay=1)
def load_table(conn, table: str, df: pd.DataFrame):
    """Load data into table with retry logic"""
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
        # Don't rollback here - let the retry decorator handle it
        raise


@retry_database_operation(max_retries=2, delay=1)
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
    
    # Now load the data using the standard load_table function
    load_table(conn, table, df)


def debug_columns(conn, table: str, df: pd.DataFrame):
    """Debug column mismatches between table and parquet"""
    existing = set(get_table_columns(conn, table))
    parquet_cols = set(df.columns)
    
    print(f"\n🔍 DEBUG COLUMN MISMATCH for {table}:")
    print(f"   Table expects: {sorted(existing)}")
    print(f"   Parquet has:   {sorted(parquet_cols)}")
    print(f"   Missing from parquet: {existing - parquet_cols}")
    print(f"   Extra in parquet:     {parquet_cols - existing}")


def validate_data_quality(df: pd.DataFrame, table: str) -> dict:
    """Basic data quality validation"""
    issues = []
    
    # Check for excessive nulls
    null_rates = df.isnull().mean()
    high_null_cols = null_rates[null_rates > 0.8]
    if len(high_null_cols) > 0:
        issues.append(f"High null rates in columns: {list(high_null_cols.index)}")
    
    # Check for duplicate rows
    duplicate_count = df.duplicated().sum()
    if duplicate_count > 0:
        issues.append(f"Found {duplicate_count} duplicate rows")
    
    # Check for reasonable data ranges for specific columns
    if table == "statcast":
        if 'release_speed' in df.columns:
            speed_issues = df[(df['release_speed'] < 50) | (df['release_speed'] > 110)]
            if len(speed_issues) > 0:
                issues.append(f"Suspicious pitch speeds: {len(speed_issues)} pitches")
    
    quality_score = max(0, 100 - len(issues) * 15)
    
    return {
        "total_records": len(df),
        "issues": issues,
        "quality_score": quality_score,
        "null_columns": len(high_null_cols)
    }


def main():
    # Test database connection first
    if not test_database_connection():
        print("❌ Cannot connect to database. Check your PG_DSN environment variable.")
        print("   Example: export PG_DSN='postgresql://user:password@localhost:5432/mlb_db'")
        return

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
    p.add_argument(
        "--debug",
        action="store_true",
        help="Show detailed column mapping debug info"
    )
    p.add_argument(
        "--enhanced",
        action="store_true",
        help="Use enhanced loader that automatically adds missing columns"
    )
    args = p.parse_args()

    in_dir = Path(args.input_dir)
    files = sorted(in_dir.glob("*.parquet"))
    if not files:
        print(f"❌ No parquet files found in {in_dir}")
        return

    print(f"📁 Found {len(files)} parquet files in {in_dir}")

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
                "umpire": "umpires",
                "umpires": "umpires"
            } 
    
            if base in table_mapping:
                tbl = table_mapping[base]
            else:
                tbl = base.rstrip("s")  # Default: remove trailing 's'
    
            return tbl in keep
            
        files = [f for f in files if matches(f)]
        if not files:
            print(f"❌ None of the files match --tables {keep}")
            return
        
        print(f"📋 Filtered to {len(files)} files matching --tables filter")

    # Connect to database
    try:
        conn = connect()
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        return

    # Process files
    successful_loads = 0
    failed_loads = 0
    total_rows_loaded = 0
    
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
        elif base in ["umpire", "umpires"]:
            table = "umpires"
        else:
            table = base.rstrip("s")
        
        print(f"\n⏳ {pq.name} → public.{table}")
        
        try:
            # Load parquet file
            df = pd.read_parquet(pq)
            print(f"   📊 Loaded parquet: {len(df)} rows, {len(df.columns)} columns")
            
            # Data quality validation
            if args.validate:
                validation = validate_data_quality(df, table)
                print(f"   📋 Quality score: {validation['quality_score']}/100")
                if validation['issues']:
                    print(f"   ⚠️  Issues: {', '.join(validation['issues'])}")
            
            # Debug column mapping
            if args.debug:
                debug_columns(conn, table, df)
            
            # Load data
            if args.enhanced:
                enhanced_load_table(conn, table, df)
            else:
                load_table(conn, table, df)
            
            successful_loads += 1
            total_rows_loaded += len(df)
            
        except Exception as e:
            print(f"   ❌ Failed to load {pq.name}: {e}")
            failed_loads += 1
            continue

    # Summary
    print(f"\n🎉 Loading complete!")
    print(f"📊 Summary:")
    print(f"   ✅ Successful loads: {successful_loads}")
    print(f"   ❌ Failed loads: {failed_loads}")
    print(f"   📈 Total rows loaded: {total_rows_loaded:,}")
    
    if failed_loads > 0:
        print(f"\n⚠️  Some files failed to load. Check the error messages above.")
    
    conn.close()


if __name__ == "__main__":
    main()