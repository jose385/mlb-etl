#!/usr/bin/env python3
"""
enhanced_load_parquet_into_pg.py – Updated loader for enhanced schema
Maps files to correct tables for the 9-table enhanced schema

Usage:
    python enhanced_load_parquet_into_pg.py [--input-dir DIR] [--tables T1 T2 ...]
"""
import os
import argparse
import io
import time
from pathlib import Path
from functools import wraps
from py.config import require_config

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
                    
                    wait_time = delay * (2 ** attempt)
                    print(f"⚠️ Database error (attempt {attempt + 1}/{max_retries}): {e}")
                    print(f"   Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                except Exception as e:
                    print(f"❌ Non-database error: {e}")
                    raise
            return None
        return wrapper
    return decorator


def get_enhanced_table_mapping(file_stem: str) -> str:
    """
    Enhanced table mapping for the 9-table schema
    Maps file prefixes to correct table names
    """
    base = file_stem.split("_", 1)[0]
    
    # Enhanced schema table mappings
    table_mapping = {
        # Core existing tables (with name changes)
        "games": "games",                    # CHANGED: was statcast → games
        "play": "play_by_play",             # CHANGED: was statsapi → play_by_play
        "weather": "weather",                # UNCHANGED
        "umpires": "umpires",               # UNCHANGED
        "umpire": "umpires",                # UNCHANGED
        "lineups": "lineups",               # CHANGED: was lineup → lineups
        "rosters": "rosters",               # CHANGED: was roster → rosters
        
        # New enhanced tables
        "game": "game_info",                # NEW: game_info files
        "recent": "recent_stats",           # NEW: recent_stats files
        "venue": "venue_factors",           # NEW: venue_factors files
        
        # Legacy mappings (for backward compatibility)
        "statcast": "games",                # Legacy support
        "statsapi": "play_by_play",         # Legacy support
        "lineup": "lineups",                # Legacy support  
        "roster": "rosters",                # Legacy support
    }
    
    # Handle multi-word prefixes
    if "play_by_play" in file_stem:
        return "play_by_play"
    elif "game_info" in file_stem:
        return "game_info"
    elif "recent_stats" in file_stem:
        return "recent_stats"
    elif "venue_factors" in file_stem:
        return "venue_factors"
    
    # Single word mappings
    if base in table_mapping:
        return table_mapping[base]
    
    # Default fallback
    return base.rstrip("s")


@retry_database_operation(max_retries=3, delay=2)
def connect():
    """Connect to PostgreSQL with configuration validation"""
    config = require_config(require_database=True)
    
    try:
        conn = psycopg2.connect(config.PG_DSN)
        conn.autocommit = True
        if config.VERBOSE:
            print(f"✅ Database connection successful")
        return conn
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print(f"   DSN: {config.PG_DSN[:50]}...")
        raise


@retry_database_operation(max_retries=2, delay=1)
def get_table_columns(conn, table: str):
    """Return column names existing in public.<table>"""
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
    """Load data into table with enhanced error handling"""
    # Get existing table columns
    existing = set(get_table_columns(conn, table))
    
    # Prune columns to match table schema
    to_load = [c for c in df.columns if c in existing]
    
    if not to_load:
        print(f"⚠️  After pruning, no columns remain for table '{table}', skipping")
        return
    
    # Show column mapping info
    missing_in_table = set(df.columns) - existing
    missing_in_data = existing - set(df.columns)
    
    if missing_in_table:
        print(f"   📋 Columns in data but not in table: {sorted(missing_in_table)}")
    if missing_in_data:
        print(f"   📋 Columns in table but not in data: {sorted(missing_in_data)}")
    
    # Prepare data for loading
    buf = io.StringIO()
    df[to_load].to_csv(buf, index=False, header=False)
    buf.seek(0)
    
    cols_csv = ", ".join(to_load)
    temp_table = f"temp_{table}_{int(time.time())}"
    copy_sql = f"COPY {temp_table} ({cols_csv}) FROM STDIN WITH (FORMAT CSV)"
    
    cur = conn.cursor()
    
    try:
        # Create temporary table
        cur.execute(f"DROP TABLE IF EXISTS {temp_table}")
        cur.execute(f"CREATE TEMP TABLE {temp_table} (LIKE public.{table})")
        
        # Load data into temp table
        cur.copy_expert(copy_sql, buf)
        
        # Handle different table types with appropriate conflict resolution
        if table in ["game_info", "venue_factors"]:
            # These have single-column primary keys
            pk_col = "game_pk" if table == "game_info" else "venue_name"
            insert_sql = f"""
                INSERT INTO public.{table} ({cols_csv})
                SELECT {cols_csv} FROM {temp_table}
                ON CONFLICT ({pk_col}) DO UPDATE SET
                {', '.join([f"{col} = EXCLUDED.{col}" for col in to_load if col != pk_col])}
            """
        elif table in ["recent_stats"]:
            # Composite primary key
            insert_sql = f"""
                INSERT INTO public.{table} ({cols_csv})
                SELECT {cols_csv} FROM {temp_table}
                ON CONFLICT (stat_date, player_id, stat_type) DO UPDATE SET
                {', '.join([f"{col} = EXCLUDED.{col}" for col in to_load if col not in ["stat_date", "player_id", "stat_type"]])}
            """
        else:
            # Standard tables - just ignore conflicts
            insert_sql = f"""
                INSERT INTO public.{table} ({cols_csv})
                SELECT {cols_csv} FROM {temp_table}
                ON CONFLICT DO NOTHING
            """
        
        cur.execute(insert_sql)
        rows_affected = cur.rowcount
        
        print(f"✅ Loaded {rows_affected} rows → public.{table} ({len(to_load)} cols)")
        
        # Clean up temp table
        cur.execute(f"DROP TABLE IF EXISTS {temp_table}")
        
    except Exception as e:
        print(f"❌ Error loading {table}: {e}")
        try:
            cur.execute(f"DROP TABLE IF EXISTS {temp_table}")
        except:
            pass
        raise


def validate_enhanced_schema(conn):
    """Validate that enhanced schema tables exist"""
    expected_tables = [
        "games", "play_by_play", "weather", "umpires", 
        "lineups", "rosters", "game_info", "recent_stats", "venue_factors"
    ]
    
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
    """)
    
    existing_tables = {row[0] for row in cur.fetchall()}
    missing_tables = set(expected_tables) - existing_tables
    
    if missing_tables:
        print(f"⚠️  Missing enhanced schema tables: {sorted(missing_tables)}")
        print(f"   Run: psql -f migrations/001_enhanced_simple_schema.sql")
        return False
    
    print(f"✅ Enhanced schema validated: {len(expected_tables)} tables found")
    return True


def show_loading_summary(files, successful_loads, failed_loads, total_rows_loaded):
    """Enhanced loading summary"""
    print(f"\n🎉 Enhanced loading complete!")
    print(f"📊 Summary:")
    print(f"   📁 Files processed: {len(files)}")
    print(f"   ✅ Successful loads: {successful_loads}")
    print(f"   ❌ Failed loads: {failed_loads}")
    print(f"   📈 Total rows loaded: {total_rows_loaded:,}")
    
    success_rate = (successful_loads / len(files)) * 100 if files else 0
    print(f"   📊 Success rate: {success_rate:.1f}%")
    
    if failed_loads > 0:
        print(f"\n⚠️  {failed_loads} files failed to load. Check error messages above.")
    
    print(f"\n💡 Next steps:")
    print(f"   1. Run analysis: python enhanced_simple_analysis.py")
    print(f"   2. Check data quality: SELECT COUNT(*) FROM games;")


def main():
    p = argparse.ArgumentParser(description="Enhanced MLB data loader for 9-table schema")
    p.add_argument(
        "--input-dir",
        default="stage",
        help="Folder containing parquet files"
    )
    p.add_argument(
        "--tables",
        nargs="*",
        help="Optional subset: games play_by_play game_info weather umpires lineups rosters recent_stats venue_factors"
    )
    p.add_argument(
        "--validate-schema",
        action="store_true",
        help="Validate enhanced schema before loading"
    )
    p.add_argument(
        "--debug",
        action="store_true",
        help="Show detailed column mapping info"
    )
    args = p.parse_args()
    
    # Connect to database
    try:
        conn = connect()
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        return
    
    # Validate schema if requested
    if args.validate_schema:
        if not validate_enhanced_schema(conn):
            print("❌ Schema validation failed")
            return
    
    # Find parquet files
    in_dir = Path(args.input_dir)
    files = sorted(in_dir.glob("*.parquet"))
    
    if not files:
        print(f"❌ No parquet files found in {in_dir}")
        return
    
    print(f"📁 Found {len(files)} parquet files in {in_dir}")
    
    # Filter by tables if specified
    if args.tables:
        keep = set(args.tables)
        
        def matches_enhanced_schema(f):
            stem = f.stem
            table_name = get_enhanced_table_mapping(stem)
            return table_name in keep
        
        files = [f for f in files if matches_enhanced_schema(f)]
        
        if not files:
            print(f"❌ None of the files match enhanced schema tables: {keep}")
            return
        
        print(f"📋 Filtered to {len(files)} files for enhanced schema")
    
    # Process files
    successful_loads = 0
    failed_loads = 0
    total_rows_loaded = 0
    
    for pq_file in files:
        stem = pq_file.stem
        table = get_enhanced_table_mapping(stem)
        
        print(f"\n⏳ {pq_file.name} → public.{table}")
        
        try:
            # Load parquet file
            df = pd.read_parquet(pq_file)
            print(f"   📊 Loaded parquet: {len(df)} rows, {len(df.columns)} columns")
            
            # Show debug info if requested
            if args.debug:
                existing_cols = set(get_table_columns(conn, table))
                parquet_cols = set(df.columns)
                print(f"   🔍 Table expects: {sorted(existing_cols)}")
                print(f"   🔍 Parquet has: {sorted(parquet_cols)}")
            
            # Load data
            load_table(conn, table, df)
            
            successful_loads += 1
            total_rows_loaded += len(df)
            
        except Exception as e:
            print(f"   ❌ Failed to load {pq_file.name}: {e}")
            failed_loads += 1
            continue
    
    # Show summary
    show_loading_summary(files, successful_loads, failed_loads, total_rows_loaded)
    
    conn.close()


if __name__ == "__main__":
    main()