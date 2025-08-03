#!/usr/bin/env python3
"""
enhanced_load_parquet_into_pg.py – Updated loader for enhanced schema with S3 integration
Maps files to correct tables for the 9-table enhanced schema
Supports loading from local files and S3

Usage:
    python enhanced_load_parquet_into_pg.py [--input-dir DIR] [--tables T1 T2 ...] [--from-s3]
"""
import os
import argparse
import io
import time
from pathlib import Path
from functools import wraps
from typing import List
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
    UPDATED: Enhanced table mapping that handles all filename patterns correctly
    Maps file prefixes to correct table names for the 9-table schema
    """
    # Handle exact filename matches first (for files like "venue_factors.parquet")
    exact_matches = {
        "venue_factors": "venue_factors",
        "recent_stats": "recent_stats",
    }
    
    if file_stem in exact_matches:
        return exact_matches[file_stem]
    
    # Handle date-suffixed files (like "games_2024-07-15")
    # Split on first underscore to get the data type
    parts = file_stem.split("_")
    if len(parts) >= 2:
        # Check if last part looks like a date (YYYY-MM-DD)
        last_part = parts[-1]
        if len(last_part) == 10 and last_part.count('-') == 2:
            # This is a date-suffixed file, get everything before the date
            base_name = "_".join(parts[:-1])
        else:
            # Not a date-suffixed file, use first part
            base_name = parts[0]
    else:
        # Single part filename
        base_name = file_stem
    
    # Enhanced schema table mappings
    table_mapping = {
        # Primary mappings (exact matches)
        "games": "games",
        "play_by_play": "play_by_play", 
        "game_info": "game_info",
        "weather": "weather",
        "umpires": "umpires",
        "lineups": "lineups",
        "rosters": "rosters",
        "recent_stats": "recent_stats",
        "venue_factors": "venue_factors",
        
        # Alternative patterns that might be created
        "play": "play_by_play",  # Handle "play_by_play" -> "play" mapping
        "game": "game_info",     # Handle "game_info" -> "game" mapping
        "umpire": "umpires",     # Handle singular/plural
        "lineup": "lineups",     # Handle singular/plural
        "roster": "rosters",     # Handle singular/plural
        
        # Legacy support (if old naming is still used somewhere)
        "statcast": "games",     # Legacy Statcast files -> games table
        "statsapi": "play_by_play",  # Legacy StatsAPI files -> play_by_play table
    }
    
    if base_name in table_mapping:
        return table_mapping[base_name]
    
    # Fallback: return the base name if no mapping found
    print(f"⚠️ Warning: No table mapping found for '{file_stem}', using '{base_name}'")
    return base_name


@retry_database_operation(max_retries=3, delay=2)
def connect():
    """Connect to PostgreSQL with configuration validation"""
    config = require_config(require_database=True)
    
    try:
        db_manager = config.get_database_manager()
        return db_manager.get_connection()
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
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
    """UPDATED: Load data into table with enhanced error handling and type conversion"""
    # Get existing table columns
    existing = set(get_table_columns(conn, table))
    
    # Prune columns to match table schema
    to_load = [c for c in df.columns if c in existing]
    
    if not to_load:
        print(f"⚠️  After pruning, no columns remain for table '{table}', skipping")
        return
    
    # Show detailed column mapping info
    missing_in_table = set(df.columns) - existing
    missing_in_data = existing - set(df.columns)
    
    print(f"   📊 Table: {table}")
    print(f"   📥 Loading {len(to_load)} columns: {sorted(to_load)}")
    
    if missing_in_table:
        print(f"   📋 Columns in data but not in table: {sorted(missing_in_table)}")
    if missing_in_data:
        print(f"   📋 Columns in table but not in data: {sorted(missing_in_data)}")
    
    # Prepare data for loading
    df_to_load = df[to_load].copy()
    
    # Handle data type conversions for PostgreSQL compatibility
    for col in df_to_load.columns:
        # Convert boolean-like values
        if df_to_load[col].dtype == 'object':
            # Handle common boolean representations
            bool_map = {'true': True, 'false': False, 'True': True, 'False': False,
                       'yes': True, 'no': False, 'Y': True, 'N': False}
            if df_to_load[col].isin(bool_map.keys()).any():
                df_to_load[col] = df_to_load[col].map(bool_map).fillna(df_to_load[col])
        
        # Handle nullable integer columns
        if str(df_to_load[col].dtype).startswith('Int'):
            df_to_load[col] = df_to_load[col].astype('float64')  # Use float to preserve NaN
    
    # Create CSV buffer
    buf = io.StringIO()
    df_to_load.to_csv(buf, index=False, header=False, na_rep='\\N')  # Use PostgreSQL NULL representation
    buf.seek(0)
    
    cols_csv = ", ".join(to_load)
    temp_table = f"temp_{table}_{int(time.time())}"
    copy_sql = f"COPY {temp_table} ({cols_csv}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')"
    
    cur = conn.cursor()
    
    try:
        # Create temporary table
        cur.execute(f"DROP TABLE IF EXISTS {temp_table}")
        cur.execute(f"CREATE TEMP TABLE {temp_table} (LIKE public.{table})")
        
        # Load data into temp table
        cur.copy_expert(copy_sql, buf)
        temp_rows = cur.rowcount
        
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
        
        print(f"✅ Loaded {rows_affected} rows → public.{table} (from {temp_rows} temp rows)")
        
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
        print(f"   Run: python initialize_database.py")
        return False
    
    print(f"✅ Enhanced schema validated: {len(expected_tables)} tables found")
    return True


def load_from_s3_if_available(input_dir: Path, s3_manager=None) -> List[Path]:
    """Load parquet files from S3 if available, otherwise use local files"""
    local_files = list(input_dir.glob("*.parquet"))
    
    if not s3_manager:
        return local_files
    
    print("🌐 Checking S3 for additional parquet files...")
    s3_files = s3_manager.list_parquet_files()
    
    downloaded_count = 0
    for s3_key in s3_files:
        filename = Path(s3_key).name
        local_file = input_dir / filename
        
        if not local_file.exists():
            print(f"📥 Downloading {filename} from S3...")
            if s3_manager.download_parquet(s3_key, local_file):
                downloaded_count += 1
    
    if downloaded_count > 0:
        print(f"📥 Downloaded {downloaded_count} files from S3")
    
    return list(input_dir.glob("*.parquet"))


def upload_results_to_s3(input_dir: Path, s3_manager=None) -> int:
    """Upload any remaining local files to S3"""
    if not s3_manager:
        return 0
    
    print("📤 Uploading local files to S3...")
    return s3_manager.upload_directory(input_dir)


def show_loading_summary(files, successful_loads, failed_loads, total_rows_loaded, s3_enabled=False):
    """Enhanced loading summary with S3 information"""
    print(f"\n🎉 Enhanced loading complete!")
    print(f"📊 Summary:")
    print(f"   📁 Files processed: {len(files)}")
    print(f"   ✅ Successful loads: {successful_loads}")
    print(f"   ❌ Failed loads: {failed_loads}")
    print(f"   📈 Total rows loaded: {total_rows_loaded:,}")
    
    success_rate = (successful_loads / len(files)) * 100 if files else 0
    print(f"   📊 Success rate: {success_rate:.1f}%")
    
    if s3_enabled:
        print(f"   ☁️ S3 integration: enabled")
    
    if failed_loads > 0:
        print(f"\n⚠️  {failed_loads} files failed to load. Check error messages above.")
    
    print(f"\n💡 Next steps:")
    print(f"   1. Run analysis: python py/enhanced_simple_analysis.py")
    print(f"   2. Check data quality: SELECT COUNT(*) FROM games;")
    print(f"   3. Validate loading: SELECT table_name, COUNT(*) FROM information_schema.tables WHERE table_schema='public' GROUP BY table_name;")


def main():
    p = argparse.ArgumentParser(description="Enhanced MLB data loader for 9-table schema with S3 integration")
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
        "--from-s3",
        action="store_true",
        help="Download files from S3 before loading"
    )
    p.add_argument(
        "--upload-to-s3",
        action="store_true", 
        help="Upload local files to S3 after processing"
    )
    p.add_argument(
        "--debug",
        action="store_true",
        help="Show detailed column mapping info"
    )
    args = p.parse_args()
    
    # Get configuration
    try:
        from py.config import get_config
        config = get_config()
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        return
    
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
    
    # Setup S3 manager if enabled
    s3_manager = None
    try:
        if config.ENABLE_S3_STORAGE and (args.from_s3 or args.upload_to_s3):
            s3_manager = config.get_s3_manager()
            print("✅ S3 integration enabled")
    except Exception as e:
        print(f"⚠️ S3 integration not available: {e}")
    
    # Setup input directory
    in_dir = Path(args.input_dir)
    in_dir.mkdir(exist_ok=True)
    
    # Load files (from S3 if requested)
    if args.from_s3 and s3_manager:
        files = load_from_s3_if_available(in_dir, s3_manager)
    else:
        files = list(in_dir.glob("*.parquet"))
    
    if not files:
        print(f"❌ No parquet files found in {in_dir}")
        if s3_manager:
            print("💡 Try using --from-s3 to download files from S3")
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
    
    # Upload to S3 if requested
    if args.upload_to_s3 and s3_manager:
        upload_results_to_s3(in_dir, s3_manager)
    
    # Show summary
    show_loading_summary(files, successful_loads, failed_loads, total_rows_loaded, s3_enabled=bool(s3_manager))
    
    conn.close()


if __name__ == "__main__":
    main()