#!/usr/bin/env python3
"""
enhanced_load_parquet_into_pg.py – FIXED: Enhanced loader for complete 9-table schema
MAJOR FIXES: Better file mapping, improved error handling, proper data type handling
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
    FIXED: Enhanced table mapping that handles all filename patterns correctly
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
    
    # FIXED: Enhanced schema table mappings with all possible variations
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
        
        # FIXED: Handle possible backfill naming variations
        "statcast_games": "games",     # If backfill names it this way
        "pbp": "play_by_play",         # Abbreviation
        "starting_lineups": "lineups", # Alternative naming
        "player_stats": "recent_stats", # Alternative naming
        
        # Legacy support (if old naming is still used somewhere)
        "statcast": "games",     # Legacy Statcast files -> games table
        "statsapi": "play_by_play",  # Legacy StatsAPI files -> play_by_play table
    }
    
    if base_name in table_mapping:
        return table_mapping[base_name]
    
    # FIXED: More intelligent fallback
    print(f"⚠️ Warning: No table mapping found for '{file_stem}', attempting intelligent guess...")
    
    # Try to guess based on common patterns
    if 'game' in base_name.lower():
        if 'info' in base_name.lower():
            return 'game_info'
        else:
            return 'games'
    elif 'play' in base_name.lower():
        return 'play_by_play'
    elif 'lineup' in base_name.lower():
        return 'lineups'
    elif 'roster' in base_name.lower():
        return 'rosters'
    elif 'umpire' in base_name.lower():
        return 'umpires'
    elif 'weather' in base_name.lower():
        return 'weather'
    elif 'venue' in base_name.lower() or 'ballpark' in base_name.lower():
        return 'venue_factors'
    elif 'stat' in base_name.lower():
        return 'recent_stats'
    
    # Final fallback: return the base name
    print(f"   Using base name: {base_name}")
    return base_name


def get_loading_order_priority(data_type: str) -> int:
    """
    CRITICAL FIX: Define loading order to respect foreign key dependencies
    Lower numbers = load first
    """
    loading_order = {
        # Load parent tables first (no dependencies)
        'game_info': 1,      # PRIMARY - All other tables reference this
        'venue_factors': 2,   # INDEPENDENT - No foreign keys
        'rosters': 3,        # SEMI-INDEPENDENT - Only references dates/teams
        
        # Load child tables second (depend on game_info)
        'games': 4,          # References game_info.game_pk
        'play_by_play': 5,   # References game_info.game_pk  
        'lineups': 6,        # References game_info.game_pk
        'umpires': 7,        # References game_info.game_pk
        'weather': 8,        # References game_info.game_pk
        'recent_stats': 9,   # INDEPENDENT but load last
    }
    
    return loading_order.get(data_type, 999)  # Unknown types load last


def sort_files_by_dependency_order(files: List[Path]) -> List[Path]:
    """
    CRITICAL FIX: Sort files to load in dependency order
    This prevents foreign key constraint violations
    """
    def get_file_priority(file_path: Path) -> tuple:
        stem = file_path.stem
        table_name = get_enhanced_table_mapping(stem)
        priority = get_loading_order_priority(table_name)
        
        # Sort by: (priority, filename) to ensure consistent ordering
        return (priority, stem)
    
    sorted_files = sorted(files, key=get_file_priority)
    
    print(f"📋 Loading order determined:")
    for file in sorted_files:
        stem = file.stem
        table_name = get_enhanced_table_mapping(stem)
        priority = get_loading_order_priority(table_name)
        print(f"   {priority:2d}. {file.name} → {table_name}")
    
    return sorted_files


@retry_database_operation(max_retries=3, delay=2)
def connect():
    """Connect to PostgreSQL with configuration validation"""
    config = require_config(require_database=True, graceful_degradation=True)
    
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
    """FIXED: Load data into table with enhanced error handling and type conversion"""
    if df.empty:
        print(f"⏭️ Skipping {table} - DataFrame is empty")
        return
    
    # Get existing table columns
    existing = set(get_table_columns(conn, table))
    
    if not existing:
        print(f"❌ Table '{table}' not found in database or has no columns")
        return
    
    # Prune columns to match table schema
    to_load = [c for c in df.columns if c in existing]
    
    if not to_load:
        print(f"⚠️  After pruning, no columns remain for table '{table}', skipping")
        print(f"   Available columns: {sorted(df.columns)}")
        print(f"   Expected columns: {sorted(existing)}")
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
    
    # FIXED: Enhanced data type conversions for PostgreSQL compatibility
    for col in df_to_load.columns:
        # Handle missing values
        if df_to_load[col].isnull().all():
            continue
            
        # Convert boolean-like values
        if df_to_load[col].dtype == 'object':
            # Handle common boolean representations
            bool_map = {'true': True, 'false': False, 'True': True, 'False': False,
                       'yes': True, 'no': False, 'Y': True, 'N': False}
            bool_mask = df_to_load[col].isin(bool_map.keys())
            if bool_mask.any():
                df_to_load[col] = df_to_load[col].map(bool_map).fillna(df_to_load[col])
        
        # Handle nullable integer columns
        if str(df_to_load[col].dtype).startswith('Int'):
            df_to_load[col] = df_to_load[col].astype('float64')  # Use float to preserve NaN
        
        # FIXED: Handle date columns properly
        if col.endswith('_date') or 'date' in col.lower():
            try:
                df_to_load[col] = pd.to_datetime(df_to_load[col]).dt.date
            except:
                pass  # Keep original if conversion fails
        
        # FIXED: Handle numeric columns with proper null handling
        if col in ['game_pk', 'person_id', 'pitcher', 'batter', 'team_id']:
            try:
                df_to_load[col] = pd.to_numeric(df_to_load[col], errors='coerce')
            except:
                pass
    
    # FIXED: Clean up any infinite values
    df_to_load = df_to_load.replace([float('inf'), float('-inf')], None)
    
    # Create CSV buffer
    buf = io.StringIO()
    df_to_load.to_csv(buf, index=False, header=False, na_rep='\\N')  # Use PostgreSQL NULL representation
    buf.seek(0)
    
    cols_csv = ", ".join(to_load)
    temp_table = f"temp_{table}_{int(time.time())}_{os.getpid()}"  # Make unique
    copy_sql = f"COPY {temp_table} ({cols_csv}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')"
    
    cur = conn.cursor()
    
    try:
        # Create temporary table
        cur.execute(f"DROP TABLE IF EXISTS {temp_table}")
        cur.execute(f"CREATE TEMP TABLE {temp_table} (LIKE public.{table})")
        
        # Load data into temp table
        cur.copy_expert(copy_sql, buf)
        temp_rows = cur.rowcount
        
        # FIXED: Enhanced conflict resolution based on table type
        if table == "game_info":
            # Primary key: game_pk
            insert_sql = f"""
                INSERT INTO public.{table} ({cols_csv})
                SELECT {cols_csv} FROM {temp_table}
                ON CONFLICT (game_pk) DO UPDATE SET
                {', '.join([f"{col} = EXCLUDED.{col}" for col in to_load if col != 'game_pk'])}
            """
        elif table == "venue_factors":
            # Primary key: venue_name
            insert_sql = f"""
                INSERT INTO public.{table} ({cols_csv})
                SELECT {cols_csv} FROM {temp_table}
                ON CONFLICT (venue_name) DO UPDATE SET
                {', '.join([f"{col} = EXCLUDED.{col}" for col in to_load if col != 'venue_name'])}
            """
        elif table == "recent_stats":
            # Composite primary key: stat_date, player_id, stat_type
            pk_cols = ["stat_date", "player_id", "stat_type"]
            non_pk_cols = [col for col in to_load if col not in pk_cols]
            if non_pk_cols:
                insert_sql = f"""
                    INSERT INTO public.{table} ({cols_csv})
                    SELECT {cols_csv} FROM {temp_table}
                    ON CONFLICT (stat_date, player_id, stat_type) DO UPDATE SET
                    {', '.join([f"{col} = EXCLUDED.{col}" for col in non_pk_cols])}
                """
            else:
                insert_sql = f"""
                    INSERT INTO public.{table} ({cols_csv})
                    SELECT {cols_csv} FROM {temp_table}
                    ON CONFLICT (stat_date, player_id, stat_type) DO NOTHING
                """
        elif table == "games":
            # Composite primary key: game_pk, at_bat_number, pitch_number
            insert_sql = f"""
                INSERT INTO public.{table} ({cols_csv})
                SELECT {cols_csv} FROM {temp_table}
                ON CONFLICT (game_pk, at_bat_number, pitch_number) DO NOTHING
            """
        elif table == "play_by_play":
            # Composite primary key: game_pk, at_bat_index, event_index
            insert_sql = f"""
                INSERT INTO public.{table} ({cols_csv})
                SELECT {cols_csv} FROM {temp_table}
                ON CONFLICT (game_pk, at_bat_index, event_index) DO NOTHING
            """
        elif table == "lineups":
            # Composite primary key: game_pk, team_id, batting_order
            insert_sql = f"""
                INSERT INTO public.{table} ({cols_csv})
                SELECT {cols_csv} FROM {temp_table}
                ON CONFLICT (game_pk, team_id, batting_order) DO NOTHING
            """
        elif table == "rosters":
            # Composite primary key: game_date, team_id, person_id
            insert_sql = f"""
                INSERT INTO public.{table} ({cols_csv})
                SELECT {cols_csv} FROM {temp_table}
                ON CONFLICT (game_date, team_id, person_id) DO NOTHING
            """
        elif table == "umpires":
            # Composite primary key: game_pk, umpire_id
            insert_sql = f"""
                INSERT INTO public.{table} ({cols_csv})
                SELECT {cols_csv} FROM {temp_table}
                ON CONFLICT (game_pk, umpire_id) DO NOTHING
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
        print(f"   Temp table: {temp_table}")
        print(f"   Data shape: {df_to_load.shape}")
        print(f"   Columns to load: {to_load[:5]}{'...' if len(to_load) > 5 else ''}")
        try:
            cur.execute(f"DROP TABLE IF EXISTS {temp_table}")
        except:
            pass
        raise


@retry_database_operation(max_retries=2, delay=1)
def load_all_files_in_transaction(conn, files_and_tables: List[tuple]):
    """
    CRITICAL FIX: Load all files in a single transaction with deferred constraints
    This allows foreign keys to be checked only at commit time
    """
    cur = conn.cursor()
    
    try:
        # Start transaction and defer constraint checking
        cur.execute("BEGIN")
        cur.execute("SET CONSTRAINTS ALL DEFERRED")
        
        total_rows_loaded = 0
        successful_loads = 0
        
        for file_path, table_name in files_and_tables:
            print(f"\n⏳ Loading {file_path.name} → {table_name}")
            
            try:
                # FIXED: Better parquet file handling
                df = pd.read_parquet(file_path)
                print(f"   📊 Read parquet: {len(df)} rows, {len(df.columns)} columns")
                
                if df.empty:
                    print(f"   ⏭️ Skipping empty file: {file_path.name}")
                    successful_loads += 1  # Count as success
                    continue
                
                # FIXED: Data validation before loading
                if 'game_pk' in df.columns:
                    # Remove rows with null game_pk
                    before_count = len(df)
                    df = df.dropna(subset=['game_pk'])
                    after_count = len(df)
                    if before_count != after_count:
                        print(f"   🧹 Removed {before_count - after_count} rows with null game_pk")
                
                # Load into database
                load_table(conn, table_name, df)
                
                successful_loads += 1
                total_rows_loaded += len(df)
                
            except Exception as e:
                print(f"   ❌ Failed to load {file_path.name}: {e}")
                # Continue with other files instead of failing entire transaction
                continue
        
        # Commit transaction - this is when foreign key constraints are checked
        print(f"\n🔄 Committing transaction with {successful_loads} successful loads...")
        cur.execute("COMMIT")
        
        print(f"✅ Transaction committed successfully!")
        print(f"   📊 Total rows loaded: {total_rows_loaded:,}")
        print(f"   📁 Files processed: {successful_loads}")
        
        return successful_loads, total_rows_loaded
        
    except Exception as e:
        print(f"❌ Transaction failed: {e}")
        try:
            cur.execute("ROLLBACK")
            print("🔄 Transaction rolled back")
        except:
            pass
        raise


def validate_enhanced_schema(conn):
    """FIXED: Validate that enhanced schema tables exist"""
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
    
    # FIXED: Also check for data in key tables
    data_check_tables = ["game_info", "games", "play_by_play"]
    for table in data_check_tables:
        if table in existing_tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            print(f"   📊 {table}: {count:,} records")
    
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
    """FIXED: Enhanced loading summary with better information"""
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
        print(f"💡 Common issues:")
        print(f"   • Missing columns: File structure doesn't match database schema")
        print(f"   • Data type errors: Invalid data formats")
        print(f"   • Foreign key violations: Parent records missing")
    
    print(f"\n💡 Next steps:")
    print(f"   1. Validate data: SELECT table_name, count(*) FROM information_schema.tables WHERE table_schema='public' GROUP BY table_name;")
    print(f"   2. Check key tables: SELECT COUNT(*) FROM games; SELECT COUNT(*) FROM game_info;")
    print(f"   3. Run analysis: python py/simple_analysis.py")
    
    if successful_loads > 0:
        print(f"   4. Test betting analysis with a specific game")


def main():
    p = argparse.ArgumentParser(description="FIXED: Enhanced MLB data loader for complete 9-table schema")
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
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be loaded without actually loading"
    )
    args = p.parse_args()
    
    # Get configuration
    try:
        from py.config import get_config
        config = get_config()
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        print("💡 Run: python setup_env.py")
        return
    
    # Connect to database
    try:
        conn = connect()
        print("✅ Connected to database")
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        print("💡 Check database configuration and ensure database is running")
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
        else:
            print("💡 Make sure backfill has been run first:")
            print("   python py/enhanced_simple_backfill.py --start YYYY-MM-DD --end YYYY-MM-DD")
        return
    
    print(f"📁 Found {len(files)} parquet files in {in_dir}")
    
    # Show what files we found
    for file in files:
        table_name = get_enhanced_table_mapping(file.stem)
        print(f"   • {file.name} → {table_name}")
    
    # Filter by tables if specified
    if args.tables:
        keep = set(args.tables)
        
        def matches_enhanced_schema(f):
            stem = f.stem
            table_name = get_enhanced_table_mapping(stem)
            return table_name in keep
        
        files = [f for f in files if matches_enhanced_schema(f)]
        
        if not files:
            print(f"❌ None of the files match requested tables: {keep}")
            return
        
        print(f"📋 Filtered to {len(files)} files for requested tables")
    
    # CRITICAL FIX: Sort files by dependency order
    files = sort_files_by_dependency_order(files)
    
    # Prepare file-to-table mapping
    files_and_tables = []
    for pq_file in files:
        stem = pq_file.stem
        table = get_enhanced_table_mapping(stem)
        files_and_tables.append((pq_file, table))
    
    # Show dry run if requested
    if args.dry_run:
        print(f"\n🔍 DRY RUN - Would load these files:")
        for pq_file, table in files_and_tables:
            print(f"   {pq_file.name} → {table}")
        print(f"\nTo actually load, run without --dry-run")
        return
    
    # CRITICAL FIX: Load all files in a single transaction with deferred constraints
    try:
        successful_loads, total_rows_loaded = load_all_files_in_transaction(conn, files_and_tables)
        failed_loads = len(files) - successful_loads
        
        # Check for constraint violations after loading
        print(f"\n🔍 Checking for constraint violations...")
        
        with conn.cursor() as cur:
            # Check if the constraint validation function exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM pg_proc 
                    WHERE proname = 'check_foreign_key_violations'
                )
            """)
            function_exists = cur.fetchone()[0]
            
            if function_exists:
                cur.execute("SELECT * FROM check_foreign_key_violations()")
                violations = cur.fetchall()
                
                total_violations = sum(row[2] for row in violations)
                if total_violations > 0:
                    print(f"⚠️ Found {total_violations} foreign key violations:")
                    for table, constraint, count in violations:
                        if count > 0:
                            print(f"   {table}.{constraint}: {count} violations")
                else:
                    print(f"✅ No foreign key violations found")
            else:
                print(f"⚠️ Constraint validation function not available")
        
    except Exception as e:
        print(f"❌ Loading failed: {e}")
        successful_loads = 0
        failed_loads = len(files)
        total_rows_loaded = 0
        
        # Show debugging info
        print(f"\n🔍 Debugging info:")
        print(f"   Files to load: {len(files)}")
        for pq_file, table in files_and_tables[:5]:  # Show first 5
            print(f"   {pq_file.name} → {table}")
        if len(files_and_tables) > 5:
            print(f"   ... and {len(files_and_tables) - 5} more")
    
    # Upload to S3 if requested
    if args.upload_to_s3 and s3_manager:
        upload_results_to_s3(in_dir, s3_manager)
    
    # Show summary
    show_loading_summary(files, successful_loads, failed_loads, total_rows_loaded, s3_enabled=bool(s3_manager))
    
    conn.close()


if __name__ == "__main__":
    main()