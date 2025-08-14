#!/usr/bin/env python3
"""
STREAMLINED MLB Data Loader - Claude-Optimized Version
Loads only the essential data that Claude cannot research:
- Statcast data (games table) - THE GOLD MINE
- Basic game info (game_info table) - CONTEXT
- Optionally play-by-play sequences - DETAILED ANALYSIS

Skips: lineups, umpires, rosters, recent_stats (Claude will research these)
"""

import argparse
import os
import sys
import pandas as pd
import psycopg2
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import time
import io
import re
import numpy as np
from functools import wraps

# Add project root to path for imports
sys.path.append(str(Path(__file__).parent.parent))

try:
    from py.config import require_config
except ImportError:
    print("❌ Could not import config. Make sure you're running from the project root.")
    sys.exit(1)

# ============================================================================
# STREAMLINED TABLE CONFIGURATION
# ============================================================================

# Only the essential tables for betting analysis
CORE_TABLES = {
    'games': {
        'file_pattern': 'games_*.parquet',
        'description': 'Statcast pitch data (THE GOLD MINE)',
        'priority': 2,
        'required': True
    },
    'game_info': {
        'file_pattern': 'game_info_*.parquet', 
        'description': 'Basic game context',
        'priority': 1,
        'required': True
    },
    'play_by_play': {
        'file_pattern': 'play_by_play_*.parquet',
        'description': 'Game sequence data (OPTIONAL)',
        'priority': 3,
        'required': False
    }
}

# Tables that Claude will research (not loaded by this system)
CLAUDE_RESEARCH_TABLES = [
    'lineups',      # Starting lineups and batting orders
    'rosters',      # Current team rosters  
    'umpires',      # Umpire assignments and tendencies
    'recent_stats', # Recent player performance trends
    'weather',      # Weather conditions
    'venue_factors' # Ballpark factors
]

# ============================================================================
# ENHANCED DATA TYPE HANDLING (from original)
# ============================================================================

def get_comprehensive_column_mappings():
    """Enhanced column mappings for live pybaseball data"""
    
    # Core integer columns for Statcast data
    integer_columns = {
        'game_pk', 'at_bat_number', 'pitch_number', 'game_year',
        'batter', 'pitcher', 'person_id', 'team_id',
        'balls', 'strikes', 'outs_when_up', 'inning', 
        'zone', 'launch_speed_angle', 'barrel',
        'home_score', 'away_score', 'rbi',
        'attendance', 'game_length_minutes',
        'series_game_number', 'home_team_rest_days', 'away_team_rest_days'
    }
    
    # Core float columns for Statcast metrics
    float_columns = {
        'release_speed', 'release_pos_x', 'release_pos_y', 'release_pos_z',
        'effective_speed', 'release_spin_rate', 'release_extension',
        'hit_distance_sc', 'launch_speed', 'launch_angle',
        'vx0', 'vy0', 'vz0', 'ax', 'ay', 'az', 'sz_top', 'sz_bot',
        'pfx_x', 'pfx_z', 'plate_x', 'plate_z', 'hc_x', 'hc_y',
        'estimated_ba_using_speedangle', 'estimated_woba_using_speedangle',
        'estimated_slg_using_speedangle', 'woba_value', 'woba_denom',
        'babip_value', 'iso_value', 'delta_run_exp'
    }
    
    # Date columns
    date_columns = {
        'game_date', 'stat_date'
    }
    
    # Boolean columns
    boolean_columns = {
        'is_home_team', 'extra_innings'
    }
    
    # String columns
    string_columns = {
        'player_name', 'description', 'des', 'events', 'type',
        'pitch_type', 'stand', 'p_throws', 'home_team', 'away_team',
        'venue_name', 'game_status', 'winning_team'
    }
    
    return {
        'integers': integer_columns,
        'floats': float_columns,
        'dates': date_columns,
        'booleans': boolean_columns,
        'strings': string_columns
    }

def normalize_column_name(col_name: str) -> str:
    """Normalize column names to handle pybaseball variations"""
    if not col_name:
        return col_name
    
    # Convert camelCase to snake_case
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', col_name)
    normalized = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
    
    # Handle common variations
    replacements = {
        'gamepk': 'game_pk',
        'game_id': 'game_pk',
        'atbatnumber': 'at_bat_number',
        'pitchnumber': 'pitch_number',
        'player_id': 'person_id',
        'playerid': 'person_id',
    }
    
    return replacements.get(normalized, normalized)

def fix_comprehensive_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """Comprehensive data type fixing for live pybaseball data"""
    if df.empty:
        return df
    
    df = df.copy()
    column_mappings = get_comprehensive_column_mappings()
    
    # Normalize column names first
    df.columns = [normalize_column_name(col) for col in df.columns]
    
    # Handle infinite values and NaN replacements
    df = df.replace([np.inf, -np.inf], None)
    
    # Fix integer columns
    for col in df.columns:
        if col in column_mappings['integers']:
            try:
                if df[col].dtype == 'object':
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                if df[col].dtype in ['float64', 'Float64', 'object']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    df[col] = df[col].round().astype('Int64')
                elif df[col].dtype in ['int64', 'int32']:
                    df[col] = df[col].astype('Int64')
                    
            except Exception as e:
                print(f"   ⚠️ Warning: Could not convert {col} to integer: {e}")
                continue
    
    # Fix float columns
    for col in df.columns:
        if col in column_mappings['floats']:
            try:
                if df[col].dtype == 'object':
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Apply appropriate precision
                if col in ['estimated_ba_using_speedangle', 'estimated_woba_using_speedangle', 'babip_value']:
                    df[col] = df[col].round(3)
                elif col in ['pfx_x', 'pfx_z', 'hc_x', 'hc_y', 'plate_x', 'plate_z']:
                    df[col] = df[col].round(1)
                else:
                    df[col] = df[col].round(2)
                    
            except Exception as e:
                print(f"   ⚠️ Warning: Could not convert {col} to float: {e}")
                continue
    
    # Fix date columns
    for col in df.columns:
        if col in column_mappings['dates'] or 'date' in col.lower():
            try:
                if df[col].dtype == 'object':
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
                elif hasattr(df[col], 'dt'):
                    df[col] = df[col].dt.date
            except Exception as e:
                print(f"   ⚠️ Warning: Could not convert {col} to date: {e}")
                continue
    
    # Fix string columns
    for col in df.columns:
        if col in column_mappings['strings'] or df[col].dtype == 'object':
            df[col] = df[col].astype(str)
            df[col] = df[col].replace('nan', None)
            df[col] = df[col].replace('<NA>', None)
            df[col] = df[col].replace('None', None)
            df[col] = df[col].where(pd.notna(df[col]), None)
    
    # Final cleanup
    df = df.replace([float('inf'), float('-inf')], None)
    
    return df

# ============================================================================
# STREAMLINED TABLE MAPPING
# ============================================================================

def get_table_mapping(file_stem: str) -> str:
    """Streamlined table mapping for essential tables only"""
    
    # Normalize the file stem
    file_stem = file_stem.lower().replace('-', '_')
    
    # Handle date-suffixed files (like "games_2024_07_15")
    date_pattern = re.compile(r'(.+?)_(\d{4})[_-](\d{1,2})[_-](\d{1,2})$')
    match = date_pattern.match(file_stem)
    if match:
        base_name = match.group(1)
    else:
        parts = file_stem.split('_')
        base_name = parts[0] if parts else file_stem
    
    # Streamlined table mappings (only essential tables)
    table_mapping = {
        # Statcast data (primary source)
        'statcast': 'games',
        'statcast_data': 'games',
        'pitch_data': 'games',
        'games': 'games',
        
        # Game info
        'game_info': 'game_info',
        'gameinfo': 'game_info',
        
        # Play-by-play (optional)
        'play_by_play': 'play_by_play',
        'pbp': 'play_by_play',
        'play_data': 'play_by_play',
    }
    
    # Try exact match first
    if base_name in table_mapping:
        return table_mapping[base_name]
    
    # Pattern matching for variations
    if any(word in base_name for word in ['pitch', 'statcast']):
        return 'games'
    elif 'game' in base_name and 'info' in base_name:
        return 'game_info'
    elif any(word in base_name for word in ['play', 'pbp']):
        return 'play_by_play'
    
    # Default fallback
    return base_name

# ============================================================================
# DATABASE CONNECTION AND UTILITIES
# ============================================================================

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

@retry_database_operation(max_retries=3, delay=2)
def connect():
    """Connect to PostgreSQL with configuration validation"""
    try:
        config = require_config(require_database=True, graceful_degradation=True)
        
        if not config.PG_DSN:
            raise ConnectionError("PG_DSN not configured")
        
        db_manager = config.get_database_manager()
        if db_manager is None:
            raise ConnectionError("Database manager not available")
        
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

def validate_table_exists(cursor, table_name: str) -> bool:
    """Check if table exists in database"""
    try:
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = %s
            );
        """, (table_name,))
        
        return cursor.fetchone()[0]
    except Exception as e:
        print(f"⚠️ Could not check if table {table_name} exists: {e}")
        return False

# ============================================================================
# FILE DISCOVERY AND VALIDATION
# ============================================================================

def discover_parquet_files(input_dir: str, target_tables: Optional[List[str]] = None) -> Dict[str, List[str]]:
    """Discover parquet files for specified tables"""
    
    if target_tables is None:
        target_tables = list(CORE_TABLES.keys())
    
    input_path = Path(input_dir)
    if not input_path.exists():
        raise FileNotFoundError(f"Input directory does not exist: {input_dir}")
    
    discovered_files = {}
    
    for table_name in target_tables:
        if table_name not in CORE_TABLES:
            print(f"⚠️ Unknown table: {table_name} (valid tables: {list(CORE_TABLES.keys())})")
            continue
        
        table_config = CORE_TABLES[table_name]
        file_pattern = table_config['file_pattern']
        
        # Find matching files
        matching_files = list(input_path.glob(file_pattern))
        
        if matching_files:
            # Sort by filename for consistent processing order
            file_paths = [str(f) for f in sorted(matching_files)]
            discovered_files[table_name] = file_paths
            print(f"   📁 Found {len(file_paths)} files for {table_name}: {[Path(f).name for f in file_paths]}")
        else:
            if table_config['required']:
                print(f"   ⚠️ No files found for required table {table_name} (pattern: {file_pattern})")
            else:
                print(f"   💡 No files found for optional table {table_name} (pattern: {file_pattern})")
    
    return discovered_files

def get_loading_order_priority(data_type: str) -> int:
    """Loading order for streamlined tables - lower numbers load first"""
    loading_order = {
        'game_info': 1,      # Load first - other tables reference this
        'games': 2,          # Core Statcast data
        'play_by_play': 3,   # Optional detailed sequences
    }
    return loading_order.get(data_type, 999)

def sort_files_by_dependency_order(files: List[Path]) -> List[Path]:
    """Sort files to load in dependency order"""
    def get_file_priority(file_path: Path) -> tuple:
        stem = file_path.stem
        table_name = get_table_mapping(stem)
        priority = get_loading_order_priority(table_name)
        return (priority, stem)
    
    sorted_files = sorted(files, key=get_file_priority)
    
    print(f"📋 Streamlined loading order:")
    for file in sorted_files:
        stem = file.stem
        table_name = get_table_mapping(stem)
        priority = get_loading_order_priority(table_name)
        description = CORE_TABLES.get(table_name, {}).get('description', 'Unknown')
        print(f"   {priority}. {file.name} → {table_name} ({description})")
    
    return sorted_files

# ============================================================================
# DATA LOADING FUNCTIONS
# ============================================================================

@retry_database_operation(max_retries=2, delay=1)
def load_table(conn, table: str, df: pd.DataFrame):
    """Load data with comprehensive column handling"""
    if df.empty:
        print(f"⏭️ Skipping {table} - DataFrame is empty")
        return
    
    # Get existing table columns
    existing = set(get_table_columns(conn, table))
    
    if not existing:
        print(f"❌ Table '{table}' not found in database or has no columns")
        return
    
    # Normalize column names in DataFrame
    df.columns = [normalize_column_name(col) for col in df.columns]
    
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
    
    # Apply comprehensive data type fixes
    df_to_load = fix_comprehensive_data_types(df_to_load)
    
    # Final cleanup for PostgreSQL
    df_to_load = df_to_load.replace([float('inf'), float('-inf')], None)
    
    # Create CSV buffer
    buf = io.StringIO()
    df_to_load.to_csv(buf, index=False, header=False, na_rep='\\N')
    buf.seek(0)
    
    cols_csv = ", ".join(to_load)
    temp_table = f"temp_{table}_{int(time.time())}_{os.getpid()}"
    copy_sql = f"COPY {temp_table} ({cols_csv}) FROM STDIN WITH (FORMAT CSV, NULL '\\N')"
    
    cur = conn.cursor()
    
    try:
        # Create temporary table
        cur.execute(f"DROP TABLE IF EXISTS {temp_table}")
        cur.execute(f"CREATE TEMP TABLE {temp_table} (LIKE public.{table})")
        
        # Load data into temp table
        cur.copy_expert(copy_sql, buf)
        temp_rows = cur.rowcount
        
        # Conflict resolution for each table
        if table == "game_info":
            insert_sql = f"""
                INSERT INTO public.{table} ({cols_csv})
                SELECT {cols_csv} FROM {temp_table}
                ON CONFLICT (game_pk) DO UPDATE SET
                {', '.join([f"{col} = EXCLUDED.{col}" for col in to_load if col != 'game_pk'])}
            """
        elif table == "games":
            pk_cols = ["game_pk", "at_bat_number", "pitch_number"]
            if all(col in to_load for col in pk_cols):
                insert_sql = f"""
                    INSERT INTO public.{table} ({cols_csv})
                    SELECT {cols_csv} FROM {temp_table}
                    ON CONFLICT (game_pk, at_bat_number, pitch_number) DO NOTHING
                """
            else:
                insert_sql = f"""
                    INSERT INTO public.{table} ({cols_csv})
                    SELECT {cols_csv} FROM {temp_table}
                """
        elif table == "play_by_play":
            pk_cols = ["game_pk", "at_bat_index", "event_index"]
            if all(col in to_load for col in pk_cols):
                insert_sql = f"""
                    INSERT INTO public.{table} ({cols_csv})
                    SELECT {cols_csv} FROM {temp_table}
                    ON CONFLICT (game_pk, at_bat_index, event_index) DO NOTHING
                """
            else:
                insert_sql = f"""
                    INSERT INTO public.{table} ({cols_csv})
                    SELECT {cols_csv} FROM {temp_table}
                """
        else:
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
    """Load all files with comprehensive error handling and validation"""
    cur = conn.cursor()
    
    try:
        cur.execute("BEGIN")
        cur.execute("SET CONSTRAINTS ALL DEFERRED")
        
        total_rows_loaded = 0
        successful_loads = 0
        
        for file_path, table_name in files_and_tables:
            print(f"\n⏳ Loading {file_path.name} → {table_name}")
            
            try:
                # Read parquet file
                df = pd.read_parquet(file_path)
                print(f"   📊 Read parquet: {len(df)} rows, {len(df.columns)} columns")
                
                if df.empty:
                    print(f"   ⏭️ Skipping empty file: {file_path.name}")
                    successful_loads += 1
                    continue
                
                # Show sample of data for debugging
                print(f"   📋 Sample columns: {list(df.columns)[:10]}")
                
                # Data validation before loading
                if 'game_pk' in df.columns:
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
                continue
        
        # Commit transaction
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

def validate_schema(conn):
    """Validate that essential tables exist"""
    required_tables = ["games", "game_info"]
    optional_tables = ["play_by_play"]
    
    cur = conn.cursor()
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
    """)
    
    existing_tables = {row[0] for row in cur.fetchall()}
    missing_required = set(required_tables) - existing_tables
    
    if missing_required:
        print(f"❌ Missing required tables: {sorted(missing_required)}")
        print(f"   Run: python initialize_database.py")
        return False
    
    missing_optional = set(optional_tables) - existing_tables
    if missing_optional:
        print(f"💡 Missing optional tables: {sorted(missing_optional)}")
    
    print(f"✅ Schema validated: Required tables found")
    
    # Check for data in key tables
    for table in required_tables:
        if table in existing_tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            print(f"   📊 {table}: {count:,} records")
    
    return True

def show_loading_summary(files, successful_loads, failed_loads, total_rows_loaded):
    """Show comprehensive loading summary"""
    print(f"\n🎉 STREAMLINED loading complete!")
    print(f"📊 Summary:")
    print(f"   📁 Files processed: {len(files)}")
    print(f"   ✅ Successful loads: {successful_loads}")
    print(f"   ❌ Failed loads: {failed_loads}")
    print(f"   📈 Total rows loaded: {total_rows_loaded:,}")
    
    success_rate = (successful_loads / len(files)) * 100 if files else 0
    print(f"   📊 Success rate: {success_rate:.1f}%")
    
    if failed_loads > 0:
        print(f"\n⚠️  {failed_loads} files failed to load. Check error messages above.")
    else:
        print(f"\n🎯 All files loaded successfully!")
    
    print(f"\n🎯 STREAMLINED APPROACH:")
    print(f"   ✅ Your system: Collects impossible-to-get Statcast data")
    print(f"   🤖 Claude: Researches lineups, weather, umpires, trends") 
    print(f"   💰 Result: Complete betting analysis with minimal complexity")
    
    print(f"\n💡 Next steps:")
    print(f"   1. Run analysis: python py/simple_analysis.py")
    print(f"   2. Send data to Claude for betting insights")
    print(f"   3. Claude will research: {', '.join(CLAUDE_RESEARCH_TABLES)}")

def main():
    p = argparse.ArgumentParser(description="Streamlined MLB data loader - Claude optimized")
    p.add_argument(
        "--input-dir",
        default="stage",
        help="Folder containing parquet files"
    )
    p.add_argument(
        "--tables",
        nargs="*",
        help="Optional subset: games game_info play_by_play"
    )
    p.add_argument(
        "--validate-schema",
        action="store_true",
        help="Validate schema before loading"
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be loaded without actually loading"
    )
    args = p.parse_args()
    
    print(f"🚀 STREAMLINED MLB Data Loader - Claude Optimized")
    print(f"🎯 Focus: Loading only essential data (Statcast + context)")
    print(f"🤖 Claude will research: {', '.join(CLAUDE_RESEARCH_TABLES)}")
    
    # Connect to database
    try:
        conn = connect()
        print("✅ Connected to database")
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        return
    
    # Validate schema if requested
    if args.validate_schema:
        if not validate_schema(conn):
            print("❌ Schema validation failed")
            return
    
    # Setup input directory
    in_dir = Path(args.input_dir)
    in_dir.mkdir(exist_ok=True)
    
    files = list(in_dir.glob("*.parquet"))
    
    if not files:
        print(f"❌ No parquet files found in {in_dir}")
        print("💡 Make sure backfill has been run first:")
        print("   python enhanced_simple_backfill.py --start YYYY-MM-DD --end YYYY-MM-DD --real-data")
        return
    
    print(f"📁 Found {len(files)} parquet files in {in_dir}")
    
    # Show what files we found with streamlined mapping
    for file in files:
        table_name = get_table_mapping(file.stem)
        if table_name in CORE_TABLES:
            print(f"   ✅ {file.name} → {table_name}")
        else:
            print(f"   ⏭️ {file.name} → {table_name} (SKIPPED - Claude will research)")
    
    # Filter to only core tables
    def is_core_table(f):
        stem = f.stem
        table_name = get_table_mapping(stem)
        return table_name in CORE_TABLES
    
    core_files = [f for f in files if is_core_table(f)]
    skipped_files = [f for f in files if not is_core_table(f)]
    
    if skipped_files:
        print(f"\n⏭️ Skipping {len(skipped_files)} files (Claude will research this data):")
        for f in skipped_files:
            print(f"   • {f.name}")
    
    if not core_files:
        print(f"❌ No core table files found!")
        print(f"💡 Expected files: {[pattern for pattern in [CORE_TABLES[t]['file_pattern'] for t in CORE_TABLES]]}")
        return
    
    # Filter by tables if specified
    if args.tables:
        keep = set(args.tables)
        
        def matches_requested(f):
            stem = f.stem
            table_name = get_table_mapping(stem)
            return table_name in keep
        
        core_files = [f for f in core_files if matches_requested(f)]
        
        if not core_files:
            print(f"❌ None of the core files match requested tables: {keep}")
            return
        
        print(f"📋 Filtered to {len(core_files)} files for requested tables")
    
    # Sort files by dependency order
    core_files = sort_files_by_dependency_order(core_files)
    
    # Prepare file-to-table mapping
    files_and_tables = []
    for pq_file in core_files:
        stem = pq_file.stem
        table = get_table_mapping(stem)
        files_and_tables.append((pq_file, table))
    
    # Show dry run if requested
    if args.dry_run:
        print(f"\n🔍 DRY RUN - Would load these files:")
        for pq_file, table in files_and_tables:
            description = CORE_TABLES.get(table, {}).get('description', 'Unknown')
            print(f"   {pq_file.name} → {table} ({description})")
        print(f"\nTo actually load, run without --dry-run")
        return
    
    # Load all files in a single transaction
    try:
        successful_loads, total_rows_loaded = load_all_files_in_transaction(conn, files_and_tables)
        failed_loads = len(core_files) - successful_loads
        
    except Exception as e:
        print(f"❌ Loading failed: {e}")
        successful_loads = 0
        failed_loads = len(core_files)
        total_rows_loaded = 0
    
    # Show summary
    show_loading_summary(core_files, successful_loads, failed_loads, total_rows_loaded)
    
    conn.close()

if __name__ == "__main__":
    main()