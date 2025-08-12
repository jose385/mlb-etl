#!/usr/bin/env python3
"""
enhanced_load_parquet_into_pg.py – STREAMLINED: Loads 7 core tables only
REMOVED: Weather and venue_factors tables (Claude handles these)
ENHANCED: Better error handling for advanced Statcast metrics

Usage:
    python enhanced_load_parquet_into_pg.py [--input-dir DIR] [--tables T1 T2 ...]
"""
import os
import argparse
import io
import time
from pathlib import Path
from functools import wraps
from typing import List
import pandas as pd
import psycopg2
import numpy as np

def fix_nullable_integers(df):
    """Fix nullable integer columns that get converted to float during CSV export"""
    integer_columns = [
        'runner_on_1b', 'runner_on_2b', 'runner_on_3b',  # play_by_play table
        'person_id', 'team_id', 'batting_order',           # lineups table  
        'umpire_id',                                        # umpires table
        'game_pk', 'at_bat_number', 'pitch_number',        # games table
        'batter', 'pitcher'                                 # games table
    ]
    
    for col in integer_columns:
        if col in df.columns:
            if df[col].dtype == 'float64':
                df[col] = df[col].astype('Int64')
            elif df[col].dtype == 'Int64':
                pass
    
    return df

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

def get_streamlined_table_mapping(file_stem: str) -> str:
    """
    STREAMLINED: Maps file prefixes to 7 core tables only
    REMOVED: weather and venue_factors mappings
    """
    # Handle exact filename matches first
    exact_matches = {
        "recent_stats": "recent_stats",
    }
    
    if file_stem in exact_matches:
        return exact_matches[file_stem]
    
    # Handle date-suffixed files (like "games_2024-07-15")
    parts = file_stem.split("_")
    if len(parts) >= 2:
        last_part = parts[-1]
        if len(last_part) == 10 and last_part.count('-') == 2:
            base_name = "_".join(parts[:-1])
        else:
            base_name = parts[0]
    else:
        base_name = file_stem
    
    # STREAMLINED: 7-table schema mappings only
    table_mapping = {
        # Core 7 tables
        "games": "games",
        "play_by_play": "play_by_play", 
        "game_info": "game_info",
        "umpires": "umpires",
        "lineups": "lineups",
        "rosters": "rosters",
        "recent_stats": "recent_stats",
        
        # Alternative patterns
        "play": "play_by_play",
        "game": "game_info",
        "umpire": "umpires",
        "lineup": "lineups",
        "roster": "rosters",
        
        # Legacy support
        "statcast_games": "games",
        "pbp": "play_by_play",
        "starting_lineups": "lineups",
        "player_stats": "recent_stats",
        "statcast": "games",
        "statsapi": "play_by_play",
    }
    
    if base_name in table_mapping:
        return table_mapping[base_name]
    
    # Intelligent fallback
    print(f"⚠️ Warning: No table mapping found for '{file_stem}', attempting intelligent guess...")
    
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
    elif 'stat' in base_name.lower():
        return 'recent_stats'
    
    # Final fallback
    print(f"   Using base name: {base_name}")
    return base_name

def get_loading_order_priority(data_type: str) -> int:
    """
    STREAMLINED: Loading order for 7 tables
    Lower numbers = load first
    """
    loading_order = {
        # Load parent tables first (no dependencies)
        'game_info': 1,      # PRIMARY - All other tables reference this
        'rosters': 2,        # INDEPENDENT - Only references dates/teams
        
        # Load child tables second (depend on game_info)
        'games': 3,          # References game_info.game_pk
        'play_by_play': 4,   # References game_info.game_pk  
        'lineups': 5,        # References game_info.game_pk
        'umpires': 6,        # References game_info.game_pk
        'recent_stats': 7,   # INDEPENDENT but load last
    }
    
    return loading_order.get(data_type, 999)

def sort_files_by_dependency_order(files: List[Path]) -> List[Path]:
    """Sort files to load in dependency order"""
    def get_file_priority(file_path: Path) -> tuple:
        stem = file_path.stem
        table_name = get_streamlined_table_mapping(stem)
        priority = get_loading_order_priority(table_name)
        return (priority, stem)
    
    sorted_files = sorted(files, key=get_file_priority)
    
    print(f"📋 Loading order determined:")
    for file in sorted_files:
        stem = file.stem
        table_name = get_streamlined_table_mapping(stem)
        priority = get_loading_order_priority(table_name)
        print(f"   {priority:2d}. {file.name} → {table_name}")
    
    return sorted_files

@retry_database_operation(max_retries=3, delay=2)
def connect():
    """Connect to PostgreSQL with configuration validation"""
    try:
        from py.config import require_config
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

def validate_enhanced_statcast_data(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """
    ENHANCED: Validate and clean advanced Statcast data
    Handles all the new advanced metrics properly
    """
    if df.empty:
        return df
    
    df = df.copy()
    
    # Replace any infinite values with None
    df = df.replace([np.inf, -np.inf], None)
    
    # Handle NaN values appropriately
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].where(pd.notna(df[col]), None)
        elif df[col].dtype in ['float64', 'float32']:
            df[col] = df[col].where(pd.notna(df[col]), None)
    
    # ENHANCED: Table-specific validations for advanced metrics
    if table == 'games':
        # Ensure pitch numbers are positive
        if 'pitch_number' in df.columns:
            df['pitch_number'] = df['pitch_number'].clip(lower=1)
        
        if 'at_bat_number' in df.columns:
            df['at_bat_number'] = df['at_bat_number'].clip(lower=1)
        
        # ENHANCED: Validate advanced Statcast metrics
        if 'estimated_ba_using_speedangle' in df.columns:
            df['estimated_ba_using_speedangle'] = df['estimated_ba_using_speedangle'].clip(lower=0.000, upper=1.000)
        
        if 'estimated_woba_using_speedangle' in df.columns:
            df['estimated_woba_using_speedangle'] = df['estimated_woba_using_speedangle'].clip(lower=0.000, upper=2.000)
        
        if 'estimated_slg_using_speedangle' in df.columns:
            df['estimated_slg_using_speedangle'] = df['estimated_slg_using_speedangle'].clip(lower=0.000, upper=4.000)
        
        if 'launch_speed_angle' in df.columns:
            df['launch_speed_angle'] = df['launch_speed_angle'].clip(lower=1, upper=8)
        
        if 'release_spin_rate' in df.columns:
            df['release_spin_rate'] = df['release_spin_rate'].clip(lower=1000, upper=4000)
        
        if 'effective_speed' in df.columns:
            df['effective_speed'] = df['effective_speed'].clip(lower=50, upper=110)
        
        if 'babip_value' in df.columns:
            df['babip_value'] = df['babip_value'].clip(lower=0.000, upper=1.000)
        
        if 'iso_value' in df.columns:
            df['iso_value'] = df['iso_value'].clip(lower=0.000, upper=2.000)
    
    elif table == 'game_info':
        # Ensure scores are non-negative
        for score_col in ['home_score', 'away_score']:
            if score_col in df.columns:
                df[score_col] = df[score_col].clip(lower=0)
    
    elif table == 'recent_stats':
        # Ensure reasonable stat values
        if 'batting_avg' in df.columns:
            df['batting_avg'] = df['batting_avg'].clip(lower=0.000, upper=1.000)
        
        if 'era' in df.columns:
            df['era'] = df['era'].clip(lower=0.00, upper=20.00)
        
        if 'ops' in df.columns:
            df['ops'] = df['ops'].clip(lower=0.000, upper=3.000)
    
    return df

@retry_database_operation(max_retries=2, delay=1)
def load_table(conn, table: str, df: pd.DataFrame):
    """STREAMLINED: Load data into streamlined 7-table schema"""
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
    
    # ENHANCED: Validate advanced Statcast data
    df_to_load = validate_enhanced_statcast_data(df_to_load, table)
    
    # Enhanced data type conversions for PostgreSQL compatibility
    for col in df_to_load.columns:
        if df_to_load[col].isnull().all():
            continue
            
        # Convert boolean-like values
        if df_to_load[col].dtype == 'object':
            bool_map = {'true': True, 'false': False, 'True': True, 'False': False,
                       'yes': True, 'no': False, 'Y': True, 'N': False}
            bool_mask = df_to_load[col].isin(bool_map.keys())
            if bool_mask.any():
                df_to_load[col] = df_to_load[col].map(bool_map).fillna(df_to_load[col])
        
        # Handle nullable integer columns
        if str(df_to_load[col].dtype).startswith('Int'):
            pass
        
        # Handle date columns properly
        if col.endswith('_date') or 'date' in col.lower():
            try:
                if df_to_load[col].dtype == 'object':
                    df_to_load[col] = pd.to_datetime(df_to_load[col], errors='coerce').dt.date
            except:
                pass
        
        # Handle numeric columns with proper null handling
        if col in ['game_pk', 'person_id', 'pitcher', 'batter', 'team_id']:
            try:
                df_to_load[col] = pd.to_numeric(df_to_load[col], errors='coerce')
            except:
                pass
        
        # ENHANCED: Handle advanced Statcast metrics
        advanced_metrics = [
            'estimated_ba_using_speedangle', 'estimated_woba_using_speedangle',
            'estimated_slg_using_speedangle', 'release_spin_rate', 'effective_speed',
            'babip_value', 'iso_value', 'pfx_x', 'pfx_z', 'hc_x', 'hc_y'
        ]
        if col in advanced_metrics:
            try:
                df_to_load[col] = pd.to_numeric(df_to_load[col], errors='coerce')
                # Round to reasonable precision
                if col in ['estimated_ba_using_speedangle', 'estimated_woba_using_speedangle', 'babip_value']:
                    df_to_load[col] = df_to_load[col].round(3)
                elif col in ['pfx_x', 'pfx_z', 'hc_x', 'hc_y']:
                    df_to_load[col] = df_to_load[col].round(1)
                else:
                    df_to_load[col] = df_to_load[col].round(2)
            except:
                pass
    
    # Clean up any infinite values
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
        
        # STREAMLINED: Conflict resolution for 7 tables
        if table == "game_info":
            # Primary key: game_pk
            insert_sql = f"""
                INSERT INTO public.{table} ({cols_csv})
                SELECT {cols_csv} FROM {temp_table}
                ON CONFLICT (game_pk) DO UPDATE SET
                {', '.join([f"{col} = EXCLUDED.{col}" for col in to_load if col != 'game_pk'])}
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
            # Composite primary key: game_pk, at_bat_index, event_index
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
        elif table == "lineups":
            # Composite primary key: game_pk, team_id, batting_order
            pk_cols = ["game_pk", "team_id", "batting_order"]
            if all(col in to_load for col in pk_cols):
                insert_sql = f"""
                    INSERT INTO public.{table} ({cols_csv})
                    SELECT {cols_csv} FROM {temp_table}
                    ON CONFLICT (game_pk, team_id, batting_order) DO NOTHING
                """
            else:
                insert_sql = f"""
                    INSERT INTO public.{table} ({cols_csv})
                    SELECT {cols_csv} FROM {temp_table}
                """
        elif table == "rosters":
            # Composite primary key: game_date, team_id, person_id
            pk_cols = ["game_date", "team_id", "person_id"]
            if all(col in to_load for col in pk_cols):
                insert_sql = f"""
                    INSERT INTO public.{table} ({cols_csv})
                    SELECT {cols_csv} FROM {temp_table}
                    ON CONFLICT (game_date, team_id, person_id) DO NOTHING
                """
            else:
                insert_sql = f"""
                    INSERT INTO public.{table} ({cols_csv})
                    SELECT {cols_csv} FROM {temp_table}
                """
        elif table == "umpires":
            # Composite primary key: game_pk, umpire_id
            pk_cols = ["game_pk", "umpire_id"]
            if all(col in to_load for col in pk_cols):
                insert_sql = f"""
                    INSERT INTO public.{table} ({cols_csv})
                    SELECT {cols_csv} FROM {temp_table}
                    ON CONFLICT (game_pk, umpire_id) DO NOTHING
                """
            else:
                insert_sql = f"""
                    INSERT INTO public.{table} ({cols_csv})
                    SELECT {cols_csv} FROM {temp_table}
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
    """Load all files in a single transaction with deferred constraints"""
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
                # Read parquet file
                df = pd.read_parquet(file_path)
                integer_columns = ['runner_on_1b', 'runner_on_2b', 'runner_on_3b', 'person_id', 'team_id', 'umpire_id', 'game_pk',
                   'season_home_runs', 'season_rbi', 'season_strikeouts', 'at_bat_number', 'pitch_number', 'batter', 'pitcher']
                for col in integer_columns:
                    if col in df.columns and df[col].dtype in ['float64', 'Float64']:
                        df[col] = df[col].round().astype('Int64')
                print(f"   📊 Read parquet: {len(df)} rows, {len(df.columns)} columns")
                
                if df.empty:
                    print(f"   ⏭️ Skipping empty file: {file_path.name}")
                    successful_loads += 1
                    continue
                
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

def validate_streamlined_schema(conn):
    """Validate that streamlined 7-table schema exists"""
    expected_tables = [
        "games", "play_by_play", "umpires", 
        "lineups", "rosters", "game_info", "recent_stats"
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
        print(f"⚠️  Missing streamlined schema tables: {sorted(missing_tables)}")
        print(f"   Run: python initialize_database.py")
        return False
    
    print(f"✅ Streamlined schema validated: {len(expected_tables)} tables found")
    
    # Check for data in key tables
    data_check_tables = ["game_info", "games", "play_by_play"]
    for table in data_check_tables:
        if table in existing_tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            print(f"   📊 {table}: {count:,} records")
    
    return True

def show_loading_summary(files, successful_loads, failed_loads, total_rows_loaded):
    """Enhanced loading summary with streamlined info"""
    print(f"\n🎉 Streamlined loading complete!")
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
    
    print(f"\n✨ STREAMLINED FEATURES:")
    print(f"   🗑️ Removed weather & venue_factors tables (Claude handles these)")
    print(f"   📊 7 core tables with enhanced Statcast metrics")
    print(f"   🚀 Faster loading with reduced complexity")
    
    print(f"\n💡 Next steps:")
    print(f"   1. Run analysis: python py/simple_analysis.py")
    print(f"   2. Ask Claude for betting recommendations!")

def main():
    p = argparse.ArgumentParser(description="STREAMLINED MLB data loader for 7-table schema with advanced Statcast metrics")
    p.add_argument(
        "--input-dir",
        default="stage",
        help="Folder containing parquet files"
    )
    p.add_argument(
        "--tables",
        nargs="*",
        help="Optional subset: games play_by_play game_info umpires lineups rosters recent_stats"
    )
    p.add_argument(
        "--validate-schema",
        action="store_true",
        help="Validate streamlined schema before loading"
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
        
        # Show mode info
        mode = "PLACEHOLDER" if getattr(config, 'USE_PLACEHOLDER_DATA', True) else "REAL"
        print(f"🔧 Loading {mode} data into streamlined 7-table schema")
        
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        return
    
    # Connect to database
    try:
        conn = connect()
        print("✅ Connected to database")
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        return
    
    # Validate schema if requested
    if args.validate_schema:
        if not validate_streamlined_schema(conn):
            print("❌ Schema validation failed")
            return
    
    # Setup input directory
    in_dir = Path(args.input_dir)
    in_dir.mkdir(exist_ok=True)
    
    files = list(in_dir.glob("*.parquet"))
    
    if not files:
        print(f"❌ No parquet files found in {in_dir}")
        print("💡 Make sure backfill has been run first:")
        print("   python py/enhanced_simple_backfill.py --start YYYY-MM-DD --end YYYY-MM-DD")
        return
    
    print(f"📁 Found {len(files)} parquet files in {in_dir}")
    
    # Show what files we found
    for file in files:
        table_name = get_streamlined_table_mapping(file.stem)
        print(f"   • {file.name} → {table_name}")
    
    # Filter by tables if specified
    if args.tables:
        keep = set(args.tables)
        
        def matches_streamlined_schema(f):
            stem = f.stem
            table_name = get_streamlined_table_mapping(stem)
            return table_name in keep
        
        files = [f for f in files if matches_streamlined_schema(f)]
        
        if not files:
            print(f"❌ None of the files match requested tables: {keep}")
            return
        
        print(f"📋 Filtered to {len(files)} files for requested tables")
    
    # Sort files by dependency order
    files = sort_files_by_dependency_order(files)
    
    # Prepare file-to-table mapping
    files_and_tables = []
    for pq_file in files:
        stem = pq_file.stem
        table = get_streamlined_table_mapping(stem)
        files_and_tables.append((pq_file, table))
    
    # Show dry run if requested
    if args.dry_run:
        print(f"\n🔍 DRY RUN - Would load these files:")
        for pq_file, table in files_and_tables:
            print(f"   {pq_file.name} → {table}")
        print(f"\nTo actually load, run without --dry-run")
        return
    
    # Load all files in a single transaction
    try:
        successful_loads, total_rows_loaded = load_all_files_in_transaction(conn, files_and_tables)
        failed_loads = len(files) - successful_loads
        
        # Check for constraint violations after loading
        print(f"\n🔍 Checking for constraint violations...")
        
        with conn.cursor() as cur:
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
    
    # Show summary
    show_loading_summary(files, successful_loads, failed_loads, total_rows_loaded)
    
    conn.close()

if __name__ == "__main__":
    main()