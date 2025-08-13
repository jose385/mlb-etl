#!/usr/bin/env python3
"""
enhanced_load_parquet_into_pg.py – LIVE DATA READY: Comprehensive loader for all MLB data
ENHANCED: Handles ~40+ additional Statcast columns with proper type conversion
IMPROVED: Robust column mapping for real pybaseball data
FIXED: All data type issues including mixed types and nullable integers

Usage:
    python enhanced_load_parquet_into_pg.py [--input-dir DIR] [--tables T1 T2 ...]
"""
import os
import argparse
import io
import time
import re
from pathlib import Path
from functools import wraps
from typing import List, Dict, Any
import pandas as pd
import psycopg2
import numpy as np

def get_comprehensive_column_mappings():
    """
    ENHANCED: Comprehensive column mappings for live pybaseball data
    Returns dictionaries for proper data type handling
    """
    # EXPANDED: All integer columns from real Statcast/pybaseball data
    integer_columns = {
        # Core game identifiers
        'game_pk', 'at_bat_number', 'pitch_number', 'game_year',
        'batter', 'pitcher', 'person_id', 'team_id', 'fielder_2', 'fielder_3',
        'fielder_4', 'fielder_5', 'fielder_6', 'fielder_7', 'fielder_8', 'fielder_9',
        
        # Umpire and lineup data
        'umpire_id', 'batting_order', 'home_team_id', 'away_team_id',
        
        # Play-by-play runners and situations
        'runner_on_1b', 'runner_on_2b', 'runner_on_3b',
        'balls', 'strikes', 'outs_when_up', 'inning', 'inning_topbot',
        'at_bat_index', 'event_index', 'outs', 'post_away_score', 'post_home_score',
        
        # Statcast metrics that should be integers
        'launch_speed_angle', 'sweet_spot_code', 'barrel', 'hit_location',
        'bb_type', 'estimated_ba_using_speedangle_int', 'estimated_woba_using_speedangle_int',
        
        # Season and historical stats
        'season_home_runs', 'season_rbi', 'season_strikeouts', 'season_walks',
        'season_hits', 'season_doubles', 'season_triples', 'season_stolen_bases',
        
        # Score tracking
        'home_score', 'away_score', 'rbi', 'bat_score', 'fld_score',
        'post_bat_score', 'post_fld_score',
        
        # Game context
        'series_game_number', 'home_team_rest_days', 'away_team_rest_days',
        'attendance', 'game_length_minutes', 'delay_minutes',
        'home_wins_before', 'home_losses_before', 
        'away_wins_before', 'away_losses_before',
        
        # Recent performance stats
        'games_played', 'home_runs', 'rbis', 'stolen_bases', 
        'strikeouts', 'walks', 'hits_allowed', 'runs_allowed',
        'quality_starts', 'saves', 'blown_saves', 'holds',
        'consecutive_games', 'consecutive_appearances',
        
        # Additional Statcast integers
        'spin_dir', 'spin_rate_deprecated', 'break_angle_deprecated',
        'break_length_deprecated', 'zone', 'des_runs', 'game_type_id'
    }
    
    # EXPANDED: All float/numeric columns that need precision handling
    float_columns = {
        # Statcast velocities and distances
        'release_speed', 'release_pos_x', 'release_pos_y', 'release_pos_z',
        'effective_speed', 'release_spin_rate', 'release_extension',
        'hit_distance_sc', 'launch_speed', 'launch_angle',
        
        # Ball tracking
        'vx0', 'vy0', 'vz0', 'ax', 'ay', 'az', 'sz_top', 'sz_bot',
        'pfx_x', 'pfx_z', 'plate_x', 'plate_z', 'hc_x', 'hc_y',
        
        # Expected/estimated metrics
        'estimated_ba_using_speedangle', 'estimated_woba_using_speedangle',
        'estimated_slg_using_speedangle', 'woba_value', 'woba_denom',
        'babip_value', 'iso_value', 'launch_speed_angle_value',
        
        # Pitcher metrics
        'delta_home_win_exp', 'delta_run_exp', 'pfx_x_norm', 'pfx_z_norm',
        'break_angle', 'break_length', 'spin_axis',
        
        # Advanced metrics
        'hit_distance', 'spray_angle', 'exit_velocity_avg', 'hard_hit_percent',
        'barrel_percent', 'whiff_percent', 'xba', 'xslg', 'xwoba',
        
        # Player performance metrics
        'batting_avg', 'on_base_percent', 'slugging_percent', 'ops',
        'era', 'whip', 'k_percent', 'bb_percent', 'hr_fb_rate',
        'babip', 'lob_percent', 'gb_percent', 'fb_percent', 'ld_percent',
        'pop_percent', 'hard_percent', 'medium_percent', 'soft_percent'
    }
    
    # Date columns that need proper datetime handling
    date_columns = {
        'game_date', 'stat_date', 'roster_date', 'birth_date',
        'debut_date', 'final_game_date', 'transaction_date'
    }
    
    # Boolean columns
    boolean_columns = {
        'is_home_team', 'is_starting_pitcher', 'is_reliever', 'is_closer',
        'is_left_handed', 'is_right_handed', 'on_il', 'is_active',
        'if_fielding_alignment', 'of_fielding_alignment'
    }
    
    # String columns that should remain as text
    string_columns = {
        'player_name', 'team_name', 'description', 'des', 'events', 'type',
        'pitch_type', 'stand', 'p_throws', 'home_team', 'away_team',
        'venue_name', 'weather', 'wind', 'field_info', 'position',
        'game_type', 'series_description', 'umpire_name', 'umpire_position'
    }
    
    return {
        'integers': integer_columns,
        'floats': float_columns,
        'dates': date_columns,
        'booleans': boolean_columns,
        'strings': string_columns
    }

def normalize_column_name(col_name: str) -> str:
    """
    ENHANCED: Normalize column names to handle pybaseball variations
    Handles camelCase, snake_case, and various naming conventions
    """
    if not col_name:
        return col_name
    
    # Convert camelCase to snake_case
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', col_name)
    normalized = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
    
    # Handle common variations
    replacements = {
        'game_pk': 'game_pk',
        'gamepk': 'game_pk',
        'game_id': 'game_pk',
        'at_bat_number': 'at_bat_number',
        'atbatnumber': 'at_bat_number',
        'pitch_number': 'pitch_number',
        'pitchnumber': 'pitch_number',
        'player_id': 'person_id',
        'playerid': 'person_id',
        'mlb_id': 'person_id',
        'batter_id': 'batter',
        'pitcher_id': 'pitcher',
        'home_team_id': 'home_team_id',
        'away_team_id': 'away_team_id',
        'estimated_ba_using_speedangle': 'estimated_ba_using_speedangle',
        'estimated_woba_using_speedangle': 'estimated_woba_using_speedangle',
    }
    
    return replacements.get(normalized, normalized)

def fix_comprehensive_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    ENHANCED: Comprehensive data type fixing for live pybaseball data
    Handles all edge cases and data type conversions
    """
    if df.empty:
        return df
    
    df = df.copy()
    column_mappings = get_comprehensive_column_mappings()
    
    # Normalize column names first
    df.columns = [normalize_column_name(col) for col in df.columns]
    
    # Handle infinite values and NaN replacements
    df = df.replace([np.inf, -np.inf], None)
    
    # Fix integer columns with comprehensive error handling
    for col in df.columns:
        if col in column_mappings['integers']:
            try:
                # Handle mixed types and object columns
                if df[col].dtype == 'object':
                    # Try to extract numeric values from strings
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Convert to nullable integer
                if df[col].dtype in ['float64', 'Float64', 'object']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    df[col] = df[col].round().astype('Int64')
                elif df[col].dtype in ['int64', 'int32']:
                    df[col] = df[col].astype('Int64')
                    
            except Exception as e:
                print(f"   ⚠️ Warning: Could not convert {col} to integer: {e}")
                continue
    
    # Fix float columns with proper precision
    for col in df.columns:
        if col in column_mappings['floats']:
            try:
                if df[col].dtype == 'object':
                    # Handle string representations of floats
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Ensure proper float type
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
                # Apply appropriate precision based on column type
                if col in ['estimated_ba_using_speedangle', 'estimated_woba_using_speedangle', 
                          'babip_value', 'batting_avg', 'on_base_percent']:
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
    
    # Fix boolean columns
    for col in df.columns:
        if col in column_mappings['booleans']:
            try:
                # Handle various boolean representations
                if df[col].dtype == 'object':
                    bool_map = {
                        'true': True, 'false': False, 'True': True, 'False': False,
                        'yes': True, 'no': False, 'Y': True, 'N': False,
                        '1': True, '0': False, 1: True, 0: False,
                        'traditional': False, 'strategic': True, 'infield_shift': True,
                        'outfield_shift': True, 'standard': False
                    }
                    df[col] = df[col].map(bool_map).fillna(df[col])
                    
            except Exception as e:
                print(f"   ⚠️ Warning: Could not convert {col} to boolean: {e}")
                continue
    
    # Handle any remaining object columns that should be strings
    for col in df.columns:
        if col in column_mappings['strings'] or df[col].dtype == 'object':
            # Ensure strings are properly handled for PostgreSQL
            df[col] = df[col].astype(str)
            df[col] = df[col].replace('nan', None)
            df[col] = df[col].replace('<NA>', None)
            df[col] = df[col].replace('None', None)
            df[col] = df[col].where(pd.notna(df[col]), None)
    
    # Final cleanup
    df = df.replace([float('inf'), float('-inf')], None)
    
    return df

def get_enhanced_table_mapping(file_stem: str) -> str:
    """
    ENHANCED: Advanced table mapping for real pybaseball data
    Handles all variations and patterns from live data sources
    """
    # Normalize the file stem
    file_stem = file_stem.lower().replace('-', '_')
    
    # Handle exact filename matches first (including date patterns)
    exact_patterns = {
        'recent_stats': 'recent_stats',
        'player_stats': 'recent_stats',
        'batting_stats': 'recent_stats',
        'pitching_stats': 'recent_stats',
    }
    
    # Check for exact matches
    for pattern, table in exact_patterns.items():
        if pattern in file_stem:
            return table
    
    # Handle date-suffixed files (like "games_2024_07_15" or "statcast_2024-07-15")
    date_pattern = re.compile(r'(.+?)_(\d{4})[_-](\d{1,2})[_-](\d{1,2})$')
    match = date_pattern.match(file_stem)
    if match:
        base_name = match.group(1)
    else:
        # Handle other patterns
        parts = file_stem.split('_')
        base_name = parts[0] if parts else file_stem
    
    # ENHANCED: Comprehensive table mappings for pybaseball data
    table_mapping = {
        # Statcast data (primary source)
        'statcast': 'games',
        'statcast_data': 'games',
        'statcast_pitch': 'games',
        'statcast_pitches': 'games',
        'pitch_data': 'games',
        'pitch_by_pitch': 'games',
        
        # Game-level data
        'games': 'games',
        'game_data': 'games',
        'mlb_games': 'games',
        
        # Play-by-play data
        'play_by_play': 'play_by_play',
        'pbp': 'play_by_play',
        'play_data': 'play_by_play',
        'statsapi_pbp': 'play_by_play',
        'events': 'play_by_play',
        'at_bats': 'play_by_play',
        
        # Game info
        'game_info': 'game_info',
        'gameinfo': 'game_info',
        'schedule': 'game_info',
        'game_schedule': 'game_info',
        'games_info': 'game_info',
        
        # Lineups
        'lineups': 'lineups',
        'lineup': 'lineups',
        'starting_lineups': 'lineups',
        'batting_order': 'lineups',
        'starters': 'lineups',
        
        # Rosters
        'rosters': 'rosters',
        'roster': 'rosters',
        'team_roster': 'rosters',
        'active_roster': 'rosters',
        'player_roster': 'rosters',
        
        # Umpires
        'umpires': 'umpires',
        'umpire': 'umpires',
        'umpire_data': 'umpires',
        'game_umpires': 'umpires',
        
        # Recent stats
        'recent_stats': 'recent_stats',
        'player_stats': 'recent_stats',
        'batting_stats': 'recent_stats',
        'pitching_stats': 'recent_stats',
        'season_stats': 'recent_stats',
        'performance': 'recent_stats',
        'stats': 'recent_stats',
        
        # Alternative naming patterns
        'mlb_data': 'games',
        'baseball_data': 'games',
        'pitch_fx': 'games',
        'trackman': 'games',
    }
    
    # Try exact match first
    if base_name in table_mapping:
        return table_mapping[base_name]
    
    # Intelligent pattern matching
    for pattern, table in table_mapping.items():
        if pattern in base_name or base_name in pattern:
            return table
    
    # Advanced pattern recognition
    if any(word in base_name for word in ['pitch', 'statcast', 'trackman']):
        return 'games'
    elif any(word in base_name for word in ['play', 'event', 'at_bat', 'pbp']):
        return 'play_by_play'
    elif any(word in base_name for word in ['game', 'schedule']) and 'info' in base_name:
        return 'game_info'
    elif any(word in base_name for word in ['game', 'schedule']):
        return 'games'
    elif any(word in base_name for word in ['lineup', 'batting_order', 'starter']):
        return 'lineups'
    elif any(word in base_name for word in ['roster', 'player']):
        return 'rosters'
    elif any(word in base_name for word in ['umpire', 'official']):
        return 'umpires'
    elif any(word in base_name for word in ['stat', 'performance', 'batting', 'pitching']):
        return 'recent_stats'
    
    # Final fallback with warning
    print(f"⚠️ Warning: No table mapping found for '{file_stem}', using base name: {base_name}")
    return base_name

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

def get_loading_order_priority(data_type: str) -> int:
    """Loading order for 7 tables - lower numbers load first"""
    loading_order = {
        'game_info': 1,      # PRIMARY - All other tables reference this
        'rosters': 2,        # INDEPENDENT - Only references dates/teams
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
        table_name = get_enhanced_table_mapping(stem)
        priority = get_loading_order_priority(table_name)
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

def validate_enhanced_data(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """
    ENHANCED: Comprehensive data validation for live data
    Handles all edge cases and validates all metrics properly
    """
    if df.empty:
        return df
    
    df = df.copy()
    
    # Replace any infinite values with None
    df = df.replace([np.inf, -np.inf], None)
    
    # Table-specific validations
    if table == 'games':
        # Core pitch validation
        if 'pitch_number' in df.columns:
            df['pitch_number'] = pd.to_numeric(df['pitch_number'], errors='coerce')
            df['pitch_number'] = df['pitch_number'].clip(lower=1).astype('Int64')
        
        if 'at_bat_number' in df.columns:
            df['at_bat_number'] = pd.to_numeric(df['at_bat_number'], errors='coerce')
            df['at_bat_number'] = df['at_bat_number'].clip(lower=1).astype('Int64')
        
        # Statcast velocity validations
        if 'release_speed' in df.columns:
            df['release_speed'] = pd.to_numeric(df['release_speed'], errors='coerce')
            df['release_speed'] = df['release_speed'].clip(lower=50, upper=110)
        
        if 'launch_speed' in df.columns:
            df['launch_speed'] = pd.to_numeric(df['launch_speed'], errors='coerce')
            df['launch_speed'] = df['launch_speed'].clip(lower=20, upper=130)
        
        # Expected metrics validation
        for metric in ['estimated_ba_using_speedangle', 'estimated_woba_using_speedangle', 'babip_value']:
            if metric in df.columns:
                df[metric] = pd.to_numeric(df[metric], errors='coerce')
                df[metric] = df[metric].clip(lower=0.000, upper=1.000)
        
        if 'estimated_slg_using_speedangle' in df.columns:
            df['estimated_slg_using_speedangle'] = pd.to_numeric(df['estimated_slg_using_speedangle'], errors='coerce')
            df['estimated_slg_using_speedangle'] = df['estimated_slg_using_speedangle'].clip(lower=0.000, upper=4.000)
        
        # Launch angle validation
        if 'launch_angle' in df.columns:
            df['launch_angle'] = pd.to_numeric(df['launch_angle'], errors='coerce')
            df['launch_angle'] = df['launch_angle'].clip(lower=-90, upper=90)
        
        # Spin rate validation
        if 'release_spin_rate' in df.columns:
            df['release_spin_rate'] = pd.to_numeric(df['release_spin_rate'], errors='coerce')
            df['release_spin_rate'] = df['release_spin_rate'].clip(lower=1000, upper=4000)
        
        # Zone validation (1-14 for strike zone system)
        if 'zone' in df.columns:
            df['zone'] = pd.to_numeric(df['zone'], errors='coerce')
            df['zone'] = df['zone'].clip(lower=1, upper=14).astype('Int64')
    
    elif table == 'game_info':
        # Score validation
        for score_col in ['home_score', 'away_score']:
            if score_col in df.columns:
                df[score_col] = pd.to_numeric(df[score_col], errors='coerce')
                df[score_col] = df[score_col].clip(lower=0).astype('Int64')
        
        # Inning validation
        if 'inning' in df.columns:
            df['inning'] = pd.to_numeric(df['inning'], errors='coerce')
            df['inning'] = df['inning'].clip(lower=1, upper=20).astype('Int64')
    
    elif table == 'recent_stats':
        # Batting average validation
        if 'batting_avg' in df.columns:
            df['batting_avg'] = pd.to_numeric(df['batting_avg'], errors='coerce')
            df['batting_avg'] = df['batting_avg'].clip(lower=0.000, upper=1.000)
        
        # ERA validation
        if 'era' in df.columns:
            df['era'] = pd.to_numeric(df['era'], errors='coerce')
            df['era'] = df['era'].clip(lower=0.00, upper=50.00)
        
        # OPS validation
        if 'ops' in df.columns:
            df['ops'] = pd.to_numeric(df['ops'], errors='coerce')
            df['ops'] = df['ops'].clip(lower=0.000, upper=3.000)
        
        # Percentage stats validation
        for pct_col in ['k_percent', 'bb_percent', 'hard_hit_percent', 'barrel_percent']:
            if pct_col in df.columns:
                df[pct_col] = pd.to_numeric(df[pct_col], errors='coerce')
                df[pct_col] = df[pct_col].clip(lower=0.0, upper=100.0)
    
    return df

@retry_database_operation(max_retries=2, delay=1)
def load_table(conn, table: str, df: pd.DataFrame):
    """ENHANCED: Load data with comprehensive column handling"""
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
    
    # Enhanced validation
    df_to_load = validate_enhanced_data(df_to_load, table)
    
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
        elif table == "recent_stats":
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
        elif table == "lineups":
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
                import traceback
                print(f"   📋 Error details: {traceback.format_exc()}")
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
    """Validate that the 7-table schema exists"""
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
        print(f"⚠️  Missing schema tables: {sorted(missing_tables)}")
        print(f"   Run: python initialize_database.py")
        return False
    
    print(f"✅ Schema validated: {len(expected_tables)} tables found")
    
    # Check for data in key tables
    data_check_tables = ["game_info", "games", "play_by_play"]
    for table in data_check_tables:
        if table in existing_tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            print(f"   📊 {table}: {count:,} records")
    
    return True

def show_loading_summary(files, successful_loads, failed_loads, total_rows_loaded):
    """Show comprehensive loading summary"""
    print(f"\n🎉 LIVE DATA loading complete!")
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
    
    print(f"\n✨ ENHANCED FEATURES:")
    print(f"   🔧 Comprehensive column mapping for live pybaseball data")
    print(f"   📊 Enhanced handling of ~40+ Statcast metrics")
    print(f"   🛡️ Robust data type conversion and validation")
    print(f"   🚀 Improved error handling and recovery")
    
    print(f"\n💡 Next steps:")
    print(f"   1. Run analysis: python py/simple_analysis.py")
    print(f"   2. Check data quality with validation queries")
    print(f"   3. Ready for live betting analysis!")

def main():
    p = argparse.ArgumentParser(description="ENHANCED MLB live data loader with comprehensive Statcast support")
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
        help="Validate schema before loading"
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
        
        mode = "PLACEHOLDER" if getattr(config, 'USE_PLACEHOLDER_DATA', True) else "LIVE"
        print(f"🔧 Loading {mode} data with enhanced live data support")
        
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
        print("   python py/enhanced_simple_backfill.py --start YYYY-MM-DD --end YYYY-MM-DD")
        return
    
    print(f"📁 Found {len(files)} parquet files in {in_dir}")
    
    # Show what files we found with enhanced mapping
    for file in files:
        table_name = get_enhanced_table_mapping(file.stem)
        print(f"   • {file.name} → {table_name}")
    
    # Filter by tables if specified
    if args.tables:
        keep = set(args.tables)
        
        def matches_schema(f):
            stem = f.stem
            table_name = get_enhanced_table_mapping(stem)
            return table_name in keep
        
        files = [f for f in files if matches_schema(f)]
        
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
        table = get_enhanced_table_mapping(stem)
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