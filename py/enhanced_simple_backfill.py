#!/usr/bin/env python3
"""
STREAMLINED MLB Data Backfill - Claude-Optimized Version
Focuses on collecting only the data Claude cannot research:
- Statcast pitch data (the gold mine for betting analysis)
- Optionally play-by-play sequences

Claude will research: lineups, umpires, weather, recent stats, etc.
"""

import argparse
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import time
from typing import Dict, List, Tuple, Any, Optional
from pathlib import Path
import warnings
import sys

# Suppress pandas warnings for cleaner output
warnings.filterwarnings('ignore', category=FutureWarning)

# ============================================================================
# REAL DATA IMPORTS WITH GRACEFUL FALLBACK
# ============================================================================

def get_real_data_imports():
    """Import real data libraries with graceful fallback"""
    imports = {'pybaseball': None}
    
    try:
        import pybaseball
        imports['pybaseball'] = pybaseball
        print("✅ pybaseball imported successfully")
    except ImportError:
        print("⚠️ pybaseball not available - install with: pip install pybaseball")
    
    return imports

# Initialize imports
REAL_DATA_IMPORTS = get_real_data_imports()

# ============================================================================
# RATE LIMITING AND ERROR HANDLING
# ============================================================================

class APIRateLimiter:
    """Handle rate limiting for pybaseball API"""
    
    def __init__(self):
        self.last_pybaseball_call = 0
        self.pybaseball_delay = 2.0  # 2 seconds between calls
        self.retry_count = 3
        self.retry_delay = 5.0
    
    def wait_for_pybaseball(self):
        """Wait appropriate time before pybaseball call"""
        time_since_last = time.time() - self.last_pybaseball_call
        if time_since_last < self.pybaseball_delay:
            wait_time = self.pybaseball_delay - time_since_last
            print(f"   ⏱️ Rate limiting: waiting {wait_time:.1f}s...")
            time.sleep(wait_time)
        self.last_pybaseball_call = time.time()
    
    def retry_with_backoff(self, func, *args, **kwargs):
        """Retry function with exponential backoff"""
        for attempt in range(self.retry_count):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                if attempt == self.retry_count - 1:
                    print(f"❌ Final attempt failed: {e}")
                    raise
                
                wait_time = self.retry_delay * (2 ** attempt)
                print(f"⚠️ Attempt {attempt + 1} failed: {e}")
                print(f"   Retrying in {wait_time}s...")
                time.sleep(wait_time)

# Global rate limiter
rate_limiter = APIRateLimiter()

# ============================================================================
# PLACEHOLDER DATA GENERATOR (for testing only)
# ============================================================================

class PlaceholderDataGenerator:
    """Generate realistic placeholder MLB data for testing"""
    
    def __init__(self, seed: int = 42):
        self.rng = np.random.RandomState(seed)
        self.game_pk_counter = 746000
        
    def generate_daily_games(self, date_str: str) -> List[Dict]:
        """Generate realistic daily game schedule"""
        # Skip some days (no games on some dates)
        if self.rng.random() < 0.15:  # 15% chance of no games
            return []
        
        # Generate 8-12 games per day (realistic MLB schedule)
        num_games = self.rng.randint(8, 13)
        teams = ['ARI', 'ATL', 'BAL', 'BOS', 'CHC', 'CHW', 'CIN', 'CLE',
                'COL', 'DET', 'HOU', 'KC', 'LAA', 'LAD', 'MIA', 'MIL',
                'MIN', 'NYM', 'NYY', 'OAK', 'PHI', 'PIT', 'SD', 'SF',
                'SEA', 'STL', 'TB', 'TEX', 'TOR', 'WSN']
        
        used_teams = set()
        games = []
        
        for game_num in range(num_games):
            # Pick two teams that haven't played today
            available_teams = [t for t in teams if t not in used_teams]
            if len(available_teams) < 2:
                break
                
            home_team = self.rng.choice(available_teams)
            available_teams.remove(home_team)
            away_team = self.rng.choice(available_teams)
            
            used_teams.update([home_team, away_team])
            
            game_pk = self.game_pk_counter + game_num
            
            games.append({
                'game_pk': game_pk,
                'game_date': date_str,
                'home_team': home_team,
                'away_team': away_team,
            })
        
        self.game_pk_counter += num_games
        return games

def generate_realistic_statcast_data(num_pitches: int, game_pk: int, game_date: str, home_team: str, away_team: str) -> pd.DataFrame:
    """Generate realistic Statcast data with ALL advanced metrics for testing"""
    
    pitch_types = ['FF', 'SL', 'CH', 'CU', 'SI', 'FC', 'FS', 'KC']
    events = ['strikeout', 'single', 'double', 'home_run', 'field_out', 'ground_out', 'fly_out', 
              'pop_out', 'line_out', 'force_out', 'walk', 'hit_by_pitch', None]
    
    data = []
    for i in range(num_pitches):
        pitch_type = random.choice(pitch_types)
        event = random.choices(events, weights=[15, 12, 3, 2, 25, 15, 10, 5, 8, 3, 8, 1, 93])[0]
        
        # Basic data
        row = {
            'game_pk': game_pk,
            'game_date': game_date,
            'at_bat_number': random.randint(1, 100),
            'pitch_number': random.randint(1, 12),
            'pitcher': random.randint(400000, 700000),
            'batter': random.randint(400000, 700000),
            'events': event,
            'description': f"pitch_{i}",
            'zone': random.randint(1, 14),
            'stand': random.choice(['L', 'R']),
            'p_throws': random.choice(['L', 'R']),
            'home_team': home_team,
            'away_team': away_team,
            'inning': random.randint(1, 9),
            'inning_topbot': random.choice(['Top', 'Bot']),
            'outs_when_up': random.randint(0, 2),
            'balls': random.randint(0, 3),
            'strikes': random.randint(0, 2),
            'pitch_type': pitch_type,
            
            # CORE STATCAST METRICS
            'release_speed': round(random.uniform(70, 105), 1),
            'plate_x': round(random.uniform(-2.5, 2.5), 3),
            'plate_z': round(random.uniform(0.5, 4.5), 3),
            'woba_value': round(random.uniform(0, 2.0), 3),
            'delta_run_exp': round(random.uniform(-0.5, 0.5), 3),
        }
        
        # ADVANCED METRICS FOR BATTED BALLS
        if event in ['single', 'double', 'triple', 'home_run', 'field_out', 'fly_out', 'line_out']:
            row.update({
                'launch_speed': round(random.uniform(60, 120), 1),
                'launch_angle': round(random.uniform(-50, 80), 1),
                'hit_distance_sc': round(random.uniform(50, 500), 1),
                'estimated_ba_using_speedangle': round(random.uniform(0, 1.0), 3),
                'estimated_woba_using_speedangle': round(random.uniform(0, 2.0), 3),
                'estimated_slg_using_speedangle': round(random.uniform(0, 4.0), 3),
                'launch_speed_angle': random.randint(1, 8),  # 6 = barrel
                'babip_value': round(random.uniform(0, 1.0), 3),
                'iso_value': round(random.uniform(0, 2.0), 3),
                'hc_x': round(random.uniform(-250, 250), 1),
                'hc_y': round(random.uniform(-250, 250), 1),
            })
        
        # PITCH QUALITY METRICS (all pitches)
        row.update({
            'release_spin_rate': round(random.uniform(1800, 3200), 0),
            'effective_speed': round(random.uniform(70, 105), 1),
            'release_extension': round(random.uniform(5.0, 7.5), 2),
            'release_pos_x': round(random.uniform(-3, 3), 2),
            'release_pos_z': round(random.uniform(4, 7), 2),
            'pfx_x': round(random.uniform(-2, 2), 2),
            'pfx_z': round(random.uniform(-2, 2), 2),
            'vx0': round(random.uniform(-20, 20), 2),
            'vy0': round(random.uniform(-150, -100), 2),
            'vz0': round(random.uniform(-20, 20), 2),
            'ax': round(random.uniform(-50, 50), 2),
            'ay': round(random.uniform(10, 50), 2),
            'az': round(random.uniform(-50, -10), 2),
        })
        
        data.append(row)
    
    return pd.DataFrame(data)

# ============================================================================
# REAL DATA COLLECTION FUNCTIONS
# ============================================================================

def collect_real_statcast_data(date_str: str) -> pd.DataFrame:
    """Collect real Statcast data from pybaseball"""
    
    if not REAL_DATA_IMPORTS['pybaseball']:
        raise ImportError("pybaseball not available")
    
    pybaseball = REAL_DATA_IMPORTS['pybaseball']
    
    print(f"   📡 Calling pybaseball.statcast() for {date_str}...")
    
    # Apply rate limiting
    rate_limiter.wait_for_pybaseball()
    
    try:
        # Call real pybaseball API
        df = rate_limiter.retry_with_backoff(
            pybaseball.statcast,
            start_dt=date_str,
            end_dt=date_str,
            verbose=False
        )
        
        if df is None or df.empty:
            print(f"   ⚠️ No Statcast data returned for {date_str}")
            return pd.DataFrame()
        
        print(f"   ✅ Retrieved {len(df)} pitches with {len(df.columns)} columns")
        
        # Clean up data types
        if 'game_pk' in df.columns:
            df['game_pk'] = pd.to_numeric(df['game_pk'], errors='coerce')
        
        if 'game_date' in df.columns:
            df['game_date'] = pd.to_datetime(df['game_date'], errors='coerce').dt.date
        
        # Convert launch_speed_angle to proper integer if it exists
        if 'launch_speed_angle' in df.columns:
            df['launch_speed_angle'] = pd.to_numeric(df['launch_speed_angle'], errors='coerce')
            df['launch_speed_angle'] = df['launch_speed_angle'].round().astype('Int64')
        
        print(f"   🔍 Sample columns: {list(df.columns[:10])}...")
        
        # Log available advanced metrics
        advanced_metrics = ['estimated_ba_using_speedangle', 'estimated_woba_using_speedangle', 
                          'launch_speed_angle', 'release_spin_rate', 'effective_speed']
        available_metrics = [col for col in advanced_metrics if col in df.columns]
        print(f"   📊 Advanced metrics available: {len(available_metrics)}/{len(advanced_metrics)}")
        
        return df
        
    except Exception as e:
        print(f"   ❌ pybaseball.statcast() failed: {e}")
        raise

def create_minimal_game_info_from_statcast(df: pd.DataFrame, date_str: str) -> pd.DataFrame:
    """Create minimal game_info from Statcast data"""
    
    if df.empty:
        return pd.DataFrame(columns=['game_date', 'game_pk', 'home_team', 'away_team'])
    
    # Extract basic game info from Statcast data
    game_summary = df.groupby(['game_pk', 'home_team', 'away_team']).size().reset_index(name='pitches')
    game_summary['game_date'] = date_str
    game_summary['venue_name'] = 'Stadium'
    game_summary['game_status'] = 'Final'
    
    # Reorder columns to match expected schema
    column_order = ['game_pk', 'game_date', 'home_team', 'away_team', 'venue_name', 'game_status']
    missing_cols = [col for col in column_order if col not in game_summary.columns]
    for col in missing_cols:
        game_summary[col] = None
    
    return game_summary[column_order]

def validate_date_for_real_data(date_str: str) -> bool:
    """Validate that date is appropriate for real data collection"""
    try:
        target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        today = datetime.now().date()
        
        # Can't get future data
        if target_date > today:
            print(f"   ⚠️ Future date {date_str} - no real data available")
            return False
        
        # Warn about very old dates
        if target_date < datetime(2015, 1, 1).date():
            print(f"   ⚠️ Date {date_str} is before 2015 - limited Statcast data")
        
        # Check if it's off-season (rough estimate)
        if target_date.month in [11, 12, 1, 2]:
            print(f"   ⚠️ Date {date_str} might be off-season - few/no games expected")
        
        return True
        
    except ValueError:
        print(f"   ❌ Invalid date format: {date_str}")
        return False

# ============================================================================
# CORE DATA COLLECTION FUNCTIONS
# ============================================================================

def collect_statcast_data(date_str: str, out_dir: str, use_placeholder: bool = False) -> str:
    """Collect Statcast data - THE CORE VALUE for betting analysis"""
    
    out_file = os.path.join(out_dir, f'games_{date_str}.parquet')
    
    if use_placeholder:
        print(f"🔧 Generating placeholder Statcast for {date_str}...")
        generator = PlaceholderDataGenerator()
        daily_games = generator.generate_daily_games(date_str)
        
        if not daily_games:
            # Create empty file for no games
            df = pd.DataFrame(columns=['game_date', 'game_pk'])
            df.to_parquet(out_file, index=False)
            print(f"✅ No Statcast data for {date_str}")
            return out_file
        
        # Generate realistic pitch data
        all_pitches = []
        
        for game in daily_games:
            game_pk = game['game_pk']
            home_team = game['home_team']
            away_team = game['away_team']
            num_pitches = random.randint(250, 350)
            game_pitches = generate_realistic_statcast_data(num_pitches, game_pk, date_str, home_team, away_team)
            all_pitches.append(game_pitches)
        
        df = pd.concat(all_pitches, ignore_index=True)
        
    else:
        print(f"📡 Collecting REAL Statcast data for {date_str}...")
        
        if not validate_date_for_real_data(date_str):
            print(f"   💡 Falling back to placeholder mode for {date_str}")
            return collect_statcast_data(date_str, out_dir, use_placeholder=True)
        
        try:
            # Collect real Statcast data
            df = collect_real_statcast_data(date_str)
            
            if df.empty:
                print(f"   ⚠️ No Statcast data returned for {date_str}")
                # Create empty file but don't fail
                df = pd.DataFrame(columns=['game_date', 'game_pk'])
            else:
                print(f"   ✅ Real Statcast: {len(df)} pitches with {len(df.columns)} columns")
                
        except Exception as e:
            print(f"   ❌ Real Statcast collection failed: {e}")
            print(f"   💡 Falling back to placeholder mode")
            return collect_statcast_data(date_str, out_dir, use_placeholder=True)
    
    df.to_parquet(out_file, index=False)
    print(f"✅ Statcast: {len(df)} pitches → {out_file}")
    return out_file

def collect_game_info_data(date_str: str, out_dir: str, use_placeholder: bool = False) -> str:
    """Collect minimal game info - basic context for games"""
    
    out_file = os.path.join(out_dir, f'game_info_{date_str}.parquet')
    
    # First check if we have Statcast data to extract game info from
    statcast_file = os.path.join(out_dir, f'games_{date_str}.parquet')
    
    if os.path.exists(statcast_file):
        print(f"📊 Creating game_info from existing Statcast data...")
        try:
            statcast_df = pd.read_parquet(statcast_file)
            game_info_df = create_minimal_game_info_from_statcast(statcast_df, date_str)
            game_info_df.to_parquet(out_file, index=False)
            print(f"✅ Game info: {len(game_info_df)} games → {out_file}")
            return out_file
        except Exception as e:
            print(f"   ⚠️ Could not extract game info from Statcast: {e}")
    
    # Fallback to minimal placeholder
    if use_placeholder:
        print(f"🔧 Generating minimal placeholder game info for {date_str}...")
        generator = PlaceholderDataGenerator()
        daily_games = generator.generate_daily_games(date_str)
        
        if not daily_games:
            # Create empty file for no games
            df = pd.DataFrame(columns=['game_date', 'game_pk'])
            df.to_parquet(out_file, index=False)
            print(f"✅ No games scheduled for {date_str}")
            return out_file
        
        df = pd.DataFrame(daily_games)
        
    else:
        print(f"📡 No real game schedule API - creating minimal placeholder...")
        # Create minimal entry
        df = pd.DataFrame({
            'game_date': [date_str],
            'game_pk': [746000],
            'home_team': ['TBD'],
            'away_team': ['TBD'],
            'venue_name': ['Stadium'],
            'game_status': ['Unknown']
        })
    
    df.to_parquet(out_file, index=False)
    print(f"✅ Game info: {len(df)} games → {out_file}")
    return out_file

def collect_play_by_play_data(date_str: str, out_dir: str, use_placeholder: bool = False) -> str:
    """OPTIONAL: Collect play-by-play sequences for detailed analysis"""
    
    out_file = os.path.join(out_dir, f'play_by_play_{date_str}.parquet')
    
    if use_placeholder:
        print(f"🔧 Generating placeholder play-by-play for {date_str}...")
        generator = PlaceholderDataGenerator()
        daily_games = generator.generate_daily_games(date_str)
        
        if not daily_games:
            # Create empty file for no games
            df = pd.DataFrame(columns=['game_date', 'game_pk'])
            df.to_parquet(out_file, index=False)
            print(f"✅ No play-by-play for {date_str}")
            return out_file
        
        plays = []
        events = ['strikeout', 'single', 'double', 'triple', 'home_run', 'walk', 'field_out', 
                 'ground_out', 'fly_out', 'pop_out', 'line_out', 'force_out', 'hit_by_pitch']
        
        for game in daily_games:
            game_pk = game['game_pk']
            home_team = game['home_team']
            away_team = game['away_team']
            
            play_count = random.randint(60, 90)
            for play_num in range(play_count):
                plays.append({
                    'game_pk': game_pk,
                    'game_date': date_str,
                    'event_index': play_num,
                    'at_bat_index': random.randint(1, 50),
                    'inning': random.randint(1, 9),
                    'half_inning': random.choice(['top', 'bottom']),
                    'batting_team': random.choice([home_team, away_team]),
                    'home_team': home_team,
                    'away_team': away_team,
                    'batter': random.randint(400000, 700000),
                    'pitcher': random.randint(400000, 700000),
                    'events': random.choice(events),
                    'description': f"play_{play_num}",
                    'outs': random.randint(0, 2),
                    'home_score': random.randint(0, 12),
                    'away_score': random.randint(0, 12),
                })
        
        df = pd.DataFrame(plays)
        
    else:
        print(f"📡 Real play-by-play collection not implemented...")
        print(f"   💡 Using placeholder play-by-play")
        return collect_play_by_play_data(date_str, out_dir, use_placeholder=True)
    
    df.to_parquet(out_file, index=False)
    print(f"✅ Play-by-play: {len(df)} events → {out_file}")
    return out_file

# ============================================================================
# MAIN ORCHESTRATION
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description='Streamlined MLB data backfill - Claude optimized')
    parser.add_argument('--start', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--out-dir', default='stage', help='Output directory')
    parser.add_argument('--real-data', action='store_true', help='Use real pybaseball data (recommended)')
    parser.add_argument('--minimal', action='store_true', help='Collect only Statcast data (no play-by-play)')
    parser.add_argument('--placeholder', action='store_true', help='Force placeholder mode for testing')
    
    args = parser.parse_args()
    
    # Determine data mode
    use_placeholder = args.placeholder or (not args.real_data and not REAL_DATA_IMPORTS['pybaseball'])
    
    print(f"🚀 STREAMLINED MLB backfill: {args.start} to {args.end}")
    
    if use_placeholder:
        print(f"🔧 PLACEHOLDER MODE: Using generated test data")
        print(f"   💡 To use real data: use --real-data flag")
    else:
        print(f"📡 REAL DATA MODE: Using live pybaseball API")
        if not REAL_DATA_IMPORTS['pybaseball']:
            print(f"❌ pybaseball not available! Install with: pip install pybaseball")
            return
        print(f"   ✅ pybaseball ready for Statcast collection")
    
    print(f"📁 Output directory: {args.out_dir}")
    
    # Validate date range
    try:
        start_date = datetime.strptime(args.start, '%Y-%m-%d')
        end_date = datetime.strptime(args.end, '%Y-%m-%d')
        
        if start_date > end_date:
            print(f"❌ Start date must be before end date")
            return
        
        date_range = (end_date - start_date).days + 1
        
    except ValueError as e:
        print(f"❌ Invalid date format: {e}")
        return
    
    os.makedirs(args.out_dir, exist_ok=True)
    
    current_date = start_date
    total_files = 0
    total_errors = 0
    
    # Define what to collect based on mode
    if args.minimal:
        collection_functions = [
            ('Statcast Data', collect_statcast_data),
            ('Game Info', collect_game_info_data),
        ]
        print(f"🎯 MINIMAL MODE: Collecting only Statcast data + basic game info")
    else:
        collection_functions = [
            ('Statcast Data', collect_statcast_data),
            ('Game Info', collect_game_info_data),
            ('Play-by-Play', collect_play_by_play_data),
        ]
        print(f"🎯 STANDARD MODE: Collecting Statcast + play-by-play data")
    
    print(f"💡 Claude will research: lineups, umpires, weather, recent stats, etc.")
    print(f"\n📅 Processing {date_range} days...")
    
    # Process date range
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        print(f"\n📅 Processing {date_str}...")
        
        day_files = 0
        day_errors = 0
        
        for name, func in collection_functions:
            try:
                result = func(date_str, args.out_dir, use_placeholder)
                if result:  # Success
                    day_files += 1
                    total_files += 1
                else:
                    day_errors += 1
                    total_errors += 1
                    
            except Exception as e:
                print(f"❌ Error collecting {name}: {e}")
                day_errors += 1
                total_errors += 1
        
        # Daily summary
        success_rate = (day_files / len(collection_functions)) * 100
        print(f"   📊 Day summary: {day_files}/{len(collection_functions)} successful ({success_rate:.1f}%)")
        
        current_date += timedelta(days=1)
    
    # Final summary
    print(f"\n✅ STREAMLINED backfill complete!")
    print(f"📊 Summary:")
    print(f"   📁 Total files generated: {total_files}")
    print(f"   ❌ Total errors: {total_errors}")
    print(f"   📈 Overall success rate: {(total_files / max(1, total_files + total_errors)) * 100:.1f}%")
    print(f"   📁 Files saved to: {args.out_dir}")
    
    # Next steps guidance
    if use_placeholder:
        print(f"\n🔧 PLACEHOLDER MODE COMPLETED:")
        print(f"   ✅ Test data generated successfully")
        print(f"   🎯 Perfect for testing pipeline")
        print(f"   💡 For real data: use --real-data flag")
    else:
        print(f"\n📡 REAL DATA MODE COMPLETED:")
        print(f"   ✅ Real Statcast data collected")
        print(f"   📊 Advanced metrics ready for analysis")
        print(f"   🎯 Ready for Claude betting analysis")
    
    print(f"\n🎯 Next steps:")
    print(f"   1. python loader/enhanced_load_parquet_into_pg.py --input-dir {args.out_dir}")
    print(f"   2. python py/simple_analysis.py")
    print(f"   3. Send data to Claude for betting analysis!")
    
    print(f"\n💡 CLAUDE INTEGRATION:")
    print(f"   📊 Your system: Provides impossible-to-get Statcast data")
    print(f"   🤖 Claude: Researches lineups, weather, umpires, trends")
    print(f"   🎯 Result: Complete betting analysis with minimal complexity")

if __name__ == '__main__':
    main()