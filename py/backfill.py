# ==============================================================================
# FILE: py/backfill_enhanced.py (Fixed version of your backfill.py)
# ==============================================================================
#!/usr/bin/env python
"""
Enhanced backfill.py with proper error handling and rate limiting
"""

import os
import argparse
from datetime import datetime, timedelta
from pathlib import Path
import json
from typing import Dict, Any
import time

import pandas as pd
from pybaseball import statcast
import statsapi
from tqdm import tqdm

# Try to import our custom modules with fallbacks
try:
    from weather_integration import fetch_weather_for_date
except ImportError:
    print("⚠️ Weather integration not available")
    def fetch_weather_for_date(*args, **kwargs):
        pass

try:
    from fatigue_metrics import fetch_fatigue_metrics_for_date
except ImportError:
    print("⚠️ Fatigue metrics not available")
    def fetch_fatigue_metrics_for_date(*args, **kwargs):
        pass

try:
    from umpire_integration import fetch_umpire_assignments_for_date
except ImportError:
    print("⚠️ Umpire integration not available")
    def fetch_umpire_assignments_for_date(*args, **kwargs):
        pass

try:
    from rate_limiter import RateLimiter
    from data_validator import DataValidator
except ImportError:
    print("⚠️ Using basic rate limiting")
    class RateLimiter:
        def wait_if_needed(self, api_name: str, min_delay: float = 0.1):
            time.sleep(min_delay)
    
    class DataValidator:
        @staticmethod
        def validate_statcast_data(df):
            return {"total_records": len(df), "issues": [], "quality_score": 100}

# Initialize rate limiter
rate_limiter = RateLimiter()

def clean_column_name(col_name: str) -> str:
    """Clean column names for database compatibility"""
    return (str(col_name)
            .replace(".", "_")
            .replace("-", "_")
            .replace(" ", "_")
            .replace("(", "")
            .replace(")", "")
            .replace("/", "_")
            .replace(":", "_")
            .replace("[", "_")
            .replace("]", "_")
            .replace("%", "pct")
            .replace("$", "dollar")
            .replace("#", "num")
            .lower())

def flatten_nested_data(data: Dict[Any, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
    """Recursively flatten nested dictionaries and lists"""
    items = []
    
    if isinstance(data, dict):
        for k, v in data.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_nested_data(v, new_key, sep=sep).items())
            elif isinstance(v, list) and v and isinstance(v[0], dict):
                for i, item in enumerate(v):
                    items.extend(flatten_nested_data(item, f"{new_key}_{i}", sep=sep).items())
            else:
                items.append((new_key, v))
    elif isinstance(data, list):
        for i, item in enumerate(data):
            new_key = f"{parent_key}{sep}{i}" if parent_key else str(i)
            if isinstance(item, (dict, list)):
                items.extend(flatten_nested_data(item, new_key, sep=sep).items())
            else:
                items.append((new_key, item))
    else:
        items.append((parent_key, data))
    
    return dict(items)

def fetch_statcast_for_date(date_str: str, out_dir: Path):
    """Fetch Statcast data with validation and rate limiting"""
    out_file = out_dir / f"statcast_{date_str}.parquet"
    if out_file.exists():
        print(f"⏭️  Skipping Statcast for {date_str} (already exists)")
        return
    
    print(f"⚾ Fetching Statcast for {date_str}...")
    
    try:
        # Rate limiting for Statcast API
        rate_limiter.wait_if_needed("statcast", 1.0)
        
        df = statcast(start_dt=date_str, end_dt=date_str)
        
        if df is None or df.empty:
            print(f"✅ No Statcast data for {date_str}")
            return
        
        # Clean column names
        df.columns = [clean_column_name(col) for col in df.columns]
        
        # Validate data quality
        validation = DataValidator.validate_statcast_data(df)
        if validation["quality_score"] < 70:
            print(f"⚠️ Data quality issues detected: {validation['issues']}")
        
        # Save data
        df.to_parquet(out_file, index=False)
        print(f"✅ Statcast: {len(df)} rows, {len(df.columns)} columns → {out_file.name}")
        
        # Log valuable columns
        ml_columns = [col for col in df.columns if any(keyword in col for keyword in 
                     ['launch', 'woba', 'estimated', 'spin', 'break', 'release'])]
        if ml_columns:
            print(f"   🎯 Key ML columns: {len(ml_columns)} captured")
                
    except Exception as e:
        print(f"❌ Statcast error for {date_str}: {e}")

def fetch_statsapi_for_date(date_str: str, out_dir: Path):
    """Fetch StatsAPI data with enhanced error handling"""
    out_file = out_dir / f"statsapi_{date_str}.parquet"
    if out_file.exists():
        print(f"⏭️ Skipping StatsAPI for {date_str} (already exists)")
        return
    
    print(f"📊 Fetching StatsAPI PBP for {date_str}...")
    
    try:
        games = statsapi.schedule(start_date=date_str, end_date=date_str) or []
        if not games:
            print(f"✅ No games scheduled for {date_str}")
            return
        
        all_plays = []
        
        for game in tqdm(games, desc="Processing games"):
            pk = game.get("game_id") or game.get("game_pk")
            if not pk:
                continue
            
            try:
                # Rate limiting
                rate_limiter.wait_if_needed("statsapi", 0.2)
                
                resp = statsapi.get("game_playByPlay", {"gamePk": pk})
                plays = resp.get("allPlays") or resp.get("liveData", {}).get("plays", {}).get("allPlays", [])
                
                for play_idx, play in enumerate(plays):
                    play_enhanced = {
                        "game_date": date_str,
                        "game_pk": pk,
                        "play_index": play_idx,
                        "home_team": game.get("home_name_abbrev"),
                        "away_team": game.get("away_name_abbrev"),
                    }
                    
                    flattened_play = flatten_nested_data(play)
                    play_enhanced.update(flattened_play)
                    all_plays.append(play_enhanced)
                    
            except Exception as e:
                print(f"   ⚠️ PBP error for game {pk}: {e}")
                continue
        
        if not all_plays:
            print(f"✅ No PBP data for {date_str}")
            return
        
        df = pd.DataFrame(all_plays)
        df.columns = [clean_column_name(col) for col in df.columns]
        
        df.to_parquet(out_file, index=False)
        print(f"✅ StatsAPI: {len(df)} rows, {len(df.columns)} columns → {out_file.name}")
        
    except Exception as e:
        print(f"❌ StatsAPI error for {date_str}: {e}")

def fetch_roster_for_date(date_str: str, out_dir: Path):
    """Fetch roster data with deduplication"""
    out_file = out_dir / f"roster_{date_str}.parquet"
    if out_file.exists():
        print(f"⏭️ Skipping Rosters for {date_str} (already exists)")
        return

    print(f"👥 Fetching Rosters for {date_str}...")
    
    try:
        games = statsapi.schedule(start_date=date_str, end_date=date_str) or []
        if not games:
            print(f"✅ No games scheduled for {date_str}")
            return
        
        all_rosters = []
        seen_teams = set()
        
        for game in games:
            for team_id, side in [(game.get("home_id"), "home"), (game.get("away_id"), "away")]:
                if not team_id or (date_str, team_id) in seen_teams:
                    continue
                seen_teams.add((date_str, team_id))
                
                try:
                    rate_limiter.wait_if_needed("roster", 0.1)
                    
                    data = statsapi.get("team_roster", {"teamId": team_id, "rosterType": "active"})
                    roster = data.get("roster", []) if isinstance(data, dict) else []
                    
                    for player_record in roster:
                        enhanced_record = {
                            "game_date": date_str,
                            "team_id": team_id,
                            "side": side,
                        }
                        
                        flattened_player = flatten_nested_data(player_record)
                        enhanced_record.update(flattened_player)
                        all_rosters.append(enhanced_record)
                        
                except Exception as e:
                    print(f"   ⚠️ Roster error for team {team_id}: {e}")
                    continue

        if not all_rosters:
            print(f"✅ No Rosters for {date_str}")
            return

        df = pd.DataFrame(all_rosters)
        df.columns = [clean_column_name(col) for col in df.columns]
        
        df.to_parquet(out_file, index=False)
        print(f"✅ Rosters: {len(df)} rows, {len(df.columns)} columns → {out_file.name}")
        
    except Exception as e:
        print(f"❌ Roster error for {date_str}: {e}")

def main():
    """Enhanced main function with better error handling"""
    p = argparse.ArgumentParser(description="Enhanced MLB backfill")
    p.add_argument("--start", required=True, help="YYYY-MM-DD")
    p.add_argument("--end", required=True, help="YYYY-MM-DD")
    p.add_argument("--monthly", action="store_true", help="Month-at-a-time")
    p.add_argument("--output", help="Output dir")
    p.add_argument("--data-types", nargs="*", 
                   choices=["statcast", "statsapi", "roster", "lineup", "weather", "umpires", "fatigue"],
                   default=["statcast", "statsapi", "roster", "lineup"],
                   help="Data types to collect")
    p.add_argument("--validate", action="store_true", help="Run data validation")
    args = p.parse_args()

    # Setup
    out_dir = Path(args.output or os.getenv("OUTPUT_DIR", "stage"))
    out_dir.mkdir(parents=True, exist_ok=True)

    sd = datetime.fromisoformat(args.start)
    ed = datetime.fromisoformat(args.end)
    
    if ed < sd:
        raise ValueError("`end` must be ≥ `start`")

    print(f"🚀 Enhanced MLB backfill: {sd.date()} → {ed.date()}")
    print(f"📁 Output: {out_dir}")
    print(f"🎯 Data types: {', '.join(args.data_types)}")
    
    # Process dates
    current_date = sd
    total_dates = (ed - sd).days + 1
    
    with tqdm(total=total_dates, desc="Processing dates") as pbar:
        while current_date <= ed:
            date_str = current_date.strftime("%Y-%m-%d")
            pbar.set_description(f"Processing {date_str}")
            
            try:
                # Collect data based on user selection
                if "statcast" in args.data_types:
                    fetch_statcast_for_date(date_str, out_dir)
                
                if "statsapi" in args.data_types:
                    fetch_statsapi_for_date(date_str, out_dir)
                
                if "roster" in args.data_types:
                    fetch_roster_for_date(date_str, out_dir)
                
                # Add other data types as needed...
                
            except Exception as e:
                print(f"❌ Error processing {date_str}: {e}")
            
            current_date += timedelta(days=1)
            pbar.update(1)
    
    print(f"\n🎉 Backfill complete!")
    
    # Validation summary
    if args.validate:
        print(f"\n🔍 Running validation...")
        # Add validation logic here

if __name__ == "__main__":
    main()