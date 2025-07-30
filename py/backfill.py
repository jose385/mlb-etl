#!/usr/bin/env python
"""
Enhanced backfill.py – Complete historical backfill for Statcast, StatsAPI PBP, Rosters, and Lineups.
Captures ALL available columns automatically for maximum ML value.

Usage:
    python backfill.py --start YYYY-MM-DD --end YYYY-MM-DD [--monthly] [--output DIR]

Flags:
  --start     YYYY-MM-DD to begin (inclusive)
  --end       YYYY-MM-DD to end   (inclusive)
  --monthly   If set, processes month-chunks instead of day by day
  --output    Output directory (parquet files) [default: $OUTPUT_DIR or "stage"]
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

# FIXED IMPORTS - Try to import our custom modules with proper error handling
try:
    from py.imports import setup_imports
    setup_imports()
except ImportError:
    pass

try:
    from py.weather_integration import fetch_weather_for_date
    WEATHER_AVAILABLE = True
    print("✅ Weather integration loaded")
except ImportError as e:
    print(f"⚠️ Weather integration not available: {e}")
    WEATHER_AVAILABLE = False
    def fetch_weather_for_date(*args, **kwargs):
        print("Weather integration not available - skipping weather data")
        pass

try:
    from py.fatigue_metrics import fetch_fatigue_metrics_for_date
    FATIGUE_AVAILABLE = True
    print("✅ Fatigue metrics loaded")
except ImportError as e:
    print(f"⚠️ Fatigue metrics not available: {e}")
    FATIGUE_AVAILABLE = False
    def fetch_fatigue_metrics_for_date(*args, **kwargs):
        print("Fatigue metrics not available - skipping fatigue data")
        pass

try:
    from py.umpire_integration import fetch_umpire_assignments_for_date
    UMPIRE_AVAILABLE = True
    print("✅ Umpire integration loaded")
except ImportError as e:
    print(f"⚠️ Umpire integration not available: {e}")
    UMPIRE_AVAILABLE = False
    def fetch_umpire_assignments_for_date(*args, **kwargs):
        print("Umpire integration not available - skipping umpire data")
        pass

# Simple rate limiter class
class RateLimiter:
    def __init__(self):
        self.last_calls = {}
    
    def wait_if_needed(self, api_name: str, min_delay: float = 0.1):
        now = time.time()
        if api_name in self.last_calls:
            time_since_last = now - self.last_calls[api_name]
            if time_since_last < min_delay:
                sleep_time = min_delay - time_since_last
                time.sleep(sleep_time)
        self.last_calls[api_name] = time.time()

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
                # Handle list of dictionaries
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


def log_schema_info(df: pd.DataFrame, data_type: str, date_str: str):
    """Log schema information for monitoring"""
    print(f"📊 {data_type} - {len(df.columns)} columns captured for {date_str}")
    
    # Save schema info for tracking changes
    schema_info = {
        'date': date_str,
        'data_type': data_type,
        'column_count': len(df.columns),
        'columns': sorted(df.columns.tolist()),
        'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()}
    }
    
    schema_dir = Path("schemas")
    schema_dir.mkdir(exist_ok=True)
    
    schema_file = schema_dir / f"{data_type}_{date_str}_schema.json"
    with open(schema_file, 'w') as f:
        json.dump(schema_info, f, indent=2)

def should_collect_weather(date_str: str) -> bool:
    """Determine if we should collect weather for this date"""
    target_date = datetime.fromisoformat(date_str)
    today = datetime.now()
    
    # Only collect weather for dates within the last 5 days
    # (when current weather is somewhat representative)
    days_ago = (today - target_date).days
    
    if days_ago <= 5:
        return True
    else:
        print(f"⏭️ Skipping weather for {date_str} (historical date, current weather not representative)")
        return False


def fetch_statcast_for_date(date_str: str, out_dir: Path):
    """Fetch ALL Statcast columns - completely dynamic"""
    out_file = out_dir / f"statcast_{date_str}.parquet"
    if out_file.exists():
        print(f"⏭️  Skipping Statcast for {date_str} (already exists)")
        return
    
    print(f"🔄 Fetching Statcast for {date_str}...")
    
    try:
        # Rate limiting for Statcast API
        rate_limiter.wait_if_needed("statcast", 1.0)
        
        # Get ALL available columns from pybaseball
        df = statcast(start_dt=date_str, end_dt=date_str)
        
        if df is None or df.empty:
            print(f"✅ No Statcast data for {date_str}")
            return
        
        # Clean all column names for database compatibility
        df.columns = [clean_column_name(col) for col in df.columns]
        
        # Log schema information
        log_schema_info(df, "statcast", date_str)
        
        # Save with all columns
        df.to_parquet(out_file, index=False)
        print(f"✅ Statcast: Wrote {len(df)} rows, {len(df.columns)} columns → {out_file.name}")
        
        # Show sample of valuable columns captured
        valuable_cols = [col for col in df.columns if any(keyword in col for keyword in 
                        ['launch', 'woba', 'temp', 'wind', 'humidity', 'delta', 'estimated'])]
        if valuable_cols:
            print(f"🆕 Key ML columns captured: {valuable_cols[:5]}...")
                
    except Exception as e:
        print(f"❌ Statcast error for {date_str}: {e}")


def fetch_statsapi_for_date(date_str: str, out_dir: Path):
    """Fetch ALL Play-by-Play data with complete JSON flattening"""
    out_file = out_dir / f"statsapi_{date_str}.parquet"
    if out_file.exists():
        print(f"⏭️ Skipping StatsAPI for {date_str} (already exists)")
        return
    
    print(f"🔄 Fetching StatsAPI PBP for {date_str}...")
    
    games = statsapi.schedule(start_date=date_str, end_date=date_str) or []
    if not games:
        print(f"✅ No games scheduled for {date_str}")
        return
    
    all_plays = []
    
    for game in games:
        pk = game.get("game_id") or game.get("game_pk")
        if not pk:
            continue
        
        try:
            # Rate limiting
            rate_limiter.wait_if_needed("statsapi", 0.2)
            
            resp = statsapi.get("game_playByPlay", {"gamePk": pk})
            plays = resp.get("allPlays") or resp.get("liveData", {}).get("plays", {}).get("allPlays", [])
            
            for play_idx, play in enumerate(plays):
                # Add game metadata to each play
                play_enhanced = {
                    "game_date": date_str,
                    "game_pk": pk,
                    "play_index": play_idx,
                    "home_team": game.get("home_name_abbrev"),
                    "away_team": game.get("away_name_abbrev"),
                }
                
                # Completely flatten the play data to capture ALL nested fields
                flattened_play = flatten_nested_data(play)
                play_enhanced.update(flattened_play)
                
                all_plays.append(play_enhanced)
                
        except Exception as e:
            print(f"❌ PBP error for game {pk}@{date_str}: {e}")
    
    if not all_plays:
        print(f"✅ No PBP data for {date_str}")
        return
    
    # Create DataFrame with ALL flattened columns
    df = pd.DataFrame(all_plays)
    
    # Clean column names
    df.columns = [clean_column_name(col) for col in df.columns]
    
    # Log schema
    log_schema_info(df, "statsapi_pbp", date_str)
    
    df.to_parquet(out_file, index=False)
    print(f"✅ StatsAPI: Wrote {len(df)} rows, {len(df.columns)} columns → {out_file.name}")


def fetch_roster_for_date(date_str: str, out_dir: Path):
    """Fetch complete roster data with ALL player details"""
    out_file = out_dir / f"roster_{date_str}.parquet"
    if out_file.exists():
        print(f"⏭️ Skipping Rosters for {date_str} (already exists)")
        return

    print(f"🔄 Fetching Rosters for {date_str}...")
    
    games = statsapi.schedule(start_date=date_str, end_date=date_str) or []
    if not games:
        print(f"✅ No games scheduled for {date_str}")
        return
    
    all_rosters = []
    seen = set()
    
    for game in games:
        for team_id, side in ((game["home_id"], "home"), (game["away_id"], "away")):
            if (date_str, team_id) in seen:
                continue
            seen.add((date_str, team_id))
            
            try:
                # Rate limiting
                rate_limiter.wait_if_needed("roster", 0.1)
                
                # Get complete roster with full player details
                data = statsapi.get("team_roster", {"teamId": team_id, "rosterType": "active"})
                
                if isinstance(data, dict) and "roster" in data:
                    for player_record in data["roster"]:
                        # Add metadata
                        enhanced_record = {
                            "game_date": date_str,
                            "team_id": team_id,
                            "side": side,
                        }
                        
                        # Flatten ALL player data to capture everything
                        flattened_player = flatten_nested_data(player_record)
                        enhanced_record.update(flattened_player)
                        
                        all_rosters.append(enhanced_record)
                        
            except Exception as e:
                print(f"❌ Roster error for team {team_id}@{date_str}: {e}")

    if not all_rosters:
        print(f"✅ No Rosters for {date_str}")
        return

    df = pd.DataFrame(all_rosters)
    df.columns = [clean_column_name(col) for col in df.columns]
    
    log_schema_info(df, "roster", date_str)
    
    df.to_parquet(out_file, index=False)
    print(f"✅ Rosters: Wrote {len(df)} rows, {len(df.columns)} columns → {out_file.name}")


def fetch_lineup_for_date(date_str: str, out_dir: Path):
    """Fetch complete lineup data with full player and game details"""
    out_file = out_dir / f"lineup_{date_str}.parquet"
    if out_file.exists():
        print(f"⏭️ Skipping Lineups for {date_str} (already exists)")
        return

    print(f"🔄 Fetching Lineups for {date_str}...")
    
    games = statsapi.schedule(start_date=date_str, end_date=date_str) or []
    all_lineups = []
    
    for game in games:
        game_pk = game.get("game_id") or game.get("game_pk")
        if not game_pk:
            continue
            
        try:
            # Rate limiting
            rate_limiter.wait_if_needed("lineup", 0.1)
            
            # Get complete boxscore with all player details
            boxscore = statsapi.get("game_boxscore", {"gamePk": game_pk})
            
            teams = boxscore.get("teams", {})
            for side in ("home", "away"):
                team_data = teams.get(side, {})
                team_id = game.get(f"{side}_id")
                
                # Get batting order
                batters = team_data.get("batters", [])
                
                # Get complete player information
                players = team_data.get("players", {})
                
                for batting_order, player_id in enumerate(batters, start=1):
                    player_info = players.get(f"ID{player_id}", {})
                    
                    lineup_record = {
                        "game_date": date_str,
                        "game_pk": game_pk,
                        "team_id": team_id,
                        "side": side,
                        "batting_order": batting_order,
                        "player_id": player_id,
                    }
                    
                    # Flatten ALL player information to capture everything
                    flattened_player = flatten_nested_data(player_info)
                    lineup_record.update(flattened_player)
                    
                    all_lineups.append(lineup_record)
                    
        except Exception as e:
            print(f"❌ Lineup error for game {game_pk}@{date_str}: {e}")

    if not all_lineups:
        print(f"✅ No Lineups for {date_str}")
        return

    df = pd.DataFrame(all_lineups)
    df.columns = [clean_column_name(col) for col in df.columns]
    
    log_schema_info(df, "lineup", date_str)
    
    df.to_parquet(out_file, index=False)
    print(f"✅ Lineups: Wrote {len(df)} rows, {len(df.columns)} columns → {out_file.name}")


def compare_schemas(data_dir: str = "schemas"):
    """Compare schemas across dates to detect new columns"""
    schema_dir = Path(data_dir)
    if not schema_dir.exists():
        print("No schema directory found")
        return
    
    schema_files = list(schema_dir.glob("*.json"))
    if len(schema_files) < 2:
        print("Need at least 2 schema files to compare")
        return
    
    # Group by data type
    by_type = {}
    for f in schema_files:
        parts = f.stem.split("_")
        if len(parts) >= 3:
            data_type = "_".join(parts[:-1])  # everything except date
            if data_type not in by_type:
                by_type[data_type] = []
            by_type[data_type].append(f)
    
    # Compare each data type
    for data_type, files in by_type.items():
        if len(files) < 2:
            continue
            
        files.sort()
        latest = files[-1]
        previous = files[-2]
        
        with open(latest) as f:
            latest_schema = json.load(f)
        with open(previous) as f:
            previous_schema = json.load(f)
        
        latest_cols = set(latest_schema['columns'])
        previous_cols = set(previous_schema['columns'])
        
        new_cols = latest_cols - previous_cols
        removed_cols = previous_cols - latest_cols
        
        if new_cols or removed_cols:
            print(f"\n🔄 Schema changes detected for {data_type}:")
            if new_cols:
                print(f"  🆕 New columns: {sorted(new_cols)}")
            if removed_cols:
                print(f"  ❌ Removed columns: {sorted(removed_cols)}")


def main():
    p = argparse.ArgumentParser(description="Enhanced MLB backfill with complete column capture")
    p.add_argument("--start", required=True, help="YYYY-MM-DD")
    p.add_argument("--end", required=True, help="YYYY-MM-DD")
    p.add_argument("--monthly", action="store_true", help="Month-at-a-time")
    p.add_argument("--output", help="Output dir (parquet)")
    p.add_argument("--compare-schemas", action="store_true", help="Compare schemas for changes")
    p.add_argument("--data-types", nargs="*", 
                   choices=["statcast", "statsapi", "roster", "lineup", "weather", "umpires", "fatigue"],
                   default=["statcast", "statsapi", "roster", "lineup"],
                   help="Data types to collect")
    args = p.parse_args()

    if args.compare_schemas:
        compare_schemas()
        return

    out_dir = Path(args.output or os.getenv("OUTPUT_DIR", "stage"))
    out_dir.mkdir(parents=True, exist_ok=True)

    sd = datetime.fromisoformat(args.start)
    ed = datetime.fromisoformat(args.end)
    if ed < sd:
        raise ValueError("`end` must be ≥ `start`")

    print(f"🚀 Enhanced MLB backfill from {sd.date()} to {ed.date()} with COMPLETE column capture")
    print(f"📁 Output directory: {out_dir}")
    print(f"🎯 Capturing ALL available data for maximum ML value")
    print(f"📊 Data types: {', '.join(args.data_types)}")
    
    if args.monthly:
        cur = sd
        while cur <= ed:
            # Process month chunks
            month_end = (cur.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
            if month_end > ed:
                month_end = ed
            
            print(f"\n▶️ Processing: {cur.date()} → {month_end.date()}")
            
            d = cur
            while d <= month_end:
                ds = d.strftime("%Y-%m-%d")
                
                try:
                    if "statcast" in args.data_types:
                        fetch_statcast_for_date(ds, out_dir)
                    if "statsapi" in args.data_types:
                        fetch_statsapi_for_date(ds, out_dir)
                    if "roster" in args.data_types:
                        fetch_roster_for_date(ds, out_dir)
                    if "lineup" in args.data_types:
                        fetch_lineup_for_date(ds, out_dir)
                except Exception as e:
                    print(f"❌ Error processing {ds}: {e}")
                
                d += timedelta(days=1)
            cur = month_end + timedelta(days=1)
    else:
        d = sd
        total_days = (ed - sd).days + 1
        
        with tqdm(total=total_days, desc="Processing dates") as pbar:
            while d <= ed:
                ds = d.strftime("%Y-%m-%d")
                pbar.set_description(f"Processing {ds}")

                try:
                    # 1. Fetch weather data first (CONDITIONAL)
                    weather_api_key = os.getenv("OPENWEATHER_API_KEY")
                    if WEATHER_AVAILABLE and weather_api_key and should_collect_weather(ds) and "weather" in args.data_types:
                        fetch_weather_for_date(ds, out_dir, weather_api_key)
                    elif WEATHER_AVAILABLE and weather_api_key and "weather" in args.data_types:
                        print(f"⏭️ Skipping weather for {ds} (too old for current weather API)")
                    elif "weather" in args.data_types:
                        print(f"⚠️ No OPENWEATHER_API_KEY set - skipping weather for {ds}")
                    
                    # 2. Fetch core baseball data
                    if "statcast" in args.data_types:
                        fetch_statcast_for_date(ds, out_dir)
                    if "statsapi" in args.data_types:
                        fetch_statsapi_for_date(ds, out_dir)
                    if "roster" in args.data_types:
                        fetch_roster_for_date(ds, out_dir)
                    if "lineup" in args.data_types:
                        fetch_lineup_for_date(ds, out_dir)

                    # 3. Fetch umpire assignments (CONDITIONAL)
                    if UMPIRE_AVAILABLE and "umpires" in args.data_types:
                        fetch_umpire_assignments_for_date(ds, out_dir)

                    # 4. Fatigue metrics LAST (CONDITIONAL)
                    if FATIGUE_AVAILABLE and "fatigue" in args.data_types:
                        fetch_fatigue_metrics_for_date(ds, out_dir)
                    
                except Exception as e:
                    print(f"❌ Error processing {ds}: {e}")
                
                d += timedelta(days=1)
                pbar.update(1)

    print("\n🎉 Enhanced backfill complete with ALL columns captured!")
    print(f"📊 Expected improvements:")
    print(f"   • Statcast: 100+ columns (vs ~25 in basic version)")
    print(f"   • StatsAPI: 50+ columns (vs ~20 in basic version)")
    print(f"   • Roster: 30+ columns (vs ~7 in basic version)")
    print(f"   • Lineup: 40+ columns (vs ~7 in basic version)")
    if UMPIRE_AVAILABLE:
        print(f"   • Umpires: 25+ columns (NEW - critical for totals betting)")
    if WEATHER_AVAILABLE:
        print(f"   • Weather: 15+ columns (air density, wind components)")
    if FATIGUE_AVAILABLE:
        print(f"   • Fatigue: 20+ columns (player rest and workload)")
    print(f"\n💡 Run with --compare-schemas to see what new columns were discovered")


if __name__ == "__main__":
    main()