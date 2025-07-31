#!/usr/bin/env python3
"""
simple_backfill.py - Streamlined MLB data collection
Focuses on essential betting data: games, weather, umpires

Usage:
    python simple_backfill.py --start YYYY-MM-DD --end YYYY-MM-DD [--output DIR]
"""

import os
import argparse
import time
import requests
import math
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import statsapi
from pybaseball import statcast
from tqdm import tqdm

# Essential stadium coordinates for weather
STADIUM_LOCATIONS = {
    "Arizona Diamondbacks": {"lat": 33.4453, "lon": -112.0667},
    "Atlanta Braves": {"lat": 33.8906, "lon": -84.4677},
    "Baltimore Orioles": {"lat": 39.2840, "lon": -76.6217},
    "Boston Red Sox": {"lat": 42.3467, "lon": -71.0972},
    "Chicago White Sox": {"lat": 41.8299, "lon": -87.6338},
    "Chicago Cubs": {"lat": 41.9484, "lon": -87.6553},
    "Cincinnati Reds": {"lat": 39.5031, "lon": -84.3668},
    "Cleveland Guardians": {"lat": 41.4958, "lon": -81.6853},
    "Colorado Rockies": {"lat": 39.7559, "lon": -104.9942},
    "Detroit Tigers": {"lat": 42.3390, "lon": -83.0485},
    "Houston Astros": {"lat": 29.7570, "lon": -95.3555},
    "Kansas City Royals": {"lat": 39.0517, "lon": -94.4803},
    "Los Angeles Angels": {"lat": 33.8003, "lon": -117.8827},
    "Los Angeles Dodgers": {"lat": 34.0739, "lon": -118.2400},
    "Miami Marlins": {"lat": 25.7781, "lon": -80.2198},
    "Milwaukee Brewers": {"lat": 43.0280, "lon": -87.9712},
    "Minnesota Twins": {"lat": 44.9817, "lon": -93.2776},
    "New York Mets": {"lat": 40.7571, "lon": -73.8458},
    "New York Yankees": {"lat": 40.8296, "lon": -73.9262},
    "Oakland Athletics": {"lat": 37.7516, "lon": -122.2005},
    "Philadelphia Phillies": {"lat": 39.9061, "lon": -75.1665},
    "Pittsburgh Pirates": {"lat": 40.4469, "lon": -80.0057},
    "San Diego Padres": {"lat": 32.7073, "lon": -117.1566},
    "San Francisco Giants": {"lat": 37.7786, "lon": -122.3893},
    "Seattle Mariners": {"lat": 47.5914, "lon": -122.3326},
    "St. Louis Cardinals": {"lat": 38.6226, "lon": -90.1928},
    "Tampa Bay Rays": {"lat": 27.7682, "lon": -82.6534},
    "Texas Rangers": {"lat": 32.7513, "lon": -97.0830},
    "Toronto Blue Jays": {"lat": 43.6414, "lon": -79.3894},
    "Washington Nationals": {"lat": 38.8730, "lon": -77.0074}
}

def get_stadium_coords(team_name: str) -> Dict[str, float]:
    """Get stadium coordinates for team"""
    for stadium_team, coords in STADIUM_LOCATIONS.items():
        if any(word in stadium_team for word in team_name.split()) or \
           any(word in team_name for word in stadium_team.split()):
            return coords
    
    # Default coordinates (New York)
    return {"lat": 40.7128, "lon": -74.0060}

def rate_limit(last_call_time: float, min_delay: float = 0.5) -> float:
    """Simple rate limiting"""
    now = time.time()
    time_since_last = now - last_call_time
    if time_since_last < min_delay:
        time.sleep(min_delay - time_since_last)
    return time.time()

def fetch_game_data(date_str: str, out_dir: Path) -> bool:
    """Fetch basic game data using Statcast"""
    out_file = out_dir / f"games_{date_str}.parquet"
    if out_file.exists():
        print(f"⏭️ Skipping games for {date_str} (already exists)")
        return True
    
    print(f"⚾ Fetching game data for {date_str}...")
    
    try:
        # Get Statcast data - this includes game info and pitch data
        df = statcast(start_dt=date_str, end_dt=date_str)
        
        if df is None or df.empty:
            print(f"✅ No games for {date_str}")
            return True
        
        # Keep only essential columns for betting analysis
        essential_columns = [
            'game_date', 'game_pk', 'home_team', 'away_team',
            'inning', 'inning_topbot', 'batter', 'pitcher',
            'events', 'description', 'zone', 'balls', 'strikes',
            'release_speed', 'release_pos_x', 'release_pos_z',
            'pfx_x', 'pfx_z', 'plate_x', 'plate_z',
            'vx0', 'vy0', 'vz0', 'ax', 'ay', 'az',
            'sz_top', 'sz_bot', 'hit_location', 'bb_type',
            'hit_distance_sc', 'launch_speed', 'launch_angle',
            'effective_speed', 'release_spin_rate',
            'woba_value', 'estimated_woba_using_speedangle'
        ]
        
        # Keep only columns that exist in the data
        available_columns = [col for col in essential_columns if col in df.columns]
        df_filtered = df[available_columns].copy()
        
        # Clean column names
        df_filtered.columns = [col.lower().replace('.', '_') for col in df_filtered.columns]
        
        # Save to parquet
        df_filtered.to_parquet(out_file, index=False)
        print(f"✅ Games: {len(df_filtered)} rows, {len(df_filtered.columns)} columns → {out_file.name}")
        return True
        
    except Exception as e:
        print(f"❌ Game data error for {date_str}: {e}")
        return False

def fetch_weather_data(date_str: str, out_dir: Path, api_key: Optional[str] = None) -> bool:
    """Fetch weather data for all games"""
    out_file = out_dir / f"weather_{date_str}.parquet"
    if out_file.exists():
        print(f"⏭️ Skipping weather for {date_str} (already exists)")
        return True
    
    if not api_key:
        print(f"⚠️ No weather API key - skipping weather for {date_str}")
        return True
    
    # Only collect weather for recent dates (current weather is representative)
    target_date = datetime.fromisoformat(date_str)
    days_ago = (datetime.now() - target_date).days
    if days_ago > 7:
        print(f"⏭️ Skipping weather for {date_str} (too old for current weather)")
        return True
    
    print(f"🌤️ Fetching weather for {date_str}...")
    
    try:
        # Get games scheduled for the date
        games = statsapi.schedule(start_date=date_str, end_date=date_str) or []
        
        if not games:
            print(f"✅ No games scheduled for {date_str}")
            return True
        
        weather_records = []
        last_call = 0.0
        
        for game in games:
            game_pk = game.get("game_id") or game.get("game_pk")
            home_team = game.get("home_name", "")
            away_team = game.get("away_name", "")
            venue_name = game.get("venue_name", "")
            
            # Get stadium coordinates
            coords = get_stadium_coords(home_team)
            
            # Rate limit API calls
            last_call = rate_limit(last_call, 1.0)
            
            # Call weather API
            weather_data = get_weather_api_data(coords["lat"], coords["lon"], api_key)
            
            if weather_data:
                # Extract essential weather info
                main = weather_data.get("main", {})
                wind = weather_data.get("wind", {})
                
                temp_f = main.get("temp", 70)
                humidity = main.get("humidity", 50)
                wind_speed = wind.get("speed", 0)
                wind_deg = wind.get("deg", 0)
                pressure = main.get("pressure", 1013)
                
                weather_record = {
                    "game_date": date_str,
                    "game_pk": game_pk,
                    "home_team": home_team,
                    "away_team": away_team,
                    "venue_name": venue_name,
                    "stadium_lat": coords["lat"],
                    "stadium_lon": coords["lon"],
                    
                    # Essential weather for betting
                    "temperature_f": round(temp_f, 1),
                    "humidity_pct": humidity,
                    "wind_speed_mph": round(wind_speed, 1),
                    "wind_direction_deg": wind_deg,
                    "pressure_mb": pressure,
                    
                    # Simple derived metrics
                    "wind_helping_hr": 1 if 180 <= wind_deg <= 270 and wind_speed > 10 else 0,
                    "hot_weather": 1 if temp_f >= 80 else 0,
                    "cold_weather": 1 if temp_f <= 50 else 0,
                    "high_humidity": 1 if humidity >= 70 else 0,
                    
                    # Simple weather impact score (0-10)
                    "weather_impact": min(10, round(
                        abs(temp_f - 72) * 0.1 + 
                        abs(humidity - 50) * 0.05 + 
                        wind_speed * 0.2, 1))
                }
            else:
                # Default weather record if API fails
                weather_record = {
                    "game_date": date_str,
                    "game_pk": game_pk,
                    "home_team": home_team,
                    "away_team": away_team,
                    "venue_name": venue_name,
                    "stadium_lat": coords["lat"],
                    "stadium_lon": coords["lon"],
                    "temperature_f": 72,  # Default neutral
                    "humidity_pct": 50,
                    "wind_speed_mph": 5,
                    "wind_direction_deg": 0,
                    "pressure_mb": 1013,
                    "wind_helping_hr": 0,
                    "hot_weather": 0,
                    "cold_weather": 0,
                    "high_humidity": 0,
                    "weather_impact": 0
                }
            
            weather_records.append(weather_record)
        
        if weather_records:
            df = pd.DataFrame(weather_records)
            df.to_parquet(out_file, index=False)
            print(f"✅ Weather: {len(df)} records → {out_file.name}")
        
        return True
        
    except Exception as e:
        print(f"❌ Weather error for {date_str}: {e}")
        return False

def get_weather_api_data(lat: float, lon: float, api_key: str) -> Optional[Dict]:
    """Get weather data from OpenWeather API"""
    try:
        url = "http://api.openweathermap.org/data/2.5/weather"
        params = {
            'lat': lat,
            'lon': lon,
            'appid': api_key,
            'units': 'imperial'
        }
        
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response.json()
        
    except Exception as e:
        print(f"⚠️ Weather API error: {e}")
        return None

def fetch_umpire_data(date_str: str, out_dir: Path) -> bool:
    """Fetch umpire assignments for games"""
    out_file = out_dir / f"umpires_{date_str}.parquet"
    if out_file.exists():
        print(f"⏭️ Skipping umpires for {date_str} (already exists)")
        return True
    
    print(f"👨‍⚖️ Fetching umpire data for {date_str}...")
    
    try:
        # Get games for the date
        games = statsapi.schedule(start_date=date_str, end_date=date_str) or []
        
        if not games:
            print(f"✅ No games scheduled for {date_str}")
            return True
        
        umpire_records = []
        last_call = 0.0
        
        for game in games:
            game_pk = game.get("game_id") or game.get("game_pk")
            
            # Rate limit API calls
            last_call = rate_limit(last_call, 0.5)
            
            try:
                # Get detailed game data including officials
                game_data = statsapi.get("game", {"gamePk": game_pk})
                
                # Navigate to officials data
                officials = (game_data.get("liveData", {})
                           .get("boxscore", {})
                           .get("officials", []))
                
                for official in officials:
                    official_info = official.get("official", {})
                    position = official.get("officialType", "")
                    
                    if not official_info.get("id"):
                        continue
                    
                    umpire_record = {
                        "game_date": date_str,
                        "game_pk": game_pk,
                        "umpire_id": official_info.get("id"),
                        "umpire_name": official_info.get("fullName", "").strip(),
                        "position": position,
                        "first_name": official_info.get("firstName", ""),
                        "last_name": official_info.get("lastName", ""),
                        
                        # Initialize metrics (would be calculated from historical data)
                        "games_worked": 0,
                        "avg_runs_per_game": None,
                        "strike_rate": None,
                        "over_under_tendency": None
                    }
                    
                    umpire_records.append(umpire_record)
                    
            except Exception as e:
                print(f"⚠️ Error getting umpires for game {game_pk}: {e}")
                continue
        
        if umpire_records:
            df = pd.DataFrame(umpire_records)
            df.to_parquet(out_file, index=False)
            
            # Show summary
            home_plate_umps = df[df['position'] == 'Home Plate']
            print(f"✅ Umpires: {len(df)} total, {len(home_plate_umps)} home plate → {out_file.name}")
            
            if len(home_plate_umps) > 0:
                print(f"   👨‍⚖️ Home plate: {', '.join(home_plate_umps['umpire_name'].unique())}")
        else:
            print(f"✅ No umpire data for {date_str}")
        
        return True
        
    except Exception as e:
        print(f"❌ Umpire error for {date_str}: {e}")
        return False

def fetch_statsapi_data(date_str: str, out_dir: Path) -> bool:
    """Fetch essential play-by-play data for betting context"""
    out_file = out_dir / f"statsapi_{date_str}.parquet"
    if out_file.exists():
        print(f"⏭️ Skipping StatsAPI for {date_str} (already exists)")
        return True
    
    print(f"📊 Fetching StatsAPI play-by-play for {date_str}...")
    
    try:
        games = statsapi.schedule(start_date=date_str, end_date=date_str) or []
        
        if not games:
            print(f"✅ No games scheduled for {date_str}")
            return True
        
        play_records = []
        last_call = 0.0
        
        for game in games:
            game_pk = game.get("game_id") or game.get("game_pk")
            
            # Rate limit
            last_call = rate_limit(last_call, 0.5)
            
            try:
                # Get play-by-play data
                pbp_data = statsapi.get("game_playByPlay", {"gamePk": game_pk})
                plays = pbp_data.get("allPlays", [])
                
                for play_idx, play in enumerate(plays):
                    # Extract essential betting context (no over-flattening)
                    about = play.get("about", {})
                    result = play.get("result", {})
                    count = play.get("count", {})
                    runners = play.get("runners", [])
                    
                    play_record = {
                        "game_date": date_str,
                        "game_pk": game_pk,
                        "play_index": play_idx,
                        
                        # Game situation
                        "inning": about.get("inning"),
                        "half_inning": about.get("halfInning"),
                        "is_top_inning": about.get("isTopInning"),
                        "home_score": about.get("homeScore"),
                        "away_score": about.get("awayScore"),
                        
                        # Play details
                        "event": result.get("event"),
                        "event_type": result.get("eventType"),
                        "description": result.get("description"),
                        "rbi": result.get("rbi", 0),
                        "away_score_after": result.get("awayScore"),
                        "home_score_after": result.get("homeScore"),
                        
                        # Count context
                        "balls": count.get("balls"),
                        "strikes": count.get("strikes"),
                        "outs": count.get("outs"),
                        
                        # Runner context (simplified)
                        "runners_on_base": len(runners),
                        "risp": any(r.get("end", {}).get("base") in [2, 3] for r in runners),
                        
                        # Leverage indicators
                        "late_inning": about.get("inning", 0) >= 7,
                        "close_game": abs((about.get("homeScore", 0) or 0) - (about.get("awayScore", 0) or 0)) <= 3,
                        
                        # Players involved
                        "batter_id": play.get("batter", {}).get("id"),
                        "pitcher_id": play.get("pitcher", {}).get("id")
                    }
                    
                    play_records.append(play_record)
                    
            except Exception as e:
                print(f"⚠️ Error getting play-by-play for game {game_pk}: {e}")
                continue
        
        if play_records:
            df = pd.DataFrame(play_records)
            df.to_parquet(out_file, index=False)
            print(f"✅ StatsAPI: {len(df)} plays → {out_file.name}")
        else:
            print(f"✅ No play-by-play data for {date_str}")
        
        return True
        
    except Exception as e:
        print(f"❌ StatsAPI error for {date_str}: {e}")
        return False

def fetch_lineup_data(date_str: str, out_dir: Path) -> bool:
    """Fetch starting lineups and batting orders"""
    out_file = out_dir / f"lineup_{date_str}.parquet"
    if out_file.exists():
        print(f"⏭️ Skipping lineups for {date_str} (already exists)")
        return True
    
    print(f"👥 Fetching lineup data for {date_str}...")
    
    try:
        games = statsapi.schedule(start_date=date_str, end_date=date_str) or []
        
        if not games:
            print(f"✅ No games scheduled for {date_str}")
            return True
        
        lineup_records = []
        last_call = 0.0
        
        for game in games:
            game_pk = game.get("game_id") or game.get("game_pk")
            
            # Rate limit
            last_call = rate_limit(last_call, 0.5)
            
            try:
                # Get boxscore with lineup data
                boxscore = statsapi.get("game_boxscore", {"gamePk": game_pk})
                teams = boxscore.get("teams", {})
                
                for side in ["home", "away"]:
                    team_data = teams.get(side, {})
                    team_id = game.get(f"{side}_id")
                    
                    # Get batting order
                    batters = team_data.get("batters", [])
                    players = team_data.get("players", {})
                    
                    for batting_order, player_id in enumerate(batters[:9], start=1):  # Top 9 only
                        player_info = players.get(f"ID{player_id}", {})
                        person = player_info.get("person", {})
                        position = player_info.get("position", {})
                        stats = player_info.get("stats", {}).get("batting", {})
                        
                        lineup_record = {
                            "game_date": date_str,
                            "game_pk": game_pk,
                            "team_id": team_id,
                            "side": side,
                            "batting_order": batting_order,
                            "person_id": player_id,
                            "person_full_name": person.get("fullName", ""),
                            
                            # Position info
                            "position_code": position.get("code"),
                            "position_name": position.get("name"),
                            
                            # Handedness (crucial for betting)
                            "bats": person.get("batSide", {}).get("code"),
                            "throws": person.get("pitchHand", {}).get("code"),
                            
                            # Basic season stats for context
                            "stats_batting_avg": stats.get("avg"),
                            "stats_batting_obp": stats.get("obp"),
                            "stats_batting_slg": stats.get("slg"),
                            "stats_batting_ops": stats.get("ops"),
                            "stats_batting_home_runs": stats.get("homeRuns"),
                            "stats_batting_rbi": stats.get("rbi"),
                            
                            # Power indicators
                            "is_power_hitter": (stats.get("homeRuns") or 0) >= 15,
                            "is_leadoff": batting_order == 1,
                            "is_cleanup": batting_order == 4
                        }
                        
                        lineup_records.append(lineup_record)
                        
            except Exception as e:
                print(f"⚠️ Error getting lineup for game {game_pk}: {e}")
                continue
        
        if lineup_records:
            df = pd.DataFrame(lineup_records)
            df.to_parquet(out_file, index=False)
            print(f"✅ Lineup: {len(df)} players → {out_file.name}")
        else:
            print(f"✅ No lineup data for {date_str}")
        
        return True
        
    except Exception as e:
        print(f"❌ Lineup error for {date_str}: {e}")
        return False

def fetch_roster_data(date_str: str, out_dir: Path) -> bool:
    """Fetch basic roster data for player identification"""
    out_file = out_dir / f"roster_{date_str}.parquet"
    if out_file.exists():
        print(f"⏭️ Skipping roster for {date_str} (already exists)")
        return True
    
    print(f"👤 Fetching roster data for {date_str}...")
    
    try:
        # Get games for the date
        games = statsapi.schedule(start_date=date_str, end_date=date_str) or []
        
        if not games:
            print(f"✅ No games scheduled for {date_str}")
            return True
        
        roster_records = []
        seen_teams = set()
        last_call = 0.0
        
        for game in games:
            for team_id in [game["home_id"], game["away_id"]]:
                if team_id in seen_teams:
                    continue
                seen_teams.add(team_id)
                
                # Rate limit
                last_call = rate_limit(last_call, 0.3)
                
                try:
                    roster_data = statsapi.get("team_roster", {
                        "teamId": team_id, 
                        "rosterType": "active"
                    })
                    
                    for player in roster_data.get("roster", []):
                        person = player.get("person", {})
                        position = player.get("position", {})
                        
                        roster_record = {
                            "game_date": date_str,
                            "team_id": team_id,
                            "person_id": person.get("id"),
                            "person_full_name": person.get("fullName", ""),
                            "jersey_number": player.get("jerseyNumber"),
                            "position_code": position.get("code"),
                            "position_name": position.get("name"),
                            "position_type": position.get("type"),
                            
                            # Basic info for analysis
                            "bats": person.get("batSide", {}).get("code"),
                            "throws": person.get("pitchHand", {}).get("code"),
                        }
                        
                        roster_records.append(roster_record)
                        
                except Exception as e:
                    print(f"⚠️ Error getting roster for team {team_id}: {e}")
                    continue
        
        if roster_records:
            df = pd.DataFrame(roster_records)
            df.to_parquet(out_file, index=False)
            print(f"✅ Roster: {len(df)} players → {out_file.name}")
        else:
            print(f"✅ No roster data for {date_str}")
        
        return True
        
    except Exception as e:
        print(f"❌ Roster error for {date_str}: {e}")
        return False

def backfill_date(date_str: str, out_dir: Path, weather_api_key: Optional[str] = None) -> Dict[str, bool]:
    """Backfill all data for a single date"""
    print(f"\n📅 Processing {date_str}")
    
    results = {
        "games": fetch_game_data(date_str, out_dir),
        "statsapi": fetch_statsapi_data(date_str, out_dir),
        "weather": fetch_weather_data(date_str, out_dir, weather_api_key),
        "umpires": fetch_umpire_data(date_str, out_dir),
        "lineup": fetch_lineup_data(date_str, out_dir),
        "roster": fetch_roster_data(date_str, out_dir)
    }
    
    success_count = sum(results.values())
    print(f"📊 {date_str}: {success_count}/6 data types collected successfully")
    
    return results

def main():
    parser = argparse.ArgumentParser(description="Simplified MLB data backfill")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--output", default="data", help="Output directory")
    args = parser.parse_args()
    
    # Parse dates
    start_date = datetime.fromisoformat(args.start)
    end_date = datetime.fromisoformat(args.end)
    
    if end_date < start_date:
        raise ValueError("End date must be >= start date")
    
    # Setup output directory
    out_dir = Path(args.output)
    out_dir.mkdir(exist_ok=True)
    
    # Get weather API key from environment
    weather_api_key = os.getenv("OPENWEATHER_API_KEY")
    if not weather_api_key:
        print("⚠️ No OPENWEATHER_API_KEY found - weather data will be skipped")
        print("   Get a free key at: https://openweathermap.org/api")
    
    print(f"🚀 Simple MLB backfill: {start_date.date()} to {end_date.date()}")
    print(f"📁 Output directory: {out_dir}")
    print(f"🎯 Collecting: games, statsapi, weather, umpires, lineup, roster")
    
    # Process each date
    current_date = start_date
    total_days = (end_date - start_date).days + 1
    overall_results = {"games": 0, "statsapi": 0, "weather": 0, "umpires": 0, "lineup": 0, "roster": 0}
    
    with tqdm(total=total_days, desc="Processing dates") as pbar:
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            pbar.set_description(f"Processing {date_str}")
            
            try:
                day_results = backfill_date(date_str, out_dir, weather_api_key)
                
                # Update overall results
                for data_type, success in day_results.items():
                    if success:
                        overall_results[data_type] += 1
                        
            except Exception as e:
                print(f"❌ Error processing {date_str}: {e}")
            
            current_date += timedelta(days=1)
            pbar.update(1)
            
            # Small delay to be respectful to APIs
            time.sleep(0.1)
    
    # Print summary
    print(f"\n🎉 Backfill complete!")
    print(f"📊 Success rates:")
    for data_type, success_count in overall_results.items():
        success_rate = (success_count / total_days) * 100
        print(f"   {data_type}: {success_count}/{total_days} days ({success_rate:.1f}%)")
    
    print(f"\n💡 Next steps:")
    print(f"   1. Load data: python run_loader.py")
    print(f"   2. Run analysis: python simple_analysis.py")
    print(f"\n📊 Data collected provides:")
    print(f"   • Game context: Statcast pitch data + play-by-play flow")
    print(f"   • Lineup intelligence: Who's batting where, L/R matchups")
    print(f"   • Weather impact: Temperature, wind effects on ball flight")
    print(f"   • Umpire tendencies: Home plate umpire assignments")
    print(f"   • Player identification: Roster for linking data")

if __name__ == "__main__":
    main()