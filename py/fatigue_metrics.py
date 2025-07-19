#!/usr/bin/env python3
"""
fatigue_metrics.py - Player fatigue and rest metrics calculation
Save this as py/fatigue_metrics.py
"""

import os
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import statsapi
from collections import defaultdict

# Team timezone mappings for travel fatigue calculations
TEAM_TIMEZONES = {
    "ARI": "US/Arizona", "ATL": "US/Eastern", "BAL": "US/Eastern", "BOS": "US/Eastern",
    "CHC": "US/Central", "CWS": "US/Central", "CIN": "US/Eastern", "CLE": "US/Eastern",
    "COL": "US/Mountain", "DET": "US/Eastern", "HOU": "US/Central", "KC": "US/Central",
    "LAA": "US/Pacific", "LAD": "US/Pacific", "MIA": "US/Eastern", "MIL": "US/Central",
    "MIN": "US/Central", "NYM": "US/Eastern", "NYY": "US/Eastern", "OAK": "US/Pacific",
    "PHI": "US/Eastern", "PIT": "US/Eastern", "SD": "US/Pacific", "SF": "US/Pacific",
    "SEA": "US/Pacific", "STL": "US/Central", "TB": "US/Eastern", "TEX": "US/Central",
    "TOR": "US/Eastern", "WSH": "US/Eastern"
}

# Stadium locations for travel distance calculations
STADIUM_LOCATIONS = {
    "Arizona Diamondbacks": {"lat": 33.4453, "lon": -112.0667, "city": "Phoenix"},
    "Atlanta Braves": {"lat": 33.8906, "lon": -84.4677, "city": "Atlanta"},
    "Baltimore Orioles": {"lat": 39.2840, "lon": -76.6217, "city": "Baltimore"},
    "Boston Red Sox": {"lat": 42.3467, "lon": -71.0972, "city": "Boston"},
    "Chicago White Sox": {"lat": 41.8299, "lon": -87.6338, "city": "Chicago"},
    "Chicago Cubs": {"lat": 41.9484, "lon": -87.6553, "city": "Chicago"},
    "Cincinnati Reds": {"lat": 39.5031, "lon": -84.3668, "city": "Cincinnati"},
    "Cleveland Guardians": {"lat": 41.4958, "lon": -81.6853, "city": "Cleveland"},
    "Colorado Rockies": {"lat": 39.7559, "lon": -104.9942, "city": "Denver"},
    "Detroit Tigers": {"lat": 42.3390, "lon": -83.0485, "city": "Detroit"},
    "Houston Astros": {"lat": 29.7570, "lon": -95.3555, "city": "Houston"},
    "Kansas City Royals": {"lat": 39.0517, "lon": -94.4803, "city": "Kansas City"},
    "Los Angeles Angels": {"lat": 33.8003, "lon": -117.8827, "city": "Anaheim"},
    "Los Angeles Dodgers": {"lat": 34.0739, "lon": -118.2400, "city": "Los Angeles"},
    "Miami Marlins": {"lat": 25.7781, "lon": -80.2198, "city": "Miami"},
    "Milwaukee Brewers": {"lat": 43.0280, "lon": -87.9712, "city": "Milwaukee"},
    "Minnesota Twins": {"lat": 44.9817, "lon": -93.2776, "city": "Minneapolis"},
    "New York Mets": {"lat": 40.7571, "lon": -73.8458, "city": "New York"},
    "New York Yankees": {"lat": 40.8296, "lon": -73.9262, "city": "New York"},
    "Oakland Athletics": {"lat": 37.7516, "lon": -122.2005, "city": "Oakland"},
    "Philadelphia Phillies": {"lat": 39.9061, "lon": -75.1665, "city": "Philadelphia"},
    "Pittsburgh Pirates": {"lat": 40.4469, "lon": -80.0057, "city": "Pittsburgh"},
    "San Diego Padres": {"lat": 32.7073, "lon": -117.1566, "city": "San Diego"},
    "San Francisco Giants": {"lat": 37.7786, "lon": -122.3893, "city": "San Francisco"},
    "Seattle Mariners": {"lat": 47.5914, "lon": -122.3326, "city": "Seattle"},
    "St. Louis Cardinals": {"lat": 38.6226, "lon": -90.1928, "city": "St. Louis"},
    "Tampa Bay Rays": {"lat": 27.7682, "lon": -82.6534, "city": "Tampa Bay"},
    "Texas Rangers": {"lat": 32.7513, "lon": -97.0830, "city": "Arlington"},
    "Toronto Blue Jays": {"lat": 43.6414, "lon": -79.3894, "city": "Toronto"},
    "Washington Nationals": {"lat": 38.8730, "lon": -77.0074, "city": "Washington"}
}

class FatigueMetricsCalculator:
    def __init__(self, lookback_days: int = 30):
        self.lookback_days = lookback_days
        
    def calculate_travel_distance(self, city1: str, city2: str) -> float:
        """Calculate approximate travel distance between cities"""
        # Simplified distance calculation
        distance_map = {
            ("New York", "Los Angeles"): 2445,
            ("Boston", "Seattle"): 2496,
            ("Miami", "Seattle"): 2724,
            ("San Diego", "Boston"): 2558,
        }
        
        # Check both directions
        key1 = (city1, city2)
        key2 = (city2, city1)
        
        if key1 in distance_map:
            return distance_map[key1]
        elif key2 in distance_map:
            return distance_map[key2]
        else:
            # Rough estimate based on coast differences
            coast_distances = {
                ("East", "West"): 2500,
                ("East", "Central"): 1000,
                ("Central", "West"): 1200,
                ("East", "East"): 500,
                ("Central", "Central"): 600,
                ("West", "West"): 800
            }
            
            # Simplified coast mapping
            east_cities = ["New York", "Boston", "Baltimore", "Philadelphia", "Washington", "Atlanta", "Miami", "Tampa Bay"]
            central_cities = ["Chicago", "Detroit", "Cleveland", "Cincinnati", "St. Louis", "Milwaukee", "Minneapolis", "Kansas City", "Houston", "Arlington"]
            
            coast1 = "East" if city1 in east_cities else "Central" if city1 in central_cities else "West"
            coast2 = "East" if city2 in east_cities else "Central" if city2 in central_cities else "West"
            
            return coast_distances.get((coast1, coast2), 1000)
    
    def get_recent_team_schedule(self, team_id: int, end_date: str, days_back: int = 10) -> List[Dict]:
        """Get recent team schedule for travel fatigue calculation"""
        try:
            start_date = (datetime.fromisoformat(end_date) - timedelta(days=days_back)).strftime("%Y-%m-%d")
            
            schedule = statsapi.schedule(
                start_date=start_date, 
                end_date=end_date, 
                team=team_id
            ) or []
            
            return sorted(schedule, key=lambda x: x.get("game_date", ""))
        except Exception as e:
            print(f"⚠️ Error getting schedule for team {team_id}: {e}")
            return []
    
    def calculate_team_travel_fatigue(self, team_schedule: List[Dict], current_date: str) -> Dict[str, float]:
        """Calculate travel fatigue based on recent games and locations"""
        if len(team_schedule) < 2:
            return {"travel_distance": 0, "timezone_changes": 0, "travel_fatigue_score": 0, "games_in_last_7": 0}
        
        total_distance = 0
        timezone_changes = 0
        games_in_last_7 = 0
        consecutive_road_games = 0
        
        current_dt = datetime.fromisoformat(current_date)
        prev_city = None
        
        for i, game in enumerate(team_schedule):
            game_date = datetime.fromisoformat(game.get("game_date", current_date))
            days_ago = (current_dt - game_date).days
            
            if days_ago <= 7:
                games_in_last_7 += 1
            
            # Determine game location
            venue_name = game.get("venue_name", "")
            home_team = game.get("home_name", "")
            
            # Find the city for this game
            current_city = None
            for team_name, location in STADIUM_LOCATIONS.items():
                if any(word in team_name for word in home_team.split()):
                    current_city = location["city"]
                    break
            
            if current_city and prev_city and current_city != prev_city:
                # Team traveled
                distance = self.calculate_travel_distance(prev_city, current_city)
                total_distance += distance
                
                # Check for timezone change (simplified)
                if distance > 1500:  # Cross-country travel
                    timezone_changes += 1
                elif distance > 800:  # Significant travel
                    timezone_changes += 0.5
            
            # Check if this is a road game
            if game.get("home_away") == "away" or "away" in game.get("home_name", "").lower():
                consecutive_road_games += 1
            else:
                consecutive_road_games = 0
            
            prev_city = current_city
        
        # Calculate fatigue score (0-100, higher = more fatigued)
        base_fatigue = min(100, 
            (total_distance / 1000) * 3 +  # Distance factor
            timezone_changes * 10 +        # Timezone factor
            max(0, games_in_last_7 - 4) * 8 +  # Game frequency factor
            consecutive_road_games * 5     # Road trip factor
        )
        
        return {
            "travel_distance": round(total_distance, 0),
            "timezone_changes": round(timezone_changes, 1),
            "travel_fatigue_score": round(base_fatigue, 1),
            "games_in_last_7": games_in_last_7,
            "consecutive_road_games": consecutive_road_games
        }
    
    def calculate_pitcher_fatigue_from_data(self, pitcher_id: int, current_date: str, 
                                          recent_data: pd.DataFrame) -> Dict[str, float]:
        """Calculate pitcher fatigue from existing Statcast/PBP data"""
        current_dt = datetime.fromisoformat(current_date)
        
        # Filter data for this pitcher
        pitcher_data = recent_data[recent_data['pitcher'] == pitcher_id].copy()
        
        if pitcher_data.empty:
            return self._default_pitcher_metrics()
        
        # Convert game_date to datetime
        pitcher_data['game_dt'] = pd.to_datetime(pitcher_data['game_date'])
        pitcher_data['days_ago'] = (current_dt - pitcher_data['game_dt']).dt.days
        
        # Calculate metrics
        days_since_last = pitcher_data['days_ago'].min() if len(pitcher_data) > 0 else 999
        
        # Appearances in different windows
        last_7_days = pitcher_data[pitcher_data['days_ago'] <= 7]
        last_15_days = pitcher_data[pitcher_data['days_ago'] <= 15]
        
        appearances_last_7 = len(last_7_days['game_date'].unique())
        appearances_last_15 = len(last_15_days['game_date'].unique())
        
        # Pitch counts (estimate from pitch_number)
        pitches_last_7 = len(last_7_days)
        pitches_last_15 = len(last_15_days)
        
        # Consecutive appearances (simplified)
        consecutive_appearances = 0
        for days_back in range(1, 8):
            check_date = current_dt - timedelta(days=days_back)
            if any(abs((pitcher_data['game_dt'] - check_date).dt.days) <= 0):
                consecutive_appearances += 1
            else:
                break
        
        # Calculate fatigue scores
        workload_score = min(100, 
            (pitches_last_7 / 100) * 25 +  # Recent pitch count
            (consecutive_appearances * 15) +  # Consecutive games
            max(0, 3 - days_since_last) * 12  # Insufficient rest
        )
        
        risk_score = min(100,
            max(0, pitches_last_7 - 80) * 0.4 +  # High recent usage
            max(0, appearances_last_7 - 3) * 15 +  # Overuse
            max(0, 2 - days_since_last) * 20  # Fatigue risk
        )
        
        return {
            "days_since_last_appearance": days_since_last,
            "appearances_last_7": appearances_last_7,
            "appearances_last_15": appearances_last_15,
            "total_pitches_last_7": pitches_last_7,
            "total_pitches_last_15": pitches_last_15,
            "consecutive_appearances": consecutive_appearances,
            "workload_fatigue_score": round(workload_score, 1),
            "performance_risk_score": round(risk_score, 1)
        }
    
    def calculate_batter_fatigue_from_data(self, batter_id: int, current_date: str,
                                         recent_data: pd.DataFrame) -> Dict[str, float]:
        """Calculate batter fatigue from existing data"""
        current_dt = datetime.fromisoformat(current_date)
        
        # Filter data for this batter
        batter_data = recent_data[recent_data['batter'] == batter_id].copy()
        
        if batter_data.empty:
            return self._default_batter_metrics()
        
        # Convert game_date to datetime
        batter_data['game_dt'] = pd.to_datetime(batter_data['game_date'])
        batter_data['days_ago'] = (current_dt - batter_data['game_dt']).dt.days
        
        # Calculate metrics
        days_since_last = batter_data['days_ago'].min() if len(batter_data) > 0 else 999
        
        # Games in different windows
        last_7_days = batter_data[batter_data['days_ago'] <= 7]
        last_15_days = batter_data[batter_data['days_ago'] <= 15]
        
        games_last_7 = len(last_7_days['game_date'].unique())
        games_last_15 = len(last_15_days['game_date'].unique())
        
        # At-bats (estimate from at_bat_number)
        at_bats_last_7 = len(last_7_days['at_bat_number'].unique()) if 'at_bat_number' in last_7_days.columns else len(last_7_days)
        
        # Consecutive games
        consecutive_games = 0
        for days_back in range(1, 8):
            check_date = current_dt - timedelta(days=days_back)
            if any(abs((batter_data['game_dt'] - check_date).dt.days) <= 0):
                consecutive_games += 1
            else:
                break
        
        # Calculate fatigue and rest scores
        fatigue_score = min(100,
            max(0, games_last_7 - 5) * 12 +  # High game frequency
            max(0, consecutive_games - 4) * 8 +  # No rest days
            max(0, at_bats_last_7 - 25) * 1.5  # High usage
        )
        
        rest_score = min(100,
            min(days_since_last, 3) * 20 +  # Recent rest
            max(0, 7 - games_last_7) * 8  # Reduced game frequency
        )
        
        return {
            "days_since_last_game": days_since_last,
            "games_last_7": games_last_7,
            "games_last_15": games_last_15,
            "at_bats_last_7": at_bats_last_7,
            "consecutive_games": consecutive_games,
            "fatigue_score": round(fatigue_score, 1),
            "rest_advantage_score": round(rest_score, 1)
        }
    
    def _default_pitcher_metrics(self) -> Dict[str, float]:
        """Default metrics when no data is available"""
        return {
            "days_since_last_appearance": 999,
            "appearances_last_7": 0,
            "appearances_last_15": 0,
            "total_pitches_last_7": 0,
            "total_pitches_last_15": 0,
            "consecutive_appearances": 0,
            "workload_fatigue_score": 0.0,
            "performance_risk_score": 0.0
        }
    
    def _default_batter_metrics(self) -> Dict[str, float]:
        """Default metrics when no data is available"""
        return {
            "days_since_last_game": 999,
            "games_last_7": 0,
            "games_last_15": 0,
            "at_bats_last_7": 0,
            "consecutive_games": 0,
            "fatigue_score": 0.0,
            "rest_advantage_score": 100.0
        }

def load_recent_data(stage_dir: Path, current_date: str, days_back: int = 15) -> pd.DataFrame:
    """Load recent Statcast data for fatigue calculation"""
    current_dt = datetime.fromisoformat(current_date)
    start_date = current_dt - timedelta(days=days_back)
    
    recent_data = []
    
    # Look for Statcast files in the date range
    for days_ago in range(days_back):
        check_date = start_date + timedelta(days=days_ago)
        date_str = check_date.strftime("%Y-%m-%d")
        
        statcast_file = stage_dir / f"statcast_{date_str}.parquet"
        if statcast_file.exists():
            try:
                df = pd.read_parquet(statcast_file)
                df['game_date'] = date_str  # Ensure date column exists
                recent_data.append(df)
            except Exception as e:
                print(f"⚠️ Error loading {statcast_file}: {e}")
    
    if recent_data:
        combined_df = pd.concat(recent_data, ignore_index=True)
        print(f"✅ Loaded {len(combined_df)} records from {len(recent_data)} days for fatigue analysis")
        return combined_df
    else:
        print(f"⚠️ No recent Statcast data found for fatigue calculation")
        return pd.DataFrame()

def fetch_fatigue_metrics_for_date(date_str: str, out_dir: Path):
    """Calculate and save fatigue metrics for all players on a given date"""
    out_file = out_dir / f"fatigue_metrics_{date_str}.parquet"
    if out_file.exists():
        print(f"⏭️ Skipping Fatigue Metrics for {date_str} (already exists)")
        return
    
    print(f"💪 Calculating Fatigue Metrics for {date_str}...")
    
    try:
        # Get games for the date
        games = statsapi.schedule(start_date=date_str, end_date=date_str) or []
        if not games:
            print(f"✅ No games scheduled for {date_str}")
            return
        
        # Load recent data for analysis
        recent_data = load_recent_data(out_dir, date_str)
        
        calculator = FatigueMetricsCalculator()
        fatigue_records = []
        
        for game in games:
            game_pk = game.get("game_id") or game.get("game_pk")
            home_team_id = game.get("home_id")
            away_team_id = game.get("away_id")
            
            # Calculate team fatigue metrics
            for team_id, team_type in [(home_team_id, "home"), (away_team_id, "away")]:
                if not team_id:
                    continue
                
                # Get recent team schedule for travel fatigue
                team_schedule = calculator.get_recent_team_schedule(team_id, date_str)
                travel_metrics = calculator.calculate_team_travel_fatigue(team_schedule, date_str)
                
                # Get roster for the date to calculate individual player metrics
                try:
                    roster_data = statsapi.get("team_roster", {"teamId": team_id, "rosterType": "active"})
                    roster = roster_data.get("roster", []) if isinstance(roster_data, dict) else []
                    
                    for player_record in roster:
                        person = player_record.get("person", {})
                        position = player_record.get("position", {})
                        
                        player_id = person.get("id")
                        if not player_id:
                            continue
                        
                        # Calculate position-specific metrics using recent data
                        position_code = position.get("code", "")
                        if position_code == "1":  # Pitcher
                            player_fatigue = calculator.calculate_pitcher_fatigue_from_data(
                                player_id, date_str, recent_data
                            )
                        else:  # Position player
                            player_fatigue = calculator.calculate_batter_fatigue_from_data(
                                player_id, date_str, recent_data
                            )
                        
                        # Combine all metrics
                        fatigue_record = {
                            "game_date": date_str,
                            "game_pk": game_pk,
                            "team_id": team_id,
                            "team_type": team_type,
                            "player_id": player_id,
                            "position_code": position_code,
                            "position_name": position.get("name", ""),
                            "player_name": person.get("fullName", ""),
                            
                            # Team travel metrics
                            **{f"team_{k}": v for k, v in travel_metrics.items()},
                            
                            # Individual player metrics
                            **player_fatigue
                        }
                        
                        fatigue_records.append(fatigue_record)
                        
                except Exception as e:
                    print(f"⚠️ Error processing roster for team {team_id}: {e}")
                    continue
        
        if not fatigue_records:
            print(f"✅ No fatigue metrics calculated for {date_str}")
            return
        
        # Save to parquet
        df = pd.DataFrame(fatigue_records)
        df.to_parquet(out_file, index=False)
        print(f"✅ Fatigue Metrics: Wrote {len(df)} records → {out_file.name}")
        
        # Show summary stats
        pitchers = df[df['position_code'] == '1']
        batters = df[df['position_code'] != '1']
        
        if len(pitchers) > 0:
            avg_pitcher_fatigue = pitchers['workload_fatigue_score'].mean()
            print(f"📊 Average pitcher workload score: {avg_pitcher_fatigue:.1f}")
        
        if len(batters) > 0:
            avg_batter_fatigue = batters['fatigue_score'].mean()
            print(f"📊 Average batter fatigue score: {avg_batter_fatigue:.1f}")
        
    except Exception as e:
        print(f"❌ Error calculating fatigue metrics for {date_str}: {e}")

if __name__ == "__main__":
    # Test the fatigue metrics calculation
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="2025-07-15", help="Date to test (YYYY-MM-DD)")
    parser.add_argument("--output", default="test_fatigue", help="Output directory")
    args = parser.parse_args()
    
    out_dir = Path(args.output)
    out_dir.mkdir(exist_ok=True)
    
    fetch_fatigue_metrics_for_date(args.date, out_dir)
    print("\n💪 Fatigue metrics test complete!")