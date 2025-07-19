#!/usr/bin/env python3
"""
fatigue_metrics.py - Simplified player fatigue and rest metrics
"""

import os
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import statsapi

def calculate_simple_fatigue_metrics(player_id: int, position_code: str, 
                                   recent_data: pd.DataFrame, current_date: str) -> Dict[str, float]:
    """Calculate simplified fatigue metrics"""
    
    if recent_data.empty:
        # No historical data - return default values
        if position_code == "1":  # Pitcher
            return {
                "days_since_last_appearance": 3,  # Assume normal rest
                "appearances_last_7": 0,
                "total_pitches_last_7": 0,
                "workload_fatigue_score": 0.0,
                "performance_risk_score": 0.0
            }
        else:  # Position player
            return {
                "days_since_last_game": 1,  # Assume played yesterday
                "games_last_7": 5,  # Assume normal playing time
                "at_bats_last_7": 20,
                "fatigue_score": 25.0,  # Moderate baseline
                "rest_advantage_score": 50.0
            }
    
    # With historical data - calculate actual metrics
    current_dt = datetime.fromisoformat(current_date)
    
    if position_code == "1":  # Pitcher
        # Filter for this pitcher
        pitcher_data = recent_data[recent_data.get('pitcher', pd.Series()) == player_id]
        
        if pitcher_data.empty:
            days_since_last = 4  # Default
            appearances = 0
            pitches = 0
        else:
            # Convert dates and calculate
            pitcher_data['game_dt'] = pd.to_datetime(pitcher_data['game_date'])
            pitcher_data['days_ago'] = (current_dt - pitcher_data['game_dt']).dt.days
            
            days_since_last = pitcher_data['days_ago'].min() if len(pitcher_data) > 0 else 4
            
            last_7_days = pitcher_data[pitcher_data['days_ago'] <= 7]
            appearances = len(last_7_days['game_date'].unique()) if len(last_7_days) > 0 else 0
            pitches = len(last_7_days)  # Rough estimate
        
        # Calculate fatigue scores
        workload_score = min(100, (pitches / 100) * 30 + max(0, 4 - days_since_last) * 15)
        risk_score = min(100, max(0, pitches - 80) * 0.5 + max(0, appearances - 3) * 20)
        
        return {
            "days_since_last_appearance": int(days_since_last),
            "appearances_last_7": appearances,
            "total_pitches_last_7": pitches,
            "workload_fatigue_score": round(workload_score, 1),
            "performance_risk_score": round(risk_score, 1)
        }
    
    else:  # Position player
        # Filter for this batter
        batter_data = recent_data[recent_data.get('batter', pd.Series()) == player_id]
        
        if batter_data.empty:
            days_since_last = 1
            games = 5
            at_bats = 20
        else:
            batter_data['game_dt'] = pd.to_datetime(batter_data['game_date'])
            batter_data['days_ago'] = (current_dt - batter_data['game_dt']).dt.days
            
            days_since_last = batter_data['days_ago'].min() if len(batter_data) > 0 else 1
            
            last_7_days = batter_data[batter_data['days_ago'] <= 7]
            games = len(last_7_days['game_date'].unique()) if len(last_7_days) > 0 else 5
            at_bats = len(last_7_days)
        
        # Calculate fatigue scores
        fatigue_score = min(100, max(0, games - 5) * 15 + max(0, at_bats - 25) * 2)
        rest_score = min(100, min(days_since_last, 3) * 25 + max(0, 7 - games) * 10)
        
        return {
            "days_since_last_game": int(days_since_last),
            "games_last_7": games,
            "at_bats_last_7": at_bats,
            "fatigue_score": round(fatigue_score, 1),
            "rest_advantage_score": round(rest_score, 1)
        }

def load_recent_data(stage_dir: Path, current_date: str, days_back: int = 10) -> pd.DataFrame:
    """Load recent Statcast data if available"""
    current_dt = datetime.fromisoformat(current_date)
    recent_data = []
    
    for days_ago in range(1, days_back + 1):
        check_date = current_dt - timedelta(days=days_ago)
        date_str = check_date.strftime("%Y-%m-%d")
        
        statcast_file = stage_dir / f"statcast_{date_str}.parquet"
        if statcast_file.exists():
            try:
                df = pd.read_parquet(statcast_file)
                df['game_date'] = date_str
                recent_data.append(df)
            except Exception as e:
                print(f"⚠️ Error loading {statcast_file}: {e}")
    
    if recent_data:
        combined_df = pd.concat(recent_data, ignore_index=True)
        print(f"✅ Loaded {len(combined_df)} records from {len(recent_data)} days for fatigue analysis")
        return combined_df
    else:
        print(f"⚠️ No recent Statcast data found - using baseline fatigue estimates")
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
        
        fatigue_records = []
        
        for game in games:
            game_pk = game.get("game_id") or game.get("game_pk")
            home_team_id = game.get("home_id")
            away_team_id = game.get("away_id")
            
            # Process teams
            for team_id, team_type in [(home_team_id, "home"), (away_team_id, "away")]:
                if not team_id:
                    continue
                
                try:
                    # Get roster for the team
                    roster_data = statsapi.get("team_roster", {"teamId": team_id, "rosterType": "active"})
                    roster = roster_data.get("roster", []) if isinstance(roster_data, dict) else []
                    
                    for player_record in roster:
                        try:
                            person = player_record.get("person", {})
                            position = player_record.get("position", {})
                            
                            player_id = person.get("id")
                            if not player_id:
                                continue
                            
                            # Get position info safely
                            position_code = str(position.get("code", "")) if isinstance(position, dict) else ""
                            position_name = str(position.get("name", "")) if isinstance(position, dict) else ""
                            player_name = str(person.get("fullName", ""))
                            
                            # Calculate fatigue metrics
                            fatigue_metrics = calculate_simple_fatigue_metrics(
                                player_id, position_code, recent_data, date_str
                            )
                            
                            # Create record
                            fatigue_record = {
                                "game_date": date_str,
                                "game_pk": game_pk,
                                "team_id": team_id,
                                "team_type": team_type,
                                "player_id": player_id,
                                "position_code": position_code,
                                "position_name": position_name,
                                "player_name": player_name,
                                
                                # Basic team metrics (simplified)
                                "team_travel_distance": 0,
                                "team_timezone_changes": 0,
                                "team_travel_fatigue_score": 10.0,  # Baseline
                                "team_games_in_last_7": 6,
                                "team_consecutive_road_games": 0,
                                
                                # Player-specific metrics
                                **fatigue_metrics
                            }
                            
                            fatigue_records.append(fatigue_record)
                            
                        except Exception as e:
                            print(f"⚠️ Error processing player in team {team_id}: {e}")
                            continue
                            
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
        
        # Show summary
        pitchers = df[df['position_code'] == '1']
        batters = df[df['position_code'] != '1']
        
        print(f"📊 Processed {len(pitchers)} pitchers, {len(batters)} position players")
        
    except Exception as e:
        print(f"❌ Error calculating fatigue metrics for {date_str}: {e}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="2025-07-15", help="Date to test (YYYY-MM-DD)")
    parser.add_argument("--output", default="test_fatigue", help="Output directory")
    args = parser.parse_args()
    
    out_dir = Path(args.output)
    out_dir.mkdir(exist_ok=True)
    
    fetch_fatigue_metrics_for_date(args.date, out_dir)
    print("\n💪 Fatigue metrics test complete!")