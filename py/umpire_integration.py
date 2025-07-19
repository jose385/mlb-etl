#!/usr/bin/env python3
"""
umpire_integration.py - MLB umpire data collection and analysis
Save this as py/umpire_integration.py
"""

import os
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Any
import statsapi
from datetime import datetime, timedelta
import time

def clean_umpire_name(name: str) -> str:
    """Clean and standardize umpire names"""
    if not name:
        return ""
    
    # Basic cleaning
    cleaned = str(name).strip()
    
    # Remove common suffixes
    suffixes = [" Jr.", " Sr.", " III", " II"]
    for suffix in suffixes:
        if cleaned.endswith(suffix):
            cleaned = cleaned[:-len(suffix)].strip()
    
    return cleaned

def fetch_umpire_assignments_for_date(date_str: str, out_dir: Path):
    """Fetch umpire assignments for all games on a given date"""
    out_file = out_dir / f"umpires_{date_str}.parquet"
    if out_file.exists():
        print(f"⏭️ Skipping Umpires for {date_str} (already exists)")
        return
    
    print(f"👨‍⚖️ Fetching Umpire data for {date_str}...")
    
    try:
        # Get games for the date
        games = statsapi.schedule(start_date=date_str, end_date=date_str) or []
        if not games:
            print(f"✅ No games scheduled for {date_str}")
            return
        
        umpire_records = []
        
        for game in games:
            game_pk = game.get("game_id") or game.get("game_pk")
            if not game_pk:
                continue
            
            try:
                # Get detailed game data including officials
                print(f"   Fetching umpires for game {game_pk}...")
                game_data = statsapi.get("game", {"gamePk": game_pk})
                
                # Navigate to officials data
                officials_data = (game_data.get("liveData", {})
                                .get("boxscore", {})
                                .get("officials", []))
                
                if not officials_data:
                    print(f"   ⚠️ No officials data found for game {game_pk}")
                    continue
                
                for official in officials_data:
                    try:
                        official_info = official.get("official", {})
                        official_type = official.get("officialType", "")
                        
                        umpire_id = official_info.get("id")
                        if not umpire_id:
                            continue
                        
                        # Create umpire record
                        umpire_record = {
                            "game_date": date_str,
                            "game_pk": game_pk,
                            "umpire_id": umpire_id,
                            "position": official_type,
                            
                            # Basic umpire information
                            "umpire_name": clean_umpire_name(official_info.get("fullName", "")),
                            "first_name": official_info.get("firstName", ""),
                            "last_name": official_info.get("lastName", ""),
                            "full_name": official_info.get("fullName", ""),
                            
                            # Initialize historical metrics (will be calculated later)
                            "total_games_officiated": 0,
                            "strikes_called_per_game": None,
                            "balls_called_per_game": None,
                            "total_pitches_per_game": None,
                            
                            # Strike zone tendencies (to be calculated from historical data)
                            "strike_rate_overall": None,
                            "strike_rate_low_zone": None,
                            "strike_rate_high_zone": None,
                            "strike_rate_outside_zone": None,
                            "strike_rate_inside_zone": None,
                            
                            # Game impact metrics (to be calculated)
                            "avg_game_length_minutes": None,
                            "avg_total_runs_in_games": None,
                            "over_under_record": None,
                            
                            # Advanced tendencies
                            "pitcher_friendly_score": None,
                            "consistency_score": None,
                            
                            # Situational metrics
                            "late_inning_strike_rate": None,
                            "close_game_strike_rate": None,
                            "runners_on_strike_rate": None,
                            
                            # Data tracking
                            "sample_size": 0,
                            "last_calculated": None,
                            "data_source": "mlb_api"
                        }
                        
                        umpire_records.append(umpire_record)
                        
                    except Exception as e:
                        print(f"   ⚠️ Error processing official: {e}")
                        continue
                
                # Add small delay to be respectful to API
                time.sleep(0.1)
                
            except Exception as e:
                print(f"   ❌ Error fetching game {game_pk}: {e}")
                continue
        
        if not umpire_records:
            print(f"✅ No umpire data collected for {date_str}")
            return
        
        # Save to parquet
        df = pd.DataFrame(umpire_records)
        df.to_parquet(out_file, index=False)
        print(f"✅ Umpires: Wrote {len(df)} records → {out_file.name}")
        
        # Show summary
        home_plate_umps = df[df['position'] == 'Home Plate']
        print(f"   📊 Found {len(home_plate_umps)} home plate umpires (most important for betting)")
        
        if len(home_plate_umps) > 0:
            print(f"   👨‍⚖️ Home plate umpires: {', '.join(home_plate_umps['umpire_name'].unique())}")
        
    except Exception as e:
        print(f"❌ Error fetching umpire data for {date_str}: {e}")

def calculate_umpire_historical_metrics(umpire_name: str, position: str, 
                                      historical_statcast_data: pd.DataFrame,
                                      historical_game_data: pd.DataFrame = None) -> Dict[str, Any]:
    """
    Calculate historical umpire tendencies from Statcast data
    This would be called periodically to update umpire metrics
    """
    
    if historical_statcast_data.empty:
        return {}
    
    # For home plate umpires, we can calculate strike zone tendencies
    if position == "Home Plate":
        return calculate_home_plate_umpire_metrics(umpire_name, historical_statcast_data)
    else:
        return calculate_base_umpire_metrics(umpire_name, historical_game_data)

def calculate_home_plate_umpire_metrics(umpire_name: str, 
                                      statcast_data: pd.DataFrame) -> Dict[str, Any]:
    """Calculate detailed metrics for home plate umpires"""
    
    # This is a simplified version - you'd need to join with umpire assignments
    # to get accurate historical data for specific umpires
    
    metrics = {
        "sample_size": len(statcast_data),
        "total_games_officiated": len(statcast_data.get('game_pk', [])),
    }
    
    # Calculate strike zone metrics if we have pitch location data
    if 'zone' in statcast_data.columns and 'description' in statcast_data.columns:
        
        # Called pitches (balls and strikes, not swings)
        called_pitches = statcast_data[
            statcast_data['description'].isin(['ball', 'called_strike', 'blocked_ball'])
        ].copy()
        
        if len(called_pitches) > 50:  # Need sufficient sample size
            
            # Overall strike rate on borderline pitches (zones 11-14 are outside)
            borderline_pitches = called_pitches[
                called_pitches['zone'].isin([11, 12, 13, 14])
            ]
            
            if len(borderline_pitches) > 10:
                strike_rate = (borderline_pitches['description'] == 'called_strike').mean()
                metrics['strike_rate_overall'] = round(strike_rate, 4)
            
            # Zone-specific rates
            # Low zone (zones 7, 8, 9)
            low_zone = called_pitches[called_pitches['zone'].isin([7, 8, 9])]
            if len(low_zone) > 5:
                metrics['strike_rate_low_zone'] = round(
                    (low_zone['description'] == 'called_strike').mean(), 4
                )
            
            # High zone (zones 1, 2, 3)  
            high_zone = called_pitches[called_pitches['zone'].isin([1, 2, 3])]
            if len(high_zone) > 5:
                metrics['strike_rate_high_zone'] = round(
                    (high_zone['description'] == 'called_strike').mean(), 4
                )
            
            # Calculate pitcher-friendly score
            if 'strike_rate_overall' in metrics:
                # Higher strike rate on borderline = more pitcher friendly
                pitcher_score = min(100, max(0, 
                    50 + (metrics['strike_rate_overall'] - 0.15) * 200
                ))
                metrics['pitcher_friendly_score'] = round(pitcher_score, 1)
    
    return metrics

def calculate_base_umpire_metrics(umpire_name: str, 
                                game_data: pd.DataFrame) -> Dict[str, Any]:
    """Calculate metrics for base umpires (less impactful for betting)"""
    
    if game_data is None or game_data.empty:
        return {"sample_size": 0}
    
    return {
        "sample_size": len(game_data),
        "total_games_officiated": len(game_data.get('game_pk', [])),
        # Base umpires have less direct impact on betting outcomes
        # Could track things like pace of play, replay challenges, etc.
    }

def update_umpire_historical_data(conn, lookback_days: int = 365):
    """
    Update historical umpire metrics by analyzing past performance
    This should be run periodically to keep umpire tendencies current
    """
    print(f"📊 Updating umpire historical metrics (last {lookback_days} days)...")
    
    try:
        # Get recent umpire assignments
        end_date = datetime.now()
        start_date = end_date - timedelta(days=lookback_days)
        
        # Query recent umpire assignments from database
        umpire_query = f"""
        SELECT DISTINCT umpire_name, position, umpire_id
        FROM umpires 
        WHERE game_date >= '{start_date.strftime('%Y-%m-%d')}'
        AND position = 'Home Plate'
        """
        
        umpires_df = pd.read_sql(umpire_query, conn)
        
        print(f"Found {len(umpires_df)} unique home plate umpires to analyze")
        
        for _, umpire in umpires_df.iterrows():
            umpire_name = umpire['umpire_name']
            umpire_id = umpire['umpire_id']
            
            print(f"   Analyzing {umpire_name}...")
            
            # Here you would:
            # 1. Get historical Statcast data for games this umpire worked
            # 2. Calculate their tendencies
            # 3. Update the umpires table with new metrics
            
            # For now, we'll update with placeholder metrics
            update_sql = f"""
            UPDATE umpires 
            SET 
                last_calculated = CURRENT_DATE,
                sample_size = 100,
                pitcher_friendly_score = 50.0,
                over_under_record = 0.52
            WHERE umpire_id = {umpire_id} 
            AND position = 'Home Plate'
            """
            
            # You would execute this update in a real implementation
            # conn.execute(update_sql)
        
        print("✅ Umpire historical analysis complete")
        
    except Exception as e:
        print(f"❌ Error updating umpire metrics: {e}")

# Integration function for betting analysis
def get_umpire_betting_impact(game_pk: int, conn) -> Dict[str, Any]:
    """
    Get umpire betting impact for a specific game
    Returns metrics useful for betting decisions
    """
    
    try:
        query = f"""
        SELECT 
            umpire_name,
            position,
            over_under_record,
            avg_total_runs_in_games,
            pitcher_friendly_score,
            strike_rate_overall,
            sample_size
        FROM umpires 
        WHERE game_pk = {game_pk}
        AND position = 'Home Plate'
        """
        
        umpire_df = pd.read_sql(query, conn)
        
        if umpire_df.empty:
            return {"error": "No home plate umpire found"}
        
        ump = umpire_df.iloc[0]
        
        return {
            "umpire_name": ump['umpire_name'],
            "over_under_tendency": ump['over_under_record'],
            "pitcher_friendly_score": ump['pitcher_friendly_score'],
            "strike_rate": ump['strike_rate_overall'],
            "betting_recommendation": get_betting_recommendation(ump),
            "confidence": "High" if ump['sample_size'] > 50 else "Medium" if ump['sample_size'] > 20 else "Low"
        }
        
    except Exception as e:
        return {"error": f"Database error: {e}"}

def get_betting_recommendation(umpire_data) -> str:
    """Generate betting recommendation based on umpire tendencies"""
    
    over_rate = umpire_data.get('over_under_record', 0.5)
    pitcher_score = umpire_data.get('pitcher_friendly_score', 50)
    
    if over_rate > 0.55 and pitcher_score < 45:
        return "OVER (Hitter-friendly umpire with high-scoring history)"
    elif over_rate < 0.45 and pitcher_score > 55:
        return "UNDER (Pitcher-friendly umpire with low-scoring history)"
    elif pitcher_score > 60:
        return "UNDER lean (Very pitcher-friendly)"
    elif pitcher_score < 40:
        return "OVER lean (Very hitter-friendly)"
    else:
        return "NEUTRAL (No strong umpire bias)"

if __name__ == "__main__":
    # Test the umpire integration
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="2024-04-15", help="Date to test (YYYY-MM-DD)")
    parser.add_argument("--output", default="test_umpires", help="Output directory")
    args = parser.parse_args()
    
    out_dir = Path(args.output)
    out_dir.mkdir(exist_ok=True)
    
    fetch_umpire_assignments_for_date(args.date, out_dir)
    print("\n👨‍⚖️ Umpire integration test complete!")