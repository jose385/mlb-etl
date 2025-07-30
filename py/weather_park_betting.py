#!/usr/bin/env python3
"""
weather_park_betting.py - Add this to your py/ directory
Combines your weather data with ballpark factors for betting insights
"""

import pandas as pd
import numpy as np
import math
from pathlib import Path
import psycopg2
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import os
try:
    from py.imports import setup_imports
    setup_imports()
except ImportError:
    pass

from py.config import require_config, get_config
# Comprehensive ballpark factors for all 30 MLB stadiums
BALLPARK_FACTORS = {
    # High-scoring parks
    "Coors Field": {
        "team": "Colorado Rockies",
        "run_factor": 1.25,
        "hr_factor": 1.35,
        "elevation": 5200,
        "foul_territory": "average",
        "dimensions": {"lf": 347, "cf": 415, "rf": 350},
        "wind_effects": {"helps_hr_direction": 270, "hurt_hr_direction": 90},
        "temp_multiplier": 1.5,  # Altitude magnifies temperature effects
        "betting_notes": "Extreme hitter's park, especially hot weather"
    },
    "Great American Ball Park": {
        "team": "Cincinnati Reds", 
        "run_factor": 1.12,
        "hr_factor": 1.18,
        "elevation": 550,
        "wind_effects": {"helps_hr_direction": 225, "hurt_hr_direction": 45},
        "betting_notes": "Hitter-friendly, wind can be a major factor"
    },
    "Yankee Stadium": {
        "team": "New York Yankees",
        "run_factor": 1.08,
        "hr_factor": 1.15,
        "elevation": 55,
        "dimensions": {"lf": 318, "cf": 408, "rf": 314},
        "wind_effects": {"helps_hr_direction": 225, "hurt_hr_direction": 45},
        "short_porch": True,
        "betting_notes": "Short right field porch, wind to RF = easy HRs"
    },
    "Fenway Park": {
        "team": "Boston Red Sox",
        "run_factor": 1.05,
        "hr_factor": 1.08,
        "elevation": 21,
        "dimensions": {"lf": 310, "cf": 420, "rf": 302},
        "green_monster": True,
        "wind_effects": {"helps_hr_direction": 270, "hurt_hr_direction": 90},
        "betting_notes": "Green Monster creates unique dynamics"
    },
    
    # Pitcher-friendly parks
    "Marlins Park": {
        "team": "Miami Marlins",
        "run_factor": 0.92,
        "hr_factor": 0.85,
        "elevation": 8,
        "dome": True,
        "large_foul_territory": True,
        "betting_notes": "Strong pitcher's park, no weather effects (dome)"
    },
    "Tropicana Field": {
        "team": "Tampa Bay Rays",
        "run_factor": 0.94,
        "hr_factor": 0.88,
        "elevation": 43,
        "dome": True,
        "betting_notes": "Pitcher-friendly dome, consistent conditions"
    },
    "Petco Park": {
        "team": "San Diego Padres",
        "run_factor": 0.95,
        "hr_factor": 0.90,
        "elevation": 62,
        "large_foul_territory": True,
        "wind_effects": {"marine_layer": True},
        "betting_notes": "Marine layer suppresses fly balls"
    },
    
    # Neutral/Average parks with specific characteristics
    "Wrigley Field": {
        "team": "Chicago Cubs",
        "run_factor": 1.02,
        "hr_factor": 1.05,
        "elevation": 595,
        "wind_effects": {"variable": True, "wrigley_wind": True},
        "dimensions": {"lf": 355, "cf": 400, "rf": 353},
        "betting_notes": "Wind is everything - can be extreme hitter's or pitcher's park"
    },
    "Kauffman Stadium": {
        "team": "Kansas City Royals",
        "run_factor": 0.98,
        "hr_factor": 0.95,
        "elevation": 750,
        "large_foul_territory": True,
        "dimensions": {"lf": 330, "cf": 410, "rf": 330},
        "betting_notes": "Large foul territory helps pitchers"
    },
    
    # Add all other stadiums with basic factors
    "Minute Maid Park": {"team": "Houston Astros", "run_factor": 1.03, "hr_factor": 1.08, "elevation": 22},
    "Globe Life Field": {"team": "Texas Rangers", "run_factor": 1.06, "hr_factor": 1.12, "elevation": 551, "dome": True},
    "T-Mobile Park": {"team": "Seattle Mariners", "run_factor": 0.97, "hr_factor": 0.92, "elevation": 134},
    "Oakland Coliseum": {"team": "Oakland Athletics", "run_factor": 0.96, "hr_factor": 0.89, "elevation": 56, "large_foul_territory": True},
    "Angel Stadium": {"team": "Los Angeles Angels", "run_factor": 0.99, "hr_factor": 0.96, "elevation": 150},
    "Dodger Stadium": {"team": "Los Angeles Dodgers", "run_factor": 0.98, "hr_factor": 0.94, "elevation": 302},
    "Chase Field": {"team": "Arizona Diamondbacks", "run_factor": 1.04, "hr_factor": 1.09, "elevation": 1086, "dome": True},
    "Truist Park": {"team": "Atlanta Braves", "run_factor": 1.01, "hr_factor": 1.03, "elevation": 1050},
    "Oriole Park": {"team": "Baltimore Orioles", "run_factor": 1.07, "hr_factor": 1.14, "elevation": 59},
    "Guaranteed Rate Field": {"team": "Chicago White Sox", "run_factor": 1.04, "hr_factor": 1.08, "elevation": 595},
    "Progressive Field": {"team": "Cleveland Guardians", "run_factor": 0.99, "hr_factor": 0.97, "elevation": 660},
    "Comerica Park": {"team": "Detroit Tigers", "run_factor": 0.97, "hr_factor": 0.93, "elevation": 585},
    "Target Field": {"team": "Minnesota Twins", "run_factor": 1.01, "hr_factor": 1.04, "elevation": 815},
    "Citi Field": {"team": "New York Mets", "run_factor": 0.98, "hr_factor": 0.95, "elevation": 39},
    "Citizens Bank Park": {"team": "Philadelphia Phillies", "run_factor": 1.05, "hr_factor": 1.11, "elevation": 20},
    "PNC Park": {"team": "Pittsburgh Pirates", "run_factor": 0.98, "hr_factor": 0.96, "elevation": 745},
    "Oracle Park": {"team": "San Francisco Giants", "run_factor": 0.94, "hr_factor": 0.87, "elevation": 43},
    "Busch Stadium": {"team": "St. Louis Cardinals", "run_factor": 1.00, "hr_factor": 1.00, "elevation": 465},
    "Rogers Centre": {"team": "Toronto Blue Jays", "run_factor": 1.02, "hr_factor": 1.05, "elevation": 300, "dome": True},
    "Nationals Park": {"team": "Washington Nationals", "run_factor": 1.01, "hr_factor": 1.03, "elevation": 56},
    "American Family Field": {"team": "Milwaukee Brewers", "run_factor": 1.00, "hr_factor": 1.02, "elevation": 635}
}

def get_ballpark_by_team(team_name: str) -> Optional[Dict]:
    """Find ballpark info by team name (handles variations)"""
    
    # Direct lookup first
    for park_name, park_info in BALLPARK_FACTORS.items():
        if park_info["team"] == team_name:
            return {**park_info, "park_name": park_name}
    
    # Fuzzy matching for team name variations
    team_keywords = team_name.lower().split()
    
    for park_name, park_info in BALLPARK_FACTORS.items():
        park_team_keywords = park_info["team"].lower().split()
        
        # Check if any significant keyword matches
        if any(keyword in park_team_keywords for keyword in team_keywords 
               if len(keyword) > 3):  # Avoid matching short words like "A's"
            return {**park_info, "park_name": park_name}
    
    return None

def calculate_wind_impact(wind_speed: float, wind_direction: float, 
                         ballpark_info: Dict) -> Dict[str, float]:
    """Calculate how wind affects this specific ballpark"""
    
    if wind_speed < 5:
        return {"wind_impact": 0.0, "hr_impact": 0.0, "description": "Calm conditions"}
    
    wind_effects = ballpark_info.get("wind_effects", {})
    
    # Special case for Wrigley Field
    if ballpark_info.get("park_name") == "Wrigley Field":
        # Wrigley's famous wind patterns
        if wind_direction >= 180 and wind_direction <= 270:  # SW to W wind
            if wind_speed >= 15:
                return {
                    "wind_impact": 1.5, 
                    "hr_impact": 2.0,
                    "description": f"STRONG WRIGLEY WIND OUT ({wind_speed} mph) - Major OVER opportunity"
                }
            elif wind_speed >= 10:
                return {
                    "wind_impact": 0.8,
                    "hr_impact": 1.2, 
                    "description": f"Wrigley wind blowing out ({wind_speed} mph) - OVER lean"
                }
        elif wind_direction >= 0 and wind_direction <= 90:  # N to E wind
            if wind_speed >= 15:
                return {
                    "wind_impact": -1.2,
                    "hr_impact": -1.8,
                    "description": f"STRONG WRIGLEY WIND IN ({wind_speed} mph) - Major UNDER opportunity"
                }
    
    # General wind impact calculation
    helps_hr_direction = wind_effects.get("helps_hr_direction", 225)  # Default SW
    
    # Calculate angle difference
    angle_diff = abs(wind_direction - helps_hr_direction)
    if angle_diff > 180:
        angle_diff = 360 - angle_diff
    
    # Wind helping (0-45 degrees from optimal)
    if angle_diff <= 45:
        impact_multiplier = 1.0 - (angle_diff / 45) * 0.7  # 1.0 to 0.3
        wind_impact = (wind_speed / 10) * impact_multiplier
        hr_impact = wind_impact * 1.5
        description = f"Wind helping HRs ({wind_speed} mph) - OVER lean"
    
    # Wind hurting (135-180 degrees from optimal) 
    elif angle_diff >= 135:
        impact_multiplier = (angle_diff - 135) / 45 * 0.8  # 0 to 0.8
        wind_impact = -(wind_speed / 10) * impact_multiplier
        hr_impact = wind_impact * 1.3
        description = f"Wind hurting HRs ({wind_speed} mph) - UNDER lean"
    
    else:
        # Crosswind - minimal impact
        wind_impact = 0.0
        hr_impact = 0.0
        description = f"Crosswind ({wind_speed} mph) - Neutral"
    
    return {
        "wind_impact": round(wind_impact, 2),
        "hr_impact": round(hr_impact, 2),
        "description": description
    }

def calculate_temperature_impact(temp_f: float, ballpark_info: Dict) -> Dict[str, float]:
    """Calculate temperature impact on ball flight"""
    
    baseline_temp = 70
    temp_diff = temp_f - baseline_temp
    
    # Base temperature effect (every 10°F = ~4 feet of carry)
    base_impact = temp_diff * 0.04  # 4 feet per 10°F = 0.4 per degree
    
    # Elevation multiplier (thin air amplifies temperature effects)
    elevation = ballpark_info.get("elevation", 100)
    elevation_multiplier = 1.0 + (elevation / 5000) * 0.5  # Coors gets 1.5x
    
    # Apply park-specific multiplier
    park_temp_multiplier = ballpark_info.get("temp_multiplier", 1.0)
    
    final_impact = base_impact * elevation_multiplier * park_temp_multiplier
    
    if temp_f >= 85:
        description = f"Hot weather ({temp_f}°F) - Strong OVER conditions"
        betting_lean = "STRONG OVER"
    elif temp_f >= 75:
        description = f"Warm weather ({temp_f}°F) - OVER lean"
        betting_lean = "OVER LEAN"
    elif temp_f <= 45:
        description = f"Cold weather ({temp_f}°F) - Strong UNDER conditions"
        betting_lean = "STRONG UNDER"
    elif temp_f <= 55:
        description = f"Cool weather ({temp_f}°F) - UNDER lean"
        betting_lean = "UNDER LEAN"
    else:
        description = f"Neutral temperature ({temp_f}°F)"
        betting_lean = "NEUTRAL"
    
    return {
        "temp_impact": round(final_impact, 2),
        "description": description,
        "betting_lean": betting_lean
    }

def analyze_weather_park_combo(weather_data: Dict, home_team: str) -> Dict[str, any]:
    """Complete weather + park analysis for betting"""
    
    # Get ballpark info
    ballpark_info = get_ballpark_by_team(home_team)
    if not ballpark_info:
        return {
            "error": f"Unknown ballpark for team: {home_team}",
            "betting_recommendation": "NO DATA"
        }
    
    # Skip weather analysis for dome stadiums
    if ballpark_info.get("dome", False):
        return {
            "ballpark": ballpark_info["park_name"],
            "team": home_team,
            "run_factor": ballpark_info["run_factor"],
            "dome_stadium": True,
            "betting_recommendation": f"Dome stadium - use base park factor ({ballpark_info['run_factor']:.2f})",
            "key_insight": ballpark_info.get("betting_notes", "Controlled environment")
        }
    
    # Extract weather data
    temp_f = weather_data.get("temperature_f", 70)
    wind_speed = weather_data.get("wind_speed_mph", 0)
    wind_direction = weather_data.get("wind_direction_deg", 0)
    humidity = weather_data.get("humidity_pct", 50)
    
    # Calculate individual impacts
    temp_analysis = calculate_temperature_impact(temp_f, ballpark_info)
    wind_analysis = calculate_wind_impact(wind_speed, wind_direction, ballpark_info)
    
    # Humidity impact (higher humidity = less ball carry)
    humidity_impact = -(humidity - 50) * 0.002  # -0.1 for 100% humidity
    
    # Combine all factors
    base_run_factor = ballpark_info["run_factor"]
    weather_adjustment = temp_analysis["temp_impact"] + wind_analysis["wind_impact"] + humidity_impact
    final_run_factor = base_run_factor * (1 + weather_adjustment)
    
    # Generate betting recommendation
    total_impact = abs(temp_analysis["temp_impact"]) + abs(wind_analysis["wind_impact"])
    
    if total_impact >= 0.15:  # Significant weather impact
        if final_run_factor >= 1.15:
            betting_rec = "STRONG OVER - Weather + Park favoring offense"
        elif final_run_factor <= 0.85:
            betting_rec = "STRONG UNDER - Weather + Park favoring pitching"
        elif final_run_factor >= 1.08:
            betting_rec = "OVER LEAN - Above average scoring conditions"
        elif final_run_factor <= 0.92:
            betting_rec = "UNDER LEAN - Below average scoring conditions"
        else:
            betting_rec = "NEUTRAL - Weather and park effects offset"
    else:
        # Minimal weather impact - go with park factor
        if base_run_factor >= 1.10:
            betting_rec = f"SLIGHT OVER - Hitter-friendly park ({base_run_factor:.2f})"
        elif base_run_factor <= 0.95:
            betting_rec = f"SLIGHT UNDER - Pitcher-friendly park ({base_run_factor:.2f})"
        else:
            betting_rec = "NEUTRAL - Average park, neutral weather"
    
    # Generate key insight
    if ballpark_info["park_name"] == "Coors Field" and temp_f >= 80:
        key_insight = "🚨 COORS FIELD + HOT WEATHER = Extreme OVER opportunity!"
    elif ballpark_info["park_name"] == "Wrigley Field" and wind_speed >= 15:
        key_insight = f"🌪️ WRIGLEY WIND GAME - {wind_analysis['description']}"
    elif total_impact >= 0.2:
        key_insight = f"🌤️ Strong weather edge: {max([temp_analysis['description'], wind_analysis['description']], key=len)}"
    else:
        key_insight = ballpark_info.get("betting_notes", "Standard park conditions")
    
    return {
        "ballpark": ballpark_info["park_name"],
        "team": home_team,
        "base_run_factor": base_run_factor,
        "weather_adjusted_factor": round(final_run_factor, 3),
        "weather_impact": round(weather_adjustment, 3),
        
        # Individual components
        "temperature_analysis": temp_analysis,
        "wind_analysis": wind_analysis,
        "humidity_impact": round(humidity_impact, 3),
        
        # Betting output
        "betting_recommendation": betting_rec,
        "key_insight": key_insight,
        "confidence": "HIGH" if total_impact >= 0.15 else "MEDIUM"
    }

def get_todays_weather_park_analysis(conn, game_date: str = None) -> List[Dict]:
    """Get weather + park analysis for all games today"""
    
    if game_date is None:
        game_date = datetime.now().strftime('%Y-%m-%d')
    
    # Query today's games with weather data
    query = """
    SELECT 
        w.game_pk,
        w.home_team,
        w.away_team,
        w.venue_name,
        w.temperature_f,
        w.humidity_pct,
        w.wind_speed_mph,
        w.wind_direction_deg,
        w.hr_distance_factor_ft,
        w.weather_impact_score
    FROM weather w
    WHERE w.game_date = %s
    ORDER BY w.game_pk
    """
    
    try:
        games_df = pd.read_sql(query, conn, params=[game_date])
        
        if games_df.empty:
            return [{'error': f'No weather data found for {game_date}'}]
        
        results = []
        
        for _, game in games_df.iterrows():
            print(f"🌤️ Analyzing {game['home_team']} vs {game['away_team']}...")
            
            weather_data = {
                'temperature_f': game['temperature_f'],
                'humidity_pct': game['humidity_pct'], 
                'wind_speed_mph': game['wind_speed_mph'],
                'wind_direction_deg': game['wind_direction_deg']
            }
            
            analysis = analyze_weather_park_combo(weather_data, game['home_team'])
            analysis['game_pk'] = game['game_pk']
            analysis['matchup'] = f"{game['away_team']} @ {game['home_team']}"
            
            results.append(analysis)
        
        return results
        
    except Exception as e:
        return [{'error': f'Database error: {e}'}]

def print_weather_park_betting_report(analysis_results: List[Dict]):
    """Print formatted weather + park betting report"""
    
    print(f"\n🌤️⚾ WEATHER + PARK BETTING ANALYSIS")
    print("=" * 70)
    
    strong_edges = []
    
    for result in analysis_results:
        if 'error' in result:
            print(f"❌ Error: {result['error']}")
            continue
        
        print(f"\n🏟️ {result['matchup']} - {result['ballpark']}")
        
        if result.get('dome_stadium'):
            print(f"   🏠 DOME STADIUM - No weather effects")
            print(f"   📊 Base park factor: {result['run_factor']:.3f}")
            print(f"   💰 {result['betting_recommendation']}")
        else:
            print(f"   📊 Base park factor: {result['base_run_factor']:.3f}")
            print(f"   🌤️ Weather adjusted: {result['weather_adjusted_factor']:.3f}")
            print(f"   🌡️ Temperature: {result['temperature_analysis']['description']}")
            print(f"   💨 Wind: {result['wind_analysis']['description']}")
            print(f"   💰 BETTING REC: {result['betting_recommendation']}")
            print(f"   🔑 {result['key_insight']}")
            
            # Flag strong edges
            if 'STRONG' in result['betting_recommendation']:
                strong_edges.append(result)
    
    # Summary of strong edges
    if strong_edges:
        print(f"\n🚨 STRONG BETTING EDGES TODAY:")
        print("=" * 50)
        for edge in strong_edges:
            print(f"   🎯 {edge['matchup']}: {edge['betting_recommendation']}")
            print(f"      💡 {edge['key_insight']}")

def main():
    """Test the weather + park analysis"""
    
    # Connect to database
    config = require_config(require_database=True)
    dsn = config.PG_DSN
    if not dsn:
        print("❌ PG_DSN environment variable must be set")
        return
    
    try:
        conn = psycopg2.connect(dsn)
        print("✅ Connected to database")
        
        # Get today's analysis
        print("🔍 Getting today's weather + park analysis...")
        results = get_todays_weather_park_analysis(conn)
        print_weather_park_betting_report(results)
        
        # Test specific scenario
        print("\n🧪 Testing Coors Field hot weather scenario...")
        coors_test = analyze_weather_park_combo(
            {
                "temperature_f": 90,
                "wind_speed_mph": 8,
                "wind_direction_deg": 225,
                "humidity_pct": 30
            },
            "Colorado Rockies"
        )
        print(f"   🎯 Result: {coors_test['betting_recommendation']}")
        print(f"   📊 Adjusted factor: {coors_test['weather_adjusted_factor']}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()