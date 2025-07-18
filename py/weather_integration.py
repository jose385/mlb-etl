# ==============================================================================
# FILE 1: py/weather_integration.py
# ==============================================================================

#!/usr/bin/env python3
"""
weather_integration.py - Weather data collection for MLB games
Save this as py/weather_integration.py
"""

import os
import requests
import pandas as pd
import math
from pathlib import Path
from typing import Dict, Any, Optional
import statsapi
from datetime import datetime

# Stadium coordinates for weather API calls
STADIUM_LOCATIONS = {
    "Arizona Diamondbacks": {"lat": 33.4453, "lon": -112.0667, "elevation": 1086},
    "Atlanta Braves": {"lat": 33.8906, "lon": -84.4677, "elevation": 1050},
    "Baltimore Orioles": {"lat": 39.2840, "lon": -76.6217, "elevation": 59},
    "Boston Red Sox": {"lat": 42.3467, "lon": -71.0972, "elevation": 21},
    "Chicago White Sox": {"lat": 41.8299, "lon": -87.6338, "elevation": 595},
    "Chicago Cubs": {"lat": 41.9484, "lon": -87.6553, "elevation": 595},
    "Cincinnati Reds": {"lat": 39.5031, "lon": -84.3668, "elevation": 550},
    "Cleveland Guardians": {"lat": 41.4958, "lon": -81.6853, "elevation": 660},
    "Colorado Rockies": {"lat": 39.7559, "lon": -104.9942, "elevation": 5200},
    "Detroit Tigers": {"lat": 42.3390, "lon": -83.0485, "elevation": 585},
    "Houston Astros": {"lat": 29.7570, "lon": -95.3555, "elevation": 22},
    "Kansas City Royals": {"lat": 39.0517, "lon": -94.4803, "elevation": 750},
    "Los Angeles Angels": {"lat": 33.8003, "lon": -117.8827, "elevation": 150},
    "Los Angeles Dodgers": {"lat": 34.0739, "lon": -118.2400, "elevation": 302},
    "Miami Marlins": {"lat": 25.7781, "lon": -80.2198, "elevation": 8},
    "Milwaukee Brewers": {"lat": 43.0280, "lon": -87.9712, "elevation": 635},
    "Minnesota Twins": {"lat": 44.9817, "lon": -93.2776, "elevation": 815},
    "New York Mets": {"lat": 40.7571, "lon": -73.8458, "elevation": 39},
    "New York Yankees": {"lat": 40.8296, "lon": -73.9262, "elevation": 55},
    "Oakland Athletics": {"lat": 37.7516, "lon": -122.2005, "elevation": 56},
    "Philadelphia Phillies": {"lat": 39.9061, "lon": -75.1665, "elevation": 20},
    "Pittsburgh Pirates": {"lat": 40.4469, "lon": -80.0057, "elevation": 745},
    "San Diego Padres": {"lat": 32.7073, "lon": -117.1566, "elevation": 62},
    "San Francisco Giants": {"lat": 37.7786, "lon": -122.3893, "elevation": 43},
    "Seattle Mariners": {"lat": 47.5914, "lon": -122.3326, "elevation": 134},
    "St. Louis Cardinals": {"lat": 38.6226, "lon": -90.1928, "elevation": 465},
    "Tampa Bay Rays": {"lat": 27.7682, "lon": -82.6534, "elevation": 43},
    "Texas Rangers": {"lat": 32.7513, "lon": -97.0830, "elevation": 551},
    "Toronto Blue Jays": {"lat": 43.6414, "lon": -79.3894, "elevation": 300},
    "Washington Nationals": {"lat": 38.8730, "lon": -77.0074, "elevation": 56}
}

class WeatherDataCollector:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "http://api.openweathermap.org/data/2.5"
        
    def get_weather_for_game(self, lat: float, lon: float, date_str: str) -> Dict[str, Any]:
        """Get weather data for a specific location and date"""
        try:
            # For current/recent dates, use current weather
            dt = datetime.fromisoformat(date_str)
            now = datetime.now()
            
            if (now - dt).days <= 5:
                # Use current weather for recent games
                url = f"{self.base_url}/weather"
                params = {
                    'lat': lat,
                    'lon': lon,
                    'appid': self.api_key,
                    'units': 'imperial'
                }
            else:
                # Use historical data for older games (requires paid plan)
                timestamp = int(dt.timestamp())
                url = f"{self.base_url}/onecall/timemachine"
                params = {
                    'lat': lat,
                    'lon': lon,
                    'dt': timestamp,
                    'appid': self.api_key,
                    'units': 'imperial'
                }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"⚠️ Weather API error for {lat}, {lon} on {date_str}: {e}")
            return {}
    
    def calculate_air_density(self, temp_f: float, pressure_mb: float, humidity: float) -> float:
        """Calculate air density - affects ball flight"""
        try:
            # Convert to metric
            temp_k = (temp_f - 32) * 5/9 + 273.15
            pressure_pa = pressure_mb * 100
            
            # Air density calculation
            R_dry = 287.05  # J/(kg*K)
            R_vapor = 461.495  # J/(kg*K)
            
            # Saturation vapor pressure
            e_sat = 611.21 * pow(10, (7.5 * (temp_k - 273.15)) / (237.3 + (temp_k - 273.15)))
            e = humidity / 100.0 * e_sat
            
            # Air density
            density = (pressure_pa - e) / (R_dry * temp_k) + e / (R_vapor * temp_k)
            return round(density, 4)
        except:
            return 1.225  # Standard air density
    
    def calculate_hr_factor(self, temp_f: float, humidity: float, 
                           pressure_mb: float, elevation_ft: float) -> float:
        """Calculate home run distance factor"""
        try:
            # Temperature effect: ~3.5 feet per 10°F
            temp_factor = (temp_f - 70) * 0.35
            
            # Humidity effect: denser air = shorter distance
            humidity_factor = -(humidity - 50) * 0.08
            
            # Pressure effect: lower pressure = less resistance
            pressure_factor = (1013.25 - pressure_mb) * 0.6
            
            # Elevation effect (Coors Field phenomenon)
            elevation_factor = elevation_ft * 0.002
            
            total_factor = temp_factor + humidity_factor + pressure_factor + elevation_factor
            return round(total_factor, 2)
        except:
            return 0.0

def fetch_weather_for_date(date_str: str, out_dir: Path, api_key: Optional[str] = None):
    """Enhanced weather fetch function - integrates with backfill.py"""
    out_file = out_dir / f"weather_{date_str}.parquet"
    if out_file.exists():
        print(f"⏭️ Skipping Weather for {date_str} (already exists)")
        return
    
    if not api_key:
        print(f"⚠️ No weather API key provided - skipping weather data for {date_str}")
        return
    
    print(f"🌤️ Fetching Weather for {date_str}...")
    
    # Get games for the date
    try:
        games = statsapi.schedule(start_date=date_str, end_date=date_str) or []
    except Exception as e:
        print(f"❌ Error fetching games for {date_str}: {e}")
        return
    
    if not games:
        print(f"✅ No games scheduled for {date_str}")
        return
    
    weather_collector = WeatherDataCollector(api_key)
    weather_records = []
    
    for game in games:
        game_pk = game.get("game_id") or game.get("game_pk")
        venue_name = game.get("venue_name", "")
        home_team = game.get("home_name", "")
        away_team = game.get("away_name", "")
        
        # Find stadium coordinates
        stadium_coords = None
        for team_name, coords in STADIUM_LOCATIONS.items():
            if any(word in team_name for word in home_team.split()) or \
               any(word in home_team for word in team_name.split()):
                stadium_coords = coords
                break
        
        if not stadium_coords:
            print(f"⚠️ No coordinates found for {home_team} - using default")
            stadium_coords = {"lat": 40.0, "lon": -74.0, "elevation": 100}
        
        # Get weather data
        weather_data = weather_collector.get_weather_for_game(
            stadium_coords["lat"], 
            stadium_coords["lon"], 
            date_str
        )
        
        if not weather_data:
            # Create default weather record if API fails
            weather_record = {
                "game_date": date_str,
                "game_pk": game_pk,
                "venue_name": venue_name,
                "home_team": home_team,
                "away_team": away_team,
                "stadium_lat": stadium_coords["lat"],
                "stadium_lon": stadium_coords["lon"],
                "stadium_elevation": stadium_coords["elevation"],
                "data_source": "default_fallback"
            }
        else:
            # Extract weather info (handle both current and historical API responses)
            if "current" in weather_data:
                # Historical data format
                current = weather_data["current"]
            else:
                # Current weather format
                current = weather_data
            
            # Extract main weather data
            main = current.get("main", current)
            wind = current.get("wind", {})
            
            temp_f = main.get("temp", 70)
            humidity = main.get("humidity", 50)
            pressure_mb = main.get("pressure", 1013.25)
            wind_speed = wind.get("speed", 0)
            wind_deg = wind.get("deg", 0)
            
            # Calculate advanced metrics
            air_density = weather_collector.calculate_air_density(
                temp_f, pressure_mb, humidity
            )
            
            hr_factor = weather_collector.calculate_hr_factor(
                temp_f, humidity, pressure_mb, stadium_coords["elevation"]
            )
            
            # Calculate wind components
            wind_x = wind_speed * math.cos(math.radians(wind_deg)) if wind_deg else 0
            wind_y = wind_speed * math.sin(math.radians(wind_deg)) if wind_deg else 0
            
            weather_record = {
                "game_date": date_str,
                "game_pk": game_pk,
                "venue_name": venue_name,
                "home_team": home_team,
                "away_team": away_team,
                "stadium_lat": stadium_coords["lat"],
                "stadium_lon": stadium_coords["lon"],
                "stadium_elevation": stadium_coords["elevation"],
                
                # Basic weather
                "temperature_f": round(temp_f, 1),
                "humidity_pct": humidity,
                "pressure_mb": round(pressure_mb, 1),
                "wind_speed_mph": round(wind_speed, 1),
                "wind_direction_deg": wind_deg,
                
                # Advanced calculations
                "air_density_kg_m3": air_density,
                "hr_distance_factor_ft": hr_factor,
                "wind_x_component": round(wind_x, 2),
                "wind_y_component": round(wind_y, 2),
                
                # Game impact scores (0-100 scale)
                "pitcher_advantage_score": min(100, max(0, 
                    50 + (humidity - 50) * 0.5 + (50 - temp_f) * 0.3)),
                "hitter_advantage_score": min(100, max(0,
                    50 + (temp_f - 70) * 0.4 - (humidity - 50) * 0.3)),
                "weather_impact_score": round(abs(temp_f - 72) + abs(humidity - 50) + wind_speed, 1),
                
                "data_source": "openweather_api"
            }
        
        weather_records.append(weather_record)
    
    if not weather_records:
        print(f"✅ No weather data collected for {date_str}")
        return
    
    # Save to parquet
    try:
        df = pd.DataFrame(weather_records)
        df.to_parquet(out_file, index=False)
        print(f"✅ Weather: Wrote {len(df)} records → {out_file.name}")
    except Exception as e:
        print(f"❌ Error saving weather data: {e}")

if __name__ == "__main__":
    # Test the weather integration
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default="2024-04-15", help="Date to test (YYYY-MM-DD)")
    parser.add_argument("--output", default="test_weather", help="Output directory")
    args = parser.parse_args()
    
    api_key = os.getenv("OPENWEATHER_API_KEY")
    if not api_key:
        print("❌ Set OPENWEATHER_API_KEY environment variable")
        print("   Get a free key at: https://openweathermap.org/api")
        exit(1)
    
    out_dir = Path(args.output)
    out_dir.mkdir(exist_ok=True)
    
    fetch_weather_for_date(args.date, out_dir, api_key)
    print("\n🎉 Weather integration test complete!")
