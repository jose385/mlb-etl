# ==============================================================================
# FILE 6: test_weather.py (Test script)
# ==============================================================================

#!/usr/bin/env python3
"""
test_weather.py - Test weather integration
Save this as test_weather.py
"""

import os
import pandas as pd
from pathlib import Path
from weather_integration import fetch_weather_for_date

def test_weather_integration():
    """Test the weather integration end-to-end"""
    
    # Check API key
    api_key = os.getenv("OPENWEATHER_API_KEY")
    if not api_key:
        print("❌ OPENWEATHER_API_KEY not set")
        return False
    
    print("✅ API key found")
    
    # Test data fetch
    test_dir = Path("test_weather_output")
    test_dir.mkdir(exist_ok=True)
    
    test_date = "2024-04-15"  # Known game day
    
    try:
        fetch_weather_for_date(test_date, test_dir, api_key)
        
        # Check if file was created
        weather_file = test_dir / f"weather_{test_date}.parquet"
        if weather_file.exists():
            # Load and inspect data
            df = pd.read_parquet(weather_file)
            print(f"✅ Created weather file with {len(df)} records")
            print(f"✅ Columns: {list(df.columns)}")
            
            # Show sample data
            if len(df) > 0:
                print("\n📊 Sample weather data:")
                sample = df.iloc[0]
                print(f"   Game: {sample.get('home_team')} vs {sample.get('away_team')}")
                print(f"   Temperature: {sample.get('temperature_f')}°F")
                print(f"   Humidity: {sample.get('humidity_pct')}%")
                print(f"   Wind: {sample.get('wind_speed_mph')} mph")
                print(f"   HR Factor: {sample.get('hr_distance_factor_ft')} ft")
            
            # Cleanup
            import shutil
            shutil.rmtree(test_dir)
            
            return True
        else:
            print("❌ Weather file not created")
            return False
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    print("🧪 Testing Weather Integration...")
    success = test_weather_integration()
    if success:
        print("\n🎉 Weather integration working correctly!")
    else:
        print("\n❌ Weather integration has issues")