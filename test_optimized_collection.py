#!/usr/bin/env python3
"""
test_optimized_collection.py - Quick test of optimized API collection
Run this to test the optimized approach before updating the main backfill
"""

import time
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime

class QuickMLBAPITest:
    """Quick test of optimized MLB API approach"""
    
    def __init__(self):
        self.call_count = 0
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'MLB-Analysis-Test/1.0'})
    
    def make_api_call(self, endpoint: str, params: dict = None):
        """Make API call with basic rate limiting"""
        if self.call_count > 0:
            print(f"🚦 Waiting 3 seconds between API calls...")
            time.sleep(3)  # Conservative delay
        
        try:
            self.call_count += 1
            url = f"https://statsapi.mlb.com/api/v1/{endpoint}"
            print(f"📡 API Call #{self.call_count}: {endpoint}")
            
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print(f"🚦 Rate limited! Status: {e.response.status_code}")
                return None
            else:
                print(f"❌ HTTP Error {e.response.status_code}: {e}")
                return None
        except Exception as e:
            print(f"❌ API Error: {e}")
            return None
    
    def test_optimized_collection(self, date_str: str = "2024-07-20"):
        """Test the optimized collection approach"""
        print(f"🎯 Testing optimized collection for {date_str}")
        print("=" * 50)
        
        start_time = time.time()
        
        # Single API call to get comprehensive schedule data
        schedule_data = self.make_api_call("schedule", {
            "date": date_str,
            "sportId": 1,  # MLB
            "hydrate": "game(content(editorial(recap))),decisions,scoreboard,probablePitcher,staff"
        })
        
        if not schedule_data or 'dates' not in schedule_data:
            print(f"❌ No data returned for {date_str}")
            return
        
        # Extract games and data
        games = []
        for date_obj in schedule_data['dates']:
            for game in date_obj.get('games', []):
                games.append(game)
        
        if not games:
            print(f"✅ No games scheduled for {date_str}")
            return
        
        print(f"✅ Found {len(games)} games")
        
        # Extract comprehensive data from single API response
        game_records = []
        for game in games:
            game_info = {
                'game_pk': game.get('gamePk'),
                'home_team': game.get('teams', {}).get('home', {}).get('team', {}).get('name', ''),
                'away_team': game.get('teams', {}).get('away', {}).get('team', {}).get('name', ''),
                'venue': game.get('venue', {}).get('name', ''),
                'status': game.get('status', {}).get('detailedState', ''),
                'game_time': game.get('gameDate', ''),
            }
            
            # Extract starting pitchers
            teams = game.get('teams', {})
            if 'probablePitcher' in teams.get('home', {}):
                game_info['home_pitcher'] = teams['home']['probablePitcher'].get('fullName')
            if 'probablePitcher' in teams.get('away', {}):
                game_info['away_pitcher'] = teams['away']['probablePitcher'].get('fullName')
            
            # Extract scores if final
            if game.get('status', {}).get('abstractGameState') == 'Final':
                game_info['home_score'] = teams.get('home', {}).get('score')
                game_info['away_score'] = teams.get('away', {}).get('score')
            
            game_records.append(game_info)
        
        # Show results
        elapsed = time.time() - start_time
        
        print(f"\n🎉 RESULTS:")
        print(f"   📊 Games collected: {len(game_records)}")
        print(f"   📡 API calls used: {self.call_count}")
        print(f"   ⏱️ Time taken: {elapsed:.1f} seconds")
        print(f"   🎯 Efficiency: {len(game_records)}/{self.call_count} games per API call")
        
        # Show data sample
        if game_records:
            print(f"\n📋 Sample data collected:")
            sample_game = game_records[0]
            for key, value in sample_game.items():
                print(f"   {key}: {value}")
        
        # Compare to old method
        old_method_calls = len(game_records) * 6  # Old method: ~6 calls per game
        print(f"\n💰 Efficiency comparison:")
        print(f"   Old method would use: ~{old_method_calls} API calls")
        print(f"   New method used: {self.call_count} API calls")
        print(f"   Calls saved: {old_method_calls - self.call_count}")
        print(f"   Efficiency improvement: {((old_method_calls - self.call_count) / old_method_calls) * 100:.1f}%")
        
        # Save test data
        if game_records:
            df = pd.DataFrame(game_records)
            output_file = Path("test_optimized_output.csv")
            df.to_csv(output_file, index=False)
            print(f"   💾 Test data saved to: {output_file}")
        
        return game_records

def main():
    """Run the optimization test"""
    tester = QuickMLBAPITest()
    
    print("🚀 Testing MLB API Optimization")
    print("This will test the new approach with minimal API calls")
    print()
    
    # Test with a recent date
    test_date = "2024-07-20"  # Use a date you know had games
    
    try:
        results = tester.test_optimized_collection(test_date)
        
        if results:
            print(f"\n✅ Optimization test successful!")
            print(f"💡 This approach should solve the rate limiting issue")
            print(f"🎯 Ready to apply to main backfill script")
        else:
            print(f"\n⚠️ Test completed but no data collected")
            print(f"💡 Try a different date or check API status")
            
    except KeyboardInterrupt:
        print(f"\n⚠️ Test cancelled by user")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()