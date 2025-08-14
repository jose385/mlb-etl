#!/usr/bin/env python3
"""
simple_analysis.py - UPDATED FOR AUGUST 11, 2025
Focuses on extracting data from August 11, 2025 games for betting analysis
"""

import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import os

def is_placeholder_data() -> bool:
    """Check if we're using placeholder data mode"""
    try:
        from py.config import get_config
        config = get_config()
        return getattr(config, 'USE_PLACEHOLDER_DATA', True)
    except:
        return True

def extract_game_context(conn, game_pk: int) -> Dict:
    """Extract core game context that Claude needs"""
    
    query = """
    SELECT gi.game_pk, gi.game_date, gi.home_team, gi.away_team, gi.venue_name,
           gi.home_score, gi.away_score, gi.winning_team, gi.game_status,
           gi.home_starting_pitcher, gi.away_starting_pitcher,
           gi.home_starter_name, gi.away_starter_name,
           gi.series_game_number, gi.home_team_rest_days, gi.away_team_rest_days,
           gi.game_time_et, gi.day_night, gi.attendance, gi.game_length_minutes
    FROM game_info gi
    WHERE gi.game_pk = %s
    """
    
    try:
        result = pd.read_sql(query, conn, params=[game_pk])
        if result.empty:
            return {"error": f"Game {game_pk} not found"}
        
        row = result.iloc[0]
        return {
            "game_pk": int(row['game_pk']),
            "game_date": str(row['game_date']),
            "home_team": row['home_team'],
            "away_team": row['away_team'],
            "venue_name": row['venue_name'],
            "home_score": int(row['home_score']) if pd.notna(row['home_score']) else None,
            "away_score": int(row['away_score']) if pd.notna(row['away_score']) else None,
            "winning_team": row['winning_team'],
            "game_status": row['game_status'],
            "home_starting_pitcher": int(row['home_starting_pitcher']) if pd.notna(row['home_starting_pitcher']) else None,
            "away_starting_pitcher": int(row['away_starting_pitcher']) if pd.notna(row['away_starting_pitcher']) else None,
            "home_starter_name": row['home_starter_name'],
            "away_starter_name": row['away_starter_name'],
            "series_game_number": int(row['series_game_number']) if pd.notna(row['series_game_number']) else None,
            "home_team_rest_days": int(row['home_team_rest_days']) if pd.notna(row['home_team_rest_days']) else None,
            "away_team_rest_days": int(row['away_team_rest_days']) if pd.notna(row['away_team_rest_days']) else None,
            "game_time_et": row['game_time_et'],
            "day_night": row['day_night'],
            "attendance": int(row['attendance']) if pd.notna(row['attendance']) else None,
            "game_length_minutes": int(row['game_length_minutes']) if pd.notna(row['game_length_minutes']) else None,
        }
        
    except Exception as e:
        return {"error": f"Game context extraction failed: {e}"}

def extract_enhanced_statcast_data(conn, game_pk: int, sample_size: int = 100) -> Dict:
    """Extract advanced Statcast metrics for Claude's analysis"""
    
    query = """
    SELECT pitcher, batter, pitch_type, release_speed, effective_speed, 
           release_spin_rate, release_extension,
           plate_x, plate_z, zone, pfx_x, pfx_z,
           launch_speed, launch_angle, hit_distance_sc, launch_speed_angle,
           estimated_ba_using_speedangle, estimated_woba_using_speedangle, 
           estimated_slg_using_speedangle, woba_value, babip_value, iso_value,
           hc_x, hc_y, events, balls, strikes, inning, inning_topbot,
           home_team, away_team
    FROM games 
    WHERE game_pk = %s 
    ORDER BY at_bat_number, pitch_number
    LIMIT %s
    """
    
    try:
        result = pd.read_sql(query, conn, params=[game_pk, sample_size])
        
        if result.empty:
            return {"error": "No Statcast data found", "sample_size": 0}
        
        # Convert to clean dict format for Claude
        statcast_summary = {
            "sample_size": len(result),
            "pitch_types": result['pitch_type'].value_counts().to_dict(),
            "avg_release_speed": round(float(result['release_speed'].mean()), 1) if pd.notna(result['release_speed']).any() else None,
            "max_release_speed": round(float(result['release_speed'].max()), 1) if pd.notna(result['release_speed']).any() else None,
            "avg_spin_rate": round(float(result['release_spin_rate'].mean()), 0) if pd.notna(result['release_spin_rate']).any() else None,
            "batted_balls": len(result[pd.notna(result['launch_speed'])]),
            "avg_exit_velo": round(float(result['launch_speed'].mean()), 1) if pd.notna(result['launch_speed']).any() else None,
            "max_exit_velo": round(float(result['launch_speed'].max()), 1) if pd.notna(result['launch_speed']).any() else None,
            "avg_launch_angle": round(float(result['launch_angle'].mean()), 1) if pd.notna(result['launch_angle']).any() else None,
            "barrels": len(result[result['launch_speed_angle'] == 6]) if 'launch_speed_angle' in result.columns else 0,
            "avg_expected_ba": round(float(result['estimated_ba_using_speedangle'].mean()), 3) if pd.notna(result['estimated_ba_using_speedangle']).any() else None,
            "avg_expected_woba": round(float(result['estimated_woba_using_speedangle'].mean()), 3) if pd.notna(result['estimated_woba_using_speedangle']).any() else None,
            "events": result['events'].value_counts().to_dict(),
            "home_team": result['home_team'].iloc[0] if not result.empty else None,
            "away_team": result['away_team'].iloc[0] if not result.empty else None,
            "data_quality": "EXCELLENT" if len(result) >= 100 else "GOOD" if len(result) >= 50 else "LIMITED"
        }
        
        return statcast_summary
        
    except Exception as e:
        return {"error": f"Statcast extraction failed: {e}"}

def extract_comprehensive_game_data(conn, game_pk: int) -> Dict:
    """Extract all core data Claude needs for betting analysis"""
    
    print(f"🔍 Extracting comprehensive data for game {game_pk}...")
    
    # Extract core game context
    print(f"   📋 Game context...")
    game_context = extract_game_context(conn, game_pk)
    
    if "error" in game_context:
        return game_context
    
    # Extract advanced Statcast data
    print(f"   ⚾ Enhanced Statcast metrics...")
    statcast_data = extract_enhanced_statcast_data(conn, game_pk)
    
    # Compile comprehensive dataset
    comprehensive_data = {
        "game_context": game_context,
        "statcast_metrics": statcast_data,
        "data_quality_summary": {
            "game_context": "error" not in game_context,
            "statcast_available": "error" not in statcast_data,
        },
        "target_date": "2025-08-11",
        "extraction_timestamp": datetime.now().isoformat()
    }
    
    return comprehensive_data

def get_games_for_august_11(conn) -> List[Dict]:
    """Get list of games from August 11, 2025"""
    
    query = """
    SELECT game_pk, home_team, away_team, venue_name, game_status,
           home_score, away_score
    FROM game_info 
    WHERE game_date = '2025-08-11'
    ORDER BY game_pk
    """
    
    try:
        games = pd.read_sql(query, conn)
        
        if games.empty:
            return [{"error": "No games found for 2025-08-11"}]
        
        return games.to_dict('records')
        
    except Exception as e:
        return [{"error": f"Database error: {e}"}]

def print_data_extraction_summary(data: Dict):
    """Print a summary of extracted data for Claude"""
    
    print(f"\n📊 AUGUST 11, 2025 - DATA EXTRACTION SUMMARY")
    print("=" * 60)
    
    game_context = data.get('game_context', {})
    if 'error' not in game_context:
        print(f"🏟️ Game: {game_context['away_team']} @ {game_context['home_team']}")
        print(f"   📍 Venue: {game_context['venue_name']}")
        print(f"   📅 Date: {game_context['game_date']}")
        if game_context.get('home_score') is not None:
            print(f"   📊 Final Score: {game_context['away_team']} {game_context['away_score']} - {game_context['home_score']} {game_context['home_team']}")
            print(f"   🏆 Winner: {game_context['winning_team']}")
    
    # Statcast summary
    statcast = data.get('statcast_metrics', {})
    if 'error' not in statcast:
        print(f"\n⚾ REAL STATCAST DATA SUMMARY:")
        print(f"   📊 Total Pitches: {statcast.get('sample_size', 0):,}")
        print(f"   ⚡ Fastest Pitch: {statcast.get('max_release_speed', 'N/A')} mph")
        print(f"   🎯 Avg Velocity: {statcast.get('avg_release_speed', 'N/A')} mph")
        print(f"   💥 Max Exit Velo: {statcast.get('max_exit_velo', 'N/A')} mph")
        print(f"   🎪 Barrels: {statcast.get('barrels', 0)}")
        print(f"   🏏 Batted Balls: {statcast.get('batted_balls', 0)}")
        print(f"   🎲 Data Quality: {statcast.get('data_quality', 'UNKNOWN')}")
        
        if statcast.get('pitch_types'):
            print(f"   🎾 Pitch Mix: {dict(list(statcast['pitch_types'].items())[:3])}")
    
    print(f"\n🎯 READY FOR BETTING ANALYSIS!")
    print(f"   🤖 This is REAL MLB data from August 11, 2025")
    print(f"   📈 Advanced Statcast metrics included")
    print(f"   💰 Ready for Claude's betting recommendations")

def main():
    """Main analysis function for August 11, 2025"""
    
    try:
        from py.config import require_config
        config = require_config(require_database=True, graceful_degradation=True)
        dsn = config.PG_DSN
        
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        return
    
    if not dsn:
        print("❌ PG_DSN environment variable must be set")
        return
    
    try:
        conn = psycopg2.connect(dsn)
        print("✅ Connected to database")
        print("🎯 Analyzing games from August 11, 2025")
        
        # Get games from August 11
        games = get_games_for_august_11(conn)
        
        if len(games) == 1 and "error" in games[0]:
            print(f"❌ {games[0]['error']}")
            print("💡 Make sure you've run the backfill for 2025-08-11")
            return
        
        print(f"📅 Found {len(games)} games from August 11, 2025:")
        
        # Show available games
        for i, game in enumerate(games, 1):
            if "error" not in game:
                score_info = ""
                if game.get('home_score') is not None:
                    score_info = f" (Final: {game['away_score']}-{game['home_score']})"
                print(f"   {i}. Game {game['game_pk']}: {game['away_team']} @ {game['home_team']}{score_info}")
        
        # Analyze all games or let user choose
        choice = input(f"\nAnalyze which game? (1-{len(games)} or 'all'): ").strip().lower()
        
        if choice == 'all':
            for game in games:
                if "error" not in game:
                    print(f"\n" + "="*60)
                    comprehensive_data = extract_comprehensive_game_data(conn, game['game_pk'])
                    print_data_extraction_summary(comprehensive_data)
        else:
            try:
                game_index = int(choice) - 1
                if 0 <= game_index < len(games):
                    game_pk = games[game_index]['game_pk']
                    print(f"\n🎯 Extracting data for Game {game_pk}...")
                    
                    # Extract comprehensive data
                    comprehensive_data = extract_comprehensive_game_data(conn, game_pk)
                    
                    # Print summary
                    print_data_extraction_summary(comprehensive_data)
                    
                    # Save to file for Claude
                    save_choice = input("\n💾 Save data to file for Claude analysis? (y/n): ").lower()
                    if save_choice == 'y':
                        import json
                        filename = f"game_{game_pk}_aug11_data.json"
                        with open(filename, 'w') as f:
                            json.dump(comprehensive_data, f, indent=2, default=str)
                        print(f"✅ Data saved to {filename}")
                        print(f"🎯 Upload this file to Claude for betting analysis!")
                else:
                    print("❌ Invalid game selection")
            except ValueError:
                print("❌ Invalid input. Please enter a number or 'all'")
        
    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()