#!/usr/bin/env python3
"""
STREAMLINED MLB Analysis - Claude-Optimized Version
Extracts only the data your system collects (Statcast + game context)
Claude will research: lineups, umpires, weather, recent stats, etc.
"""

import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import os
import json

def is_placeholder_data() -> bool:
    """Check if we're using placeholder data mode"""
    try:
        from py.config import get_config
        config = get_config()
        return getattr(config, 'USE_PLACEHOLDER_DATA', True)
    except:
        return True

def extract_game_context(conn, game_pk: int) -> Dict:
    """Extract basic game context from game_info table"""
    
    query = """
    SELECT gi.game_pk, gi.game_date, gi.home_team, gi.away_team, gi.venue_name,
           gi.home_score, gi.away_score, gi.winning_team, gi.game_status,
           gi.game_time_et, gi.day_night, gi.attendance, gi.game_length_minutes
    FROM game_info gi
    WHERE gi.game_pk = %s
    """
    
    try:
        result = pd.read_sql(query, conn, params=[game_pk])
        if result.empty:
            return {"error": f"Game {game_pk} not found in game_info table"}
        
        row = result.iloc[0]
        return {
            "game_pk": int(row['game_pk']),
            "game_date": str(row['game_date']),
            "home_team": row['home_team'],
            "away_team": row['away_team'],
            "venue_name": row['venue_name'],
            "matchup": f"{row['away_team']} @ {row['home_team']}",
            "home_score": int(row['home_score']) if pd.notna(row['home_score']) else None,
            "away_score": int(row['away_score']) if pd.notna(row['away_score']) else None,
            "winning_team": row['winning_team'],
            "game_status": row['game_status'],
            "game_time_et": row['game_time_et'],
            "day_night": row['day_night'],
            "attendance": int(row['attendance']) if pd.notna(row['attendance']) else None,
            "game_length_minutes": int(row['game_length_minutes']) if pd.notna(row['game_length_minutes']) else None,
        }
        
    except Exception as e:
        return {"error": f"Game context extraction failed: {e}"}

def extract_advanced_statcast_metrics(conn, game_pk: int, sample_size: int = 500) -> Dict:
    """Extract the impossible-to-research Statcast data - THE GOLD MINE"""
    
    query = """
    SELECT pitcher, batter, pitch_type, release_speed, effective_speed, 
           release_spin_rate, release_extension,
           plate_x, plate_z, zone, pfx_x, pfx_z,
           launch_speed, launch_angle, hit_distance_sc, launch_speed_angle,
           estimated_ba_using_speedangle, estimated_woba_using_speedangle, 
           estimated_slg_using_speedangle, woba_value, babip_value, iso_value,
           hc_x, hc_y, events, balls, strikes, inning, inning_topbot,
           home_team, away_team, at_bat_number, pitch_number,
           stand, p_throws, outs_when_up
    FROM games 
    WHERE game_pk = %s 
    ORDER BY at_bat_number, pitch_number
    LIMIT %s
    """
    
    try:
        result = pd.read_sql(query, conn, params=[game_pk, sample_size])
        
        if result.empty:
            return {"error": "No Statcast data found", "sample_size": 0}
        
        # Calculate advanced metrics that Claude can't get anywhere else
        statcast_analysis = {
            "basic_info": {
                "total_pitches": len(result),
                "home_team": result['home_team'].iloc[0] if not result.empty else None,
                "away_team": result['away_team'].iloc[0] if not result.empty else None,
                "date_range": f"First {min(len(result), sample_size)} pitches"
            },
            
            "pitch_velocity_analysis": {
                "avg_release_speed": round(float(result['release_speed'].mean()), 1) if pd.notna(result['release_speed']).any() else None,
                "max_release_speed": round(float(result['release_speed'].max()), 1) if pd.notna(result['release_speed']).any() else None,
                "min_release_speed": round(float(result['release_speed'].min()), 1) if pd.notna(result['release_speed']).any() else None,
                "velocity_by_pitch_type": {}
            },
            
            "spin_rate_analysis": {
                "avg_spin_rate": round(float(result['release_spin_rate'].mean()), 0) if pd.notna(result['release_spin_rate']).any() else None,
                "max_spin_rate": round(float(result['release_spin_rate'].max()), 0) if pd.notna(result['release_spin_rate']).any() else None,
                "spin_rate_by_pitch_type": {}
            },
            
            "batted_ball_analysis": {
                "total_batted_balls": len(result[pd.notna(result['launch_speed'])]),
                "avg_exit_velocity": round(float(result['launch_speed'].mean()), 1) if pd.notna(result['launch_speed']).any() else None,
                "max_exit_velocity": round(float(result['launch_speed'].max()), 1) if pd.notna(result['launch_speed']).any() else None,
                "avg_launch_angle": round(float(result['launch_angle'].mean()), 1) if pd.notna(result['launch_angle']).any() else None,
                "barrels": len(result[result['launch_speed_angle'] == 6]) if 'launch_speed_angle' in result.columns else 0,
                "hard_hit_balls": len(result[result['launch_speed'] >= 95]) if pd.notna(result['launch_speed']).any() else 0
            },
            
            "expected_performance": {
                "avg_expected_ba": round(float(result['estimated_ba_using_speedangle'].mean()), 3) if pd.notna(result['estimated_ba_using_speedangle']).any() else None,
                "avg_expected_woba": round(float(result['estimated_woba_using_speedangle'].mean()), 3) if pd.notna(result['estimated_woba_using_speedangle']).any() else None,
                "avg_expected_slg": round(float(result['estimated_slg_using_speedangle'].mean()), 3) if pd.notna(result['estimated_slg_using_speedangle']).any() else None,
            },
            
            "pitch_mix": result['pitch_type'].value_counts().to_dict(),
            "outcome_distribution": result['events'].value_counts().to_dict(),
            
            "advanced_metrics": {
                "strike_percentage": round((len(result[result['strikes'] > 0]) / len(result)) * 100, 1) if len(result) > 0 else 0,
                "zone_rate": round((len(result[result['zone'] <= 9]) / len(result)) * 100, 1) if len(result) > 0 else 0,
                "contact_rate": round((len(result[pd.notna(result['launch_speed'])]) / len(result)) * 100, 1) if len(result) > 0 else 0,
            },
            
            "data_quality": {
                "completeness": "EXCELLENT" if len(result) >= 250 else "GOOD" if len(result) >= 100 else "LIMITED",
                "has_statcast_metrics": pd.notna(result['launch_speed']).any(),
                "has_pitch_tracking": pd.notna(result['release_speed']).any(),
                "has_expected_stats": pd.notna(result['estimated_ba_using_speedangle']).any()
            }
        }
        
        # Calculate pitch type breakdowns
        if not result['pitch_type'].isna().all():
            for pitch_type in result['pitch_type'].unique():
                if pd.notna(pitch_type):
                    pitch_data = result[result['pitch_type'] == pitch_type]
                    if len(pitch_data) > 0 and pd.notna(pitch_data['release_speed']).any():
                        statcast_analysis["pitch_velocity_analysis"]["velocity_by_pitch_type"][pitch_type] = round(float(pitch_data['release_speed'].mean()), 1)
                    if len(pitch_data) > 0 and pd.notna(pitch_data['release_spin_rate']).any():
                        statcast_analysis["spin_rate_analysis"]["spin_rate_by_pitch_type"][pitch_type] = round(float(pitch_data['release_spin_rate'].mean()), 0)
        
        return statcast_analysis
        
    except Exception as e:
        return {"error": f"Statcast extraction failed: {e}"}

def extract_play_by_play_sequences(conn, game_pk: int, limit: int = 50) -> Dict:
    """Extract play-by-play sequences if available (optional detailed analysis)"""
    
    query = """
    SELECT event_index, at_bat_index, inning, half_inning, 
           batting_team, batter, pitcher, events, description,
           outs, home_score, away_score, rbi
    FROM play_by_play 
    WHERE game_pk = %s 
    ORDER BY at_bat_index, event_index
    LIMIT %s
    """
    
    try:
        result = pd.read_sql(query, conn, params=[game_pk, limit])
        
        if result.empty:
            return {"available": False, "reason": "No play-by-play data in database"}
        
        return {
            "available": True,
            "total_plays": len(result),
            "scoring_plays": len(result[result['rbi'] > 0]) if 'rbi' in result.columns else 0,
            "sample_plays": result.head(10).to_dict('records'),
            "inning_summary": result.groupby('inning').size().to_dict(),
            "events_summary": result['events'].value_counts().to_dict()
        }
        
    except Exception as e:
        return {"available": False, "error": f"Play-by-play extraction failed: {e}"}

def extract_comprehensive_game_data(conn, game_pk: int) -> Dict:
    """Extract all data your system provides - Claude will research the rest"""
    
    print(f"🔍 Extracting streamlined data for game {game_pk}...")
    
    # Extract basic game context
    print(f"   📋 Game context...")
    game_context = extract_game_context(conn, game_pk)
    
    if "error" in game_context:
        return game_context
    
    # Extract the Statcast gold mine
    print(f"   ⚾ Advanced Statcast metrics...")
    statcast_data = extract_advanced_statcast_metrics(conn, game_pk)
    
    # Optionally extract play-by-play
    print(f"   🎯 Play-by-play sequences...")
    play_by_play_data = extract_play_by_play_sequences(conn, game_pk)
    
    # What Claude will research
    claude_research_areas = [
        "Starting lineups and batting orders",
        "Umpire assignments and historical tendencies", 
        "Weather conditions and wind patterns",
        "Recent player performance trends (last 7/15/30 days)",
        "Injury reports and roster moves",
        "Ballpark factors and dimensions",
        "Head-to-head historical matchups",
        "Pitcher vs. hitter historical data",
        "Team form and recent results",
        "Betting line movements and market sentiment"
    ]
    
    # Compile streamlined dataset
    comprehensive_data = {
        "data_source_summary": {
            "your_system_provides": "Advanced Statcast metrics + game context",
            "claude_will_research": claude_research_areas,
            "division_of_labor": "Your system = impossible-to-get data, Claude = easy-to-research context"
        },
        
        "game_context": game_context,
        "statcast_metrics": statcast_data,
        "play_by_play": play_by_play_data,
        
        "data_quality_summary": {
            "game_context": "error" not in game_context,
            "statcast_available": "error" not in statcast_data,
            "play_by_play_available": play_by_play_data.get("available", False),
            "data_completeness": statcast_data.get("data_quality", {}).get("completeness", "UNKNOWN") if "error" not in statcast_data else "ERROR"
        },
        
        "betting_analysis_ready": {
            "core_data": "error" not in game_context and "error" not in statcast_data,
            "recommended_analysis": "Send this data to Claude for complete betting insights",
            "claude_instructions": "Claude: Please research the missing context and provide betting recommendations"
        },
        
        "extraction_metadata": {
            "extraction_timestamp": datetime.now().isoformat(),
            "data_mode": "PLACEHOLDER" if is_placeholder_data() else "LIVE",
            "system_approach": "STREAMLINED - Focus on Statcast gold mine"
        }
    }
    
    return comprehensive_data

def get_games_for_date(conn, target_date: str) -> List[Dict]:
    """Get list of games for a specific date"""
    
    query = """
    SELECT game_pk, home_team, away_team, venue_name, game_status,
           home_score, away_score, game_date
    FROM game_info 
    WHERE game_date = %s
    ORDER BY game_pk
    """
    
    try:
        games = pd.read_sql(query, conn, params=[target_date])
        
        if games.empty:
            return [{"error": f"No games found for {target_date}"}]
        
        return games.to_dict('records')
        
    except Exception as e:
        return [{"error": f"Database error: {e}"}]

def get_available_dates(conn, days_back: int = 7) -> List[str]:
    """Get list of dates with available game data"""
    
    query = """
    SELECT DISTINCT game_date 
    FROM game_info 
    WHERE game_date >= CURRENT_DATE - INTERVAL '%s days'
    ORDER BY game_date DESC
    """
    
    try:
        result = pd.read_sql(query, conn, params=[days_back])
        return [str(date) for date in result['game_date'].tolist()]
    except Exception as e:
        print(f"⚠️ Could not get available dates: {e}")
        return []

def print_streamlined_summary(data: Dict):
    """Print a summary of extracted data for Claude"""
    
    print(f"\n📊 STREAMLINED DATA EXTRACTION SUMMARY")
    print("=" * 60)
    
    game_context = data.get('game_context', {})
    if 'error' not in game_context:
        print(f"🏟️ Game: {game_context['matchup']}")
        print(f"   📍 Venue: {game_context['venue_name']}")
        print(f"   📅 Date: {game_context['game_date']}")
        if game_context.get('home_score') is not None:
            print(f"   📊 Final Score: {game_context['away_team']} {game_context['away_score']} - {game_context['home_score']} {game_context['home_team']}")
            print(f"   🏆 Winner: {game_context['winning_team']}")
    
    # Statcast summary
    statcast = data.get('statcast_metrics', {})
    if 'error' not in statcast:
        basic_info = statcast.get('basic_info', {})
        velocity = statcast.get('pitch_velocity_analysis', {})
        batted_balls = statcast.get('batted_ball_analysis', {})
        data_quality = statcast.get('data_quality', {})
        
        print(f"\n⚾ STATCAST DATA (THE GOLD MINE):")
        print(f"   📊 Total Pitches: {basic_info.get('total_pitches', 0):,}")
        print(f"   ⚡ Velocity Range: {velocity.get('min_release_speed', 'N/A')} - {velocity.get('max_release_speed', 'N/A')} mph")
        print(f"   💥 Max Exit Velo: {batted_balls.get('max_exit_velocity', 'N/A')} mph")
        print(f"   🎪 Barrels: {batted_balls.get('barrels', 0)}")
        print(f"   💪 Hard Hit Balls: {batted_balls.get('hard_hit_balls', 0)}")
        print(f"   🎲 Data Quality: {data_quality.get('completeness', 'UNKNOWN')}")
        
        pitch_mix = statcast.get('pitch_mix', {})
        if pitch_mix:
            top_pitches = dict(list(pitch_mix.items())[:3])
            print(f"   🎾 Top Pitch Types: {top_pitches}")
    
    # Play-by-play summary
    pbp = data.get('play_by_play', {})
    if pbp.get('available'):
        print(f"\n🎯 PLAY-BY-PLAY SEQUENCES:")
        print(f"   📊 Total Plays: {pbp.get('total_plays', 0)}")
        print(f"   🏃 Scoring Plays: {pbp.get('scoring_plays', 0)}")
    else:
        print(f"\n🎯 PLAY-BY-PLAY: Not available ({pbp.get('reason', 'Unknown')})")
    
    # Claude research areas
    research_areas = data.get('data_source_summary', {}).get('claude_will_research', [])
    print(f"\n🤖 CLAUDE WILL RESEARCH:")
    for area in research_areas[:5]:  # Show first 5
        print(f"   • {area}")
    if len(research_areas) > 5:
        print(f"   • ... and {len(research_areas) - 5} more areas")
    
    print(f"\n🎯 READY FOR BETTING ANALYSIS!")
    print(f"   ✅ Your system: Provides impossible-to-get Statcast data")
    print(f"   🤖 Claude: Will research easy-to-find context")
    print(f"   💰 Result: Complete betting analysis with minimal complexity")

def main():
    """Main streamlined analysis function"""
    
    print(f"🚀 STREAMLINED MLB Analysis - Claude Optimized")
    print(f"🎯 Focus: Extract only the data Claude cannot research")
    
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
        
        # Get available dates
        available_dates = get_available_dates(conn, days_back=14)
        
        if not available_dates:
            print("❌ No game data found in database")
            print("💡 Make sure you've run the streamlined backfill:")
            print("   python enhanced_simple_backfill.py --start YYYY-MM-DD --end YYYY-MM-DD --real-data")
            return
        
        print(f"📅 Available dates with game data: {', '.join(available_dates[:5])}")
        
        # Let user choose date or use most recent
        if len(available_dates) == 1:
            target_date = available_dates[0]
            print(f"🎯 Using date: {target_date}")
        else:
            date_choice = input(f"\nChoose date ({available_dates[0]} is most recent): ").strip()
            target_date = date_choice if date_choice in available_dates else available_dates[0]
            print(f"🎯 Using date: {target_date}")
        
        # Get games for the chosen date
        games = get_games_for_date(conn, target_date)
        
        if len(games) == 1 and "error" in games[0]:
            print(f"❌ {games[0]['error']}")
            return
        
        print(f"📅 Found {len(games)} games for {target_date}:")
        
        # Show available games
        for i, game in enumerate(games, 1):
            if "error" not in game:
                score_info = ""
                if game.get('home_score') is not None:
                    score_info = f" (Final: {game['away_score']}-{game['home_score']})"
                print(f"   {i}. Game {game['game_pk']}: {game['away_team']} @ {game['home_team']}{score_info}")
        
        # Analyze games
        choice = input(f"\nAnalyze which game? (1-{len(games)} or 'all'): ").strip().lower()
        
        if choice == 'all':
            for game in games:
                if "error" not in game:
                    print(f"\n" + "="*60)
                    comprehensive_data = extract_comprehensive_game_data(conn, game['game_pk'])
                    print_streamlined_summary(comprehensive_data)
        else:
            try:
                game_index = int(choice) - 1
                if 0 <= game_index < len(games):
                    game_pk = games[game_index]['game_pk']
                    print(f"\n🎯 Extracting streamlined data for Game {game_pk}...")
                    
                    # Extract comprehensive data
                    comprehensive_data = extract_comprehensive_game_data(conn, game_pk)
                    
                    # Print summary
                    print_streamlined_summary(comprehensive_data)
                    
                    # Save to file for Claude
                    save_choice = input("\n💾 Save data to file for Claude analysis? (y/n): ").lower()
                    if save_choice == 'y':
                        game_context = comprehensive_data.get('game_context', {})
                        matchup = game_context.get('matchup', f'game_{game_pk}').replace(' @ ', '_vs_').replace(' ', '_')
                        date_str = game_context.get('game_date', target_date).replace('-', '')
                        filename = f"{matchup}_{date_str}_statcast_data.json"
                        
                        with open(filename, 'w') as f:
                            json.dump(comprehensive_data, f, indent=2, default=str)
                        
                        print(f"✅ Data saved to {filename}")
                        print(f"🎯 Send this file to Claude with this prompt:")
                        print(f"   'Here's the Statcast data from {game_context.get('matchup', 'this game')} on {game_context.get('game_date', target_date)}.")
                        print(f"    Please research the missing context and provide betting analysis!'")
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