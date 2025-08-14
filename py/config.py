#!/usr/bin/env python3

import os
import sys
import pandas as pd
import json
import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from py.config import config

def get_db_connection():
    """Create database connection using existing config setup"""
    try:
        import psycopg2
        return psycopg2.connect(config.PG_DSN)
    except ImportError:
        # Fallback to database manager if psycopg2 not available
        try:
            db_manager = config.get_database_manager()
            if db_manager:
                return db_manager.get_connection()
            else:
                raise Exception("Database manager not available")
        except Exception as e:
            raise Exception(f"Cannot connect to database: {e}")
    except Exception as e:
        raise Exception(f"Database connection failed: {e}")

def get_available_dates(conn, days_back: int = 14) -> List[str]:
    """Get list of available dates with game data"""
    query = """
    SELECT DISTINCT game_date 
    FROM game_info 
    WHERE game_date IS NOT NULL 
    ORDER BY game_date DESC 
    LIMIT %s
    """
    
    result = pd.read_sql(query, conn, params=[days_back])
    return [str(date) for date in result['game_date'].tolist()]

def get_most_recent_game_date(conn) -> str:
    """Get the most recent date with game data"""
    query = """
    SELECT MAX(game_date) 
    FROM game_info 
    WHERE game_date IS NOT NULL
    """
    
    result = pd.read_sql(query, conn)
    return str(result.iloc[0, 0]) if not result.empty else None

def get_games_for_date(conn, target_date: str) -> List[Dict]:
    """Get all games for a specific date"""
    query = """
    SELECT game_pk, home_team, away_team, home_score, away_score, venue_name
    FROM game_info 
    WHERE game_date = %s
    ORDER BY game_pk
    """
    
    try:
        games_df = pd.read_sql(query, conn, params=[target_date])
        
        if games_df.empty:
            return [{"error": f"No games found for {target_date}"}]
        
        games = []
        for _, row in games_df.iterrows():
            game_info = {
                "game_pk": int(row['game_pk']),
                "matchup": f"{row['away_team']} @ {row['home_team']}",
                "score": f"{row['home_score']}-{row['away_score']}" if pd.notna(row['home_score']) else "N/A",
                "venue": row['venue_name'] or "Unknown Venue"
            }
            games.append(game_info)
        
        return games
        
    except Exception as e:
        return [{"error": f"Database error: {str(e)}"}]

def analyze_pitcher_performance(df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze individual pitcher performance with velocity trends and contact quality allowed"""
    pitcher_analysis = {}
    
    for pitcher_id in df['pitcher'].unique():
        if pd.isna(pitcher_id):
            continue
            
        pitcher_data = df[df['pitcher'] == pitcher_id].copy()
        
        # Only analyze pitchers with sufficient data
        if len(pitcher_data) < 10:
            continue
        
        # Basic metrics
        total_pitches = len(pitcher_data)
        avg_velocity = pitcher_data['release_speed'].mean()
        
        # Velocity trends by inning
        velocity_by_inning = {}
        velocity_decline = None
        
        if 'inning' in pitcher_data.columns:
            inning_velocity = pitcher_data.groupby('inning')['release_speed'].mean()
            velocity_by_inning = {int(inning): round(vel, 1) for inning, vel in inning_velocity.items() if pd.notna(vel)}
            
            # Calculate velocity decline (first vs last inning)
            if len(velocity_by_inning) >= 2:
                first_inning_vel = list(velocity_by_inning.values())[0]
                last_inning_vel = list(velocity_by_inning.values())[-1]
                velocity_decline = round(last_inning_vel - first_inning_vel, 1)
        
        # Contact quality allowed
        batted_balls = pitcher_data.dropna(subset=['launch_speed'])
        hard_contact_allowed = 0
        barrels_allowed = 0
        avg_exit_velo_against = None
        
        if len(batted_balls) > 0:
            hard_contact_allowed = len(batted_balls[batted_balls['launch_speed'] >= 95])
            barrels_allowed = len(batted_balls[batted_balls['launch_speed_angle'] == 6])
            avg_exit_velo_against = round(batted_balls['launch_speed'].mean(), 1)
        
        # Expected stats against
        expected_ba_against = None
        expected_woba_against = None
        
        if 'estimated_ba_using_speedangle' in pitcher_data.columns:
            expected_ba_data = pitcher_data.dropna(subset=['estimated_ba_using_speedangle'])
            if len(expected_ba_data) > 0:
                expected_ba_against = round(expected_ba_data['estimated_ba_using_speedangle'].mean(), 3)
        
        if 'estimated_woba_using_speedangle' in pitcher_data.columns:
            expected_woba_data = pitcher_data.dropna(subset=['estimated_woba_using_speedangle'])
            if len(expected_woba_data) > 0:
                expected_woba_against = round(expected_woba_data['estimated_woba_using_speedangle'].mean(), 3)
        
        # Pitch mix
        pitch_mix = {}
        if 'pitch_type' in pitcher_data.columns:
            pitch_counts = pitcher_data['pitch_type'].value_counts()
            pitch_mix = {pitch_type: int(count) for pitch_type, count in pitch_counts.items() if pd.notna(pitch_type)}
        
        # Innings estimate (rough calculation)
        innings_estimate = round(total_pitches / 15, 1)  # ~15 pitches per inning average
        
        pitcher_analysis[str(pitcher_id)] = {
            "total_pitches": total_pitches,
            "innings_estimate": innings_estimate,
            "avg_velocity": round(avg_velocity, 1) if pd.notna(avg_velocity) else None,
            "velocity_by_inning": velocity_by_inning,
            "velocity_decline": velocity_decline,
            "hard_contact_allowed": hard_contact_allowed,
            "barrels_allowed": barrels_allowed,
            "avg_exit_velo_against": avg_exit_velo_against,
            "expected_ba_against": expected_ba_against,
            "expected_woba_against": expected_woba_against,
            "pitch_mix": pitch_mix
        }
    
    return pitcher_analysis

def analyze_batter_performance(df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze individual batter contact quality and expected performance"""
    batter_analysis = {}
    
    for batter_id in df['batter'].unique():
        if pd.isna(batter_id):
            continue
            
        batter_data = df[df['batter'] == batter_id].copy()
        
        # Only analyze batters with at least one plate appearance
        if len(batter_data) == 0:
            continue
        
        total_plate_appearances = len(batter_data)
        
        # Contact quality metrics
        batted_balls = batter_data.dropna(subset=['launch_speed'])
        avg_exit_velocity = None
        max_exit_velocity = None
        barrels = 0
        hard_hit_balls = 0
        weak_contact = 0
        
        if len(batted_balls) > 0:
            avg_exit_velocity = round(batted_balls['launch_speed'].mean(), 1)
            max_exit_velocity = round(batted_balls['launch_speed'].max(), 1)
            hard_hit_balls = len(batted_balls[batted_balls['launch_speed'] >= 95])
            weak_contact = len(batted_balls[batted_balls['launch_speed'] < 80])
            
            # Barrels
            if 'launch_speed_angle' in batted_balls.columns:
                barrels = len(batted_balls[batted_balls['launch_speed_angle'] == 6])
        
        # Expected performance
        expected_batting_avg = None
        expected_woba = None
        
        if 'estimated_ba_using_speedangle' in batter_data.columns:
            expected_ba_data = batter_data.dropna(subset=['estimated_ba_using_speedangle'])
            if len(expected_ba_data) > 0:
                expected_batting_avg = round(expected_ba_data['estimated_ba_using_speedangle'].mean(), 3)
        
        if 'estimated_woba_using_speedangle' in batter_data.columns:
            expected_woba_data = batter_data.dropna(subset=['estimated_woba_using_speedangle'])
            if len(expected_woba_data) > 0:
                expected_woba = round(expected_woba_data['estimated_woba_using_speedangle'].mean(), 3)
        
        # Platoon data
        batter_handedness = None
        pitcher_handedness = None
        platoon_advantage = None
        
        if 'stand' in batter_data.columns:
            stand_values = batter_data['stand'].dropna().unique()
            if len(stand_values) > 0:
                batter_handedness = stand_values[0]  # Should be consistent for same batter
        
        if 'p_throws' in batter_data.columns:
            pitcher_throws = batter_data['p_throws'].dropna().unique()
            if len(pitcher_throws) == 1:  # Faced only one pitcher handedness
                pitcher_handedness = pitcher_throws[0]
                platoon_advantage = batter_handedness != pitcher_handedness
        
        batter_analysis[str(batter_id)] = {
            "total_plate_appearances": total_plate_appearances,
            "avg_exit_velocity": avg_exit_velocity,
            "max_exit_velocity": max_exit_velocity,
            "barrels": barrels,
            "hard_hit_balls": hard_hit_balls,
            "weak_contact": weak_contact,
            "expected_batting_avg": expected_batting_avg,
            "expected_woba": expected_woba,
            "batter_handedness": batter_handedness,
            "pitcher_handedness": pitcher_handedness,
            "platoon_advantage": platoon_advantage
        }
    
    return batter_analysis

def analyze_performance_trends(df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze fatigue and performance trends"""
    trends = {
        "pitcher_fatigue_indicators": {},
        "hot_hitters": [],
        "struggling_hitters": []
    }
    
    # Pitcher fatigue analysis
    velocity_drops = {}
    for pitcher_id in df['pitcher'].unique():
        if pd.isna(pitcher_id):
            continue
            
        pitcher_data = df[df['pitcher'] == pitcher_id]
        if len(pitcher_data) < 15:  # Need sufficient sample
            continue
            
        if 'inning' in pitcher_data.columns:
            inning_velocity = pitcher_data.groupby('inning')['release_speed'].mean()
            if len(inning_velocity) >= 2:
                velocity_drop = inning_velocity.iloc[-1] - inning_velocity.iloc[0]
                velocity_drops[str(pitcher_id)] = round(velocity_drop, 1)
    
    trends["pitcher_fatigue_indicators"]["velocity_drop_by_pitcher"] = velocity_drops
    
    # Hot and cold hitters
    for batter_id in df['batter'].unique():
        if pd.isna(batter_id):
            continue
            
        batter_data = df[df['batter'] == batter_id]
        batted_balls = batter_data.dropna(subset=['launch_speed'])
        
        if len(batted_balls) >= 2:  # Need at least 2 batted balls
            avg_exit_velo = batted_balls['launch_speed'].mean()
            barrels = len(batted_balls[batted_balls['launch_speed_angle'] == 6]) if 'launch_speed_angle' in batted_balls.columns else 0
            
            if avg_exit_velo >= 92:  # Hot hitter threshold
                trends["hot_hitters"].append({
                    "batter_id": str(batter_id),
                    "avg_exit_velo": round(avg_exit_velo, 1),
                    "barrels": barrels,
                    "batted_balls": len(batted_balls)
                })
            elif avg_exit_velo <= 78:  # Struggling hitter threshold
                weak_contact_rate = (len(batted_balls[batted_balls['launch_speed'] < 80]) / len(batted_balls)) * 100
                trends["struggling_hitters"].append({
                    "batter_id": str(batter_id),
                    "avg_exit_velo": round(avg_exit_velo, 1),
                    "weak_contact_rate": round(weak_contact_rate, 1),
                    "batted_balls": len(batted_balls)
                })
    
    return trends

def extract_comprehensive_game_data(conn, game_pk: int) -> Dict[str, Any]:
    """Extract comprehensive game data including individual player analysis"""
    
    # Game context query
    game_info_query = """
    SELECT game_date, home_team, away_team, home_score, away_score, venue_name
    FROM game_info 
    WHERE game_pk = %s
    """
    
    # Detailed pitch data query
    pitch_data_query = """
    SELECT *
    FROM games 
    WHERE game_pk = %s
    ORDER BY at_bat_number, pitch_number
    """
    
    try:
        # Get game context
        game_info = pd.read_sql(game_info_query, conn, params=[game_pk])
        if game_info.empty:
            return {"error": f"No game info found for game_pk {game_pk}"}
        
        game_row = game_info.iloc[0]
        game_context = {
            "game_pk": game_pk,
            "game_date": str(game_row['game_date']),
            "matchup": f"{game_row['away_team']} @ {game_row['home_team']}",
            "final_score": f"{game_row['away_team']} {game_row['away_score']}, {game_row['home_team']} {game_row['home_score']}" if pd.notna(game_row['home_score']) else "In Progress",
            "venue_name": game_row['venue_name'] or "Unknown Venue"
        }
        
        # Get pitch data
        df = pd.read_sql(pitch_data_query, conn, params=[game_pk])
        if df.empty:
            return {"error": f"No pitch data found for game_pk {game_pk}"}
        
        # Basic game metrics (existing functionality)
        total_pitches = len(df)
        
        # Velocity analysis
        velocity_data = df.dropna(subset=['release_speed'])
        velocity_stats = {}
        if len(velocity_data) > 0:
            velocity_stats = {
                "avg_release_speed": round(velocity_data['release_speed'].mean(), 1),
                "velocity_range": f"{velocity_data['release_speed'].min():.1f}-{velocity_data['release_speed'].max():.1f} mph"
            }
            
            # Velocity by pitch type
            if 'pitch_type' in velocity_data.columns:
                pitch_type_velocity = velocity_data.groupby('pitch_type')['release_speed'].mean()
                velocity_stats["velocity_by_pitch_type"] = {
                    pitch_type: round(vel, 1) 
                    for pitch_type, vel in pitch_type_velocity.items() 
                    if pd.notna(vel)
                }
        
        # Batted ball analysis
        batted_balls = df.dropna(subset=['launch_speed'])
        batted_ball_stats = {}
        if len(batted_balls) > 0:
            barrels = len(batted_balls[batted_balls['launch_speed_angle'] == 6]) if 'launch_speed_angle' in batted_balls.columns else 0
            hard_hit = len(batted_balls[batted_balls['launch_speed'] >= 95])
            
            batted_ball_stats = {
                "total_batted_balls": len(batted_balls),
                "max_exit_velocity": round(batted_balls['launch_speed'].max(), 1),
                "avg_exit_velocity": round(batted_balls['launch_speed'].mean(), 1),
                "barrels": barrels,
                "hard_hit_balls": hard_hit
            }
        
        # Expected performance
        expected_stats = {}
        if 'estimated_ba_using_speedangle' in df.columns:
            xba_data = df.dropna(subset=['estimated_ba_using_speedangle'])
            if len(xba_data) > 0:
                expected_stats["avg_expected_ba"] = round(xba_data['estimated_ba_using_speedangle'].mean(), 3)
        
        if 'estimated_woba_using_speedangle' in df.columns:
            xwoba_data = df.dropna(subset=['estimated_woba_using_speedangle'])
            if len(xwoba_data) > 0:
                expected_stats["avg_expected_woba"] = round(xwoba_data['estimated_woba_using_speedangle'].mean(), 3)
        
        # Data quality assessment
        data_quality = {
            "total_records": total_pitches,
            "has_velocity_data": len(velocity_data) > 0,
            "has_batted_ball_data": len(batted_balls) > 0,
            "has_statcast_metrics": 'launch_speed_angle' in df.columns,
            "has_expected_stats": 'estimated_ba_using_speedangle' in df.columns,
            "completeness": "EXCELLENT" if total_pitches > 200 else "GOOD" if total_pitches > 100 else "LIMITED"
        }
        
        # NEW: Individual player analysis
        individual_pitchers = analyze_pitcher_performance(df)
        individual_batters = analyze_batter_performance(df)
        performance_trends = analyze_performance_trends(df)
        
        comprehensive_data = {
            "game_context": game_context,
            "statcast_metrics": {
                "basic_info": {"total_pitches": total_pitches},
                "pitch_velocity_analysis": velocity_stats,
                "batted_ball_analysis": batted_ball_stats,
                "expected_performance": expected_stats
            },
            "individual_pitchers": individual_pitchers,
            "individual_batters": individual_batters,
            "performance_trends": performance_trends,
            "data_quality": data_quality,
            "analysis_instructions": {
                "for_claude": "This game provides detailed Statcast data including individual player performance. Please research lineup information, recent player form, weather conditions, umpire assignments, and team context to provide comprehensive betting analysis.",
                "focus_areas": ["Pitcher fatigue indicators", "Individual batter contact quality", "Expected vs actual performance", "Platoon advantages"]
            }
        }
        
        return comprehensive_data
        
    except Exception as e:
        return {"error": f"Analysis error: {str(e)}"}

def export_games_to_csv(conn, target_date: str, output_file: str = None) -> str:
    """Export daily games to CSV for Claude analysis"""
    
    if output_file is None:
        output_file = f"mlb_games_{target_date.replace('-', '')}.csv"
    
    query = """
    SELECT 
        g.game_pk,
        gi.game_date,
        gi.home_team,
        gi.away_team,
        gi.home_score,
        gi.away_score,
        gi.venue_name,
        g.pitcher,
        g.batter, 
        g.pitch_type,
        g.release_speed,
        g.release_spin_rate,
        g.launch_speed,
        g.launch_angle,
        g.launch_speed_angle,
        g.estimated_ba_using_speedangle,
        g.estimated_woba_using_speedangle,
        g.pfx_x,
        g.pfx_z,
        g.plate_x,
        g.plate_z,
        g.events,
        g.inning,
        g.balls,
        g.strikes,
        g.outs_when_up,
        g.stand,
        g.p_throws
    FROM games g
    JOIN game_info gi ON g.game_pk = gi.game_pk  
    WHERE gi.game_date = %s
    ORDER BY g.game_pk, g.at_bat_number, g.pitch_number
    """
    
    df = pd.read_sql(query, conn, params=[target_date])
    df.to_csv(output_file, index=False)
    
    print(f"✅ Exported {len(df):,} pitch records to {output_file}")
    print(f"📊 Games included: {df['game_pk'].nunique()}")
    
    return output_file

def main():
    parser = argparse.ArgumentParser(description='MLB Analysis with enhanced individual player analysis')
    parser.add_argument('--auto', action='store_true', help='Use most recent date automatically')
    parser.add_argument('--date', help='Specific date (YYYY-MM-DD)')
    parser.add_argument('--export-csv', action='store_true', help='Export CSV for Claude')
    parser.add_argument('--output-dir', default='.', help='Output directory for files')
    
    args = parser.parse_args()
    
    # Get database connection
    conn = get_db_connection()
    if not conn:
        print("❌ Failed to connect to database")
        return
    
    try:
        if args.auto or args.date:
            # Automated mode
            if args.date:
                target_date = args.date
            else:
                target_date = get_most_recent_game_date(conn)
            
            print(f"🎯 Automated analysis for: {target_date}")
            
            if args.export_csv:
                csv_file = export_games_to_csv(conn, target_date)
                print(f"📤 Upload to Claude: {csv_file}")
            else:
                # Generate analysis for all games
                games = get_games_for_date(conn, target_date)
                
                if not games or "error" in games[0]:
                    print(f"❌ No games found for {target_date}")
                    return
                
                print(f"📅 Found {len(games)} games for {target_date}:")
                for i, game in enumerate(games, 1):
                    if "error" not in game:
                        print(f"   {i}. Game {game['game_pk']}: {game['matchup']} (Final: {game['score']})")
                
                # Analyze all games
                for game in games:
                    if "error" not in game:
                        print(f"\n🎯 Analyzing {game['matchup']}...")
                        comprehensive_data = extract_comprehensive_game_data(conn, game['game_pk'])
                        
                        if "error" not in comprehensive_data:
                            # Save analysis to file
                            matchup = game['matchup'].replace(' @ ', '_vs_').replace(' ', '_')
                            date_str = target_date.replace('-', '')
                            filename = f"{matchup}_{date_str}_analysis.json"
                            
                            with open(filename, 'w') as f:
                                json.dump(comprehensive_data, f, indent=2, default=str)
                            
                            print(f"✅ Saved: {filename}")
                        else:
                            print(f"❌ Error analyzing game: {comprehensive_data['error']}")
        
        else:
            # Interactive mode (existing functionality)
            available_dates = get_available_dates(conn, days_back=14)
            
            if not available_dates:
                print("❌ No game data found in database")
                return
            
            print(f"📅 Available dates: {', '.join(available_dates[:5])}")
            if len(available_dates) > 5:
                print(f"   ... and {len(available_dates) - 5} more dates")
            
            date_choice = input(f"\nChoose date ({available_dates[0]} is most recent): ").strip()
            target_date = date_choice if date_choice in available_dates else available_dates[0]
            
            games = get_games_for_date(conn, target_date)
            
            if not games or "error" in games[0]:
                print(f"❌ {games[0]['error'] if games else 'No games found'}")
                return
            
            print(f"\n📅 Found {len(games)} games for {target_date}:")
            for i, game in enumerate(games, 1):
                if "error" not in game:
                    print(f"   {i}. Game {game['game_pk']}: {game['matchup']} (Final: {game['score']})")
            
            game_choice = input(f"\nAnalyze which game? (1-{len(games)} or 'all'): ").strip()
            
            if game_choice.lower() == 'all':
                for game in games:
                    if "error" not in game:
                        print(f"\n🎯 Analyzing {game['matchup']}...")
                        comprehensive_data = extract_comprehensive_game_data(conn, game['game_pk'])
                        
                        if "error" not in comprehensive_data:
                            print(json.dumps(comprehensive_data, indent=2, default=str))
                        else:
                            print(f"❌ Error: {comprehensive_data['error']}")
            else:
                try:
                    game_index = int(game_choice) - 1
                    if 0 <= game_index < len(games):
                        selected_game = games[game_index]
                        
                        if "error" not in selected_game:
                            print(f"\n🎯 Analyzing {selected_game['matchup']}...")
                            comprehensive_data = extract_comprehensive_game_data(conn, selected_game['game_pk'])
                            
                            if "error" not in comprehensive_data:
                                print(json.dumps(comprehensive_data, indent=2, default=str))
                            else:
                                print(f"❌ Error: {comprehensive_data['error']}")
                        else:
                            print(f"❌ Error: {selected_game['error']}")
                    else:
                        print("❌ Invalid game selection")
                        
                except ValueError:
                    print("❌ Invalid input - please enter a number or 'all'")
    
    finally:
        conn.close()

if __name__ == "__main__":
    main()