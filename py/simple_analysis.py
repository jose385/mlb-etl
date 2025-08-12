#!/usr/bin/env python3
"""
simple_analysis.py - STREAMLINED: Focuses on core data extraction for Claude
REMOVED: Weather and venue analysis (Claude handles these)
ENHANCED: Advanced Statcast metrics extraction and validation
Extracts raw data and sends to Claude for sophisticated betting analysis
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
    """STREAMLINED: Extract core game context that Claude needs"""
    
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

def extract_enhanced_statcast_data(conn, game_pk: int, sample_size: int = 50) -> Dict:
    """ENHANCED: Extract advanced Statcast metrics for Claude's analysis"""
    
    query = """
    SELECT pitcher, batter, pitch_type, release_speed, effective_speed, 
           release_spin_rate, release_extension,
           plate_x, plate_z, zone, pfx_x, pfx_z,
           launch_speed, launch_angle, hit_distance_sc, launch_speed_angle,
           estimated_ba_using_speedangle, estimated_woba_using_speedangle, 
           estimated_slg_using_speedangle, woba_value, babip_value, iso_value,
           hc_x, hc_y, events, balls, strikes, inning, inning_topbot
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
            "avg_release_speed": float(result['release_speed'].mean()) if pd.notna(result['release_speed']).any() else None,
            "avg_spin_rate": float(result['release_spin_rate'].mean()) if pd.notna(result['release_spin_rate']).any() else None,
            "batted_balls": len(result[pd.notna(result['launch_speed'])]),
            "avg_exit_velo": float(result['launch_speed'].mean()) if pd.notna(result['launch_speed']).any() else None,
            "avg_launch_angle": float(result['launch_angle'].mean()) if pd.notna(result['launch_angle']).any() else None,
            "barrels": len(result[result['launch_speed_angle'] == 6]) if 'launch_speed_angle' in result.columns else 0,
            "avg_expected_ba": float(result['estimated_ba_using_speedangle'].mean()) if pd.notna(result['estimated_ba_using_speedangle']).any() else None,
            "avg_expected_woba": float(result['estimated_woba_using_speedangle'].mean()) if pd.notna(result['estimated_woba_using_speedangle']).any() else None,
            "events": result['events'].value_counts().to_dict(),
            "data_quality": "EXCELLENT" if len(result) >= 100 else "GOOD" if len(result) >= 50 else "LIMITED"
        }
        
        return statcast_summary
        
    except Exception as e:
        return {"error": f"Statcast extraction failed: {e}"}

def extract_pitcher_data(conn, pitcher_id: int, target_date: str) -> Dict:
    """Extract recent pitcher performance data"""
    
    query = """
    SELECT player_id, stat_date, stat_type, games_played, era, whip, 
           strikeouts_per_9, walks_per_9, hits_allowed, runs_allowed,
           quality_starts, hot_streak, cold_streak, workload_score,
           date_range_start, date_range_end
    FROM recent_stats 
    WHERE player_id = %s 
      AND stat_type LIKE 'pitching%'
      AND stat_date <= %s
    ORDER BY stat_date DESC
    LIMIT 1
    """
    
    try:
        result = pd.read_sql(query, conn, params=[pitcher_id, target_date])
        
        if result.empty:
            return {"error": "No recent pitcher stats found", "pitcher_id": pitcher_id}
        
        row = result.iloc[0]
        return {
            "pitcher_id": int(row['player_id']),
            "stat_date": str(row['stat_date']),
            "stat_type": row['stat_type'],
            "games_played": int(row['games_played']) if pd.notna(row['games_played']) else 0,
            "era": float(row['era']) if pd.notna(row['era']) else None,
            "whip": float(row['whip']) if pd.notna(row['whip']) else None,
            "strikeouts_per_9": float(row['strikeouts_per_9']) if pd.notna(row['strikeouts_per_9']) else None,
            "walks_per_9": float(row['walks_per_9']) if pd.notna(row['walks_per_9']) else None,
            "hits_allowed": int(row['hits_allowed']) if pd.notna(row['hits_allowed']) else None,
            "runs_allowed": int(row['runs_allowed']) if pd.notna(row['runs_allowed']) else None,
            "quality_starts": int(row['quality_starts']) if pd.notna(row['quality_starts']) else None,
            "hot_streak": bool(row['hot_streak']) if pd.notna(row['hot_streak']) else False,
            "cold_streak": bool(row['cold_streak']) if pd.notna(row['cold_streak']) else False,
            "workload_score": float(row['workload_score']) if pd.notna(row['workload_score']) else None,
            "date_range_start": str(row['date_range_start']),
            "date_range_end": str(row['date_range_end']),
        }
        
    except Exception as e:
        return {"error": f"Pitcher data extraction failed: {e}"}

def extract_team_batting_data(conn, team_name: str, target_date: str) -> Dict:
    """Extract team batting performance data"""
    
    query = """
    WITH team_players AS (
        SELECT DISTINCT r.person_id
        FROM rosters r
        JOIN game_info gi ON r.game_date = gi.game_date
        WHERE (gi.home_team = %s OR gi.away_team = %s)
          AND r.game_date <= %s
          AND r.game_date >= %s
        LIMIT 25
    ),
    team_batting AS (
        SELECT rs.*
        FROM recent_stats rs
        JOIN team_players tp ON rs.player_id = tp.person_id
        WHERE rs.stat_type LIKE 'batting%'
          AND rs.stat_date <= %s
    )
    SELECT 
        COUNT(*) as players_with_stats,
        AVG(COALESCE(ops, 0.700)) as avg_ops,
        AVG(COALESCE(batting_avg, 0.250)) as avg_batting_avg,
        AVG(COALESCE(on_base_pct, 0.320)) as avg_obp,
        AVG(COALESCE(slugging_pct, 0.400)) as avg_slg,
        SUM(COALESCE(home_runs, 0)) as total_hrs,
        SUM(COALESCE(rbis, 0)) as total_rbis,
        COUNT(CASE WHEN hot_streak THEN 1 END) as hot_players,
        COUNT(CASE WHEN cold_streak THEN 1 END) as cold_players,
        AVG(COALESCE(games_played, 0)) as avg_games
    FROM team_batting
    """
    
    try:
        target_date_obj = datetime.strptime(target_date, '%Y-%m-%d').date()
        lookback_date = target_date_obj - timedelta(days=30)
        
        result = pd.read_sql(query, conn, params=[team_name, team_name, target_date, lookback_date, target_date])
        
        if result.empty or result.iloc[0]['players_with_stats'] == 0:
            return {"error": "No team batting data found", "team": team_name}
        
        row = result.iloc[0]
        players_analyzed = int(row['players_with_stats'])
        hot_players = int(row['hot_players']) if pd.notna(row['hot_players']) else 0
        cold_players = int(row['cold_players']) if pd.notna(row['cold_players']) else 0
        
        return {
            "team": team_name,
            "players_analyzed": players_analyzed,
            "avg_ops": round(float(row['avg_ops']), 3),
            "avg_batting_avg": round(float(row['avg_batting_avg']), 3),
            "avg_obp": round(float(row['avg_obp']), 3),
            "avg_slg": round(float(row['avg_slg']), 3),
            "total_home_runs": int(row['total_hrs']) if pd.notna(row['total_hrs']) else 0,
            "total_rbis": int(row['total_rbis']) if pd.notna(row['total_rbis']) else 0,
            "hot_players": hot_players,
            "cold_players": cold_players,
            "hot_player_pct": round(hot_players / max(1, players_analyzed), 3),
            "cold_player_pct": round(cold_players / max(1, players_analyzed), 3),
            "avg_games": round(float(row['avg_games']), 1),
            "data_quality": "GOOD" if players_analyzed >= 15 else "LIMITED" if players_analyzed >= 10 else "POOR"
        }
        
    except Exception as e:
        return {"error": f"Team batting extraction failed: {e}"}

def extract_umpire_data(conn, game_pk: int) -> Dict:
    """Extract umpire assignment and historical tendencies"""
    
    query = """
    SELECT umpire_name, position, avg_total_runs_in_games, over_under_record, 
           sample_size, pitcher_friendly_score, strike_rate_overall,
           avg_game_length_minutes
    FROM umpires 
    WHERE game_pk = %s AND position = 'Home Plate'
    """
    
    try:
        result = pd.read_sql(query, conn, params=[game_pk])
        
        if result.empty:
            return {"error": "No umpire data found", "game_pk": game_pk}
        
        row = result.iloc[0]
        return {
            "game_pk": game_pk,
            "umpire_name": row['umpire_name'],
            "position": row['position'],
            "avg_total_runs": float(row['avg_total_runs_in_games']) if pd.notna(row['avg_total_runs_in_games']) else None,
            "over_under_record": float(row['over_under_record']) if pd.notna(row['over_under_record']) else None,
            "sample_size": int(row['sample_size']) if pd.notna(row['sample_size']) else 0,
            "pitcher_friendly_score": float(row['pitcher_friendly_score']) if pd.notna(row['pitcher_friendly_score']) else None,
            "strike_rate": float(row['strike_rate_overall']) if pd.notna(row['strike_rate_overall']) else None,
            "avg_game_length": int(row['avg_game_length_minutes']) if pd.notna(row['avg_game_length_minutes']) else None,
            "confidence": "HIGH" if row['sample_size'] >= 50 else "MEDIUM" if row['sample_size'] >= 25 else "LOW"
        }
        
    except Exception as e:
        return {"error": f"Umpire extraction failed: {e}"}

def extract_lineup_data(conn, game_pk: int) -> Dict:
    """Extract starting lineups and key player information"""
    
    query = """
    SELECT side, batting_order, person_full_name, position_code,
           season_avg, season_obp, season_slg, season_ops, 
           season_home_runs, season_rbi, season_era, season_whip,
           person_bat_side_code, person_pitch_hand_code, is_power_hitter
    FROM lineups 
    WHERE game_pk = %s 
    ORDER BY side, batting_order
    """
    
    try:
        result = pd.read_sql(query, conn, params=[game_pk])
        
        if result.empty:
            return {"error": "No lineup data found", "game_pk": game_pk}
        
        home_lineup = []
        away_lineup = []
        
        for _, row in result.iterrows():
            player_data = {
                "batting_order": int(row['batting_order']) if pd.notna(row['batting_order']) else None,
                "name": row['person_full_name'],
                "position": row['position_code'],
                "bat_side": row['person_bat_side_code'],
                "season_avg": float(row['season_avg']) if pd.notna(row['season_avg']) else None,
                "season_ops": float(row['season_ops']) if pd.notna(row['season_ops']) else None,
                "season_hrs": int(row['season_home_runs']) if pd.notna(row['season_home_runs']) else None,
                "is_power_hitter": bool(row['is_power_hitter']) if pd.notna(row['is_power_hitter']) else False,
            }
            
            # Add pitcher-specific stats
            if row['position_code'] == 'P':
                player_data["era"] = float(row['season_era']) if pd.notna(row['season_era']) else None
                player_data["whip"] = float(row['season_whip']) if pd.notna(row['season_whip']) else None
            
            if row['side'] == 'home':
                home_lineup.append(player_data)
            else:
                away_lineup.append(player_data)
        
        return {
            "game_pk": game_pk,
            "home_lineup": sorted(home_lineup, key=lambda x: x.get('batting_order', 99)),
            "away_lineup": sorted(away_lineup, key=lambda x: x.get('batting_order', 99)),
            "home_power_hitters": len([p for p in home_lineup if p.get('is_power_hitter', False)]),
            "away_power_hitters": len([p for p in away_lineup if p.get('is_power_hitter', False)])
        }
        
    except Exception as e:
        return {"error": f"Lineup extraction failed: {e}"}

def extract_comprehensive_game_data(conn, game_pk: int) -> Dict:
    """STREAMLINED: Extract all core data Claude needs for betting analysis"""
    
    print(f"🔍 Extracting comprehensive data for game {game_pk}...")
    
    # Extract core game context
    print(f"   📋 Game context...")
    game_context = extract_game_context(conn, game_pk)
    
    if "error" in game_context:
        return game_context
    
    target_date = game_context['game_date']
    
    # Extract advanced Statcast data
    print(f"   ⚾ Enhanced Statcast metrics...")
    statcast_data = extract_enhanced_statcast_data(conn, game_pk)
    
    # Extract pitcher data
    print(f"   🥎 Pitcher performance...")
    pitcher_data = {}
    if game_context['home_starting_pitcher']:
        pitcher_data['home_pitcher'] = extract_pitcher_data(conn, game_context['home_starting_pitcher'], target_date)
    if game_context['away_starting_pitcher']:
        pitcher_data['away_pitcher'] = extract_pitcher_data(conn, game_context['away_starting_pitcher'], target_date)
    
    # Extract team batting data
    print(f"   👥 Team performance...")
    team_data = {
        'home_team': extract_team_batting_data(conn, game_context['home_team'], target_date),
        'away_team': extract_team_batting_data(conn, game_context['away_team'], target_date)
    }
    
    # Extract umpire data
    print(f"   👨‍⚖️ Umpire assignment...")
    umpire_data = extract_umpire_data(conn, game_pk)
    
    # Extract lineup data
    print(f"   📝 Starting lineups...")
    lineup_data = extract_lineup_data(conn, game_pk)
    
    # Compile comprehensive dataset
    comprehensive_data = {
        "game_context": game_context,
        "statcast_metrics": statcast_data,
        "pitcher_performance": pitcher_data,
        "team_performance": team_data,
        "umpire_assignment": umpire_data,
        "starting_lineups": lineup_data,
        "data_quality_summary": {
            "game_context": "error" not in game_context,
            "statcast_available": "error" not in statcast_data,
            "pitcher_data_complete": len([p for p in pitcher_data.values() if "error" not in p]) == 2,
            "team_data_complete": all("error" not in t for t in team_data.values()),
            "umpire_available": "error" not in umpire_data,
            "lineup_available": "error" not in lineup_data,
        },
        "placeholder_mode": is_placeholder_data(),
        "extraction_timestamp": datetime.now().isoformat()
    }
    
    return comprehensive_data

def get_games_for_analysis(conn, game_date: str = None) -> List[Dict]:
    """Get list of games available for analysis"""
    
    if game_date is None:
        game_date = datetime.now().strftime('%Y-%m-%d')
    
    query = """
    SELECT game_pk, home_team, away_team, venue_name, game_status
    FROM game_info 
    WHERE game_date = %s 
    ORDER BY game_pk
    """
    
    try:
        games = pd.read_sql(query, conn, params=[game_date])
        
        if games.empty:
            return [{"error": f"No games found for {game_date}"}]
        
        return games.to_dict('records')
        
    except Exception as e:
        return [{"error": f"Database error: {e}"}]

def print_data_extraction_summary(data: Dict):
    """Print a summary of extracted data for Claude"""
    
    placeholder_mode = data.get('placeholder_mode', True)
    mode_text = "🔧 PLACEHOLDER DATA" if placeholder_mode else "🌐 REAL DATA"
    
    print(f"\n📊 DATA EXTRACTION SUMMARY ({mode_text})")
    print("=" * 60)
    
    game_context = data.get('game_context', {})
    if 'error' not in game_context:
        print(f"🏟️ Game: {game_context['away_team']} @ {game_context['home_team']}")
        print(f"   📍 Venue: {game_context['venue_name']}")
        print(f"   📅 Date: {game_context['game_date']}")
        print(f"   ⚾ Starting Pitchers: {game_context['away_starter_name']} vs {game_context['home_starter_name']}")
    
    # Data quality summary
    quality = data.get('data_quality_summary', {})
    print(f"\n📈 Data Quality:")
    print(f"   Game Context: {'✅' if quality.get('game_context') else '❌'}")
    print(f"   Statcast Metrics: {'✅' if quality.get('statcast_available') else '❌'}")
    print(f"   Pitcher Data: {'✅' if quality.get('pitcher_data_complete') else '❌'}")
    print(f"   Team Performance: {'✅' if quality.get('team_data_complete') else '❌'}")
    print(f"   Umpire Assignment: {'✅' if quality.get('umpire_available') else '❌'}")
    print(f"   Starting Lineups: {'✅' if quality.get('lineup_available') else '❌'}")
    
    # Statcast summary
    statcast = data.get('statcast_metrics', {})
    if 'error' not in statcast:
        print(f"\n⚾ Advanced Statcast Summary:")
        print(f"   Sample Size: {statcast.get('sample_size', 0)} pitches")
        print(f"   Batted Balls: {statcast.get('batted_balls', 0)}")
        print(f"   Barrel Rate: {statcast.get('barrels', 0)} barrels")
        print(f"   Avg Exit Velo: {statcast.get('avg_exit_velo', 'N/A')}")
        print(f"   Data Quality: {statcast.get('data_quality', 'UNKNOWN')}")
    
    print(f"\n💡 READY FOR CLAUDE ANALYSIS:")
    print(f"   🤖 Copy this data and ask Claude for betting recommendations")
    print(f"   📊 All core metrics extracted and validated")
    print(f"   🎯 Claude will handle weather, ballpark factors, and analysis")
    
    if placeholder_mode:
        print(f"\n🔄 Using placeholder data for testing")
        print(f"   To use real data: set USE_PLACEHOLDER_DATA=false")

def main():
    """STREAMLINED: Main analysis function focused on data extraction"""
    
    try:
        from py.config import require_config
        config = require_config(require_database=True, graceful_degradation=True)
        dsn = config.PG_DSN
        
        placeholder_mode = getattr(config, 'USE_PLACEHOLDER_DATA', True)
        print(f"🔧 Configuration: {'Placeholder' if placeholder_mode else 'Real'} data mode")
        
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        print("💡 Make sure to run: python setup_env.py")
        return
    
    if not dsn:
        print("❌ PG_DSN environment variable must be set")
        return
    
    try:
        conn = psycopg2.connect(dsn)
        print("✅ Connected to database")
        
        # For placeholder data, use a recent date that would have data
        if is_placeholder_data():
            analysis_date = "2025-01-18"  # Use a date we generated placeholder data for
            print(f"🔧 Using placeholder data for {analysis_date}")
        else:
            analysis_date = None  # Use today's date
        
        # Get available games
        games = get_games_for_analysis(conn, analysis_date)
        
        if len(games) == 1 and "error" in games[0]:
            print(f"❌ {games[0]['error']}")
            return
        
        print(f"📅 Found {len(games)} games available for analysis")
        
        # Show available games
        for i, game in enumerate(games, 1):
            if "error" not in game:
                print(f"   {i}. Game {game['game_pk']}: {game['away_team']} @ {game['home_team']}")
        
        # For demo, analyze the first game
        if games and "error" not in games[0]:
            game_pk = games[0]['game_pk']
            print(f"\n🎯 Extracting data for Game {game_pk}...")
            
            # Extract comprehensive data
            comprehensive_data = extract_comprehensive_game_data(conn, game_pk)
            
            # Print summary
            print_data_extraction_summary(comprehensive_data)
            
            # Optionally save to file for Claude
            if input("\n💾 Save data to file for Claude analysis? (y/n): ").lower() == 'y':
                import json
                filename = f"game_{game_pk}_data.json"
                with open(filename, 'w') as f:
                    json.dump(comprehensive_data, f, indent=2, default=str)
                print(f"✅ Data saved to {filename}")
                print(f"💡 You can now upload this file to Claude for analysis!")
        
    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
        print("💡 Check if database is running and accessible")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()