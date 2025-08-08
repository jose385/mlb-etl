#!/usr/bin/env python3
"""
simple_analysis.py - FIXED: Streamlined MLB betting analysis with placeholder data support
MAJOR FIXES: Handles both placeholder and real data, better error handling, graceful degradation
Combines weather, umpire, pitcher, and team trends into actionable betting insights
"""

import pandas as pd
import psycopg2
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import os

# Essential ballpark factors (simplified)
BALLPARK_FACTORS = {
    "Coors Field": {"team": "Colorado Rockies", "run_factor": 1.25, "temp_multiplier": 1.5},
    "Great American Ball Park": {"team": "Cincinnati Reds", "run_factor": 1.12},
    "Yankee Stadium": {"team": "New York Yankees", "run_factor": 1.08},
    "Fenway Park": {"team": "Boston Red Sox", "run_factor": 1.05},
    "Wrigley Field": {"team": "Chicago Cubs", "run_factor": 1.02, "wind_sensitive": True},
    "Marlins Park": {"team": "Miami Marlins", "run_factor": 0.92, "dome": True},
    "loanDepot park": {"team": "Miami Marlins", "run_factor": 0.92, "dome": True},
    "Tropicana Field": {"team": "Tampa Bay Rays", "run_factor": 0.94, "dome": True},
    "Petco Park": {"team": "San Diego Padres", "run_factor": 0.95},
    "Oracle Park": {"team": "San Francisco Giants", "run_factor": 0.94},
    "Globe Life Field": {"team": "Texas Rangers", "run_factor": 1.10},
    "Minute Maid Park": {"team": "Houston Astros", "run_factor": 1.06},
}

def get_ballpark_factor(team_name: str, venue_name: str = None) -> float:
    """Get simplified run factor for team's ballpark with better matching"""
    # Try venue name first
    if venue_name:
        for park, park_info in BALLPARK_FACTORS.items():
            if park.lower() in venue_name.lower() or venue_name.lower() in park.lower():
                return park_info["run_factor"]
    
    # Fallback to team name
    for park_info in BALLPARK_FACTORS.values():
        if team_name and team_name in park_info["team"] or park_info["team"] in str(team_name):
            return park_info["run_factor"]
    return 1.0  # Neutral for unknown parks

def is_placeholder_data() -> bool:
    """Check if we're using placeholder data mode"""
    try:
        from py.config import get_config
        config = get_config()
        return getattr(config, 'USE_PLACEHOLDER_DATA', True)
    except:
        return True  # Default to placeholder mode if config fails

def analyze_weather_impact(conn, game_pk: int) -> Dict:
    """FIXED: Weather analysis with placeholder data support and proper error handling"""
    
    query = """
    SELECT temperature_f, wind_speed_mph, wind_direction_deg, 
           home_team, venue_name, data_source, over_under_lean
    FROM weather 
    WHERE game_pk = %s
    """
    
    try:
        result = pd.read_sql(query, conn, params=[game_pk])
        if result.empty:
            return {"impact": "NEUTRAL", "factor": 1.0, "reason": "No weather data available"}
        
        row = result.iloc[0]
        temp = row['temperature_f'] if pd.notna(row['temperature_f']) else 72
        wind_speed = row['wind_speed_mph'] if pd.notna(row['wind_speed_mph']) else 0
        home_team = row['home_team'] if pd.notna(row['home_team']) else ''
        venue_name = row['venue_name'] if pd.notna(row['venue_name']) else ''
        data_source = row.get('data_source', 'unknown')
        over_under_lean = row.get('over_under_lean', 'NEUTRAL')
        
        # For placeholder data, use the pre-calculated lean
        if data_source == 'placeholder' and over_under_lean in ['OVER', 'UNDER', 'NEUTRAL']:
            if over_under_lean == 'OVER':
                impact = "OVER LEAN"
                factor = 1.08
            elif over_under_lean == 'UNDER':
                impact = "UNDER LEAN"
                factor = 0.92
            else:
                impact = "NEUTRAL"
                factor = 1.0
            
            reason = f"Placeholder weather: {temp}°F, Wind: {wind_speed}mph, {venue_name}"
            confidence = "MEDIUM"
            
            return {
                "impact": impact,
                "factor": round(factor, 3),
                "reason": reason,
                "confidence": confidence,
                "data_source": "placeholder"
            }
        
        # For real data, calculate impact
        # Get ballpark info
        park_factor = get_ballpark_factor(home_team, venue_name)
        
        # Temperature impact (simplified)
        if temp >= 85:
            temp_impact = 0.15  # Strong OVER
            temp_desc = f"Hot weather ({temp}°F)"
        elif temp >= 75:
            temp_impact = 0.08  # OVER lean
            temp_desc = f"Warm weather ({temp}°F)"
        elif temp <= 45:
            temp_impact = -0.15  # Strong UNDER
            temp_desc = f"Cold weather ({temp}°F)"
        elif temp <= 55:
            temp_impact = -0.08  # UNDER lean
            temp_desc = f"Cool weather ({temp}°F)"
        else:
            temp_impact = 0.0
            temp_desc = f"Neutral temp ({temp}°F)"
        
        # Wind impact (simplified)
        wind_impact = 0.0
        wind_desc = "Calm conditions"
        
        if wind_speed >= 15:
            # Check if it's Wrigley (wind-sensitive)
            if "Cubs" in str(home_team) or "Wrigley" in str(venue_name):
                wind_direction = row['wind_direction_deg'] if pd.notna(row['wind_direction_deg']) else 0
                wind_impact = 0.20 if 180 <= wind_direction <= 270 else -0.20
                wind_desc = f"Strong Wrigley wind ({wind_speed:.1f} mph)"
            else:
                wind_impact = 0.10  # General strong wind impact
                wind_desc = f"Strong wind ({wind_speed:.1f} mph)"
        elif wind_speed >= 10:
            wind_impact = 0.05
            wind_desc = f"Moderate wind ({wind_speed:.1f} mph)"
        
        # Combine impacts
        total_weather_impact = temp_impact + wind_impact
        final_factor = park_factor * (1 + total_weather_impact)
        
        # Generate recommendation
        if final_factor >= 1.15:
            impact = "STRONG OVER"
        elif final_factor >= 1.08:
            impact = "OVER LEAN"
        elif final_factor <= 0.85:
            impact = "STRONG UNDER"
        elif final_factor <= 0.92:
            impact = "UNDER LEAN"
        else:
            impact = "NEUTRAL"
        
        reason = f"{temp_desc}, {wind_desc}, Park factor: {park_factor:.2f}"
        
        return {
            "impact": impact,
            "factor": round(final_factor, 3),
            "reason": reason,
            "confidence": "HIGH" if abs(total_weather_impact) >= 0.15 else "MEDIUM",
            "data_source": data_source
        }
        
    except Exception as e:
        return {"impact": "ERROR", "factor": 1.0, "reason": f"Weather analysis error: {e}"}

def analyze_umpire_impact(conn, game_pk: int) -> Dict:
    """FIXED: Umpire analysis with placeholder data support and proper handling of empty data"""
    
    query = """
    SELECT umpire_name, avg_total_runs_in_games, over_under_record, sample_size,
           pitcher_friendly_score, position
    FROM umpires 
    WHERE game_pk = %s AND position = 'Home Plate'
    """
    
    try:
        result = pd.read_sql(query, conn, params=[game_pk])
        if result.empty:
            return {"impact": "NEUTRAL", "reason": "No umpire data available"}
        
        row = result.iloc[0]
        umpire_name = row['umpire_name'] if pd.notna(row['umpire_name']) else 'Unknown'
        avg_runs = row['avg_total_runs_in_games'] if pd.notna(row['avg_total_runs_in_games']) else 8.5
        over_pct = row['over_under_record'] if pd.notna(row['over_under_record']) else 0.5
        sample_size = row['sample_size'] if pd.notna(row['sample_size']) else 0
        pitcher_friendly = row.get('pitcher_friendly_score', 50) if pd.notna(row.get('pitcher_friendly_score', 50)) else 50
        
        mlb_average = 8.5
        runs_diff = avg_runs - mlb_average
        
        # For placeholder data, provide realistic but simplified analysis
        if is_placeholder_data():
            # Adjust confidence based on whether this looks like placeholder data
            if sample_size == 0 or umpire_name == 'Unknown':
                confidence = "LOW"
            elif sample_size >= 50:
                confidence = "HIGH"
            elif sample_size >= 25:
                confidence = "MEDIUM"
            else:
                confidence = "LOW"
        else:
            # For real data, use standard confidence calculation
            if sample_size >= 30:
                confidence = "HIGH"
            elif sample_size >= 15:
                confidence = "MEDIUM"
            else:
                confidence = "LOW"
            
        # Impact assessment
        if runs_diff >= 1.0 or over_pct >= 0.60:
            impact = "STRONG OVER"
        elif runs_diff >= 0.5 or over_pct >= 0.55:
            impact = "OVER LEAN"
        elif runs_diff <= -1.0 or over_pct <= 0.40:
            impact = "STRONG UNDER"
        elif runs_diff <= -0.5 or over_pct <= 0.45:
            impact = "UNDER LEAN"
        else:
            impact = "NEUTRAL"
        
        reason = f"{umpire_name}: {avg_runs:.1f} runs/game avg, {over_pct:.1%} OVER rate ({sample_size} games)"
        
        return {
            "impact": impact,
            "umpire_name": umpire_name,
            "avg_runs": round(avg_runs, 1),
            "sample_size": int(sample_size),
            "reason": reason,
            "confidence": confidence,
            "data_source": "placeholder" if is_placeholder_data() else "real"
        }
        
    except Exception as e:
        return {"impact": "ERROR", "reason": f"Umpire analysis error: {e}"}

def analyze_pitcher_trends(conn, game_pk: int) -> Dict:
    """FIXED: Starting pitcher analysis with placeholder data support and better error handling"""
    
    # First, try to get starting pitchers from game_info
    starter_query = """
    SELECT home_starting_pitcher, away_starting_pitcher, 
           home_starter_name, away_starter_name
    FROM game_info 
    WHERE game_pk = %s
    """
    
    try:
        starter_result = pd.read_sql(starter_query, conn, params=[game_pk])
        if starter_result.empty:
            return {"impact": "NEUTRAL", "reason": "No starting pitcher information available"}
        
        starter_row = starter_result.iloc[0]
        home_pitcher = starter_row['home_starting_pitcher']
        away_pitcher = starter_row['away_starting_pitcher']
        
        if pd.isna(home_pitcher) and pd.isna(away_pitcher):
            return {"impact": "NEUTRAL", "reason": "Starting pitcher IDs not available"}
        
        # Get recent stats for starting pitchers
        pitcher_ids = [pid for pid in [home_pitcher, away_pitcher] if pd.notna(pid)]
        
        if not pitcher_ids:
            return {"impact": "NEUTRAL", "reason": "No valid starting pitcher IDs"}
        
        # Query recent stats for these pitchers
        recent_stats_query = """
        SELECT player_id, era, whip, hot_streak, cold_streak, 
               games_played, workload_score, stat_type
        FROM recent_stats 
        WHERE player_id = ANY(%s) 
        AND stat_type LIKE 'pitching%'
        ORDER BY stat_date DESC
        """
        
        recent_result = pd.read_sql(recent_stats_query, conn, params=[pitcher_ids])
        
        if recent_result.empty:
            # Fallback: try to get basic data from games table
            fallback_query = """
            SELECT pitcher, COUNT(*) as appearances,
                   COUNT(CASE WHEN events = 'strikeout' THEN 1 END) as strikeouts,
                   COUNT(CASE WHEN events IN ('single', 'double', 'triple', 'home_run') THEN 1 END) as hits_allowed
            FROM games 
            WHERE pitcher = ANY(%s) 
            AND game_date >= %s
            GROUP BY pitcher
            """
            
            fallback_date = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
            fallback_result = pd.read_sql(fallback_query, conn, params=[pitcher_ids, fallback_date])
            
            if fallback_result.empty:
                return {"impact": "NEUTRAL", "reason": "No recent pitcher data available"}
            
            # Simple analysis from games data
            pitcher_insights = []
            for _, pitcher in fallback_result.iterrows():
                if pitcher['appearances'] >= 1:
                    k_rate = pitcher['strikeouts'] / max(1, pitcher['appearances'])
                    
                    if k_rate >= 8:
                        trend = "GOOD FORM"
                        impact = "UNDER LEAN (opponent runs)"
                    elif k_rate <= 4:
                        trend = "STRUGGLING"
                        impact = "OVER LEAN (opponent runs)"
                    else:
                        trend = "AVERAGE"
                        impact = "NEUTRAL"
                    
                    pitcher_insights.append({
                        "trend": trend,
                        "impact": impact,
                        "k_rate": round(k_rate, 1),
                        "appearances": int(pitcher['appearances']),
                        "data_source": "games_table"
                    })
            
            return {
                "pitchers": pitcher_insights,
                "reason": f"Basic analysis for {len(pitcher_insights)} starting pitcher(s)"
            }
        
        # Analysis using recent_stats table
        pitcher_insights = []
        
        for _, pitcher_stat in recent_result.iterrows():
            era = pitcher_stat.get('era', 4.50) if pd.notna(pitcher_stat.get('era')) else 4.50
            whip = pitcher_stat.get('whip', 1.30) if pd.notna(pitcher_stat.get('whip')) else 1.30
            hot_streak = pitcher_stat.get('hot_streak', False)
            cold_streak = pitcher_stat.get('cold_streak', False)
            games_played = pitcher_stat.get('games_played', 0) if pd.notna(pitcher_stat.get('games_played')) else 0
            workload_score = pitcher_stat.get('workload_score', 50) if pd.notna(pitcher_stat.get('workload_score')) else 50
            
            # Determine trend based on stats
            if hot_streak or era <= 2.50:
                trend = "DOMINANT"
                impact = "STRONG UNDER (opponent runs)"
            elif era <= 3.50 and whip <= 1.20:
                trend = "GOOD FORM"
                impact = "UNDER LEAN (opponent runs)"
            elif cold_streak or era >= 5.50:
                trend = "STRUGGLING"
                impact = "OVER LEAN (opponent runs)"
            elif era >= 4.50 or whip >= 1.40:
                trend = "POOR FORM"
                impact = "SLIGHT OVER (opponent runs)"
            else:
                trend = "AVERAGE"
                impact = "NEUTRAL"
            
            # Adjust for workload (fatigue)
            if workload_score >= 80:
                if "UNDER" in impact:
                    impact = impact.replace("STRONG UNDER", "UNDER LEAN").replace("UNDER LEAN", "SLIGHT UNDER")
                trend += " (HIGH WORKLOAD)"
            
            pitcher_insights.append({
                "trend": trend,
                "impact": impact,
                "era": round(era, 2),
                "whip": round(whip, 2),
                "games": int(games_played),
                "workload": round(workload_score, 1),
                "hot_streak": bool(hot_streak),
                "cold_streak": bool(cold_streak),
                "data_source": "recent_stats"
            })
        
        if not pitcher_insights:
            return {"impact": "NEUTRAL", "reason": "No pitcher performance data available"}
        
        return {
            "pitchers": pitcher_insights,
            "reason": f"Recent stats analysis for {len(pitcher_insights)} starting pitcher(s)"
        }
        
    except Exception as e:
        return {"impact": "ERROR", "reason": f"Pitcher analysis error: {e}"}

def analyze_team_performance(conn, team_name, game_pk):
    """
    ROBUST team analysis that handles missing data gracefully
    Works with both placeholder and real data
    """
    
    # Query for team batting stats with error handling
    team_stats_query = """
        WITH team_players AS (
            SELECT DISTINCT person_id
            FROM rosters r
            JOIN game_info gi ON r.game_date = gi.game_date
            WHERE (gi.home_team = %s OR gi.away_team = %s)
            AND r.game_date >= %s
            LIMIT 25
        ),
        team_batting AS (
            SELECT rs.*
            FROM recent_stats rs
            JOIN team_players tp ON rs.player_id = tp.person_id
            WHERE rs.stat_type LIKE 'batting%'
            AND rs.stat_date >= %s
        )
        SELECT 
            COUNT(*) as players_with_stats,
            AVG(COALESCE(ops, 0.700)) as avg_ops,
            AVG(COALESCE(batting_avg, 0.250)) as avg_batting_avg,
            COUNT(CASE WHEN hot_streak THEN 1 END) as hot_players,
            COUNT(CASE WHEN cold_streak THEN 1 END) as cold_players,
            AVG(COALESCE(games_played, 0)) as avg_games
        FROM team_batting
    """
    
    try:
        # Calculate date range for stats
        game_date = pd.read_sql("SELECT game_date FROM game_info WHERE game_pk = %s", 
                               conn, params=[game_pk])['game_date'].iloc[0]
        lookback_date = game_date - pd.Timedelta(days=15)
        
        # Execute query with proper error handling
        team_result = pd.read_sql(team_stats_query, conn, 
                                 params=[team_name, team_name, lookback_date, lookback_date])
        
        # Check if we got valid results
        if len(team_result) == 0 or team_result.empty:
            print(f"⚠️ No recent stats found for {team_name}")
            return {
                'avg_ops': 0.700,
                'avg_batting_avg': 0.250, 
                'hot_players': 0,
                'cold_players': 0,
                'players_analyzed': 0,
                'data_quality': 'NO_DATA'
            }
        
        # Extract results safely
        row = team_result.iloc[0]
        players_with_stats = row['players_with_stats'] if pd.notna(row['players_with_stats']) else 0
        
        # Handle case where no players have stats
        if players_with_stats == 0:
            print(f"⚠️ {team_name}: Found players but no matching stats")
            return {
                'avg_ops': 0.700,
                'avg_batting_avg': 0.250,
                'hot_players': 0, 
                'cold_players': 0,
                'players_analyzed': 0,
                'data_quality': 'NO_MATCHING_STATS'
            }
        
        # Return successful analysis
        return {
            'avg_ops': float(row['avg_ops']) if pd.notna(row['avg_ops']) else 0.700,
            'avg_batting_avg': float(row['avg_batting_avg']) if pd.notna(row['avg_batting_avg']) else 0.250,
            'hot_players': int(row['hot_players']) if pd.notna(row['hot_players']) else 0,
            'cold_players': int(row['cold_players']) if pd.notna(row['cold_players']) else 0,
            'players_analyzed': int(players_with_stats),
            'data_quality': 'GOOD' if players_with_stats >= 15 else 'LIMITED'
        }
        
    except Exception as e:
        print(f"⚠️ Error analyzing {team_name}: {str(e)}")
        return {
            'avg_ops': 0.700,
            'avg_batting_avg': 0.250,
            'hot_players': 0,
            'cold_players': 0, 
            'players_analyzed': 0,
            'data_quality': 'ERROR'
        }

def analyze_team_trends(conn, game_pk: int) -> Dict:
    """FIXED: Team form analysis with robust error handling and graceful degradation"""
    
    # Get teams for this game
    teams_query = """
    SELECT home_team, away_team
    FROM game_info
    WHERE game_pk = %s
    """
    
    try:
        teams_result = pd.read_sql(teams_query, conn, params=[game_pk])
        if teams_result.empty:
            return {"impact": "NEUTRAL", "reason": "No team information available"}
        
        teams_row = teams_result.iloc[0]
        home_team = teams_row['home_team']
        away_team = teams_row['away_team']
        
        if pd.isna(home_team) or pd.isna(away_team):
            return {"impact": "NEUTRAL", "reason": "Team names not available"}
        
        team_insights = []
        
        # Analyze each team using the robust function
        for team_name, side in [(home_team, 'home'), (away_team, 'away')]:
            team_stats = analyze_team_performance(conn, team_name, game_pk)
            
            # Convert team stats to team insights format
            avg_ops = team_stats['avg_ops']
            hot_players = team_stats['hot_players']
            cold_players = team_stats['cold_players']
            players_analyzed = team_stats['players_analyzed']
            data_quality = team_stats['data_quality']
            
            # Calculate team form based on stats
            if data_quality in ['NO_DATA', 'ERROR']:
                trend = "UNKNOWN"
                impact = "NEUTRAL"
                est_runs_per_game = 4.5
            else:
                # Calculate hot/cold percentages
                hot_pct = hot_players / max(1, players_analyzed) if players_analyzed > 0 else 0
                cold_pct = cold_players / max(1, players_analyzed) if players_analyzed > 0 else 0
                
                # Estimate runs per game (rough conversion)
                est_runs_per_game = 2.0 + (avg_ops - 0.650) * 8.0  # Simplified formula
                est_runs_per_game = max(2.0, min(8.0, est_runs_per_game))  # Reasonable bounds
                
                if est_runs_per_game >= 6.0 or hot_pct >= 0.4:
                    trend = "HOT OFFENSE"
                    impact = "OVER LEAN"
                elif est_runs_per_game >= 4.5 or hot_pct >= 0.2:
                    trend = "GOOD OFFENSE"
                    impact = "SLIGHT OVER"
                elif est_runs_per_game <= 2.5 or cold_pct >= 0.4:
                    trend = "COLD OFFENSE"
                    impact = "UNDER LEAN"
                elif est_runs_per_game <= 3.5:
                    trend = "STRUGGLING OFFENSE"
                    impact = "SLIGHT UNDER"
                else:
                    trend = "AVERAGE OFFENSE"
                    impact = "NEUTRAL"
            
            team_insights.append({
                "team": side,
                "team_name": team_name,
                "trend": trend,
                "impact": impact,
                "est_runs_per_game": round(est_runs_per_game, 1),
                "avg_ops": round(avg_ops, 3),
                "hot_players": int(hot_players),
                "cold_players": int(cold_players),
                "players_analyzed": int(players_analyzed),
                "data_quality": data_quality,
                "data_source": "robust_analysis"
            })
        
        if not team_insights:
            return {"impact": "NEUTRAL", "reason": "No team performance data available"}
        
        return {
            "teams": team_insights,
            "reason": f"Robust team analysis for {len(team_insights)} team(s)"
        }
        
    except Exception as e:
        print(f"⚠️ Team analysis error: {str(e)}")
        return {"impact": "ERROR", "reason": f"Team analysis error: {e}"}

def generate_combined_recommendation(weather: Dict, umpire: Dict, 
                                   pitchers: Dict, teams: Dict) -> Dict:
    """FIXED: Combine all factors with placeholder data awareness"""
    
    # Collect all directional signals
    over_signals = []
    under_signals = []
    
    # Weather signals (only if no error)
    if weather.get("impact") not in ["ERROR", None]:
        if weather["impact"] in ["STRONG OVER", "OVER LEAN"]:
            weight = 3 if "STRONG" in weather["impact"] else 1
            over_signals.extend([weather["impact"]] * weight)
        elif weather["impact"] in ["STRONG UNDER", "UNDER LEAN"]:
            weight = 3 if "STRONG" in weather["impact"] else 1
            under_signals.extend([weather["impact"]] * weight)
    
    # Umpire signals (only if no error)
    if umpire.get("impact") not in ["ERROR", None]:
        if umpire["impact"] in ["STRONG OVER", "OVER LEAN"]:
            weight = 2 if "STRONG" in umpire["impact"] and umpire.get("confidence") == "HIGH" else 1
            over_signals.extend([umpire["impact"]] * weight)
        elif umpire["impact"] in ["STRONG UNDER", "UNDER LEAN"]:
            weight = 2 if "STRONG" in umpire["impact"] and umpire.get("confidence") == "HIGH" else 1
            under_signals.extend([umpire["impact"]] * weight)
    
    # Pitcher signals (only if no error)
    if "pitchers" in pitchers and pitchers.get("impact") != "ERROR":
        for pitcher in pitchers["pitchers"]:
            if "OVER" in pitcher["impact"]:
                over_signals.append(pitcher["impact"])
            elif "UNDER" in pitcher["impact"]:
                under_signals.append(pitcher["impact"])
    
    # Team signals (only if no error)
    if "teams" in teams and teams.get("impact") != "ERROR":
        for team in teams["teams"]:
            if "OVER" in team["impact"]:
                over_signals.append(team["impact"])
            elif "UNDER" in team["impact"]:
                under_signals.append(team["impact"])
    
    # Calculate recommendation
    over_strength = len(over_signals)
    under_strength = len(under_signals)
    
    # Adjust confidence based on data quality
    data_quality_factors = []
    if weather.get("data_source") == "placeholder":
        data_quality_factors.append("placeholder weather")
    if umpire.get("data_source") == "placeholder":
        data_quality_factors.append("placeholder umpire")
    if any(p.get("data_source") == "recent_stats" for p in pitchers.get("pitchers", [])):
        data_quality_factors.append("real pitcher stats")
    
    confidence_modifier = ""
    if data_quality_factors:
        if "placeholder" in str(data_quality_factors):
            confidence_modifier = " (with placeholder data)"
        else:
            confidence_modifier = " (with real data)"
    
    if over_strength >= under_strength + 3:
        recommendation = f"🔥 STRONG OVER - Multiple factors align{confidence_modifier}"
        confidence = "HIGH"
    elif over_strength >= under_strength + 2:
        recommendation = f"📈 OVER LEAN - Factors favor higher scoring{confidence_modifier}"
        confidence = "MEDIUM"
    elif under_strength >= over_strength + 3:
        recommendation = f"🧊 STRONG UNDER - Multiple factors align{confidence_modifier}"
        confidence = "HIGH"
    elif under_strength >= over_strength + 2:
        recommendation = f"📉 UNDER LEAN - Factors favor lower scoring{confidence_modifier}"
        confidence = "MEDIUM"
    else:
        recommendation = f"😐 NEUTRAL - Conflicting or weak signals{confidence_modifier}"
        confidence = "LOW"
    
    return {
        "recommendation": recommendation,
        "confidence": confidence,
        "over_signals": over_strength,
        "under_signals": under_strength,
        "key_factors": [
            weather.get("reason", "No weather data"),
            umpire.get("reason", "No umpire data"),
            pitchers.get("reason", "No pitcher data"),
            teams.get("reason", "No team data")
        ],
        "data_quality": data_quality_factors
    }

def analyze_game(conn, game_pk: int) -> Dict:
    """FIXED: Complete analysis with placeholder data support and graceful error handling"""
    
    print(f"🔍 Analyzing game {game_pk}...")
    
    # FIXED: Check if game exists in database first
    game_check_query = """
    SELECT game_pk, game_date, home_team, away_team, venue_name
    FROM game_info 
    WHERE game_pk = %s
    LIMIT 1
    """
    
    try:
        game_info = pd.read_sql(game_check_query, conn, params=[game_pk])
        if game_info.empty:
            return {"error": f"Game {game_pk} not found in database"}
        
        game_row = game_info.iloc[0]
        
        print(f"   🏟️ {game_row['away_team']} @ {game_row['home_team']} at {game_row.get('venue_name', 'Unknown Venue')}")
        
        # Analyze all factors with error handling
        print(f"   🌤️ Analyzing weather...")
        weather = analyze_weather_impact(conn, game_pk)
        
        print(f"   👨‍⚖️ Analyzing umpire...")
        umpire = analyze_umpire_impact(conn, game_pk)
        
        print(f"   ⚾ Analyzing pitchers...")
        pitchers = analyze_pitcher_trends(conn, game_pk)
        
        print(f"   👥 Analyzing teams...")
        teams = analyze_team_trends(conn, game_pk)
        
        # Generate combined recommendation
        print(f"   🎯 Generating recommendation...")
        combined = generate_combined_recommendation(weather, umpire, pitchers, teams)
        
        return {
            "game_pk": game_pk,
            "game_date": str(game_row['game_date']),
            "home_team": game_row['home_team'],
            "away_team": game_row['away_team'],
            "venue_name": game_row.get('venue_name', 'Unknown'),
            "weather_analysis": weather,
            "umpire_analysis": umpire,
            "pitcher_analysis": pitchers,
            "team_analysis": teams,
            "final_recommendation": combined,
            "placeholder_mode": is_placeholder_data()
        }
        
    except Exception as e:
        return {"error": f"Analysis failed for game {game_pk}: {e}"}

def get_todays_analysis(conn, game_date: str = None) -> List[Dict]:
    """FIXED: Analyze all games for a given date with placeholder data support"""
    
    if game_date is None:
        game_date = datetime.now().strftime('%Y-%m-%d')
    
    # FIXED: Use 'game_info' table instead of 'statcast'
    games_query = """
    SELECT game_pk, home_team, away_team, venue_name
    FROM game_info 
    WHERE game_date = %s 
    ORDER BY game_pk
    """
    
    try:
        games = pd.read_sql(games_query, conn, params=[game_date])
        
        if games.empty:
            return [{"error": f"No games found for {game_date}"}]
        
        print(f"📅 Found {len(games)} games for {game_date}")
        if is_placeholder_data():
            print(f"🔧 Using placeholder data mode")
        
        results = []
        for _, game in games.iterrows():
            analysis = analyze_game(conn, game['game_pk'])
            results.append(analysis)
        
        return results
        
    except Exception as e:
        return [{"error": f"Database error: {e}"}]

def print_daily_report(analyses: List[Dict]):
    """FIXED: Print formatted daily betting report with placeholder data awareness"""
    
    placeholder_mode = is_placeholder_data()
    mode_text = "🔧 PLACEHOLDER DATA MODE" if placeholder_mode else "🌐 REAL DATA MODE"
    
    print(f"\n🎯 DAILY MLB BETTING ANALYSIS ({mode_text})")
    print("=" * 80)
    
    if placeholder_mode:
        print("💡 Using generated test data - perfect for testing the pipeline!")
        print("   To use real data: set USE_PLACEHOLDER_DATA=false in .env")
        print("")
    
    strong_bets = []
    moderate_bets = []
    error_count = 0
    
    for analysis in analyses:
        if "error" in analysis:
            error_count += 1
            print(f"❌ Error: {analysis['error']}")
            continue
        
        game_pk = analysis['game_pk']
        home_team = analysis.get('home_team', 'Unknown')
        away_team = analysis.get('away_team', 'Unknown')
        venue_name = analysis.get('venue_name', 'Unknown')
        final_rec = analysis['final_recommendation']
        
        print(f"\n🏟️ Game {game_pk}: {away_team} @ {home_team}")
        print(f"   📍 Venue: {venue_name}")
        print(f"   🎯 RECOMMENDATION: {final_rec['recommendation']}")
        print(f"   📊 Confidence: {final_rec['confidence']}")
        print(f"   🌤️ Weather: {analysis['weather_analysis']['impact']}")
        print(f"   👨‍⚖️ Umpire: {analysis['umpire_analysis']['impact']}")
        
        # Show data quality info
        if 'data_quality' in final_rec and final_rec['data_quality']:
            print(f"   📈 Data: {', '.join(final_rec['data_quality'])}")
        
        # Collect strong bets
        if final_rec['confidence'] == 'HIGH':
            strong_bets.append(analysis)
        elif final_rec['confidence'] == 'MEDIUM':
            moderate_bets.append(analysis)
    
    # Summary
    total_games = len(analyses)
    successful_analyses = total_games - error_count
    
    print(f"\n📋 DAILY SUMMARY:")
    print(f"   🎲 Total games: {total_games}")
    print(f"   ✅ Successful analyses: {successful_analyses}")
    print(f"   ❌ Analysis errors: {error_count}")
    print(f"   🚨 High confidence bets: {len(strong_bets)}")
    print(f"   📊 Medium confidence bets: {len(moderate_bets)}")
    
    if strong_bets:
        print(f"\n🚨 TODAY'S BEST BETS:")
        for bet in strong_bets:
            home_team = bet.get('home_team', 'Unknown')
            away_team = bet.get('away_team', 'Unknown')
            rec_text = bet['final_recommendation']['recommendation']
            # Clean up the recommendation text for display
            clean_rec = rec_text.split(' - ')[0]  # Remove the detailed part
            print(f"   🎯 {away_team} @ {home_team}: {clean_rec}")
    
    if error_count > 0:
        print(f"\n💡 Note: {error_count} games had analysis errors.")
        if placeholder_mode:
            print("   This is normal with placeholder data - some generated games may have incomplete data.")
        else:
            print("   This could indicate missing real data or API issues.")
    
    if placeholder_mode:
        print(f"\n🔄 Ready to switch to real data?")
        print(f"   1. Set USE_PLACEHOLDER_DATA=false in .env")
        print(f"   2. Use past dates with real games: --start 2024-07-15 --end 2024-07-15")
        print(f"   3. Same analysis commands work identically!")

def main():
    """FIXED: Run daily analysis with placeholder data support and better error handling"""
    
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
        print("💡 Run: python setup_env.py to configure database connection")
        return
    
    try:
        conn = psycopg2.connect(dsn)
        print("✅ Connected to database")
        
        # Run today's analysis (or recent date for placeholder data)
        if is_placeholder_data():
            # For placeholder data, use a recent date that would have data
            analysis_date = "2025-01-18"  # Use a date we generated placeholder data for
            print(f"🔧 Using placeholder data for {analysis_date}")
        else:
            analysis_date = None  # Use today's date
        
        analyses = get_todays_analysis(conn, analysis_date)
        print_daily_report(analyses)
        
    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
        print("💡 Check if database is running and accessible")
        print("💡 Make sure you've run: python initialize_database.py")
        print("💡 And loaded data: python loader/enhanced_load_parquet_into_pg.py")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        if is_placeholder_data():
            print("💡 If using placeholder data, make sure you've run the backfill:")
            print("   python py/enhanced_simple_backfill.py --start 2025-01-18 --end 2025-01-18")
        import traceback
        traceback.print_exc()
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()