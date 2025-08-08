#!/usr/bin/env python3
"""
simple_analysis.py - FIXED: Streamlined MLB betting analysis
MAJOR FIXES: Correct table names, empty DataFrame handling, proper error handling
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
    "Tropicana Field": {"team": "Tampa Bay Rays", "run_factor": 0.94, "dome": True},
    "Petco Park": {"team": "San Diego Padres", "run_factor": 0.95},
    "Oracle Park": {"team": "San Francisco Giants", "run_factor": 0.94},
}

def get_ballpark_factor(team_name: str) -> float:
    """Get simplified run factor for team's ballpark"""
    for park_info in BALLPARK_FACTORS.values():
        if team_name in park_info["team"] or park_info["team"] in team_name:
            return park_info["run_factor"]
    return 1.0  # Neutral for unknown parks

def analyze_weather_impact(conn, game_pk: int) -> Dict:
    """FIXED: Weather analysis with proper error handling"""
    
    query = """
    SELECT temperature_f, wind_speed_mph, wind_direction_deg, 
           home_team, venue_name
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
        
        # Get ballpark info
        park_factor = get_ballpark_factor(home_team)
        
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
            if "Cubs" in home_team or "Wrigley" in str(row.get('venue_name', '')):
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
            "confidence": "HIGH" if abs(total_weather_impact) >= 0.15 else "MEDIUM"
        }
        
    except Exception as e:
        return {"impact": "ERROR", "factor": 1.0, "reason": f"Weather analysis error: {e}"}

def analyze_umpire_impact(conn, game_pk: int) -> Dict:
    """FIXED: Umpire analysis with proper handling of empty data"""
    
    query = """
    SELECT umpire_name, avg_total_runs_in_games, over_under_record, sample_size
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
        
        mlb_average = 8.5
        runs_diff = avg_runs - mlb_average
        
        # Confidence based on sample size
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
            "confidence": confidence
        }
        
    except Exception as e:
        return {"impact": "ERROR", "reason": f"Umpire analysis error: {e}"}

def analyze_pitcher_trends(conn, game_pk: int) -> Dict:
    """FIXED: Starting pitcher analysis using correct table names and handling empty data"""
    
    # FIXED: Use 'games' table instead of 'statcast' and 'rosters' instead of 'roster'
    query = """
    WITH todays_starters AS (
        SELECT DISTINCT g.pitcher, r.team_id
        FROM games g
        JOIN rosters r ON g.pitcher = r.person_id AND g.game_date = r.game_date
        WHERE g.game_pk = %s AND g.inning = 1
        LIMIT 2  -- Both starting pitchers
    ),
    pitcher_recent AS (
        SELECT ts.pitcher, ts.team_id,
               COUNT(DISTINCT g.game_pk) as recent_starts,
               -- Simplified performance metrics
               COUNT(CASE WHEN g.events IN ('single', 'double', 'triple', 'home_run') THEN 1 END) as hits_allowed,
               COUNT(CASE WHEN g.events = 'home_run' THEN 1 END) as hrs_allowed,
               COUNT(CASE WHEN g.events LIKE '%strikeout%' OR g.events = 'strikeout' THEN 1 END) as strikeouts,
               COUNT(CASE WHEN g.events IS NOT NULL THEN 1 END) as batters_faced
        FROM todays_starters ts
        JOIN games g ON ts.pitcher = g.pitcher
        WHERE g.game_date >= %s AND g.game_date < %s
        GROUP BY ts.pitcher, ts.team_id
    )
    SELECT pitcher, team_id, recent_starts, hits_allowed, hrs_allowed, 
           strikeouts, batters_faced
    FROM pitcher_recent
    WHERE recent_starts >= 1  -- At least 1 start
    """
    
    game_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    try:
        result = pd.read_sql(query, conn, params=[game_pk, start_date, game_date])
        if result.empty:
            return {"impact": "NEUTRAL", "reason": "No recent pitcher data available"}
        
        pitcher_insights = []
        
        for _, pitcher in result.iterrows():
            batters_faced = pitcher['batters_faced']
            if batters_faced > 0:
                hits_per_bf = pitcher['hits_allowed'] / batters_faced
                k_rate = pitcher['strikeouts'] / batters_faced
                
                # Simple trend assessment
                if hits_per_bf <= 0.20 and k_rate >= 0.25:
                    trend = "DOMINANT"
                    impact = "STRONG UNDER (opponent runs)"
                elif hits_per_bf <= 0.25:
                    trend = "GOOD FORM"
                    impact = "UNDER LEAN (opponent runs)"
                elif hits_per_bf >= 0.35:
                    trend = "STRUGGLING"
                    impact = "OVER LEAN (opponent runs)"
                else:
                    trend = "AVERAGE"
                    impact = "NEUTRAL"
                
                pitcher_insights.append({
                    "trend": trend,
                    "impact": impact,
                    "hits_per_bf": round(hits_per_bf, 3),
                    "k_rate": round(k_rate, 3),
                    "starts": int(pitcher['recent_starts'])
                })
        
        if not pitcher_insights:
            return {"impact": "NEUTRAL", "reason": "No pitcher performance data available"}
        
        return {
            "pitchers": pitcher_insights,
            "reason": f"Analyzed {len(pitcher_insights)} starting pitcher(s)"
        }
        
    except Exception as e:
        return {"impact": "ERROR", "reason": f"Pitcher analysis error: {e}"}

def analyze_team_trends(conn, game_pk: int) -> Dict:
    """FIXED: Team form analysis using correct table names and handling empty data"""
    
    # FIXED: Use 'games' table instead of 'statcast' and 'rosters' instead of 'roster'
    query = """
    WITH game_teams AS (
        SELECT DISTINCT gi.home_team, gi.away_team
        FROM game_info gi
        WHERE gi.game_pk = %s
        LIMIT 1
    ),
    team_recent_performance AS (
        SELECT 
            gt.home_team as team_name,
            'home' as home_away,
            COUNT(DISTINCT g.game_pk) as games_played,
            -- Estimate offensive performance
            COUNT(CASE WHEN g.events IN ('single', 'double', 'triple', 'home_run') THEN 1 END) as hits,
            COUNT(CASE WHEN g.events = 'home_run' THEN 1 END) as home_runs,
            COUNT(CASE WHEN g.events IS NOT NULL THEN 1 END) as total_abs
        FROM game_teams gt
        JOIN game_info gi ON (gi.home_team = gt.home_team OR gi.away_team = gt.home_team)
        JOIN games g ON gi.game_pk = g.game_pk
        WHERE g.game_date >= %s AND g.game_date < %s
        GROUP BY gt.home_team
        
        UNION ALL
        
        SELECT 
            gt.away_team as team_name,
            'away' as home_away,
            COUNT(DISTINCT g.game_pk) as games_played,
            COUNT(CASE WHEN g.events IN ('single', 'double', 'triple', 'home_run') THEN 1 END) as hits,
            COUNT(CASE WHEN g.events = 'home_run' THEN 1 END) as home_runs,
            COUNT(CASE WHEN g.events IS NOT NULL THEN 1 END) as total_abs
        FROM game_teams gt
        JOIN game_info gi ON (gi.home_team = gt.away_team OR gi.away_team = gt.away_team)
        JOIN games g ON gi.game_pk = g.game_pk
        WHERE g.game_date >= %s AND g.game_date < %s
        GROUP BY gt.away_team
    )
    SELECT team_name, home_away, games_played, hits, home_runs, total_abs
    FROM team_recent_performance
    WHERE games_played >= 1  -- At least 1 game
    """
    
    game_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
    
    try:
        result = pd.read_sql(query, conn, params=[game_pk, start_date, game_date, start_date, game_date])
        if result.empty:
            return {"impact": "NEUTRAL", "reason": "No recent team data available"}
        
        team_insights = []
        
        for _, team in result.iterrows():
            games_played = team['games_played']
            total_abs = team['total_abs']
            
            if games_played > 0 and total_abs > 0:
                # Calculate basic offensive metrics
                hits_per_game = team['hits'] / games_played
                hr_per_game = team['home_runs'] / games_played
                
                # Estimate runs per game (rough conversion)
                est_runs_per_game = hits_per_game * 0.6 + hr_per_game * 2.0
                
                if est_runs_per_game >= 6.0:
                    trend = "HOT OFFENSE"
                    impact = "OVER LEAN"
                elif est_runs_per_game >= 4.5:
                    trend = "GOOD OFFENSE"
                    impact = "SLIGHT OVER"
                elif est_runs_per_game <= 2.5:
                    trend = "COLD OFFENSE"
                    impact = "UNDER LEAN"
                else:
                    trend = "AVERAGE OFFENSE"
                    impact = "NEUTRAL"
                
                team_insights.append({
                    "team": team['home_away'],
                    "team_name": team['team_name'],
                    "trend": trend,
                    "impact": impact,
                    "est_runs_per_game": round(est_runs_per_game, 1),
                    "games": int(games_played)
                })
        
        if not team_insights:
            return {"impact": "NEUTRAL", "reason": "No team performance data available"}
        
        return {
            "teams": team_insights,
            "reason": f"Analyzed recent form for {len(team_insights)} team(s)"
        }
        
    except Exception as e:
        return {"impact": "ERROR", "reason": f"Team analysis error: {e}"}

def generate_combined_recommendation(weather: Dict, umpire: Dict, 
                                   pitchers: Dict, teams: Dict) -> Dict:
    """FIXED: Combine all factors with proper error handling"""
    
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
    
    if over_strength >= under_strength + 3:
        recommendation = "🔥 STRONG OVER - Multiple factors align"
        confidence = "HIGH"
    elif over_strength >= under_strength + 2:
        recommendation = "📈 OVER LEAN - Factors favor higher scoring"
        confidence = "MEDIUM"
    elif under_strength >= over_strength + 3:
        recommendation = "🧊 STRONG UNDER - Multiple factors align"
        confidence = "HIGH"
    elif under_strength >= over_strength + 2:
        recommendation = "📉 UNDER LEAN - Factors favor lower scoring"
        confidence = "MEDIUM"
    else:
        recommendation = "😐 NEUTRAL - Conflicting or weak signals"
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
        ]
    }

def analyze_game(conn, game_pk: int) -> Dict:
    """FIXED: Complete analysis with proper error handling"""
    
    print(f"🔍 Analyzing game {game_pk}...")
    
    # FIXED: Check if game exists in database first
    game_check_query = """
    SELECT game_pk, game_date, home_team, away_team
    FROM game_info 
    WHERE game_pk = %s
    LIMIT 1
    """
    
    try:
        game_info = pd.read_sql(game_check_query, conn, params=[game_pk])
        if game_info.empty:
            return {"error": f"Game {game_pk} not found in database"}
        
        game_row = game_info.iloc[0]
        
        # Analyze all factors
        weather = analyze_weather_impact(conn, game_pk)
        umpire = analyze_umpire_impact(conn, game_pk)
        pitchers = analyze_pitcher_trends(conn, game_pk)
        teams = analyze_team_trends(conn, game_pk)
        
        # Generate combined recommendation
        combined = generate_combined_recommendation(weather, umpire, pitchers, teams)
        
        return {
            "game_pk": game_pk,
            "game_date": str(game_row['game_date']),
            "home_team": game_row['home_team'],
            "away_team": game_row['away_team'],
            "weather_analysis": weather,
            "umpire_analysis": umpire,
            "pitcher_analysis": pitchers,
            "team_analysis": teams,
            "final_recommendation": combined
        }
        
    except Exception as e:
        return {"error": f"Analysis failed for game {game_pk}: {e}"}

def get_todays_analysis(conn, game_date: str = None) -> List[Dict]:
    """FIXED: Analyze all games for a given date using correct table"""
    
    if game_date is None:
        game_date = datetime.now().strftime('%Y-%m-%d')
    
    # FIXED: Use 'game_info' table instead of 'statcast'
    games_query = """
    SELECT game_pk, home_team, away_team
    FROM game_info 
    WHERE game_date = %s 
    ORDER BY game_pk
    """
    
    try:
        games = pd.read_sql(games_query, conn, params=[game_date])
        
        if games.empty:
            return [{"error": f"No games found for {game_date}"}]
        
        results = []
        for _, game in games.iterrows():
            analysis = analyze_game(conn, game['game_pk'])
            results.append(analysis)
        
        return results
        
    except Exception as e:
        return [{"error": f"Database error: {e}"}]

def print_daily_report(analyses: List[Dict]):
    """FIXED: Print formatted daily betting report with better error handling"""
    
    print(f"\n🎯 DAILY MLB BETTING ANALYSIS")
    print("=" * 80)
    
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
        final_rec = analysis['final_recommendation']
        
        print(f"\n🏟️ Game {game_pk}: {away_team} @ {home_team}")
        print(f"   🎯 RECOMMENDATION: {final_rec['recommendation']}")
        print(f"   📊 Confidence: {final_rec['confidence']}")
        print(f"   🌤️ Weather: {analysis['weather_analysis']['impact']}")
        print(f"   👨‍⚖️ Umpire: {analysis['umpire_analysis']['impact']}")
        
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
            print(f"   🎯 {away_team} @ {home_team}: {bet['final_recommendation']['recommendation']}")
    
    if error_count > 0:
        print(f"\n💡 Note: {error_count} games had analysis errors. This is normal if data is still being collected.")

def main():
    """FIXED: Run daily analysis with better error handling"""
    
    try:
        from py.config import require_config
        config = require_config(require_database=True, graceful_degradation=True)
        dsn = config.PG_DSN
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
        
        # Run today's analysis
        analyses = get_todays_analysis(conn)
        print_daily_report(analyses)
        
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