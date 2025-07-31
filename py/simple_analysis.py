#!/usr/bin/env python3
"""
simple_analysis.py - Streamlined MLB betting analysis
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
    # Add essential parks - defaulting others to neutral
}

def get_ballpark_factor(team_name: str) -> float:
    """Get simplified run factor for team's ballpark"""
    for park_info in BALLPARK_FACTORS.values():
        if team_name in park_info["team"] or park_info["team"] in team_name:
            return park_info["run_factor"]
    return 1.0  # Neutral for unknown parks

def analyze_weather_impact(conn, game_pk: int) -> Dict:
    """Simple weather analysis focused on key betting factors"""
    
    query = """
    SELECT temperature_f, wind_speed_mph, wind_direction_deg, 
           home_team, venue_name
    FROM weather 
    WHERE game_pk = %s
    """
    
    try:
        result = pd.read_sql(query, conn, params=[game_pk])
        if result.empty:
            return {"impact": "NEUTRAL", "factor": 1.0, "reason": "No weather data"}
        
        row = result.iloc[0]
        temp = row['temperature_f'] or 70
        wind_speed = row['wind_speed_mph'] or 0
        home_team = row['home_team']
        
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
        if wind_speed >= 15:
            # Check if it's Wrigley (wind-sensitive)
            if "Cubs" in home_team or "Wrigley" in str(row.get('venue_name', '')):
                wind_impact = 0.20 if 180 <= (row['wind_direction_deg'] or 0) <= 270 else -0.20
                wind_desc = f"Strong Wrigley wind ({wind_speed} mph)"
            else:
                wind_impact = 0.10  # General strong wind impact
                wind_desc = f"Strong wind ({wind_speed} mph)"
        elif wind_speed >= 10:
            wind_impact = 0.05
            wind_desc = f"Moderate wind ({wind_speed} mph)"
        else:
            wind_impact = 0.0
            wind_desc = "Calm conditions"
        
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
        return {"impact": "ERROR", "factor": 1.0, "reason": f"Error: {e}"}

def analyze_umpire_impact(conn, game_pk: int) -> Dict:
    """Simple umpire analysis focused on run totals"""
    
    query = """
    WITH ump_history AS (
        SELECT u.umpire_name,
               COUNT(*) as games_worked,
               -- Estimate runs per game from Statcast events
               AVG(
                   (SELECT COUNT(*) FROM statcast s2 
                    WHERE s2.game_pk = u.game_pk 
                    AND s2.events IN ('single', 'double', 'triple', 'home_run', 'walk', 'hit_by_pitch')
                   ) * 0.3  -- Rough conversion to runs
               ) as avg_estimated_runs
        FROM umpires u
        WHERE u.position = 'Home Plate'
        AND u.game_date >= %s
        GROUP BY u.umpire_name
        HAVING COUNT(*) >= 10  -- Minimum sample size
    )
    SELECT u.umpire_name, h.games_worked, h.avg_estimated_runs
    FROM umpires u
    LEFT JOIN ump_history h ON u.umpire_name = h.umpire_name
    WHERE u.game_pk = %s AND u.position = 'Home Plate'
    """
    
    lookback_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    
    try:
        result = pd.read_sql(query, conn, params=[lookback_date, game_pk])
        if result.empty:
            return {"impact": "NEUTRAL", "reason": "No umpire data"}
        
        row = result.iloc[0]
        umpire_name = row['umpire_name']
        games_worked = row['games_worked'] or 0
        avg_runs = row['avg_estimated_runs'] or 8.5
        
        mlb_average = 8.5
        runs_diff = avg_runs - mlb_average
        
        if games_worked >= 30:
            confidence = "HIGH"
        elif games_worked >= 15:
            confidence = "MEDIUM"
        else:
            confidence = "LOW"
            
        if runs_diff >= 1.0:
            impact = "STRONG OVER"
        elif runs_diff >= 0.5:
            impact = "OVER LEAN"
        elif runs_diff <= -1.0:
            impact = "STRONG UNDER"
        elif runs_diff <= -0.5:
            impact = "UNDER LEAN"
        else:
            impact = "NEUTRAL"
        
        reason = f"{umpire_name}: {avg_runs:.1f} runs/game avg ({games_worked} games)"
        
        return {
            "impact": impact,
            "umpire_name": umpire_name,
            "avg_runs": round(avg_runs, 1),
            "sample_size": int(games_worked),
            "reason": reason,
            "confidence": confidence
        }
        
    except Exception as e:
        return {"impact": "ERROR", "reason": f"Error: {e}"}

def analyze_pitcher_trends(conn, game_pk: int) -> Dict:
    """Simple starting pitcher recent form analysis"""
    
    query = """
    WITH todays_starters AS (
        SELECT DISTINCT s.pitcher, r.team_id
        FROM statcast s
        JOIN roster r ON s.pitcher = r.person_id AND s.game_date = r.game_date
        WHERE s.game_pk = %s AND s.inning = 1
        LIMIT 2  -- Both starting pitchers
    ),
    pitcher_recent AS (
        SELECT ts.pitcher, ts.team_id,
               COUNT(DISTINCT s.game_pk) as recent_starts,
               -- Simplified ERA calculation
               SUM(CASE WHEN s.events IN ('single', 'double', 'triple', 'home_run') THEN 1 ELSE 0 END) as hits_allowed,
               SUM(CASE WHEN s.events = 'home_run' THEN 1 ELSE 0 END) as hrs_allowed,
               COUNT(CASE WHEN s.events LIKE '%%strikeout%%' THEN 1 END) as strikeouts,
               COUNT(CASE WHEN s.events IS NOT NULL THEN 1 END) as batters_faced
        FROM todays_starters ts
        JOIN statcast s ON ts.pitcher = s.pitcher
        WHERE s.game_date >= %s AND s.game_date < %s
        GROUP BY ts.pitcher, ts.team_id
    )
    SELECT pitcher, team_id, recent_starts, hits_allowed, hrs_allowed, 
           strikeouts, batters_faced
    FROM pitcher_recent
    WHERE recent_starts >= 2
    """
    
    game_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    
    try:
        result = pd.read_sql(query, conn, params=[game_pk, start_date, game_date])
        if result.empty:
            return {"impact": "NEUTRAL", "reason": "No recent pitcher data"}
        
        pitcher_insights = []
        
        for _, pitcher in result.iterrows():
            if pitcher['batters_faced'] > 0:
                hits_per_bf = pitcher['hits_allowed'] / pitcher['batters_faced']
                k_rate = pitcher['strikeouts'] / pitcher['batters_faced']
                
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
        
        return {
            "pitchers": pitcher_insights,
            "reason": f"Analyzed {len(pitcher_insights)} starting pitchers"
        }
        
    except Exception as e:
        return {"impact": "ERROR", "reason": f"Error: {e}"}

def analyze_team_trends(conn, game_pk: int) -> Dict:
    """Simple team form analysis (last 10 games)"""
    
    query = """
    WITH game_teams AS (
        SELECT DISTINCT 
            CASE WHEN r.team_id = (SELECT DISTINCT r2.team_id FROM roster r2 
                                   JOIN statcast s2 ON r2.person_id = s2.batter 
                                   WHERE s2.game_pk = %s AND s2.inning_topbot = 'Bot' LIMIT 1)
                 THEN 'home' ELSE 'away' END as home_away,
            r.team_id
        FROM roster r 
        JOIN statcast s ON r.person_id = s.batter AND r.game_date = s.game_date
        WHERE s.game_pk = %s
        LIMIT 2
    ),
    team_recent_games AS (
        SELECT gt.team_id, gt.home_away,
               COUNT(DISTINCT s.game_pk) as games_played,
               -- Estimate runs scored
               SUM(CASE WHEN s.events IN ('single', 'double', 'triple', 'home_run') THEN 1 ELSE 0 END) * 0.4 as est_runs_scored,
               COUNT(CASE WHEN s.events = 'home_run' THEN 1 END) as home_runs
        FROM game_teams gt
        JOIN roster r ON gt.team_id = r.team_id
        JOIN statcast s ON r.person_id = s.batter AND r.game_date = s.game_date
        WHERE s.game_date >= %s AND s.game_date < %s
        GROUP BY gt.team_id, gt.home_away
    )
    SELECT team_id, home_away, games_played, est_runs_scored, home_runs
    FROM team_recent_games
    WHERE games_played >= 5
    """
    
    game_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
    
    try:
        result = pd.read_sql(query, conn, params=[game_pk, game_pk, start_date, game_date])
        if result.empty:
            return {"impact": "NEUTRAL", "reason": "No recent team data"}
        
        team_insights = []
        
        for _, team in result.iterrows():
            if team['games_played'] > 0:
                runs_per_game = team['est_runs_scored'] / team['games_played']
                
                if runs_per_game >= 6.0:
                    trend = "HOT OFFENSE"
                    impact = "OVER LEAN"
                elif runs_per_game >= 5.0:
                    trend = "GOOD OFFENSE"
                    impact = "SLIGHT OVER"
                elif runs_per_game <= 3.0:
                    trend = "COLD OFFENSE"
                    impact = "UNDER LEAN"
                else:
                    trend = "AVERAGE OFFENSE"
                    impact = "NEUTRAL"
                
                team_insights.append({
                    "team": team['home_away'],
                    "trend": trend,
                    "impact": impact,
                    "runs_per_game": round(runs_per_game, 1),
                    "games": int(team['games_played'])
                })
        
        return {
            "teams": team_insights,
            "reason": f"Analyzed recent form for both teams"
        }
        
    except Exception as e:
        return {"impact": "ERROR", "reason": f"Error: {e}"}

def generate_combined_recommendation(weather: Dict, umpire: Dict, 
                                   pitchers: Dict, teams: Dict) -> Dict:
    """Combine all factors into final betting recommendation"""
    
    # Collect all directional signals
    over_signals = []
    under_signals = []
    
    # Weather signals
    if weather["impact"] in ["STRONG OVER", "OVER LEAN"]:
        weight = 3 if "STRONG" in weather["impact"] else 1
        over_signals.extend([weather["impact"]] * weight)
    elif weather["impact"] in ["STRONG UNDER", "UNDER LEAN"]:
        weight = 3 if "STRONG" in weather["impact"] else 1
        under_signals.extend([weather["impact"]] * weight)
    
    # Umpire signals
    if umpire["impact"] in ["STRONG OVER", "OVER LEAN"]:
        weight = 2 if "STRONG" in umpire["impact"] and umpire.get("confidence") == "HIGH" else 1
        over_signals.extend([umpire["impact"]] * weight)
    elif umpire["impact"] in ["STRONG UNDER", "UNDER LEAN"]:
        weight = 2 if "STRONG" in umpire["impact"] and umpire.get("confidence") == "HIGH" else 1
        under_signals.extend([umpire["impact"]] * weight)
    
    # Pitcher signals (for opponent scoring)
    if "pitchers" in pitchers:
        for pitcher in pitchers["pitchers"]:
            if "OVER" in pitcher["impact"]:
                over_signals.append(pitcher["impact"])
            elif "UNDER" in pitcher["impact"]:
                under_signals.append(pitcher["impact"])
    
    # Team signals
    if "teams" in teams:
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
            weather["reason"],
            umpire["reason"],
            pitchers["reason"],
            teams["reason"]
        ]
    }

def analyze_game(conn, game_pk: int) -> Dict:
    """Complete analysis for a single game"""
    
    print(f"🔍 Analyzing game {game_pk}...")
    
    # Get basic game info
    game_query = """
    SELECT DISTINCT 
        r1.team_id as home_team_id,
        r2.team_id as away_team_id,
        s.game_date
    FROM statcast s
    JOIN roster r1 ON s.batter = r1.person_id AND s.game_date = r1.game_date
    JOIN roster r2 ON s.pitcher = r2.person_id AND s.game_date = r2.game_date  
    WHERE s.game_pk = %s 
    AND s.inning_topbot = 'Bot'  -- Home team batting
    LIMIT 1
    """
    
    try:
        game_info = pd.read_sql(game_query, conn, params=[game_pk])
        if game_info.empty:
            return {"error": "Game not found"}
        
        # Analyze all factors
        weather = analyze_weather_impact(conn, game_pk)
        umpire = analyze_umpire_impact(conn, game_pk)
        pitchers = analyze_pitcher_trends(conn, game_pk)
        teams = analyze_team_trends(conn, game_pk)
        
        # Generate combined recommendation
        combined = generate_combined_recommendation(weather, umpire, pitchers, teams)
        
        return {
            "game_pk": game_pk,
            "game_date": game_info.iloc[0]['game_date'],
            "weather_analysis": weather,
            "umpire_analysis": umpire,
            "pitcher_analysis": pitchers,
            "team_analysis": teams,
            "final_recommendation": combined
        }
        
    except Exception as e:
        return {"error": f"Analysis failed: {e}"}

def get_todays_analysis(conn, game_date: str = None) -> List[Dict]:
    """Analyze all games for a given date"""
    
    if game_date is None:
        game_date = datetime.now().strftime('%Y-%m-%d')
    
    # Get all games for the date
    games_query = """
    SELECT DISTINCT game_pk 
    FROM statcast 
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
    """Print formatted daily betting report"""
    
    print(f"\n🎯 DAILY MLB BETTING ANALYSIS")
    print("=" * 80)
    
    strong_bets = []
    moderate_bets = []
    
    for analysis in analyses:
        if "error" in analysis:
            print(f"❌ Error: {analysis['error']}")
            continue
        
        game_pk = analysis['game_pk']
        final_rec = analysis['final_recommendation']
        
        print(f"\n🏟️ Game {game_pk}")
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
    print(f"\n📋 DAILY SUMMARY:")
    print(f"   🚨 High confidence bets: {len(strong_bets)}")
    print(f"   📊 Medium confidence bets: {len(moderate_bets)}")
    
    if strong_bets:
        print(f"\n🚨 TODAY'S BEST BETS:")
        for bet in strong_bets:
            print(f"   🎯 Game {bet['game_pk']}: {bet['final_recommendation']['recommendation']}")

def main():
    """Run daily analysis"""
    
    from py.config import require_config
    
    config = require_config(require_database=True)
    dsn = config.PG_DSN
    
    if not dsn:
        print("❌ PG_DSN environment variable must be set")
        return
    
    try:
        conn = psycopg2.connect(dsn)
        print("✅ Connected to database")
        
        # Run today's analysis
        analyses = get_todays_analysis(conn)
        print_daily_report(analyses)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()