#!/usr/bin/env python3
"""
bullpen_usage_tracking.py - Add this to your py/ directory
Tracks bullpen usage, availability, and effectiveness for betting insights
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import psycopg2
from typing import Dict, List, Optional, Tuple
import json
try:
    from py.imports import setup_imports
    setup_imports()
except ImportError:
    pass

from py.config import require_config, get_config
def calculate_bullpen_availability(conn, team_id: int, game_date: str, 
                                 lookback_days: int = 7) -> Dict:
    """Calculate bullpen availability and fatigue for a team"""
    
    end_date = datetime.fromisoformat(game_date)
    start_date = end_date - timedelta(days=lookback_days)
    
    # Get recent bullpen usage
    query = """
    WITH recent_pitching AS (
        SELECT 
            s.game_date,
            s.game_pk,
            s.pitcher,
            r.person_full_name as pitcher_name,
            r.person_primary_position_code,
            COUNT(*) as pitches_thrown,
            COUNT(DISTINCT s.at_bat_number) as batters_faced,
            MAX(s.inning) as max_inning_pitched
        FROM statcast s
        JOIN roster r ON s.pitcher = r.person_id 
                    AND s.game_date = r.game_date
                    AND r.team_id = %s
        WHERE s.game_date >= %s 
        AND s.game_date < %s
        AND r.person_primary_position_code = '1'  -- Pitchers only
        GROUP BY s.game_date, s.game_pk, s.pitcher, r.person_full_name, r.person_primary_position_code
        HAVING COUNT(*) >= 1  -- At least 1 pitch (any appearance)
    ),
    starter_innings AS (
        -- Identify likely starters (pitched in early innings with many pitches)
        SELECT 
            game_date,
            game_pk,
            pitcher,
            pitches_thrown,
            batters_faced,
            CASE WHEN max_inning_pitched <= 2 AND pitches_thrown >= 50 THEN 'starter'
                 WHEN max_inning_pitched >= 3 AND pitches_thrown >= 40 THEN 'starter'
                 ELSE 'reliever' END as pitcher_role
        FROM recent_pitching
    )
    SELECT 
        rp.*,
        si.pitcher_role,
        -- Calculate days rest
        %s::date - rp.game_date::date as days_rest
    FROM recent_pitching rp
    JOIN starter_innings si ON rp.game_date = si.game_date 
                            AND rp.game_pk = si.game_pk 
                            AND rp.pitcher = si.pitcher
    WHERE si.pitcher_role = 'reliever'  -- Focus on bullpen
    ORDER BY rp.game_date DESC, rp.pitches_thrown DESC
    """
    
    try:
        df = pd.read_sql(query, conn, params=[team_id, start_date, game_date, game_date])
        
        if df.empty:
            return {
                "team_id": team_id,
                "bullpen_status": "UNKNOWN",
                "available_relievers": 0,
                "fatigued_relievers": 0,
                "total_recent_usage": 0
            }
        
        # Calculate availability metrics
        relievers_analysis = []
        
        for pitcher_id in df['pitcher'].unique():
            pitcher_data = df[df['pitcher'] == pitcher_id].copy()
            pitcher_name = pitcher_data.iloc[0]['pitcher_name']
            
            # Calculate fatigue factors
            total_pitches = pitcher_data['pitches_thrown'].sum()
            appearances = len(pitcher_data)
            days_since_last = pitcher_data['days_rest'].min()
            
            # Availability scoring
            if days_since_last == 0:  # Pitched yesterday
                availability = "UNAVAILABLE" if total_pitches >= 25 else "LIMITED"
            elif days_since_last == 1:  # Pitched day before
                if total_pitches >= 40:
                    availability = "LIMITED"
                elif appearances >= 3:
                    availability = "LIMITED"  
                else:
                    availability = "AVAILABLE"
            else:  # 2+ days rest
                availability = "AVAILABLE"
            
            # Effectiveness scoring (simplified)
            recent_effectiveness = calculate_reliever_effectiveness(pitcher_data)
            
            relievers_analysis.append({
                "pitcher_id": pitcher_id,
                "pitcher_name": pitcher_name,
                "availability": availability,
                "total_pitches_7d": total_pitches,
                "appearances_7d": appearances,
                "days_since_last": days_since_last,
                "effectiveness_score": recent_effectiveness
            })
        
        # Summarize bullpen status
        available_count = len([r for r in relievers_analysis if r['availability'] == 'AVAILABLE'])
        limited_count = len([r for r in relievers_analysis if r['availability'] == 'LIMITED'])
        unavailable_count = len([r for r in relievers_analysis if r['availability'] == 'UNAVAILABLE'])
        
        # Determine overall bullpen status
        if available_count >= 5:
            bullpen_status = "FRESH"
        elif available_count >= 3:
            bullpen_status = "AVERAGE"
        elif available_count >= 2:
            bullpen_status = "THIN"
        else:
            bullpen_status = "DEPLETED"
        
        return {
            "team_id": team_id,
            "game_date": game_date,
            "bullpen_status": bullpen_status,
            "available_relievers": available_count,
            "limited_relievers": limited_count,
            "unavailable_relievers": unavailable_count,
            "total_relievers_used": len(relievers_analysis),
            "relievers_detail": relievers_analysis,
            "betting_impact": generate_bullpen_betting_insight(bullpen_status, available_count, relievers_analysis)
        }
        
    except Exception as e:
        return {"error": f"Database error: {e}"}

def calculate_reliever_effectiveness(pitcher_data: pd.DataFrame) -> float:
    """Calculate a simple effectiveness score for a reliever"""
    
    # This is simplified - you could use more sophisticated metrics
    avg_pitches_per_appearance = pitcher_data['pitches_thrown'].mean()
    avg_batters_per_appearance = pitcher_data['batters_faced'].mean()
    
    # Efficiency score (fewer pitches per batter = better)
    if avg_batters_per_appearance > 0:
        efficiency = avg_pitches_per_appearance / avg_batters_per_appearance
        # Scale to 0-100 (lower pitches/batter = higher score)
        effectiveness = max(0, 100 - (efficiency - 3.5) * 20)
    else:
        effectiveness = 50  # Neutral
    
    return round(min(100, max(0, effectiveness)), 1)

def analyze_bullpen_vs_bullpen(conn, home_team_id: int, away_team_id: int, 
                              game_date: str) -> Dict:
    """Compare bullpen strength between two teams"""
    
    home_bullpen = calculate_bullpen_availability(conn, home_team_id, game_date)
    away_bullpen = calculate_bullpen_availability(conn, away_team_id, game_date)
    
    if 'error' in home_bullpen or 'error' in away_bullpen:
        return {"error": "Could not analyze both bullpens"}
    
    # Compare key metrics
    comparison = {
        "home_team_id": home_team_id,
        "away_team_id": away_team_id,
        "game_date": game_date,
        
        # Availability comparison
        "home_available": home_bullpen['available_relievers'],
        "away_available": away_bullpen['available_relievers'],
        "home_status": home_bullpen['bullpen_status'],
        "away_status": away_bullpen['bullpen_status'],
        
        # Determine advantage
        "availability_advantage": determine_bullpen_advantage(home_bullpen, away_bullpen),
        
        # Betting insights
        "betting_recommendation": generate_comparative_bullpen_insight(home_bullpen, away_bullpen)
    }
    
    return comparison

def determine_bullpen_advantage(home_bullpen: Dict, away_bullpen: Dict) -> str:
    """Determine which team has the bullpen advantage"""
    
    home_score = score_bullpen_strength(home_bullpen)
    away_score = score_bullpen_strength(away_bullpen)
    
    diff = home_score - away_score
    
    if diff >= 20:
        return "HOME_MAJOR"
    elif diff >= 10:
        return "HOME_MODERATE"
    elif diff <= -20:
        return "AWAY_MAJOR"
    elif diff <= -10:
        return "AWAY_MODERATE"
    else:
        return "NEUTRAL"

def score_bullpen_strength(bullpen_data: Dict) -> int:
    """Score bullpen strength (0-100)"""
    
    status_scores = {
        "FRESH": 90,
        "AVERAGE": 70,
        "THIN": 40,
        "DEPLETED": 10
    }
    
    base_score = status_scores.get(bullpen_data['bullpen_status'], 50)
    
    # Adjust based on available relievers
    available_adjustment = min(20, bullpen_data['available_relievers'] * 3)
    
    # Factor in effectiveness if available
    if 'relievers_detail' in bullpen_data:
        avg_effectiveness = np.mean([r['effectiveness_score'] for r in bullpen_data['relievers_detail']])
        effectiveness_adjustment = (avg_effectiveness - 50) * 0.3
    else:
        effectiveness_adjustment = 0
    
    final_score = base_score + available_adjustment + effectiveness_adjustment
    
    return round(min(100, max(0, final_score)))

def track_closer_availability(conn, team_id: int, game_date: str) -> Dict:
    """Specifically track closer/high-leverage reliever availability"""
    
    # Get recent high-leverage appearances (9th inning, save situations)
    query = """
    WITH high_leverage_apps AS (
        SELECT 
            s.game_date,
            s.pitcher,
            r.person_full_name,
            COUNT(*) as pitches,
            COUNT(DISTINCT s.at_bat_number) as batters_faced,
            s.inning,
            -- Identify potential save situations (rough approximation)
            CASE WHEN s.inning >= 9 THEN 'closer_situation'
                 WHEN s.inning >= 7 THEN 'setup_situation'
                 ELSE 'other' END as situation_type
        FROM statcast s
        JOIN roster r ON s.pitcher = r.person_id 
                    AND s.game_date = r.game_date
                    AND r.team_id = %s
        WHERE s.game_date >= %s 
        AND s.game_date < %s
        AND s.inning >= 7  -- Late innings only
        AND r.person_primary_position_code = '1'
        GROUP BY s.game_date, s.pitcher, r.person_full_name, s.inning
        HAVING COUNT(*) >= 5  -- Meaningful appearance
    )
    SELECT 
        pitcher,
        person_full_name,
        situation_type,
        SUM(pitches) as total_pitches,
        COUNT(*) as appearances,
        MAX(game_date) as last_appearance,
        %s::date - MAX(game_date)::date as days_rest
    FROM high_leverage_apps
    WHERE situation_type IN ('closer_situation', 'setup_situation')
    GROUP BY pitcher, person_full_name, situation_type
    ORDER BY situation_type DESC, total_pitches DESC
    """
    
    end_date = datetime.fromisoformat(game_date)
    start_date = end_date - timedelta(days=7)
    
    try:
        df = pd.read_sql(query, conn, params=[team_id, start_date, game_date, game_date])
        
        closer_status = {
            "team_id": team_id,
            "closer_available": True,
            "setup_available": True,
            "closer_name": None,
            "closer_rest_days": None,
            "late_inning_depth": 0
        }
        
        if df.empty:
            closer_status["status"] = "NO_RECENT_DATA"
            return closer_status
        
        # Find likely closer (most closer_situation appearances)
        closers = df[df['situation_type'] == 'closer_situation']
        if len(closers) > 0:
            primary_closer = closers.iloc[0]
            closer_status["closer_name"] = primary_closer['person_full_name']
            closer_status["closer_rest_days"] = primary_closer['days_rest']
            
            # Determine availability
            if primary_closer['days_rest'] == 0:
                closer_status["closer_available"] = False
            elif primary_closer['days_rest'] == 1 and primary_closer['total_pitches'] >= 25:
                closer_status["closer_available"] = False
        
        # Count late-inning depth
        closer_status["late_inning_depth"] = len(df['pitcher'].unique())
        
        # Generate recommendation
        if not closer_status["closer_available"]:
            closer_status["betting_impact"] = "NEGATIVE - Closer unavailable, weaker late-inning options"
        elif closer_status["late_inning_depth"] <= 2:
            closer_status["betting_impact"] = "NEGATIVE - Thin late-inning relief depth"
        else:
            closer_status["betting_impact"] = "NEUTRAL - Adequate late-inning options"
        
        return closer_status
        
    except Exception as e:
        return {"error": f"Database error: {e}"}

def generate_bullpen_betting_insight(status: str, available_count: int, 
                                   relievers_detail: List[Dict]) -> str:
    """Generate betting insights based on bullpen status"""
    
    if status == "DEPLETED":
        return "STRONG NEGATIVE - Bullpen depleted, bet opponent runs/totals OVER"
    elif status == "THIN":
        return "NEGATIVE - Thin bullpen, lean opponent props OVER"
    elif status == "FRESH" and available_count >= 6:
        return "POSITIVE - Fresh bullpen, good late-inning support"
    else:
        return "NEUTRAL - Average bullpen availability"

def generate_comparative_bullpen_insight(home_bullpen: Dict, away_bullpen: Dict) -> str:
    """Generate betting insight comparing both bullpens"""
    
    home_score = score_bullpen_strength(home_bullpen)
    away_score = score_bullpen_strength(away_bullpen)
    
    diff = home_score - away_score
    
    if diff >= 25:
        return "BET HOME TEAM - Major bullpen advantage"
    elif diff >= 15:
        return "LEAN HOME TEAM - Moderate bullpen advantage"
    elif diff <= -25:
        return "BET AWAY TEAM - Major bullpen advantage"
    elif diff <= -15:
        return "LEAN AWAY TEAM - Moderate bullpen advantage"
    else:
        return "NEUTRAL - Similar bullpen strength"

def get_todays_bullpen_analysis(conn, game_date: str = None) -> List[Dict]:
    """Get bullpen analysis for all today's games"""
    
    if game_date is None:
        game_date = datetime.now().strftime('%Y-%m-%d')
    
    # Get today's games
    query = """
    SELECT DISTINCT 
        l1.game_pk,
        l1.team_id as home_team_id,
        l2.team_id as away_team_id,
        MAX(CASE WHEN l1.side = 'home' THEN l1.team_id END) as home_team,
        MAX(CASE WHEN l1.side = 'away' THEN l1.team_id END) as away_team
    FROM lineup l1
    JOIN lineup l2 ON l1.game_pk = l2.game_pk AND l1.team_id != l2.team_id
    WHERE l1.game_date = %s
    GROUP BY l1.game_pk, l1.team_id, l2.team_id
    """
    
    try:
        games = pd.read_sql(query, conn, params=[game_date])
        
        if games.empty:
            return [{"error": "No games found for today"}]
        
        results = []
        
        for _, game in games.iterrows():
            home_team_id = game['home_team_id']
            away_team_id = game['away_team_id']
            
            if home_team_id and away_team_id:
                print(f"📊 Analyzing bullpen matchup: {away_team_id} @ {home_team_id}")
                
                comparison = analyze_bullpen_vs_bullpen(conn, home_team_id, away_team_id, game_date)
                comparison['game_pk'] = game['game_pk']
                
                results.append(comparison)
        
        return results
        
    except Exception as e:
        return [{"error": f"Database error: {e}"}]

def print_bullpen_analysis_report(analysis_results: List[Dict]):
    """Print formatted bullpen analysis report"""
    
    print(f"\n🎯 BULLPEN USAGE & AVAILABILITY ANALYSIS")
    print("=" * 70)
    
    for result in analysis_results:
        if 'error' in result:
            print(f"❌ {result['error']}")
            continue
        
        print(f"\n🏟️ Game {result['game_pk']}: Teams {result['away_team_id']} @ {result['home_team_id']}")
        print(f"   🏠 Home Bullpen: {result['home_status']} ({result['home_available']} available)")
        print(f"   ✈️  Away Bullpen: {result['away_status']} ({result['away_available']} available)")
        print(f"   ⚖️  Advantage: {result['availability_advantage']}")
        print(f"   💰 BETTING REC: {result['betting_recommendation']}")
        
        if result['availability_advantage'] in ['HOME_MAJOR', 'AWAY_MAJOR']:
            print("   🚨 SIGNIFICANT BULLPEN EDGE - Consider betting opportunity!")

def main():
    """Test bullpen analysis"""
    
    config = require_config(require_database=True)
    dsn = config.PG_DSN
    if not dsn:
        print("❌ PG_DSN environment variable must be set")
        return
    
    try:
        conn = psycopg2.connect(dsn)
        
        print("🎯 Bullpen Usage Analysis")
        print("=" * 60)
        
        # Get today's bullpen analysis
        results = get_todays_bullpen_analysis(conn)
        print_bullpen_analysis_report(results)
        
        # Test individual team analysis
        print("\n🧪 Testing individual team bullpen analysis...")
        
        # Example: Analyze Yankees bullpen (team_id would be actual Yankees ID)
        sample_analysis = calculate_bullpen_availability(conn, 147, "2024-04-15")  # Example
        
        if 'error' not in sample_analysis:
            print(f"   Team {sample_analysis['team_id']} Bullpen Status: {sample_analysis['bullpen_status']}")
            print(f"   Available relievers: {sample_analysis['available_relievers']}")
            print(f"   Betting Impact: {sample_analysis['betting_impact']}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()