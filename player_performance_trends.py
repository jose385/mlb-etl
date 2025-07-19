#!/usr/bin/env python3
"""
player_performance_trends.py - Add this to your py/ directory
Analyzes player hot/cold streaks and recent form for betting insights
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import psycopg2
from typing import Dict, List, Optional

def calculate_batter_hot_cold_streaks(conn, player_id: int, lookback_days: int = 30) -> Dict:
    """Analyze batter's recent performance trends"""
    
    query = """
    WITH recent_games AS (
        SELECT 
            s.game_date,
            s.batter,
            COUNT(CASE WHEN s.events IN ('single', 'double', 'triple', 'home_run') THEN 1 END) as hits,
            COUNT(CASE WHEN s.events IS NOT NULL AND s.events != '' THEN 1 END) as at_bats,
            COUNT(CASE WHEN s.events = 'home_run' THEN 1 END) as home_runs,
            COUNT(CASE WHEN s.events IN ('walk', 'hit_by_pitch') THEN 1 END) as walks_hbp,
            SUM(COALESCE(s.woba_value, 0)) as total_woba_value,
            COUNT(CASE WHEN s.woba_value IS NOT NULL THEN 1 END) as woba_opportunities
        FROM statcast s
        WHERE s.batter = %s 
        AND s.game_date >= %s
        AND s.game_date <= %s
        GROUP BY s.game_date, s.batter
        ORDER BY s.game_date DESC
    ),
    game_stats AS (
        SELECT 
            game_date,
            batter,
            CASE WHEN at_bats > 0 THEN hits::float / at_bats ELSE 0 END as avg,
            CASE WHEN woba_opportunities > 0 THEN total_woba_value / woba_opportunities ELSE 0 END as woba,
            home_runs,
            walks_hbp,
            at_bats
        FROM recent_games
        WHERE at_bats > 0
    )
    SELECT 
        game_date,
        avg,
        woba,
        home_runs,
        walks_hbp,
        at_bats,
        ROW_NUMBER() OVER (ORDER BY game_date DESC) as game_recency
    FROM game_stats
    ORDER BY game_date DESC
    """
    
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=lookback_days)
    
    try:
        df = pd.read_sql(query, conn, params=[player_id, start_date, end_date])
        
        if df.empty:
            return {"error": "No recent data", "trend": "UNKNOWN"}
        
        # Calculate rolling averages
        df['rolling_7_avg'] = df['avg'].rolling(window=min(7, len(df)), min_periods=1).mean()
        df['rolling_15_avg'] = df['avg'].rolling(window=min(15, len(df)), min_periods=1).mean()
        
        # Recent form (last 7 games)
        recent_avg = df.head(7)['avg'].mean()
        season_avg = df['avg'].mean()
        
        # Streak analysis
        current_streak = calculate_hitting_streak(df)
        
        # Hot/Cold determination
        if recent_avg >= season_avg + 0.100:  # 100 points above average
            trend = "🔥 RED HOT"
            confidence = "HIGH"
        elif recent_avg >= season_avg + 0.050:
            trend = "📈 HOT STREAK"  
            confidence = "MEDIUM"
        elif recent_avg <= season_avg - 0.100:
            trend = "🧊 ICE COLD"
            confidence = "HIGH"
        elif recent_avg <= season_avg - 0.050:
            trend = "📉 COLD STREAK"
            confidence = "MEDIUM"
        else:
            trend = "😐 NEUTRAL"
            confidence = "LOW"
        
        return {
            "player_id": player_id,
            "games_analyzed": len(df),
            "recent_avg_7games": round(recent_avg, 3),
            "season_avg": round(season_avg, 3),
            "avg_difference": round(recent_avg - season_avg, 3),
            "current_hitting_streak": current_streak,
            "trend": trend,
            "confidence": confidence,
            "betting_insight": generate_batter_betting_insight(trend, recent_avg, current_streak)
        }
        
    except Exception as e:
        return {"error": f"Database error: {e}"}

def calculate_hitting_streak(df: pd.DataFrame) -> int:
    """Calculate current hitting streak"""
    streak = 0
    for _, game in df.iterrows():
        if game['avg'] > 0:  # Had a hit
            streak += 1
        else:
            break
    return streak

def analyze_pitcher_recent_form(conn, player_id: int, lookback_days: int = 30) -> Dict:
    """Analyze pitcher's recent performance and trends"""
    
    query = """
    WITH pitcher_games AS (
        SELECT 
            s.game_date,
            s.pitcher,
            COUNT(*) as total_pitches,
            COUNT(CASE WHEN s.events IN ('single', 'double', 'triple', 'home_run') THEN 1 END) as hits_allowed,
            COUNT(CASE WHEN s.events = 'home_run' THEN 1 END) as hrs_allowed,
            COUNT(CASE WHEN s.events LIKE '%strikeout%' THEN 1 END) as strikeouts,
            COUNT(CASE WHEN s.events IN ('walk', 'hit_by_pitch') THEN 1 END) as walks_hbp,
            AVG(CASE WHEN s.release_speed IS NOT NULL THEN s.release_speed END) as avg_velocity,
            COUNT(CASE WHEN s.events IS NOT NULL AND s.events != '' THEN 1 END) as batters_faced
        FROM statcast s
        WHERE s.pitcher = %s 
        AND s.game_date >= %s
        AND s.game_date <= %s
        GROUP BY s.game_date, s.pitcher
        HAVING COUNT(*) >= 20  -- At least 20 pitches (meaningful appearance)
        ORDER BY s.game_date DESC
    )
    SELECT 
        game_date,
        total_pitches,
        hits_allowed,
        hrs_allowed,
        strikeouts,
        walks_hbp,
        avg_velocity,
        batters_faced,
        CASE WHEN batters_faced > 0 THEN hits_allowed::float / batters_faced ELSE 0 END as hits_per_bf,
        CASE WHEN batters_faced > 0 THEN strikeouts::float / batters_faced ELSE 0 END as k_rate
    FROM pitcher_games
    ORDER BY game_date DESC
    """
    
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=lookback_days)
    
    try:
        df = pd.read_sql(query, conn, params=[player_id, start_date, end_date])
        
        if df.empty:
            return {"error": "No recent pitching data", "trend": "UNKNOWN"}
        
        # Calculate trends
        recent_games = df.head(3)  # Last 3 starts
        
        recent_hits_per_bf = recent_games['hits_per_bf'].mean()
        recent_k_rate = recent_games['k_rate'].mean()
        recent_velocity = recent_games['avg_velocity'].mean()
        
        season_hits_per_bf = df['hits_per_bf'].mean()
        season_k_rate = df['k_rate'].mean()
        season_velocity = df['avg_velocity'].mean()
        
        # Velocity decline check (fatigue indicator)
        velocity_decline = season_velocity - recent_velocity if recent_velocity and season_velocity else 0
        
        # Performance trend
        if recent_hits_per_bf <= season_hits_per_bf - 0.050 and recent_k_rate >= season_k_rate + 0.030:
            trend = "🔥 DOMINANT FORM"
        elif recent_hits_per_bf <= season_hits_per_bf - 0.030:
            trend = "📈 GOOD FORM"
        elif recent_hits_per_bf >= season_hits_per_bf + 0.050 or velocity_decline >= 2.0:
            trend = "⚠️ STRUGGLING/FATIGUED"
        elif recent_hits_per_bf >= season_hits_per_bf + 0.030:
            trend = "📉 POOR FORM"
        else:
            trend = "😐 AVERAGE"
        
        return {
            "player_id": player_id,
            "starts_analyzed": len(df),
            "recent_hits_per_bf": round(recent_hits_per_bf, 3),
            "recent_k_rate": round(recent_k_rate, 3),
            "recent_velocity": round(recent_velocity, 1) if recent_velocity else None,
            "velocity_decline": round(velocity_decline, 1),
            "trend": trend,
            "betting_insight": generate_pitcher_betting_insight(trend, recent_k_rate, velocity_decline)
        }
        
    except Exception as e:
        return {"error": f"Database error: {e}"}

def generate_batter_betting_insight(trend: str, recent_avg: float, streak: int) -> str:
    """Generate betting insights for batters"""
    
    if "RED HOT" in trend:
        return f"STRONG BET: Player props OVER (avg {recent_avg:.3f}, {streak} game hit streak)"
    elif "HOT STREAK" in trend:
        return f"BET LEAN: Player props OVER (recent form: {recent_avg:.3f})"
    elif "ICE COLD" in trend:
        return f"STRONG BET: Player props UNDER (cold streak, avg {recent_avg:.3f})"
    elif "COLD STREAK" in trend:
        return f"BET LEAN: Player props UNDER (poor recent form)"
    else:
        return "No strong player prop edge"

def generate_pitcher_betting_insight(trend: str, k_rate: float, velocity_decline: float) -> str:
    """Generate betting insights for pitchers"""
    
    if "DOMINANT" in trend:
        return f"STRONG BET: Strikeout props OVER, team total UNDER (K rate: {k_rate:.1%})"
    elif "STRUGGLING" in trend or velocity_decline >= 2.0:
        return f"STRONG BET: Pitcher props UNDER, opponent total OVER (velocity down {velocity_decline:.1f} mph)"
    elif "GOOD FORM" in trend:
        return f"BET LEAN: Strikeout props OVER (good recent form)"
    elif "POOR FORM" in trend:
        return f"BET LEAN: Opponent props OVER (allowing more contact)"
    else:
        return "No strong pitcher prop edge"

def get_todays_player_trends(conn, game_date: str = None) -> List[Dict]:
    """Get player trends for today's games"""
    
    if game_date is None:
        game_date = datetime.now().strftime('%Y-%m-%d')
    
    # Get today's starting lineups
    lineup_query = """
    SELECT DISTINCT 
        l.person_id,
        l.full_name,
        l.team_id,
        l.batting_order,
        l.position_code
    FROM lineup l
    WHERE l.game_date = %s
    ORDER BY l.team_id, l.batting_order
    """
    
    try:
        lineups = pd.read_sql(lineup_query, conn, params=[game_date])
        
        if lineups.empty:
            return [{"error": "No lineup data for today"}]
        
        results = []
        
        # Analyze top batters (1-4 in order) and starting pitchers
        for _, player in lineups.iterrows():
            if player['batting_order'] <= 4 or player['position_code'] == '1':  # Top 4 hitters or pitchers
                
                if player['position_code'] == '1':  # Pitcher
                    analysis = analyze_pitcher_recent_form(conn, player['person_id'])
                else:  # Batter
                    analysis = calculate_batter_hot_cold_streaks(conn, player['person_id'])
                
                analysis['player_name'] = player['full_name']
                analysis['team_id'] = player['team_id']
                analysis['position'] = 'Pitcher' if player['position_code'] == '1' else f"Batter #{player['batting_order']}"
                
                results.append(analysis)
        
        return results
        
    except Exception as e:
        return [{"error": f"Database error: {e}"}]

def main():
    """Test player trends analysis"""
    
    dsn = os.getenv("PG_DSN")
    if not dsn:
        print("❌ PG_DSN environment variable must be set")
        return
    
    try:
        conn = psycopg2.connect(dsn)
        
        print("🔥 Player Performance Trends Analysis")
        print("=" * 60)
        
        trends = get_todays_player_trends(conn)
        
        for trend in trends:
            if 'error' in trend:
                print(f"❌ {trend['error']}")
                continue
            
            print(f"\n👤 {trend['player_name']} ({trend['position']})")
            print(f"   📊 Trend: {trend['trend']}")
            print(f"   💰 Betting Insight: {trend['betting_insight']}")
            
            if trend.get('recent_avg_7games'):
                print(f"   📈 Recent avg: {trend['recent_avg_7games']:.3f} (vs season: {trend['season_avg']:.3f})")
            if trend.get('recent_k_rate'):
                print(f"   ⚾ Recent K rate: {trend['recent_k_rate']:.1%}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()