#!/usr/bin/env python3
"""
enhanced_umpire_betting.py - Add this to your py/ directory
Builds on your existing umpire_integration.py with betting-focused analysis
"""

import pandas as pd
import numpy as np
from pathlib import Path
import psycopg2
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import os

def analyze_umpire_betting_impact(conn, umpire_name: str, lookback_days: int = 365) -> Dict[str, any]:
    """
    Analyze specific umpire's impact on betting outcomes
    Returns actionable betting insights
    """
    
    # Query historical games with this umpire
    query = f"""
    WITH umpire_games AS (
        SELECT DISTINCT 
            u.game_pk,
            u.game_date,
            u.umpire_name,
            u.position
        FROM umpires u 
        WHERE u.umpire_name = %s 
        AND u.position = 'Home Plate'
        AND u.game_date >= %s
    ),
    game_totals AS (
        SELECT 
            ug.game_pk,
            ug.game_date,
            ug.umpire_name,
            COUNT(DISTINCT s.game_pk) as has_statcast_data,
            -- Calculate total runs (rough estimate from Statcast events)
            SUM(CASE 
                WHEN s.events IN ('single', 'double', 'triple', 'home_run', 'walk', 'hit_by_pitch') 
                THEN 1 ELSE 0 
            END) as estimated_baserunners,
            COUNT(CASE WHEN s.events = 'home_run' THEN 1 END) as home_runs,
            COUNT(CASE WHEN s.events LIKE '%strikeout%' OR s.events = 'strikeout' THEN 1 END) as strikeouts,
            COUNT(s.*) as total_pitches,
            
            -- Strike zone analysis
            COUNT(CASE WHEN s.description = 'called_strike' THEN 1 END) as called_strikes,
            COUNT(CASE WHEN s.description = 'ball' THEN 1 END) as called_balls,
            
            -- Game pace indicators
            MAX(s.at_bat_number) as total_at_bats
        FROM umpire_games ug
        LEFT JOIN statcast s ON ug.game_pk = s.game_pk
        GROUP BY ug.game_pk, ug.game_date, ug.umpire_name
    )
    SELECT 
        COUNT(*) as total_games,
        AVG(estimated_baserunners::float / NULLIF(total_at_bats, 0) * 9) as avg_estimated_runs_per_game,
        AVG(home_runs::float) as avg_home_runs_per_game,
        AVG(strikeouts::float) as avg_strikeouts_per_game,
        AVG(total_pitches::float) as avg_pitches_per_game,
        AVG(called_strikes::float / NULLIF(called_strikes + called_balls, 0)) as strike_call_rate,
        STDDEV(estimated_baserunners::float / NULLIF(total_at_bats, 0) * 9) as run_volatility
    FROM game_totals
    WHERE has_statcast_data > 0;
    """
    
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=lookback_days)
    
    try:
        df = pd.read_sql(query, conn, params=[umpire_name, start_date])
        
        if df.empty or df.iloc[0]['total_games'] is None:
            return {
                'umpire_name': umpire_name,
                'error': 'No historical data found',
                'betting_recommendation': 'INSUFFICIENT DATA'
            }
        
        row = df.iloc[0]
        
        # Calculate betting insights
        avg_runs = row['avg_estimated_runs_per_game'] or 8.5  # Default MLB average
        strike_rate = row['strike_call_rate'] or 0.5
        avg_strikeouts = row['avg_strikeouts_per_game'] or 16
        sample_size = row['total_games'] or 0
        
        # Determine confidence level
        if sample_size >= 50:
            confidence = "HIGH"
        elif sample_size >= 20:
            confidence = "MEDIUM"
        else:
            confidence = "LOW"
        
        # Generate betting insights
        betting_insights = generate_betting_recommendations(
            avg_runs, strike_rate, avg_strikeouts, sample_size
        )
        
        return {
            'umpire_name': umpire_name,
            'sample_size': int(sample_size),
            'confidence_level': confidence,
            
            # Key metrics
            'avg_runs_per_game': round(avg_runs, 2),
            'strike_call_rate': round(strike_rate, 3),
            'avg_strikeouts_per_game': round(avg_strikeouts, 1),
            'avg_pitches_per_game': round(row['avg_pitches_per_game'] or 280, 0),
            
            # Betting analysis
            'runs_vs_mlb_average': round(avg_runs - 8.5, 2),
            'over_under_lean': betting_insights['over_under'],
            'strikeout_prop_lean': betting_insights['strikeouts'],
            'game_pace': betting_insights['pace'],
            
            # Detailed recommendation
            'betting_recommendation': betting_insights['recommendation'],
            'key_insight': betting_insights['key_insight']
        }
        
    except Exception as e:
        return {
            'umpire_name': umpire_name,
            'error': f"Database error: {e}",
            'betting_recommendation': 'ERROR - CHECK LOGS'
        }

def generate_betting_recommendations(avg_runs: float, strike_rate: float, 
                                   avg_strikeouts: float, sample_size: int) -> Dict[str, str]:
    """Generate specific betting recommendations based on umpire tendencies"""
    
    recommendations = {}
    
    # Over/Under Analysis
    if avg_runs >= 9.5:
        recommendations['over_under'] = "STRONG OVER LEAN"
        ou_reason = f"Averages {avg_runs:.1f} runs/game (vs 8.5 MLB avg)"
    elif avg_runs <= 7.5:
        recommendations['over_under'] = "STRONG UNDER LEAN" 
        ou_reason = f"Averages {avg_runs:.1f} runs/game (vs 8.5 MLB avg)"
    elif avg_runs >= 9.0:
        recommendations['over_under'] = "SLIGHT OVER LEAN"
        ou_reason = f"Slightly higher scoring: {avg_runs:.1f} runs/game"
    elif avg_runs <= 8.0:
        recommendations['over_under'] = "SLIGHT UNDER LEAN"
        ou_reason = f"Slightly lower scoring: {avg_runs:.1f} runs/game"
    else:
        recommendations['over_under'] = "NEUTRAL"
        ou_reason = f"Average scoring: {avg_runs:.1f} runs/game"
    
    # Strikeout Props Analysis
    if avg_strikeouts >= 18:
        recommendations['strikeouts'] = "OVER STRIKEOUT PROPS"
    elif avg_strikeouts <= 14:
        recommendations['strikeouts'] = "UNDER STRIKEOUT PROPS"
    else:
        recommendations['strikeouts'] = "NEUTRAL ON STRIKEOUTS"
    
    # Game Pace Analysis
    if strike_rate >= 0.55:
        recommendations['pace'] = "FASTER GAME (Pitcher-friendly zone)"
    elif strike_rate <= 0.45:
        recommendations['pace'] = "SLOWER GAME (Tight zone, more counts)"
    else:
        recommendations['pace'] = "AVERAGE PACE"
    
    # Overall recommendation
    if recommendations['over_under'] in ['STRONG OVER LEAN', 'STRONG UNDER LEAN'] and sample_size >= 30:
        recommendations['recommendation'] = f"{recommendations['over_under']} - {ou_reason}"
        recommendations['key_insight'] = f"Strong edge with {sample_size} game sample"
    elif recommendations['over_under'] in ['SLIGHT OVER LEAN', 'SLIGHT UNDER LEAN']:
        recommendations['recommendation'] = f"{recommendations['over_under']} - {ou_reason}"
        recommendations['key_insight'] = f"Moderate edge with {sample_size} game sample"
    else:
        recommendations['recommendation'] = "NO STRONG BETTING EDGE"
        recommendations['key_insight'] = f"Neutral umpire impact ({sample_size} games)"
    
    return recommendations

def get_todays_umpire_betting_analysis(conn, game_date: str = None) -> List[Dict]:
    """Get betting analysis for all umpires working today"""
    
    if game_date is None:
        game_date = datetime.now().strftime('%Y-%m-%d')
    
    # Get today's umpires
    query = """
    SELECT DISTINCT umpire_name, game_pk, position
    FROM umpires 
    WHERE game_date = %s 
    AND position = 'Home Plate'
    ORDER BY game_pk
    """
    
    try:
        todays_umps = pd.read_sql(query, conn, params=[game_date])
        
        if todays_umps.empty:
            return [{'error': f'No umpire data found for {game_date}'}]
        
        results = []
        
        for _, ump in todays_umps.iterrows():
            print(f"🔍 Analyzing {ump['umpire_name']} for game {ump['game_pk']}...")
            
            analysis = analyze_umpire_betting_impact(conn, ump['umpire_name'])
            analysis['game_pk'] = ump['game_pk']
            
            results.append(analysis)
        
        return results
        
    except Exception as e:
        return [{'error': f'Database error: {e}'}]

def print_umpire_betting_report(analysis_results: List[Dict]):
    """Print a formatted betting report"""
    
    print(f"\n🎯 UMPIRE BETTING ANALYSIS REPORT")
    print("=" * 60)
    
    for result in analysis_results:
        if 'error' in result:
            print(f"❌ {result.get('umpire_name', 'Unknown')}: {result['error']}")
            continue
        
        print(f"\n👨‍⚖️ {result['umpire_name']} (Game {result.get('game_pk', 'TBD')})")
        print(f"   📊 Sample Size: {result['sample_size']} games ({result['confidence_level']} confidence)")
        print(f"   🎯 Avg Runs/Game: {result['avg_runs_per_game']} (MLB avg: 8.5)")
        print(f"   ⚾ Strike Rate: {result['strike_call_rate']:.1%}")
        print(f"   🥎 Avg K's/Game: {result['avg_strikeouts_per_game']}")
        print(f"   💰 BETTING LEAN: {result['betting_recommendation']}")
        print(f"   🔑 Key Insight: {result['key_insight']}")
        
        if result['confidence_level'] == 'HIGH' and 'STRONG' in result['betting_recommendation']:
            print("   🚨 HIGH-CONFIDENCE BETTING OPPORTUNITY!")

def main():
    """Test the enhanced umpire analysis"""
    
    # Connect to database
    dsn = os.getenv("PG_DSN")
    if not dsn:
        print("❌ PG_DSN environment variable must be set")
        return
    
    try:
        conn = psycopg2.connect(dsn)
        print("✅ Connected to database")
        
        # Test with today's date
        print("🔍 Getting today's umpire betting analysis...")
        results = get_todays_umpire_betting_analysis(conn)
        print_umpire_betting_report(results)
        
        # Test with specific umpire (if you have historical data)
        print("\n🧪 Testing specific umpire analysis...")
        specific_analysis = analyze_umpire_betting_impact(conn, "Angel Hernandez", lookback_days=365)
        print_umpire_betting_report([specific_analysis])
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()