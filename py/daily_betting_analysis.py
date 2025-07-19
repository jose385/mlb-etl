#!/usr/bin/env python3
"""
daily_betting_analysis.py - Add this to your py/ directory
Combines umpire and weather+park analysis for complete betting insights
"""

import os
import psycopg2
from datetime import datetime
from py.enhanced_umpire_betting import get_todays_umpire_betting_analysis, print_umpire_betting_report
from py.weather_park_betting import get_todays_weather_park_analysis, print_weather_park_betting_report

def get_complete_betting_analysis(conn, game_date: str = None) -> dict:
    """Get complete betting analysis combining all factors"""
    
    if game_date is None:
        game_date = datetime.now().strftime('%Y-%m-%d')
    
    print(f"📅 Getting complete betting analysis for {game_date}...")
    
    # Get umpire analysis
    umpire_results = get_todays_umpire_betting_analysis(conn, game_date)
    
    # Get weather + park analysis  
    weather_park_results = get_todays_weather_park_analysis(conn, game_date)
    
    # Combine insights by game
    combined_analysis = {}
    
    # Index umpire data by game_pk
    umpire_by_game = {}
    for ump_result in umpire_results:
        if 'game_pk' in ump_result and 'error' not in ump_result:
            umpire_by_game[ump_result['game_pk']] = ump_result
    
    # Combine with weather/park data
    for wp_result in weather_park_results:
        if 'error' in wp_result:
            continue
            
        game_pk = wp_result.get('game_pk')
        if not game_pk:
            continue
        
        # Get corresponding umpire data
        umpire_data = umpire_by_game.get(game_pk, {})
        
        # Combine the insights
        combined = {
            'game_pk': game_pk,
            'matchup': wp_result.get('matchup'),
            'ballpark': wp_result.get('ballpark'),
            
            # Weather + Park factors
            'weather_park_rec': wp_result.get('betting_recommendation'),
            'weather_park_factor': wp_result.get('weather_adjusted_factor'),
            'weather_confidence': wp_result.get('confidence'),
            'weather_insight': wp_result.get('key_insight'),
            
            # Umpire factors
            'umpire_name': umpire_data.get('umpire_name'),
            'umpire_rec': umpire_data.get('betting_recommendation'),
            'umpire_runs_avg': umpire_data.get('avg_runs_per_game'),
            'umpire_confidence': umpire_data.get('confidence_level'),
            'umpire_sample_size': umpire_data.get('sample_size'),
            
            # Combined recommendation
            'combined_recommendation': generate_combined_recommendation(wp_result, umpire_data),
            'edge_strength': calculate_edge_strength(wp_result, umpire_data)
        }
        
        combined_analysis[game_pk] = combined
    
    return {
        'date': game_date,
        'games': combined_analysis,
        'summary': generate_daily_summary(combined_analysis)
    }

def generate_combined_recommendation(weather_park_data: dict, umpire_data: dict) -> dict:
    """Generate combined betting recommendation from both factors"""
    
    wp_rec = weather_park_data.get('betting_recommendation', '')
    ump_rec = umpire_data.get('betting_recommendation', '')
    
    # Extract betting directions
    wp_over = 'OVER' in wp_rec
    wp_under = 'UNDER' in wp_rec  
    wp_strong = 'STRONG' in wp_rec
    
    ump_over = 'OVER' in ump_rec
    ump_under = 'UNDER' in ump_rec
    ump_strong = 'STRONG' in ump_rec
    
    # Combine the recommendations
    if wp_over and ump_over:
        if wp_strong or ump_strong:
            recommendation = "🔥 STRONG OVER - Weather/Park + Umpire both favor OVER"
            confidence = "VERY HIGH"
        else:
            recommendation = "⬆️ OVER LEAN - Both factors favor OVER"
            confidence = "HIGH"
    elif wp_under and ump_under:
        if wp_strong or ump_strong:
            recommendation = "🧊 STRONG UNDER - Weather/Park + Umpire both favor UNDER"  
            confidence = "VERY HIGH"
        else:
            recommendation = "⬇️ UNDER LEAN - Both factors favor UNDER"
            confidence = "HIGH"
    elif (wp_over and ump_under) or (wp_under and ump_over):
        recommendation = "⚖️ CONFLICTING SIGNALS - Weather/Park vs Umpire disagree"
        confidence = "LOW"
    elif wp_over or ump_over:
        factor = "Weather/Park" if wp_over else "Umpire"
        strength = "STRONG" if (wp_strong or ump_strong) else "SLIGHT"
        recommendation = f"📈 {strength} OVER LEAN - {factor} favors OVER"
        confidence = "MEDIUM"
    elif wp_under or ump_under:
        factor = "Weather/Park" if wp_under else "Umpire"
        strength = "STRONG" if (wp_strong or ump_strong) else "SLIGHT"
        recommendation = f"📉 {strength} UNDER LEAN - {factor} favors UNDER"
        confidence = "MEDIUM"
    else:
        recommendation = "😐 NEUTRAL - No strong directional bias"
        confidence = "LOW"
    
    return {
        'recommendation': recommendation,
        'confidence': confidence,
        'weather_park_direction': 'OVER' if wp_over else 'UNDER' if wp_under else 'NEUTRAL',
        'umpire_direction': 'OVER' if ump_over else 'UNDER' if ump_under else 'NEUTRAL'
    }

def calculate_edge_strength(weather_park_data: dict, umpire_data: dict) -> str:
    """Calculate overall edge strength for this game"""
    
    wp_confidence = weather_park_data.get('confidence', 'LOW')
    ump_confidence = umpire_data.get('confidence_level', 'LOW')
    ump_sample = umpire_data.get('sample_size', 0)
    
    wp_rec = weather_park_data.get('betting_recommendation', '')
    ump_rec = umpire_data.get('betting_recommendation', '')
    
    # Count strong indicators
    strong_indicators = 0
    if 'STRONG' in wp_rec:
        strong_indicators += 1
    if 'STRONG' in ump_rec and ump_sample >= 30:
        strong_indicators += 1
    
    # Agreement bonus
    wp_direction = 'OVER' if 'OVER' in wp_rec else 'UNDER' if 'UNDER' in wp_rec else 'NEUTRAL'
    ump_direction = 'OVER' if 'OVER' in ump_rec else 'UNDER' if 'UNDER' in ump_rec else 'NEUTRAL'
    
    agreement = wp_direction == ump_direction and wp_direction != 'NEUTRAL'
    
    if strong_indicators >= 2 and agreement:
        return "🚨 MAXIMUM EDGE"
    elif strong_indicators >= 1 and agreement:
        return "🎯 STRONG EDGE"
    elif agreement:
        return "📊 MODERATE EDGE"
    elif strong_indicators >= 1:
        return "⚡ SINGLE FACTOR EDGE"
    else:
        return "🤷 MINIMAL EDGE"

def generate_daily_summary(games_analysis: dict) -> dict:
    """Generate summary of today's best betting opportunities"""
    
    strong_edges = []
    moderate_edges = []
    total_games = len(games_analysis)
    
    for game_pk, analysis in games_analysis.items():
        edge_strength = analysis['edge_strength']
        
        if 'MAXIMUM' in edge_strength or 'STRONG' in edge_strength:
            strong_edges.append(analysis)
        elif 'MODERATE' in edge_strength:
            moderate_edges.append(analysis)
    
    return {
        'total_games': total_games,
        'strong_edges': len(strong_edges),
        'moderate_edges': len(moderate_edges),
        'best_bets': strong_edges[:3],  # Top 3 strongest edges
        'summary_text': f"Found {len(strong_edges)} strong edges and {len(moderate_edges)} moderate edges out of {total_games} games"
    }

def print_complete_betting_report(analysis: dict):
    """Print comprehensive betting report"""
    
    print(f"\n🎯 COMPLETE MLB BETTING ANALYSIS - {analysis['date']}")
    print("=" * 80)
    
    summary = analysis['summary']
    print(f"📊 {summary['summary_text']}")
    
    if summary['best_bets']:
        print(f"\n🚨 TODAY'S BEST BETTING OPPORTUNITIES:")
        print("=" * 60)
        
        for i, bet in enumerate(summary['best_bets'], 1):
            print(f"\n#{i} - {bet['matchup']} ({bet['ballpark']})")
            print(f"   {bet['edge_strength']}")
            print(f"   🎯 RECOMMENDATION: {bet['combined_recommendation']['recommendation']}")
            print(f"   📈 Weather/Park: {bet['weather_park_rec']}")
            if bet['umpire_name']:
                print(f"   👨‍⚖️ Umpire ({bet['umpire_name']}): {bet['umpire_rec']}")
            print(f"   🔍 Confidence: {bet['combined_recommendation']['confidence']}")
    
    print(f"\n📋 ALL GAMES ANALYSIS:")
    print("-" * 60)
    
    for game_pk, game in analysis['games'].items():
        print(f"\n🏟️ {game['matchup']} - {game['ballpark']}")
        print(f"   {game['edge_strength']}")
        print(f"   💰 {game['combined_recommendation']['recommendation']}")
        
        if game['umpire_name']:
            ump_confidence = f"({game['umpire_sample_size']} games)" if game['umpire_sample_size'] else ""
            print(f"   👨‍⚖️ Umpire: {game['umpire_name']} {ump_confidence}")
        
        print(f"   🌤️ Weather/Park insight: {game['weather_insight']}")

def main():
    """Run complete daily betting analysis"""
    
    # Connect to database
    dsn = os.getenv("PG_DSN")
    if not dsn:
        print("❌ PG_DSN environment variable must be set")
        return
    
    try:
        conn = psycopg2.connect(dsn)
        print("✅ Connected to database")
        
        # Get complete analysis
        analysis = get_complete_betting_analysis(conn)
        
        # Print the report
        print_complete_betting_report(analysis)
        
        # Print individual reports too
        print(f"\n" + "="*80)
        print("DETAILED UMPIRE ANALYSIS:")
        umpire_results = get_todays_umpire_betting_analysis(conn)
        print_umpire_betting_report(umpire_results)
        
        print(f"\n" + "="*80) 
        print("DETAILED WEATHER + PARK ANALYSIS:")
        weather_results = get_todays_weather_park_analysis(conn)
        print_weather_park_betting_report(weather_results)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()