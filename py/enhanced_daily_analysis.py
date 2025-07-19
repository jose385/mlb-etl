#!/usr/bin/env python3
"""
enhanced_daily_analysis.py - Master analysis combining all modules
"""

import psycopg2
from datetime import datetime
from daily_betting_analysis import get_complete_betting_analysis  # Your existing
from player_performance_trends import get_todays_player_trends
from bullpen_usage_tracking import get_todays_bullpen_analysis  
from advanced_statcast_metrics import AdvancedStatcastAnalyzer
from team_level_analytics import TeamAnalytics

def run_complete_enhanced_analysis(conn, game_date=None):
    """Run all analysis modules and combine insights"""
    
    if game_date is None:
        game_date = datetime.now().strftime('%Y-%m-%d')
    
    print(f"🎯 ENHANCED MLB ANALYSIS - {game_date}")
    print("=" * 80)
    
    # 1. Your existing comprehensive analysis
    print("🔄 Running base analysis (weather, umpire, fatigue)...")
    base_analysis = get_complete_betting_analysis(conn, game_date)
    
    # 2. Player performance trends
    print("🔥 Analyzing player trends...")
    player_trends = get_todays_player_trends(conn, game_date)
    
    # 3. Bullpen analysis
    print("🎯 Analyzing bullpen usage...")
    bullpen_analysis = get_todays_bullpen_analysis(conn, game_date)
    
    # 4. Advanced Statcast metrics for key players
    print("📊 Running advanced Statcast analysis...")
    statcast_analyzer = AdvancedStatcastAnalyzer(conn)
    
    # 5. Team-level analytics
    print("🏟️ Analyzing team matchups...")
    team_analyzer = TeamAnalytics(conn)
    
    # Combine all insights
    enhanced_analysis = {
        "analysis_date": game_date,
        "base_analysis": base_analysis,
        "player_trends": player_trends,
        "bullpen_analysis": bullpen_analysis,
        "enhanced_insights": [],
        "top_betting_opportunities": []
    }
    
    # Process each game and combine insights
    games = base_analysis.get('games', {})
    
    for game_pk, game_data in games.items():
        print(f"\n🎮 Enhanced analysis for game {game_pk}...")
        
        # Get team IDs (you'll need to extract from your data)
        home_team_id = extract_team_id(game_data.get('matchup', ''), 'home')
        away_team_id = extract_team_id(game_data.get('matchup', ''), 'away')
        
        if home_team_id and away_team_id:
            # Team matchup analysis
            team_matchup = team_analyzer.get_comprehensive_team_matchup(
                home_team_id, away_team_id, game_date
            )
            
            # Find corresponding bullpen analysis
            game_bullpen = next((b for b in bullpen_analysis 
                               if b.get('game_pk') == game_pk), {})
            
            # Combine insights
            combined_insight = combine_game_insights(
                game_data, team_matchup, game_bullpen, player_trends
            )
            
            enhanced_analysis["enhanced_insights"].append(combined_insight)
    
    # Generate top opportunities
    enhanced_analysis["top_betting_opportunities"] = rank_betting_opportunities(
        enhanced_analysis["enhanced_insights"]
    )
    
    # Print results
    print_enhanced_analysis_report(enhanced_analysis)
    
    return enhanced_analysis

def extract_team_id(matchup_str, team_type):
    """Extract team ID from matchup string - implement based on your data format"""
    # This depends on how your matchup data is structured
    # Example implementation:
    if '@' in matchup_str:
        teams = matchup_str.split(' @ ')
        if team_type == 'away' and len(teams) > 0:
            return get_team_id_by_name(teams[0].strip())
        elif team_type == 'home' and len(teams) > 1:
            return get_team_id_by_name(teams[1].strip())
    return None

def get_team_id_by_name(team_name):
    """Convert team name to team ID - implement based on your roster table"""
    # Query your roster table to map team names to IDs
    return None  # Placeholder

def combine_game_insights(base_game, team_matchup, bullpen_data, player_trends):
    """Combine insights from all analysis modules for a single game"""
    
    combined = {
        "game_pk": base_game.get('game_pk'),
        "matchup": base_game.get('matchup'),
        "base_recommendation": base_game.get('combined_recommendation', {}).get('recommendation'),
        "base_confidence": base_game.get('combined_recommendation', {}).get('confidence'),
        "enhancement_factors": [],
        "final_recommendation": "",
        "confidence_boost": 0,
        "edge_strength": base_game.get('edge_strength', '🤷 MINIMAL EDGE')
    }
    
    # Team matchup enhancements
    if 'error' not in team_matchup:
        matchup_advantages = team_matchup.get('matchup_advantages', [])
        if matchup_advantages:
            combined["enhancement_factors"].extend([
                f"TEAM EDGE: {adv}" for adv in matchup_advantages[:2]
            ])
            combined["confidence_boost"] += 10
    
    # Bullpen enhancements
    if 'error' not in bullpen_data:
        bullpen_rec = bullpen_data.get('betting_recommendation', '')
        if 'BET' in bullpen_rec:
            combined["enhancement_factors"].append(f"BULLPEN: {bullpen_rec}")
            combined["confidence_boost"] += 15
    
    # Player trend enhancements
    hot_players = [p for p in player_trends if 'error' not in p and 'RED HOT' in p.get('trend', '')]
    if hot_players:
        combined["enhancement_factors"].append(
            f"HOT PLAYERS: {len(hot_players)} players in red hot form"
        )
        combined["confidence_boost"] += 5 * len(hot_players)
    
    # Determine final recommendation
    combined["final_recommendation"] = determine_final_recommendation(combined)
    
    return combined

def determine_final_recommendation(combined_insight):
    """Determine final betting recommendation based on all factors"""
    
    base_rec = combined_insight["base_recommendation"]
    confidence_boost = combined_insight["confidence_boost"]
    enhancement_factors = combined_insight["enhancement_factors"]
    
    if confidence_boost >= 25 and len(enhancement_factors) >= 2:
        if "OVER" in base_rec:
            return "🚨 STRONG OVER - Multiple edges align"
        elif "UNDER" in base_rec:
            return "🚨 STRONG UNDER - Multiple edges align"
        else:
            return "🎯 HIGH CONFIDENCE PLAY - Enhanced edge detected"
    elif confidence_boost >= 15:
        return f"📈 ENHANCED: {base_rec}"
    else:
        return base_rec

def rank_betting_opportunities(enhanced_insights):
    """Rank betting opportunities by edge strength"""
    
    def opportunity_score(insight):
        base_score = 0
        
        # Base recommendation scoring
        if "STRONG" in insight["final_recommendation"]:
            base_score += 50
        elif "ENHANCED" in insight["final_recommendation"]:
            base_score += 30
        elif "HIGH CONFIDENCE" in insight["final_recommendation"]:
            base_score += 40
        
        # Enhancement factor bonus
        base_score += len(insight["enhancement_factors"]) * 5
        base_score += insight["confidence_boost"]
        
        return base_score
    
    ranked = sorted(enhanced_insights, key=opportunity_score, reverse=True)
    return ranked[:5]  # Top 5 opportunities

def print_enhanced_analysis_report(analysis):
    """Print comprehensive enhanced analysis report"""
    
    print(f"\n🎯 ENHANCED ANALYSIS SUMMARY")
    print("=" * 80)
    
    top_opportunities = analysis["top_betting_opportunities"]
    
    if top_opportunities:
        print(f"\n🚨 TOP BETTING OPPORTUNITIES:")
        print("-" * 60)
        
        for i, opp in enumerate(top_opportunities, 1):
            print(f"\n#{i} - {opp['matchup']}")
            print(f"   🎯 RECOMMENDATION: {opp['final_recommendation']}")
            print(f"   📊 Base analysis: {opp['base_recommendation']}")
            
            if opp['enhancement_factors']:
                print(f"   🚀 ENHANCEMENT FACTORS:")
                for factor in opp['enhancement_factors']:
                    print(f"      • {factor}")
            
            print(f"   📈 Edge strength: {opp['edge_strength']}")
    
    # Summary statistics
    total_games = len(analysis["enhanced_insights"])
    strong_edges = len([o for o in top_opportunities if "STRONG" in o["final_recommendation"]])
    enhanced_edges = len([o for o in top_opportunities if "ENHANCED" in o["final_recommendation"]])
    
    print(f"\n📊 ANALYSIS SUMMARY:")
    print(f"   🎮 Total games analyzed: {total_games}")
    print(f"   🚨 Strong edges detected: {strong_edges}")
    print(f"   📈 Enhanced opportunities: {enhanced_edges}")
    print(f"   🎯 Recommendation: Focus on top {min(3, len(top_opportunities))} opportunities")

if __name__ == "__main__":
    import os
    
    dsn = os.getenv("PG_DSN")
    if not dsn:
        print("❌ PG_DSN environment variable must be set")
        exit(1)
    
    try:
        conn = psycopg2.connect(dsn)
        run_complete_enhanced_analysis(conn)
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()