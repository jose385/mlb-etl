#!/usr/bin/env python3
"""
enhanced_betting_integration.py - Master integration module
Combines matchup history and pitch tunneling with existing betting analysis
"""

import psycopg2
from datetime import datetime
from typing import Dict, List, Optional

# Import existing modules
try:
    from py.imports import setup_imports
    setup_imports()
except ImportError:
    pass
try:
    from py.daily_betting_analysis import get_complete_betting_analysis
except ImportError:
    print("⚠️ daily_betting_analysis not available")
    def get_complete_betting_analysis(*args, **kwargs):
        return {"error": "Module not available"}

try:
    from py.batter_pitcher_matchups import BatterPitcherMatchupAnalyzer, get_todays_matchup_edges
except ImportError:
    print("⚠️ batter_pitcher_matchups not available")
    class BatterPitcherMatchupAnalyzer:
        def __init__(self, conn): pass
    def get_todays_matchup_edges(*args, **kwargs):
        return [{"error": "Module not available"}]

try:
    from py.pitch_tunneling_analysis import PitchTunnelingAnalyzer, get_todays_tunneling_edges
except ImportError:
    print("⚠️ pitch_tunneling_analysis not available")
    class PitchTunnelingAnalyzer:
        def __init__(self, conn): pass
    def get_todays_tunneling_edges(*args, **kwargs):
        return [{"error": "Module not available"}]

class EnhancedBettingAnalyzer:
    """Master analyzer combining all betting insights"""
    
    def __init__(self, conn):
        self.conn = conn
        self.matchup_analyzer = BatterPitcherMatchupAnalyzer(conn)
        self.tunneling_analyzer = PitchTunnelingAnalyzer(conn)
    
    def get_complete_enhanced_analysis(self, game_date: str = None) -> Dict:
        """Get complete enhanced betting analysis for a date"""
        
        if game_date is None:
            game_date = datetime.now().strftime('%Y-%m-%d')
        
        print(f"🎯 Running Complete Enhanced Analysis for {game_date}")
        print("=" * 80)
        
        # 1. Get base analysis (weather, umpire, fatigue)
        print("📊 Getting base analysis...")
        base_analysis = get_complete_betting_analysis(self.conn, game_date)
        
        # 2. Get matchup edges
        print("⚾ Getting matchup analysis...")
        matchup_edges = get_todays_matchup_edges(self.conn, game_date, min_edge_strength=30)
        
        # 3. Get tunneling edges  
        print("🌪️ Getting tunneling analysis...")
        tunneling_edges = get_todays_tunneling_edges(self.conn, game_date, min_tunnel_quality=50)
        
        # 4. Get detailed game-by-game analysis
        print("🎮 Getting game-by-game integration...")
        game_integrations = self.integrate_game_analysis(base_analysis, matchup_edges, tunneling_edges)
        
        # 5. Generate master recommendations
        print("💎 Generating master recommendations...")
        master_recommendations = self.generate_master_recommendations(
            base_analysis, matchup_edges, tunneling_edges, game_integrations
        )
        
        return {
            "analysis_date": game_date,
            "base_analysis": base_analysis,
            "matchup_edges": matchup_edges,
            "tunneling_edges": tunneling_edges,
            "game_integrations": game_integrations,
            "master_recommendations": master_recommendations,
            "analysis_timestamp": datetime.now().isoformat()
        }
    
    def integrate_game_analysis(self, base_analysis: Dict, matchup_edges: List[Dict], 
                               tunneling_edges: List[Dict]) -> List[Dict]:
        """Integrate all analysis types for each game"""
        
        game_integrations = []
        
        # Get base games from analysis
        base_games = base_analysis.get('games', {})
        
        for game_pk, base_game in base_games.items():
            
            # Find relevant matchup edges for this game
            game_matchups = [m for m in matchup_edges if m.get('game_pk') == game_pk]
            
            # Find relevant tunneling edges for this game
            game_tunneling = [t for t in tunneling_edges if t.get('game_pk') == game_pk]
            
            # Integrate all factors
            integrated_game = self.integrate_single_game(
                game_pk, base_game, game_matchups, game_tunneling
            )
            
            game_integrations.append(integrated_game)
        
        return sorted(game_integrations, key=lambda x: x.get('total_edge_score', 0), reverse=True)
    
    def integrate_single_game(self, game_pk: int, base_game: Dict, 
                             game_matchups: List[Dict], game_tunneling: List[Dict]) -> Dict:
        """Integrate all analysis for a single game"""
        
        integration = {
            "game_pk": game_pk,
            "matchup": base_game.get('matchup', 'Unknown'),
            "ballpark": base_game.get('ballpark', 'Unknown'),
            
            # Base analysis
            "base_recommendation": base_game.get('combined_recommendation', {}).get('recommendation', ''),
            "weather_park_factor": base_game.get('weather_park_factor', 1.0),
            "umpire_name": base_game.get('umpire_name', ''),
            "umpire_recommendation": base_game.get('umpire_rec', ''),
            
            # Enhancement factors
            "matchup_enhancements": [],
            "tunneling_enhancements": [],
            
            # Integrated scores
            "total_edge_score": 0,
            "confidence_multiplier": 1.0,
            "final_recommendation": "",
            "betting_focus": []
        }
        
        # Process matchup enhancements
        if game_matchups:
            strong_matchups = [m for m in game_matchups if m.get('edge_strength', 0) >= 60]
            moderate_matchups = [m for m in game_matchups if 40 <= m.get('edge_strength', 0) < 60]
            
            integration["matchup_enhancements"] = {
                "strong_edges": len(strong_matchups),
                "moderate_edges": len(moderate_matchups),
                "top_matchups": strong_matchups[:3],  # Top 3
                "matchup_summary": self.summarize_matchups(strong_matchups + moderate_matchups)
            }
            
            # Add to edge score
            integration["total_edge_score"] += len(strong_matchups) * 15 + len(moderate_matchups) * 8
        
        # Process tunneling enhancements
        if game_tunneling:
            elite_tunnelers = [t for t in game_tunneling if t.get('avg_tunnel_quality', 0) >= 75]
            good_tunnelers = [t for t in game_tunneling if 60 <= t.get('avg_tunnel_quality', 0) < 75]
            
            integration["tunneling_enhancements"] = {
                "elite_tunnelers": len(elite_tunnelers),
                "good_tunnelers": len(good_tunnelers),
                "top_tunnelers": elite_tunnelers + good_tunnelers,
                "tunneling_summary": self.summarize_tunneling(elite_tunnelers + good_tunnelers)
            }
            
            # Add to edge score  
            integration["total_edge_score"] += len(elite_tunnelers) * 20 + len(good_tunnelers) * 10
        
        # Base analysis contribution
        base_edge = self.calculate_base_edge_score(base_game)
        integration["total_edge_score"] += base_edge
        
        # Generate final integrated recommendation
        integration["final_recommendation"] = self.generate_integrated_recommendation(integration)
        integration["betting_focus"] = self.identify_betting_focus(integration)
        
        return integration
    
    def summarize_matchups(self, matchups: List[Dict]) -> str:
        """Summarize key matchup insights"""
        
        if not matchups:
            return "No significant matchup edges"
        
        summaries = []
        
        # Count recommendation types
        strong_bets = [m for m in matchups if 'STRONG BET' in m.get('betting_recommendation', '')]
        moderate_bets = [m for m in matchups if 'MODERATE BET' in m.get('betting_recommendation', '')]
        
        if strong_bets:
            summaries.append(f"{len(strong_bets)} strong matchup bets")
        
        if moderate_bets:
            summaries.append(f"{len(moderate_bets)} moderate matchup edges")
        
        # Highlight best matchup
        best_matchup = max(matchups, key=lambda x: x.get('edge_strength', 0))
        if best_matchup:
            summaries.append(f"Best: {best_matchup.get('batter_name', '')} vs pitcher ({best_matchup.get('edge_strength', 0)}/100)")
        
        return "; ".join(summaries)
    
    def summarize_tunneling(self, tunnelers: List[Dict]) -> str:
        """Summarize key tunneling insights"""
        
        if not tunnelers:
            return "No significant tunneling edges"
        
        summaries = []
        
        # Count by quality
        elite_count = len([t for t in tunnelers if t.get('avg_tunnel_quality', 0) >= 80])
        if elite_count:
            summaries.append(f"{elite_count} elite tunneling pitchers")
        
        # Highlight best tunneler
        best_tunneler = max(tunnelers, key=lambda x: x.get('avg_tunnel_quality', 0))
        if best_tunneler:
            quality = best_tunneler.get('avg_tunnel_quality', 0)
            impact = best_tunneler.get('strikeout_prop_impact', 1.0)
            summaries.append(f"Best: {best_tunneler.get('pitcher_name', '')} ({quality}/100, {impact:.2f}x K boost)")
        
        return "; ".join(summaries)
    
    def calculate_base_edge_score(self, base_game: Dict) -> float:
        """Calculate edge score from base analysis"""
        
        score = 0
        
        # Weather/Park edge
        weather_rec = base_game.get('weather_park_rec', '')
        if 'STRONG' in weather_rec:
            score += 25
        elif 'LEAN' in weather_rec:
            score += 10
        
        # Umpire edge
        ump_rec = base_game.get('umpire_rec', '')
        if 'STRONG' in ump_rec:
            score += 20
        elif 'LEAN' in ump_rec:
            score += 8
        
        # Confidence boost
        confidence = base_game.get('combined_recommendation', {}).get('confidence', 'LOW')
        if confidence == 'VERY HIGH':
            score += 15
        elif confidence == 'HIGH':
            score += 10
        elif confidence == 'MEDIUM':
            score += 5
        
        return score
    
    def generate_integrated_recommendation(self, integration: Dict) -> str:
        """Generate final integrated betting recommendation"""
        
        total_score = integration["total_edge_score"]
        base_rec = integration["base_recommendation"]
        
        # Count enhancement factors
        strong_matchups = integration.get("matchup_enhancements", {}).get("strong_edges", 0)
        elite_tunnelers = integration.get("tunneling_enhancements", {}).get("elite_tunnelers", 0)
        
        # Determine recommendation strength
        if total_score >= 80:
            strength = "🚨 MAXIMUM CONFIDENCE"
        elif total_score >= 60:
            strength = "⭐ HIGH CONFIDENCE"  
        elif total_score >= 40:
            strength = "📈 MODERATE CONFIDENCE"
        elif total_score >= 20:
            strength = "💡 SLIGHT EDGE"
        else:
            strength = "😐 MINIMAL EDGE"
        
        # Identify primary betting direction
        over_signals = 0
        under_signals = 0
        
        if 'OVER' in base_rec:
            over_signals += 1
        elif 'UNDER' in base_rec:
            under_signals += 1
        
        if strong_matchups > 0:
            over_signals += 1  # Usually favor offensive props
        
        if elite_tunnelers > 0:
            under_signals += 1  # Usually favor strikeout props
        
        # Generate direction
        if over_signals > under_signals:
            direction = "FAVOR OVER / OFFENSIVE PROPS"
        elif under_signals > over_signals:
            direction = "FAVOR UNDER / PITCHING PROPS"
        else:
            direction = "MIXED SIGNALS - SELECT SPECIFIC PROPS"
        
        return f"{strength} - {direction}"
    
    def identify_betting_focus(self, integration: Dict) -> List[str]:
        """Identify specific betting focuses for this game"""
        
        focus_areas = []
        
        # Base analysis focus
        base_rec = integration["base_recommendation"]
        if 'OVER' in base_rec:
            focus_areas.append("Game totals OVER")
        elif 'UNDER' in base_rec:
            focus_areas.append("Game totals UNDER")
        
        # Matchup focus
        matchup_enhancements = integration.get("matchup_enhancements", {})
        strong_matchups = matchup_enhancements.get("strong_edges", 0)
        if strong_matchups > 0:
            focus_areas.append("Player prop opportunities")
            focus_areas.append("Specific batter props")
        
        # Tunneling focus
        tunneling_enhancements = integration.get("tunneling_enhancements", {})
        elite_tunnelers = tunneling_enhancements.get("elite_tunnelers", 0)
        if elite_tunnelers > 0:
            focus_areas.append("Pitcher strikeout props")
            focus_areas.append("Opposing team under props")
        
        # Umpire focus
        if integration.get("umpire_name"):
            focus_areas.append("Umpire-influenced props")
        
        return focus_areas[:4]  # Limit to top 4 focus areas
    
    def generate_master_recommendations(self, base_analysis: Dict, matchup_edges: List[Dict],
                                      tunneling_edges: List[Dict], game_integrations: List[Dict]) -> Dict:
        """Generate master betting recommendations across all games"""
        
        # Top opportunities
        top_games = sorted(game_integrations, key=lambda x: x.get('total_edge_score', 0), reverse=True)[:5]
        
        # Category breakdown
        total_opportunities = {
            "maximum_confidence": len([g for g in game_integrations if g.get('total_edge_score', 0) >= 80]),
            "high_confidence": len([g for g in game_integrations if 60 <= g.get('total_edge_score', 0) < 80]),
            "moderate_confidence": len([g for g in game_integrations if 40 <= g.get('total_edge_score', 0) < 60])
        }
        
        # Best specific opportunities
        best_matchups = sorted([m for m in matchup_edges if 'error' not in m and 'message' not in m], 
                              key=lambda x: x.get('edge_strength', 0), reverse=True)[:5]
        
        best_tunneling = sorted([t for t in tunneling_edges if 'error' not in t and 'message' not in t],
                               key=lambda x: x.get('avg_tunnel_quality', 0), reverse=True)[:3]
        
        # Daily strategy
        daily_strategy = self.formulate_daily_strategy(total_opportunities, top_games)
        
        return {
            "total_games_analyzed": len(game_integrations),
            "total_opportunities": total_opportunities,
            "top_games": top_games,
            "best_matchup_opportunities": best_matchups,
            "best_tunneling_opportunities": best_tunneling,
            "daily_strategy": daily_strategy,
            "key_insights": self.extract_key_insights(game_integrations, matchup_edges, tunneling_edges)
        }
    
    def formulate_daily_strategy(self, opportunities: Dict, top_games: List[Dict]) -> str:
        """Formulate overall daily betting strategy"""
        
        max_conf = opportunities["maximum_confidence"]
        high_conf = opportunities["high_confidence"] 
        
        if max_conf >= 2:
            return f"AGGRESSIVE DAY - {max_conf} maximum confidence plays available. Focus on top-rated games with multiple edge confirmations."
        elif max_conf == 1:
            return "SELECTIVE DAY - One maximum confidence play. Focus resources on best opportunity and be selective elsewhere."
        elif high_conf >= 3:
            return f"SOLID DAY - {high_conf} high confidence opportunities. Diversify across multiple good spots."
        elif high_conf >= 1:
            return "CAUTIOUS DAY - Limited high-confidence opportunities. Consider smaller positions and wait for better spots."
        else:
            return "MINIMAL DAY - Very few strong edges detected. Consider skipping or very light action."
    
    def extract_key_insights(self, game_integrations: List[Dict], matchup_edges: List[Dict],
                            tunneling_edges: List[Dict]) -> List[str]:
        """Extract key insights across all analysis"""
        
        insights = []
        
        # Game-level insights
        max_conf_games = [g for g in game_integrations if g.get('total_edge_score', 0) >= 80]
        if max_conf_games:
            insights.append(f"🚨 {len(max_conf_games)} maximum confidence games with multiple edge confirmations")
        
        # Matchup insights
        strong_matchup_edges = [m for m in matchup_edges if 'error' not in m and m.get('edge_strength', 0) >= 70]
        if strong_matchup_edges:
            insights.append(f"⚾ {len(strong_matchup_edges)} strong historical matchup edges detected")
        
        # Tunneling insights
        elite_tunneling_edges = [t for t in tunneling_edges if 'error' not in t and t.get('avg_tunnel_quality', 0) >= 80]
        if elite_tunneling_edges:
            insights.append(f"🌪️ {len(elite_tunneling_edges)} elite pitch tunneling opportunities for strikeout props")
        
        # Combined insights
        multi_factor_games = [g for g in game_integrations 
                             if g.get("matchup_enhancements", {}).get("strong_edges", 0) > 0 
                             and g.get("tunneling_enhancements", {}).get("elite_tunnelers", 0) > 0]
        if multi_factor_games:
            insights.append(f"💎 {len(multi_factor_games)} games with BOTH strong matchups AND elite tunneling")
        
        return insights

def print_enhanced_betting_report(analysis: Dict):
    """Print comprehensive enhanced betting report"""
    
    print(f"\n💎 ENHANCED MLB BETTING ANALYSIS REPORT")
    print("=" * 80)
    
    master_recs = analysis["master_recommendations"]
    
    # Daily Overview
    print(f"\n📊 DAILY OVERVIEW - {analysis['analysis_date']}")
    print(f"🎯 Total Games: {master_recs['total_games_analyzed']}")
    print(f"🚨 Maximum Confidence: {master_recs['total_opportunities']['maximum_confidence']}")
    print(f"⭐ High Confidence: {master_recs['total_opportunities']['high_confidence']}")
    print(f"📈 Moderate Confidence: {master_recs['total_opportunities']['moderate_confidence']}")
    
    # Daily Strategy
    print(f"\n🎲 DAILY STRATEGY:")
    print(f"   {master_recs['daily_strategy']}")
    
    # Key Insights
    if master_recs['key_insights']:
        print(f"\n🔍 KEY INSIGHTS:")
        for insight in master_recs['key_insights']:
            print(f"   • {insight}")
    
    # Top Games
    print(f"\n🏆 TOP BETTING OPPORTUNITIES:")
    print("-" * 60)
    
    for i, game in enumerate(master_recs['top_games'][:3], 1):
        print(f"\n#{i} - {game['matchup']} ({game['ballpark']})")
        print(f"   🎯 Edge Score: {game['total_edge_score']}/100")
        print(f"   💰 RECOMMENDATION: {game['final_recommendation']}")
        print(f"   🎮 Focus Areas: {', '.join(game['betting_focus'])}")
        
        # Show enhancements
        matchup_strong = game.get("matchup_enhancements", {}).get("strong_edges", 0)
        tunneling_elite = game.get("tunneling_enhancements", {}).get("elite_tunnelers", 0)
        
        enhancements = []
        if matchup_strong:
            enhancements.append(f"{matchup_strong} strong matchups")
        if tunneling_elite:
            enhancements.append(f"{tunneling_elite} elite tunnelers")
        
        if enhancements:
            print(f"   ⚡ Enhancements: {', '.join(enhancements)}")
    
    # Best Specific Opportunities  
    print(f"\n🎯 BEST SPECIFIC OPPORTUNITIES:")
    print("-" * 60)
    
    # Top matchup
    best_matchups = master_recs.get('best_matchup_opportunities', [])
    if best_matchups:
        top_matchup = best_matchups[0]
        print(f"\n⚾ Best Matchup: {top_matchup.get('batter_name')} vs {top_matchup.get('pitcher_name')}")
        print(f"   Edge: {top_matchup.get('edge_strength', 0)}/100 | {top_matchup.get('betting_recommendation', '')}")
    
    # Top tunneling
    best_tunneling = master_recs.get('best_tunneling_opportunities', [])
    if best_tunneling:
        top_tunneling = best_tunneling[0]
        print(f"\n🌪️ Best Tunneling: {top_tunneling.get('pitcher_name')}")
        print(f"   Quality: {top_tunneling.get('avg_tunnel_quality', 0)}/100 | Impact: {top_tunneling.get('strikeout_prop_impact', 1.0):.2f}x")

def main():
    """Run enhanced betting analysis"""
    
    import os
    dsn = os.getenv("PG_DSN") 
    if not dsn:
        print("❌ PG_DSN environment variable must be set")
        return
    
    try:
        conn = psycopg2.connect(dsn)
        
        print("💎 Enhanced MLB Betting Analysis")
        print("=" * 50)
        
        # Initialize enhanced analyzer
        analyzer = EnhancedBettingAnalyzer(conn)
        
        # Run complete analysis
        analysis = analyzer.get_complete_enhanced_analysis()
        
        # Print comprehensive report
        print_enhanced_betting_report(analysis)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()