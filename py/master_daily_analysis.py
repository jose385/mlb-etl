#!/usr/bin/env python3
"""
master_daily_analysis.py - Your new master analysis script
This replaces/supplements your daily_betting_analysis.py with all the new features

Usage: python py/master_daily_analysis.py [--date YYYY-MM-DD] [--update-data]
"""

import os
import sys
import psycopg2
import argparse
from datetime import datetime
from typing import Dict, List

# Import all your existing and new modules
try:
    from .daily_betting_analysis import get_complete_betting_analysis
except ImportError:
    print("⚠️ daily_betting_analysis not available")
    def get_complete_betting_analysis(*args, **kwargs):
        return {"error": "Module not available"}

try:
    from .batter_pitcher_matchups import get_todays_matchup_edges, update_all_matchups
except ImportError:
    print("⚠️ batter_pitcher_matchups not available")
    def get_todays_matchup_edges(*args, **kwargs):
        return [{"error": "Module not available"}]
    def update_all_matchups(*args, **kwargs):
        print("⚠️ update_all_matchups not available")

try:
    from .pitch_tunneling_analysis import get_todays_tunneling_edges, update_pitcher_tunneling_data
except ImportError:
    print("⚠️ pitch_tunneling_analysis not available")
    def get_todays_tunneling_edges(*args, **kwargs):
        return [{"error": "Module not available"}]
    def update_pitcher_tunneling_data(*args, **kwargs):
        print("⚠️ update_pitcher_tunneling_data not available")

try:
    from .enhanced_betting_integration import EnhancedBettingAnalyzer, print_enhanced_betting_report
except ImportError:
    print("⚠️ enhanced_betting_integration not available")
    class EnhancedBettingAnalyzer:
        def __init__(self, conn): pass
        def get_complete_enhanced_analysis(self, *args, **kwargs):
            return {"error": "Module not available"}
    def print_enhanced_betting_report(*args, **kwargs):
        print("⚠️ print_enhanced_betting_report not available")

try:
    from .backfill_matchup_tunneling import update_matchup_and_tunneling_data
except ImportError:
    print("⚠️ backfill_matchup_tunneling not available")
    def update_matchup_and_tunneling_data(*args, **kwargs):
        print("⚠️ update_matchup_and_tunneling_data not available")

class MasterDailyAnalyzer:
    """Master daily analysis combining all betting insights"""
    
    def __init__(self, conn):
        self.conn = conn
        self.enhanced_analyzer = EnhancedBettingAnalyzer(conn)
    
    def run_complete_daily_analysis(self, game_date: str = None, update_data: bool = False) -> Dict:
        """Run complete daily analysis with all features"""
        
        if game_date is None:
            game_date = datetime.now().strftime('%Y-%m-%d')
        
        print(f"🚀 MASTER MLB DAILY ANALYSIS - {game_date}")
        print("=" * 80)
        
        # Step 1: Update advanced analytics data if requested
        if update_data:
            print("🔄 Updating advanced analytics data...")
            try:
                update_matchup_and_tunneling_data(self.conn, game_date)
                print("✅ Advanced analytics data updated")
            except Exception as e:
                print(f"⚠️ Warning: Could not update advanced data: {e}")
                print("Continuing with existing data...")
        
        # Step 2: Run comprehensive enhanced analysis
        print("\n💎 Running enhanced betting analysis...")
        enhanced_analysis = self.enhanced_analyzer.get_complete_enhanced_analysis(game_date)
        
        # Step 3: Generate summary insights
        print("\n📋 Generating summary insights...")
        summary_insights = self.generate_summary_insights(enhanced_analysis)
        
        # Step 4: Create action plan
        print("\n🎯 Creating daily action plan...")
        action_plan = self.create_daily_action_plan(enhanced_analysis)
        
        # Combine everything
        master_analysis = {
            "analysis_date": game_date,
            "enhanced_analysis": enhanced_analysis,
            "summary_insights": summary_insights,
            "action_plan": action_plan,
            "generated_at": datetime.now().isoformat()
        }
        
        return master_analysis
    
    def generate_summary_insights(self, enhanced_analysis: Dict) -> Dict:
        """Generate high-level summary insights"""
        
        master_recs = enhanced_analysis.get("master_recommendations", {})
        matchup_edges = enhanced_analysis.get("matchup_edges", [])
        tunneling_edges = enhanced_analysis.get("tunneling_edges", [])
        
        # Count opportunities by confidence level
        opportunities = master_recs.get("total_opportunities", {})
        max_conf = opportunities.get("maximum_confidence", 0)
        high_conf = opportunities.get("high_confidence", 0)
        
        # Count specific edges
        strong_matchups = len([m for m in matchup_edges 
                              if isinstance(m, dict) and m.get('edge_strength', 0) >= 60])
        elite_tunnelers = len([t for t in tunneling_edges 
                              if isinstance(t, dict) and t.get('avg_tunnel_quality', 0) >= 80])
        
        # Overall day rating
        total_strong_opportunities = max_conf + high_conf + strong_matchups + elite_tunnelers
        
        if total_strong_opportunities >= 8:
            day_rating = "🔥 EXCELLENT DAY"
        elif total_strong_opportunities >= 5:
            day_rating = "⭐ GOOD DAY"
        elif total_strong_opportunities >= 3:
            day_rating = "📊 AVERAGE DAY"
        elif total_strong_opportunities >= 1:
            day_rating = "😐 QUIET DAY"
        else:
            day_rating = "💤 SLOW DAY"
        
        return {
            "day_rating": day_rating,
            "total_strong_opportunities": total_strong_opportunities,
            "maximum_confidence_games": max_conf,
            "high_confidence_games": high_conf,
            "strong_matchup_edges": strong_matchups,
            "elite_tunneling_opportunities": elite_tunnelers,
            "recommended_approach": self.determine_recommended_approach(total_strong_opportunities, max_conf)
        }
    
    def determine_recommended_approach(self, total_opportunities: int, max_conf: int) -> str:
        """Determine recommended betting approach for the day"""
        
        if max_conf >= 2:
            return "AGGRESSIVE - Focus on maximum confidence plays with larger positions"
        elif max_conf == 1 and total_opportunities >= 4:
            return "BALANCED - One big play plus several smaller opportunities"
        elif total_opportunities >= 5:
            return "DIVERSIFIED - Spread action across multiple good opportunities"
        elif total_opportunities >= 2:
            return "SELECTIVE - Choose 1-2 best spots and be patient"
        else:
            return "CAUTIOUS - Very limited action or consider sitting out"
    
    def create_daily_action_plan(self, enhanced_analysis: Dict) -> Dict:
        """Create specific daily action plan"""
        
        master_recs = enhanced_analysis.get("master_recommendations", {})
        top_games = master_recs.get("top_games", [])
        best_matchups = master_recs.get("best_matchup_opportunities", [])
        best_tunneling = master_recs.get("best_tunneling_opportunities", [])
        
        action_plan = {
            "primary_targets": [],
            "secondary_opportunities": [],
            "specific_bets": [],
            "avoid_list": [],
            "bankroll_allocation": {}
        }
        
        # Primary targets (top games)
        for i, game in enumerate(top_games[:2]):  # Top 2 games
            priority = "PRIORITY 1" if i == 0 else "PRIORITY 2"
            action_plan["primary_targets"].append({
                "priority": priority,
                "game": game["matchup"],
                "edge_score": game.get("total_edge_score", 0),
                "recommendation": game.get("final_recommendation", ""),
                "focus_areas": game.get("betting_focus", [])
            })
        
        # Secondary opportunities (games 3-5)
        for game in top_games[2:5]:
            action_plan["secondary_opportunities"].append({
                "game": game["matchup"],
                "edge_score": game.get("total_edge_score", 0),
                "recommendation": game.get("final_recommendation", "")
            })
        
        # Specific betting opportunities
        # Top matchups
        for matchup in best_matchups[:3]:
            if isinstance(matchup, dict) and matchup.get('edge_strength', 0) >= 50:
                action_plan["specific_bets"].append({
                    "type": "MATCHUP PROP",
                    "bet": f"{matchup.get('batter_name', '')} vs {matchup.get('pitcher_name', '')}",
                    "recommendation": matchup.get('betting_recommendation', ''),
                    "edge_strength": matchup.get('edge_strength', 0)
                })
        
        # Top tunneling
        for tunneling in best_tunneling[:2]:
            if isinstance(tunneling, dict) and tunneling.get('avg_tunnel_quality', 0) >= 70:
                action_plan["specific_bets"].append({
                    "type": "STRIKEOUT PROP",
                    "bet": f"{tunneling.get('pitcher_name', '')} strikeouts",
                    "recommendation": f"OVER (Tunneling Quality: {tunneling.get('avg_tunnel_quality', 0)}/100)",
                    "impact_multiplier": tunneling.get('strikeout_prop_impact', 1.0)
                })
        
        # Bankroll allocation recommendations
        summary = enhanced_analysis.get("summary_insights", {})
        max_conf_games = summary.get("maximum_confidence_games", 0)
        
        if max_conf_games >= 2:
            action_plan["bankroll_allocation"] = {
                "primary_targets": "40-50% of daily bankroll",
                "secondary_opportunities": "30-40% of daily bankroll", 
                "specific_props": "10-20% of daily bankroll",
                "reserve": "Keep 10-20% in reserve"
            }
        elif max_conf_games == 1:
            action_plan["bankroll_allocation"] = {
                "primary_targets": "30-40% of daily bankroll",
                "secondary_opportunities": "40-50% of daily bankroll",
                "specific_props": "10-20% of daily bankroll", 
                "reserve": "Keep 20% in reserve"
            }
        else:
            action_plan["bankroll_allocation"] = {
                "primary_targets": "20-30% of daily bankroll",
                "secondary_opportunities": "30-40% of daily bankroll",
                "specific_props": "20-30% of daily bankroll",
                "reserve": "Keep 30% in reserve"
            }
        
        return action_plan

def print_master_daily_report(analysis: Dict):
    """Print comprehensive master daily report"""
    
    print(f"\n🎯 MASTER DAILY BETTING ANALYSIS REPORT")
    print("=" * 80)
    
    summary = analysis.get("summary_insights", {})
    action_plan = analysis.get("action_plan", {})
    
    # Daily Overview
    print(f"\n📊 DAILY OVERVIEW - {analysis['analysis_date']}")
    print(f"🏆 Day Rating: {summary.get('day_rating', 'UNKNOWN')}")
    print(f"🎯 Strong Opportunities: {summary.get('total_strong_opportunities', 0)}")
    print(f"🚨 Maximum Confidence: {summary.get('maximum_confidence_games', 0)}")
    print(f"⭐ High Confidence: {summary.get('high_confidence_games', 0)}")
    print(f"⚾ Strong Matchups: {summary.get('strong_matchup_edges', 0)}")
    print(f"🌪️ Elite Tunneling: {summary.get('elite_tunneling_opportunities', 0)}")
    
    # Recommended Approach
    print(f"\n🎲 RECOMMENDED APPROACH:")
    print(f"   {summary.get('recommended_approach', 'No specific approach')}")
    
    # Action Plan
    print(f"\n🎯 DAILY ACTION PLAN:")
    print("-" * 50)
    
    # Primary Targets
    if action_plan.get("primary_targets"):
        print(f"\n🔥 PRIMARY TARGETS:")
        for target in action_plan["primary_targets"]:
            print(f"   {target['priority']}: {target['game']} (Edge: {target['edge_score']}/100)")
            print(f"      💰 {target['recommendation']}")
            if target.get('focus_areas'):
                print(f"      🎮 Focus: {', '.join(target['focus_areas'][:3])}")
    
    # Secondary Opportunities
    if action_plan.get("secondary_opportunities"):
        print(f"\n📈 SECONDARY OPPORTUNITIES:")
        for opp in action_plan["secondary_opportunities"]:
            print(f"   • {opp['game']} (Edge: {opp['edge_score']}/100)")
    
    # Specific Bets
    if action_plan.get("specific_bets"):
        print(f"\n🎲 SPECIFIC BETTING OPPORTUNITIES:")
        for bet in action_plan["specific_bets"]:
            print(f"   {bet['type']}: {bet['bet']}")
            print(f"      💰 {bet['recommendation']}")
            if 'edge_strength' in bet:
                print(f"      📊 Edge: {bet['edge_strength']}/100")
    
    # Bankroll Allocation
    if action_plan.get("bankroll_allocation"):
        print(f"\n💰 BANKROLL ALLOCATION GUIDE:")
        allocation = action_plan["bankroll_allocation"]
        for category, percentage in allocation.items():
            print(f"   {category.replace('_', ' ').title()}: {percentage}")
    
    # Enhanced Analysis Summary
    enhanced_analysis = analysis.get("enhanced_analysis", {})
    if enhanced_analysis:
        print(f"\n💎 DETAILED ANALYSIS:")
        print_enhanced_betting_report(enhanced_analysis)

def main():
    """Run master daily analysis"""
    
    parser = argparse.ArgumentParser(description="Master MLB Daily Analysis")
    parser.add_argument("--date", help="Date to analyze (YYYY-MM-DD)")
    parser.add_argument("--update-data", action="store_true", 
                       help="Update matchup and tunneling data before analysis")
    parser.add_argument("--quiet", action="store_true", help="Minimal output")
    parser.add_argument("--save-report", help="Save report to file")
    
    args = parser.parse_args()
    
    # Connect to database
    dsn = os.getenv("PG_DSN")
    if not dsn:
        print("❌ PG_DSN environment variable must be set")
        sys.exit(1)
    
    try:
        conn = psycopg2.connect(dsn)
        
        if not args.quiet:
            print("✅ Connected to database")
        
        # Initialize analyzer
        analyzer = MasterDailyAnalyzer(conn)
        
        # Run complete analysis
        analysis = analyzer.run_complete_daily_analysis(
            game_date=args.date, 
            update_data=args.update_data
        )
        
        # Print report
        if not args.quiet:
            print_master_daily_report(analysis)
        
        # Save report if requested
        if args.save_report:
            with open(args.save_report, 'w') as f:
                import json
                json.dump(analysis, f, indent=2, default=str)
            print(f"\n💾 Report saved to {args.save_report}")
        
        # Return success code based on opportunities found
        summary = analysis.get("summary_insights", {})
        total_opportunities = summary.get("total_strong_opportunities", 0)
        
        if total_opportunities >= 5:
            sys.exit(0)  # Great day
        elif total_opportunities >= 2:
            sys.exit(1)  # Moderate day  
        else:
            sys.exit(2)  # Slow day
        
    except KeyboardInterrupt:
        print("\n⏹️ Analysis interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"❌ Error: {e}")
        if not args.quiet:
            import traceback
            traceback.print_exc()
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()