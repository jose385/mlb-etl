#!/usr/bin/env python3
"""
test_implementation.py - Comprehensive test of your new matchup and tunneling features
Run this after implementing everything to verify it's working correctly

Usage: python test_implementation.py
"""

import os
import sys
import psycopg2
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

def test_database_tables() -> Tuple[bool, List[str]]:
    """Test that the new database tables exist and have proper structure"""
    
    print("🔍 Testing database tables...")
    
    dsn = os.getenv("PG_DSN")
    if not dsn:
        return False, ["PG_DSN environment variable not set"]
    
    try:
        conn = psycopg2.connect(dsn)
        cur = conn.cursor()
        
        issues = []
        
        # Test batter_pitcher_matchups table
        try:
            cur.execute("""
                SELECT COUNT(*) as row_count,
                       COUNT(DISTINCT batter_id) as unique_batters,
                       COUNT(DISTINCT pitcher_id) as unique_pitchers
                FROM batter_pitcher_matchups
            """)
            result = cur.fetchone()
            
            print(f"   ✅ batter_pitcher_matchups table exists")
            print(f"      📊 {result[0]} total matchups, {result[1]} batters, {result[2]} pitchers")
            
            if result[0] == 0:
                issues.append("batter_pitcher_matchups table is empty - run initial population")
            
        except Exception as e:
            issues.append(f"batter_pitcher_matchups table error: {e}")
        
        # Test pitch_tunneling table
        try:
            cur.execute("""
                SELECT COUNT(*) as row_count,
                       COUNT(DISTINCT pitcher_id) as unique_pitchers,
                       AVG(tunnel_quality_score) as avg_quality
                FROM pitch_tunneling
            """)
            result = cur.fetchone()
            
            print(f"   ✅ pitch_tunneling table exists")
            print(f"      📊 {result[0]} tunnel combinations, {result[1]} pitchers, {result[2]:.1f} avg quality")
            
            if result[0] == 0:
                issues.append("pitch_tunneling table is empty - run initial population")
            
        except Exception as e:
            issues.append(f"pitch_tunneling table error: {e}")
        
        # Test indexes
        try:
            cur.execute("""
                SELECT schemaname, tablename, indexname 
                FROM pg_indexes 
                WHERE tablename IN ('batter_pitcher_matchups', 'pitch_tunneling')
                ORDER BY tablename, indexname
            """)
            indexes = cur.fetchall()
            
            print(f"   ✅ Found {len(indexes)} indexes on new tables")
            
        except Exception as e:
            issues.append(f"Index check error: {e}")
        
        conn.close()
        return len(issues) == 0, issues
        
    except Exception as e:
        return False, [f"Database connection error: {e}"]

def test_python_modules() -> Tuple[bool, List[str]]:
    """Test that all Python modules can be imported and have basic functionality"""
    
    print("🐍 Testing Python modules...")
    
    issues = []
    
    # Test module imports
    modules_to_test = [
        'batter_pitcher_matchups',
        'pitch_tunneling_analysis', 
        'enhanced_betting_integration',
        'backfill_matchup_tunneling'
    ]
    
    for module_name in modules_to_test:
        try:
            __import__(f"py.{module_name}", fromlist=[module_name])
            print(f"   ✅ {module_name} imports successfully")
        except ImportError as e:
            issues.append(f"Cannot import {module_name}: {e}")
        except Exception as e:
            issues.append(f"Error importing {module_name}: {e}")
    
    # Test specific class imports
    try:
        from py.batter_pitcher_matchups import BatterPitcherMatchupAnalyzer
        print(f"   ✅ BatterPitcherMatchupAnalyzer class available")
    except Exception as e:
        issues.append(f"Cannot import BatterPitcherMatchupAnalyzer: {e}")
    
    try:
        from py.pitch_tunneling_analysis import PitchTunnelingAnalyzer
        print(f"   ✅ PitchTunnelingAnalyzer class available")
    except Exception as e:
        issues.append(f"Cannot import PitchTunnelingAnalyzer: {e}")
    
    try:
        from py.enhanced_betting_integration import EnhancedBettingAnalyzer
        print(f"   ✅ EnhancedBettingAnalyzer class available")
    except Exception as e:
        issues.append(f"Cannot import EnhancedBettingAnalyzer: {e}")
    
    return len(issues) == 0, issues

def test_matchup_analysis() -> Tuple[bool, List[str]]:
    """Test matchup analysis functionality"""
    
    print("⚾ Testing matchup analysis...")
    
    try:
        dsn = os.getenv("PG_DSN")
        conn = psycopg2.connect(dsn)
        
        from py.batter_pitcher_matchups import BatterPitcherMatchupAnalyzer, get_todays_matchup_edges
        
        issues = []
        
        # Test analyzer initialization
        analyzer = BatterPitcherMatchupAnalyzer(conn)
        print(f"   ✅ Matchup analyzer initialized")
        
        # Test getting today's edges
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            edges = get_todays_matchup_edges(conn, today, min_edge_strength=30)
            
            if isinstance(edges, list):
                print(f"   ✅ Retrieved {len(edges)} matchup edges for today")
                
                # Check for valid edge data
                valid_edges = [e for e in edges if isinstance(e, dict) and 'edge_strength' in e]
                if valid_edges:
                    best_edge = max(valid_edges, key=lambda x: x.get('edge_strength', 0))
                    print(f"   📊 Best edge: {best_edge.get('edge_strength', 0)}/100")
                else:
                    print(f"   ⚠️ No valid edges found (this is normal if no games today)")
            else:
                issues.append("get_todays_matchup_edges returned invalid data")
                
        except Exception as e:
            issues.append(f"Error getting matchup edges: {e}")
        
        conn.close()
        return len(issues) == 0, issues
        
    except Exception as e:
        return False, [f"Matchup analysis test error: {e}"]

def test_tunneling_analysis() -> Tuple[bool, List[str]]:
    """Test tunneling analysis functionality"""
    
    print("🌪️ Testing tunneling analysis...")
    
    try:
        dsn = os.getenv("PG_DSN")
        conn = psycopg2.connect(dsn)
        
        from py.pitch_tunneling_analysis import PitchTunnelingAnalyzer, get_todays_tunneling_edges
        
        issues = []
        
        # Test analyzer initialization
        analyzer = PitchTunnelingAnalyzer(conn)
        print(f"   ✅ Tunneling analyzer initialized")
        
        # Test getting today's edges
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            edges = get_todays_tunneling_edges(conn, today, min_tunnel_quality=40)
            
            if isinstance(edges, list):
                print(f"   ✅ Retrieved {len(edges)} tunneling edges for today")
                
                # Check for valid edge data
                valid_edges = [e for e in edges if isinstance(e, dict) and 'avg_tunnel_quality' in e]
                if valid_edges:
                    best_tunneler = max(valid_edges, key=lambda x: x.get('avg_tunnel_quality', 0))
                    print(f"   📊 Best tunneling: {best_tunneler.get('avg_tunnel_quality', 0)}/100")
                else:
                    print(f"   ⚠️ No valid tunneling found (this is normal if no games today)")
            else:
                issues.append("get_todays_tunneling_edges returned invalid data")
                
        except Exception as e:
            issues.append(f"Error getting tunneling edges: {e}")
        
        conn.close()
        return len(issues) == 0, issues
        
    except Exception as e:
        return False, [f"Tunneling analysis test error: {e}"]

def test_enhanced_integration() -> Tuple[bool, List[str]]:
    """Test enhanced betting integration"""
    
    print("💎 Testing enhanced integration...")
    
    try:
        dsn = os.getenv("PG_DSN")
        conn = psycopg2.connect(dsn)
        
        from py.enhanced_betting_integration import EnhancedBettingAnalyzer
        
        issues = []
        
        # Test analyzer initialization
        analyzer = EnhancedBettingAnalyzer(conn)
        print(f"   ✅ Enhanced analyzer initialized")
        
        # Test getting complete analysis (this will be quick since we're just testing structure)
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            analysis = analyzer.get_complete_enhanced_analysis(today)
            
            if isinstance(analysis, dict):
                print(f"   ✅ Enhanced analysis completed")
                
                # Check required keys
                required_keys = ['analysis_date', 'base_analysis', 'matchup_edges', 'tunneling_edges', 'master_recommendations']
                missing_keys = [key for key in required_keys if key not in analysis]
                
                if missing_keys:
                    issues.append(f"Missing keys in analysis: {missing_keys}")
                else:
                    print(f"   ✅ All required analysis keys present")
                
                # Check master recommendations
                master_recs = analysis.get('master_recommendations', {})
                if 'total_games_analyzed' in master_recs:
                    games_analyzed = master_recs['total_games_analyzed']
                    print(f"   📊 Analyzed {games_analyzed} games")
                else:
                    issues.append("Master recommendations missing game count")
                    
            else:
                issues.append("Enhanced analysis returned invalid data type")
                
        except Exception as e:
            issues.append(f"Error running enhanced analysis: {e}")
        
        conn.close()
        return len(issues) == 0, issues
        
    except Exception as e:
        return False, [f"Enhanced integration test error: {e}"]

def test_data_availability() -> Tuple[bool, List[str]]:
    """Test that sufficient data is available for analysis"""
    
    print("📊 Testing data availability...")
    
    try:
        dsn = os.getenv("PG_DSN")
        conn = psycopg2.connect(dsn)
        cur = conn.cursor()
        
        issues = []
        
        # Check Statcast data availability
        cur.execute("""
            SELECT 
                COUNT(*) as total_pitches,
                COUNT(DISTINCT game_date) as unique_dates,
                COUNT(DISTINCT pitcher) as unique_pitchers,
                COUNT(DISTINCT batter) as unique_batters,
                MAX(game_date) as latest_date
            FROM statcast 
            WHERE game_date >= CURRENT_DATE - INTERVAL '7 days'
        """)
        
        result = cur.fetchone()
        if result:
            total_pitches, unique_dates, unique_pitchers, unique_batters, latest_date = result
            
            print(f"   📈 Recent Statcast data (last 7 days):")
            print(f"      Pitches: {total_pitches:,}")
            print(f"      Dates: {unique_dates}")
            print(f"      Pitchers: {unique_pitchers}")
            print(f"      Batters: {unique_batters}")
            print(f"      Latest: {latest_date}")
            
            if total_pitches < 1000:
                issues.append("Very little recent Statcast data - analysis may be limited")
            elif total_pitches < 5000:
                issues.append("Limited recent Statcast data - some analysis may be limited")
            else:
                print(f"   ✅ Sufficient recent Statcast data available")
        
        # Check required columns for tunneling
        cur.execute("""
            SELECT COUNT(*) 
            FROM statcast 
            WHERE game_date >= CURRENT_DATE - INTERVAL '3 days'
            AND release_pos_x IS NOT NULL 
            AND release_pos_y IS NOT NULL 
            AND release_pos_z IS NOT NULL
            AND pitch_type IS NOT NULL
        """)
        
        tunneling_ready = cur.fetchone()[0]
        if tunneling_ready > 100:
            print(f"   ✅ Sufficient data for tunneling analysis ({tunneling_ready} pitches)")
        else:
            issues.append("Insufficient data for tunneling analysis - missing release point data")
        
        # Check lineup data (needed for today's analysis)
        cur.execute("""
            SELECT COUNT(*) 
            FROM lineup 
            WHERE game_date >= CURRENT_DATE - INTERVAL '2 days'
        """)
        
        lineup_count = cur.fetchone()[0]
        if lineup_count > 50:
            print(f"   ✅ Recent lineup data available ({lineup_count} lineup entries)")
        else:
            issues.append("Limited lineup data - today's analysis may be restricted")
        
        conn.close()
        return len(issues) == 0, issues
        
    except Exception as e:
        return False, [f"Data availability test error: {e}"]

def run_full_test() -> bool:
    """Run all tests and return overall success"""
    
    print("🧪 TESTING NEW MATCHUP & TUNNELING IMPLEMENTATION")
    print("=" * 70)
    
    all_tests_passed = True
    all_issues = []
    
    # Run all tests
    tests = [
        ("Database Tables", test_database_tables),
        ("Python Modules", test_python_modules),
        ("Data Availability", test_data_availability),
        ("Matchup Analysis", test_matchup_analysis),
        ("Tunneling Analysis", test_tunneling_analysis),
        ("Enhanced Integration", test_enhanced_integration)
    ]
    
    for test_name, test_func in tests:
        print(f"\n{'='*20} {test_name} {'='*20}")
        
        try:
            success, issues = test_func()
            
            if success:
                print(f"✅ {test_name} - ALL TESTS PASSED")
            else:
                print(f"❌ {test_name} - ISSUES FOUND:")
                for issue in issues:
                    print(f"   • {issue}")
                all_tests_passed = False
                all_issues.extend(issues)
                
        except Exception as e:
            print(f"💥 {test_name} - TEST CRASHED: {e}")
            all_tests_passed = False
            all_issues.append(f"{test_name} test crashed: {e}")
    
    # Print summary
    print(f"\n{'='*70}")
    print(f"🏁 TEST SUMMARY")
    print(f"{'='*70}")
    
    if all_tests_passed:
        print(f"🎉 ALL TESTS PASSED!")
        print(f"✅ Your matchup and tunneling implementation is working correctly")
        print(f"🚀 You can now run: python py/master_daily_analysis.py")
        return True
    else:
        print(f"❌ SOME TESTS FAILED")
        print(f"📋 Issues to fix ({len(all_issues)} total):")
        for i, issue in enumerate(all_issues, 1):
            print(f"   {i}. {issue}")
        
        print(f"\n🔧 Common fixes:")
        print(f"   • Run database migrations: psql \"$PG_DSN\" -f migrations/010_matchup_history_tables.sql")
        print(f"   • Populate initial data: python py/batter_pitcher_matchups.py")
        print(f"   • Check file locations: ensure all .py files are in py/ directory")
        print(f"   • Update recent data: python py/backfill.py --start yesterday --end yesterday")
        
        return False

def main():
    """Main test function"""
    
    # Check basic environment
    if not os.getenv("PG_DSN"):
        print("❌ PG_DSN environment variable not set")
        print("Set it like: export PG_DSN='postgresql://user:pass@host:port/db'")
        sys.exit(1)
    
    # Add py directory to path for imports
    py_dir = os.path.join(os.path.dirname(__file__), 'py')
    if os.path.exists(py_dir):
        sys.path.insert(0, os.path.dirname(__file__))
    
    # Run full test suite
    success = run_full_test()
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()