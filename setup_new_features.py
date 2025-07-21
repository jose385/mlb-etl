#!/usr/bin/env python3
"""
setup_new_features.py - Complete setup script for matchup and tunneling features
Run this after installing PostgreSQL client tools
"""

import os
import sys
import psycopg2

# Migration SQL
MIGRATION_SQL = """
-- migrations/010_matchup_history_tables.sql
-- Player vs Pitcher matchup history and pitch tunneling tables

-- Batter vs Pitcher historical matchups
CREATE TABLE IF NOT EXISTS public.batter_pitcher_matchups (
    id SERIAL PRIMARY KEY,
    batter_id INTEGER NOT NULL,
    pitcher_id INTEGER NOT NULL,
    batter_name TEXT,
    pitcher_name TEXT,
    
    -- Time period for this analysis
    analysis_start_date DATE,
    analysis_end_date DATE,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Basic counting stats
    plate_appearances INTEGER DEFAULT 0,
    at_bats INTEGER DEFAULT 0,
    hits INTEGER DEFAULT 0,
    home_runs INTEGER DEFAULT 0,
    doubles INTEGER DEFAULT 0,
    triples INTEGER DEFAULT 0,
    walks INTEGER DEFAULT 0,
    hit_by_pitch INTEGER DEFAULT 0,
    strikeouts INTEGER DEFAULT 0,
    
    -- Advanced metrics
    batting_avg REAL,
    on_base_pct REAL,
    slugging_pct REAL,
    ops REAL,
    woba REAL,
    xwoba REAL,
    
    -- Expected stats
    expected_batting_avg REAL,
    expected_slugging REAL,
    
    -- Contact quality
    avg_exit_velocity REAL,
    max_exit_velocity REAL,
    avg_launch_angle REAL,
    barrel_rate REAL,
    hard_hit_rate REAL,
    
    -- Pitch-specific performance
    fastball_ops REAL,
    breaking_ball_ops REAL,
    offspeed_ops REAL,
    
    -- Situational performance
    risp_avg REAL,
    two_strike_ops REAL,
    late_count_ops REAL,
    
    -- Recency weighting (more recent games weighted higher)
    weighted_ops REAL,
    recent_form_factor REAL, -- 1.0 = normal, >1.0 = hot, <1.0 = cold
    
    -- Statistical significance
    sample_size_rating TEXT, -- 'LARGE', 'MEDIUM', 'SMALL', 'INSUFFICIENT'
    confidence_level REAL,
    
    -- Betting insights
    betting_edge_strength REAL, -- 0-100 scale
    betting_recommendation TEXT,
    edge_description TEXT,
    
    UNIQUE(batter_id, pitcher_id, analysis_end_date)
);

-- Pitch tunneling analysis table
CREATE TABLE IF NOT EXISTS public.pitch_tunneling (
    id SERIAL PRIMARY KEY,
    pitcher_id INTEGER NOT NULL,
    pitcher_name TEXT,
    game_date DATE,
    game_pk INTEGER,
    
    -- Pitch pair being analyzed for tunneling
    pitch_type_1 TEXT NOT NULL, -- e.g., 'FF' (4-seam fastball)
    pitch_type_2 TEXT NOT NULL, -- e.g., 'SL' (slider)
    
    -- Release point similarity (key for tunneling)
    release_point_diff_x REAL, -- Horizontal difference in inches
    release_point_diff_y REAL, -- Vertical difference in inches
    release_point_diff_z REAL, -- Depth difference in inches
    release_point_similarity REAL, -- 0-100 scale
    
    -- Tunnel point analysis (where pitches start to separate)
    tunnel_break_distance REAL, -- Distance from plate when pitches separate
    tunnel_quality_score REAL, -- 0-100, higher = better tunneling
    
    -- Movement differential at plate
    horizontal_break_diff REAL, -- Difference in horizontal movement
    vertical_break_diff REAL,   -- Difference in vertical movement
    movement_contrast REAL,     -- How different the movements are
    
    -- Velocity differential
    velocity_diff REAL,         -- Speed difference between pitches
    velocity_similarity REAL,   -- How similar speeds are out of hand
    
    -- Usage patterns
    pitch_1_usage_rate REAL,    -- How often pitch 1 is thrown
    pitch_2_usage_rate REAL,    -- How often pitch 2 is thrown
    sequence_frequency REAL,    -- How often thrown in sequence
    
    -- Effectiveness metrics
    whiff_rate_improvement REAL, -- How much tunneling improves whiff rate
    chase_rate_improvement REAL, -- How much it improves chase rate on pitch 2
    called_strike_rate_diff REAL,
    
    -- Opponent performance against tunnel
    opponent_avg_vs_tunnel REAL,
    opponent_slugging_vs_tunnel REAL,
    opponent_whiff_rate REAL,
    
    -- Sample size and confidence
    pitch_1_count INTEGER,
    pitch_2_count INTEGER,
    tunneling_sequences INTEGER,
    statistical_confidence TEXT, -- 'HIGH', 'MEDIUM', 'LOW'
    
    -- Betting implications
    strikeout_prop_impact REAL, -- How this affects K prop betting
    betting_insight TEXT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(pitcher_id, game_date, pitch_type_1, pitch_type_2)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_matchups_batter ON public.batter_pitcher_matchups(batter_id);
CREATE INDEX IF NOT EXISTS idx_matchups_pitcher ON public.batter_pitcher_matchups(pitcher_id);
CREATE INDEX IF NOT EXISTS idx_matchups_updated ON public.batter_pitcher_matchups(last_updated);
CREATE INDEX IF NOT EXISTS idx_matchups_sample_size ON public.batter_pitcher_matchups(sample_size_rating);
CREATE INDEX IF NOT EXISTS idx_matchups_betting_edge ON public.batter_pitcher_matchups(betting_edge_strength) WHERE betting_edge_strength > 15;

CREATE INDEX IF NOT EXISTS idx_tunneling_pitcher ON public.pitch_tunneling(pitcher_id);
CREATE INDEX IF NOT EXISTS idx_tunneling_date ON public.pitch_tunneling(game_date);
CREATE INDEX IF NOT EXISTS idx_tunneling_quality ON public.pitch_tunneling(tunnel_quality_score) WHERE tunnel_quality_score > 70;
CREATE INDEX IF NOT EXISTS idx_tunneling_pitches ON public.pitch_tunneling(pitch_type_1, pitch_type_2);

-- Comments for documentation
COMMENT ON TABLE public.batter_pitcher_matchups IS 'Historical performance data for specific batter vs pitcher matchups';
COMMENT ON TABLE public.pitch_tunneling IS 'Advanced pitch tunneling analysis for deception and effectiveness';
COMMENT ON COLUMN public.batter_pitcher_matchups.betting_edge_strength IS 'Strength of betting edge (0-100), higher values indicate stronger opportunities';
COMMENT ON COLUMN public.pitch_tunneling.tunnel_quality_score IS 'Quality of pitch tunneling (0-100), measures how well pitches tunnel together';
"""

def check_environment():
    """Check that everything is set up correctly"""
    print("🔍 Checking environment...")
    
    issues = []
    
    # Check PG_DSN
    dsn = os.getenv("PG_DSN")
    if not dsn:
        issues.append("PG_DSN environment variable not set")
    else:
        print(f"   ✅ PG_DSN found: {dsn[:50]}...")
    
    # Check Python packages
    try:
        import psycopg2
        print("   ✅ psycopg2 available")
    except ImportError:
        issues.append("psycopg2 not installed - run: pip install psycopg2-binary")
    
    try:
        import pandas
        print("   ✅ pandas available")
    except ImportError:
        issues.append("pandas not installed - run: pip install pandas")
    
    try:
        import numpy
        print("   ✅ numpy available")
    except ImportError:
        issues.append("numpy not installed - run: pip install numpy")
    
    return issues

def test_database_connection():
    """Test database connection"""
    print("🔗 Testing database connection...")
    
    dsn = os.getenv("PG_DSN")
    try:
        conn = psycopg2.connect(dsn)
        
        # Test basic query
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()[0]
        print(f"   ✅ Connected to: {version[:60]}...")
        
        # Check for existing tables
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name IN ('statcast', 'lineup', 'roster')
        """)
        existing_tables = [row[0] for row in cur.fetchall()]
        print(f"   ✅ Found existing tables: {existing_tables}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"   ❌ Connection failed: {e}")
        return False

def run_migration():
    """Run the database migration"""
    print("📝 Running database migration...")
    
    dsn = os.getenv("PG_DSN")
    try:
        conn = psycopg2.connect(dsn)
        cur = conn.cursor()
        
        # Execute migration
        cur.execute(MIGRATION_SQL)
        conn.commit()
        
        # Verify tables were created
        cur.execute("""
            SELECT table_name, 
                   (SELECT COUNT(*) FROM information_schema.columns 
                    WHERE table_name = t.table_name AND table_schema = 'public') as column_count
            FROM information_schema.tables t
            WHERE table_schema = 'public' 
            AND table_name IN ('batter_pitcher_matchups', 'pitch_tunneling')
            ORDER BY table_name
        """)
        
        tables = cur.fetchall()
        
        if len(tables) == 2:
            print("   ✅ Migration successful!")
            for table_name, col_count in tables:
                print(f"      📊 {table_name}: {col_count} columns")
        else:
            print(f"   ⚠️ Expected 2 tables, created {len(tables)}")
        
        # Check indexes
        cur.execute("""
            SELECT COUNT(*) 
            FROM pg_indexes 
            WHERE tablename IN ('batter_pitcher_matchups', 'pitch_tunneling')
        """)
        index_count = cur.fetchone()[0]
        print(f"   ✅ Created {index_count} indexes")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"   ❌ Migration failed: {e}")
        return False

def check_data_availability():
    """Check if we have enough data for the new features"""
    print("📊 Checking data availability...")
    
    dsn = os.getenv("PG_DSN")
    try:
        conn = psycopg2.connect(dsn)
        cur = conn.cursor()
        
        # Check Statcast data
        cur.execute("""
            SELECT 
                COUNT(*) as total_pitches,
                COUNT(DISTINCT pitcher) as unique_pitchers,
                COUNT(DISTINCT batter) as unique_batters,
                COUNT(DISTINCT game_date) as unique_dates,
                MAX(game_date) as latest_date
            FROM statcast 
            WHERE game_date >= CURRENT_DATE - INTERVAL '30 days'
        """)
        
        result = cur.fetchone()
        if result and result[0] > 0:
            pitches, pitchers, batters, dates, latest = result
            print(f"   ✅ Recent Statcast data (30 days):")
            print(f"      🎾 {pitches:,} pitches")
            print(f"      👨‍🎨 {pitchers} unique pitchers") 
            print(f"      🏏 {batters} unique batters")
            print(f"      📅 {dates} game dates")
            print(f"      📆 Latest: {latest}")
            
            if pitches < 1000:
                print("   ⚠️ Limited recent data - some analysis may be restricted")
            else:
                print("   ✅ Sufficient data for analysis")
        else:
            print("   ⚠️ No recent Statcast data found")
        
        # Check lineup data
        cur.execute("""
            SELECT COUNT(*) 
            FROM lineup 
            WHERE game_date >= CURRENT_DATE - INTERVAL '7 days'
        """)
        
        lineup_count = cur.fetchone()[0]
        if lineup_count > 0:
            print(f"   ✅ Recent lineup data: {lineup_count} entries")
        else:
            print("   ⚠️ No recent lineup data")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"   ❌ Data check failed: {e}")
        return False

def main():
    """Main setup function"""
    print("🚀 MLB MATCHUP & TUNNELING SETUP")
    print("=" * 60)
    
    # Step 1: Check environment
    issues = check_environment()
    if issues:
        print("❌ Environment issues found:")
        for issue in issues:
            print(f"   • {issue}")
        print("\nPlease fix these issues and run again.")
        return 1
    
    # Step 2: Test database connection
    if not test_database_connection():
        print("❌ Cannot connect to database. Please check your PG_DSN.")
        return 1
    
    # Step 3: Run migration
    if not run_migration():
        print("❌ Migration failed. Please check the error above.")
        return 1
    
    # Step 4: Check data availability
    check_data_availability()
    
    # Step 5: Success message
    print("\n🎉 SETUP COMPLETE!")
    print("=" * 60)
    print("✅ Database tables created successfully")
    print("✅ Indexes created for performance")
    print("✅ Ready for advanced analysis")
    print("\n🚀 Next steps:")
    print("   1. Copy the Python modules to py/ directory")
    print("   2. Run: python test_implementation.py")
    print("   3. Start analysis: python py/master_daily_analysis.py")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())