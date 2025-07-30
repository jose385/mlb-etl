#!/usr/bin/env python3
"""
backfill_matchup_tunneling.py - Integration module for backfill process
Adds matchup and tunneling analysis to your existing backfill workflow
"""

import os
import psycopg2
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional
try:
    from py.imports import setup_imports
    setup_imports()
except ImportError:
    pass

from py.config import require_config, get_config

from py.batter_pitcher_matchups import update_all_matchups
from py.pitch_tunneling_analysis import update_pitcher_tunneling_data

def update_matchup_and_tunneling_data(conn, game_date: str = None, 
                                     force_update: bool = False):
    """
    Update both matchup and tunneling data for a given date
    This should be run after your main backfill process
    """
    
    if game_date is None:
        game_date = datetime.now().strftime('%Y-%m-%d')
    
    print(f"\n🔄 Updating Advanced Analytics for {game_date}")
    print("=" * 60)
    
    # Check if we have sufficient Statcast data for this date
    check_query = """
    SELECT COUNT(*) as pitch_count, COUNT(DISTINCT pitcher) as pitcher_count
    FROM statcast 
    WHERE game_date = %s
    """
    
    try:
        cur = conn.cursor()
        cur.execute(check_query, (game_date,))
        result = cur.fetchone()
        
        pitch_count = result[0] if result else 0
        pitcher_count = result[1] if result else 0
        
        print(f"📊 Found {pitch_count:,} pitches from {pitcher_count} pitchers for {game_date}")
        
        if pitch_count < 100 and not force_update:
            print(f"⚠️ Insufficient data for {game_date} - skipping advanced analytics")
            return
        
        # 1. Update matchup data
        print(f"\n⚾ Updating batter-pitcher matchups...")
        update_matchups_for_date(conn, game_date)
        
        # 2. Update tunneling data  
        print(f"\n🌪️ Updating pitch tunneling analysis...")
        update_tunneling_for_date(conn, game_date)
        
        print(f"\n✅ Advanced analytics update complete for {game_date}")
        
    except Exception as e:
        print(f"❌ Error updating advanced analytics: {e}")
        raise

def update_matchups_for_date(conn, game_date: str):
    """Update matchups for pitchers who played on this date"""
    
    # Find unique batter-pitcher combinations from this date
    query = """
    SELECT DISTINCT 
        s.batter,
        s.pitcher,
        r1.person_full_name as batter_name,
        r2.person_full_name as pitcher_name,
        COUNT(*) as pitches_today
    FROM statcast s
    LEFT JOIN roster r1 ON s.batter = r1.person_id AND s.game_date = r1.game_date
    LEFT JOIN roster r2 ON s.pitcher = r2.person_id AND s.game_date = r2.game_date  
    WHERE s.game_date = %s
    GROUP BY s.batter, s.pitcher, r1.person_full_name, r2.person_full_name
    ORDER BY pitches_today DESC
    """
    
    try:
        import pandas as pd
        from batter_pitcher_matchups import BatterPitcherMatchupAnalyzer
        
        matchups_df = pd.read_sql(query, conn, params=[game_date])
        
        if matchups_df.empty:
            print("   No matchups found for this date")
            return
        
        print(f"   📊 Found {len(matchups_df)} unique matchups to analyze")
        
        analyzer = BatterPitcherMatchupAnalyzer(conn)
        updated_count = 0
        
        cur = conn.cursor()
        
        for _, matchup in matchups_df.iterrows():
            batter_id = matchup['batter']
            pitcher_id = matchup['pitcher']
            
            # Only analyze meaningful matchups (5+ pitches today indicates real AB)
            if matchup['pitches_today'] >= 5:
                
                print(f"   🔄 {matchup['batter_name']} vs {matchup['pitcher_name']}")
                
                # Analyze historical matchup
                analysis = analyzer.analyze_matchup(batter_id, pitcher_id, lookback_days=1095)
                
                if 'error' in analysis:
                    continue
                
                # Update database
                update_matchup_record(cur, analysis, game_date)
                updated_count += 1
        
        conn.commit()
        print(f"   ✅ Updated {updated_count} matchup records")
        
    except Exception as e:
        print(f"   ❌ Error updating matchups: {e}")
        conn.rollback()

def update_matchup_record(cur, analysis: Dict, game_date: str):
    """Update a single matchup record in the database"""
    
    upsert_sql = """
    INSERT INTO public.batter_pitcher_matchups (
        batter_id, pitcher_id, batter_name, pitcher_name,
        analysis_start_date, analysis_end_date, last_updated,
        plate_appearances, at_bats, hits, home_runs, doubles, triples,
        walks, hit_by_pitch, strikeouts,
        batting_avg, on_base_pct, slugging_pct, ops, woba, xwoba,
        expected_batting_avg, avg_exit_velocity, max_exit_velocity, 
        hard_hit_rate, barrel_rate, risp_avg, recent_form_factor,
        sample_size_rating, confidence_level, betting_edge_strength, 
        betting_recommendation, edge_description
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    ) ON CONFLICT (batter_id, pitcher_id, analysis_end_date)
    DO UPDATE SET
        last_updated = EXCLUDED.last_updated,
        plate_appearances = EXCLUDED.plate_appearances,
        batting_avg = EXCLUDED.batting_avg,
        ops = EXCLUDED.ops,
        betting_edge_strength = EXCLUDED.betting_edge_strength,
        betting_recommendation = EXCLUDED.betting_recommendation,
        recent_form_factor = EXCLUDED.recent_form_factor
    """
    
    # Extract situational performance
    situational = analysis.get('situational_performance', {})
    
    # Parse data period
    data_period = analysis.get('data_period', '')
    start_date = None
    if ' to ' in data_period:
        start_date = data_period.split(' to ')[0]
    
    cur.execute(upsert_sql, (
        analysis.get('batter_id'),
        analysis.get('pitcher_id'),
        analysis.get('batter_name', ''),
        analysis.get('pitcher_name', ''), 
        start_date,
        analysis.get('analysis_date'),
        datetime.now(),
        analysis.get('plate_appearances', 0),
        analysis.get('at_bats', 0),
        analysis.get('hits', 0),
        analysis.get('home_runs', 0),
        analysis.get('doubles', 0),
        analysis.get('triples', 0),
        analysis.get('walks', 0),
        analysis.get('hit_by_pitch', 0),
        analysis.get('strikeouts', 0),
        analysis.get('batting_avg', 0),
        analysis.get('on_base_pct', 0), 
        analysis.get('slugging_pct', 0),
        analysis.get('ops', 0),
        analysis.get('woba', 0),
        analysis.get('xwoba', 0),
        analysis.get('expected_batting_avg', 0),
        analysis.get('avg_exit_velocity', 0),
        analysis.get('max_exit_velocity', 0),
        analysis.get('hard_hit_rate', 0),
        analysis.get('barrel_rate', 0),
        situational.get('risp_avg', 0),
        analysis.get('recent_form_factor', 1.0),
        analysis.get('sample_size_rating', 'UNKNOWN'),
        analysis.get('confidence_level', 0.5),
        analysis.get('betting_edge_strength', 0),
        analysis.get('betting_recommendation', ''),
        analysis.get('edge_description', '')
    ))

def update_tunneling_for_date(conn, game_date: str):
    """Update tunneling data for pitchers who played on this date"""
    
    # Find pitchers who pitched on this date
    query = """
    SELECT DISTINCT 
        s.pitcher,
        r.person_full_name as pitcher_name,
        COUNT(*) as pitches_today,
        COUNT(DISTINCT s.pitch_type) as pitch_varieties
    FROM statcast s
    LEFT JOIN roster r ON s.pitcher = r.person_id AND s.game_date = r.game_date
    WHERE s.game_date = %s 
    AND s.pitch_type IS NOT NULL
    AND s.release_pos_x IS NOT NULL
    AND s.release_pos_y IS NOT NULL
    AND s.release_pos_z IS NOT NULL
    GROUP BY s.pitcher, r.person_full_name
    HAVING COUNT(*) >= 20  -- Meaningful start/appearance
    AND COUNT(DISTINCT s.pitch_type) >= 2  -- Multiple pitches for tunneling
    ORDER BY pitches_today DESC
    """
    
    try:
        import pandas as pd
        from pitch_tunneling_analysis import PitchTunnelingAnalyzer
        
        pitchers_df = pd.read_sql(query, conn, params=[game_date])
        
        if pitchers_df.empty:
            print("   No qualifying pitchers found for tunneling analysis")
            return
        
        print(f"   📊 Found {len(pitchers_df)} pitchers to analyze")
        
        analyzer = PitchTunnelingAnalyzer(conn)
        updated_count = 0
        
        cur = conn.cursor()
        
        for _, pitcher in pitchers_df.iterrows():
            pitcher_id = pitcher['pitcher']
            pitcher_name = pitcher.get('pitcher_name', '')
            
            print(f"   🌪️ {pitcher_name} ({pitcher['pitches_today']} pitches, {pitcher['pitch_varieties']} types)")
            
            # Analyze pitcher's tunneling (60-day lookback)
            analysis = analyzer.analyze_pitcher_tunneling(pitcher_id, lookback_days=60)
            
            if 'error' in analysis:
                continue
            
            # Update tunneling combinations
            for combo_name, combo_data in analysis.get('tunnel_combinations', {}).items():
                update_tunneling_record(cur, pitcher_id, pitcher_name, game_date, combo_data, analysis)
                updated_count += 1
        
        conn.commit()
        print(f"   ✅ Updated {updated_count} tunneling records")
        
    except Exception as e:
        print(f"   ❌ Error updating tunneling: {e}")
        conn.rollback()

def update_tunneling_record(cur, pitcher_id: int, pitcher_name: str, game_date: str,
                           combo_data: Dict, analysis: Dict):
    """Update a single tunneling record"""
    
    pitch_types = combo_data.get('pitch_type_1', ''), combo_data.get('pitch_type_2', '')
    
    if not pitch_types[0] or not pitch_types[1]:
        return
    
    upsert_sql = """
    INSERT INTO public.pitch_tunneling (
        pitcher_id, pitcher_name, game_date,
        pitch_type_1, pitch_type_2,
        release_point_diff_x, release_point_diff_y, release_point_diff_z,
        release_point_similarity, tunnel_break_distance, tunnel_quality_score,
        horizontal_break_diff, vertical_break_diff, movement_contrast,
        velocity_diff, velocity_similarity,
        pitch_1_usage_rate, pitch_2_usage_rate, sequence_frequency,
        whiff_rate_improvement, chase_rate_improvement,
        pitch_1_count, pitch_2_count, tunneling_sequences,
        statistical_confidence, strikeout_prop_impact, betting_insight
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    ) ON CONFLICT (pitcher_id, game_date, pitch_type_1, pitch_type_2)
    DO UPDATE SET
        tunnel_quality_score = EXCLUDED.tunnel_quality_score,
        betting_insight = EXCLUDED.betting_insight,
        strikeout_prop_impact = EXCLUDED.strikeout_prop_impact,
        release_point_similarity = EXCLUDED.release_point_similarity,
        movement_contrast = EXCLUDED.movement_contrast
    """
    
    cur.execute(upsert_sql, (
        pitcher_id, pitcher_name, game_date,
        pitch_types[0], pitch_types[1],
        combo_data.get('release_point_diff_x', 0),
        combo_data.get('release_point_diff_y', 0),
        combo_data.get('release_point_diff_z', 0),
        combo_data.get('release_point_similarity', 0),
        combo_data.get('tunnel_break_distance', 175),
        combo_data.get('tunnel_quality_score', 0),
        combo_data.get('horizontal_break_diff', 0),
        combo_data.get('vertical_break_diff', 0),
        combo_data.get('movement_contrast', 0),
        combo_data.get('velocity_diff', 0),
        combo_data.get('velocity_similarity', 0),
        combo_data.get('pitch_1_usage_rate', 0),
        combo_data.get('pitch_2_usage_rate', 0), 
        combo_data.get('sequence_frequency', 0),
        combo_data.get('pitch2_whiff_rate', 0) - combo_data.get('pitch1_whiff_rate', 0),
        combo_data.get('pitch2_chase_rate', 0) - combo_data.get('pitch1_chase_rate', 0),
        combo_data.get('pitch_1_count', 0),
        combo_data.get('pitch_2_count', 0),
        combo_data.get('total_sequences', 0),
        analysis.get('confidence_level', 'MEDIUM'),
        analysis.get('strikeout_prop_impact', 1.0),
        analysis.get('betting_recommendation', '')
    ))

def run_weekly_matchup_update(conn):
    """Run comprehensive weekly update of all matchups"""
    
    print(f"\n📅 Running Weekly Comprehensive Matchup Update")
    print("=" * 60)
    
    try:
        # Update all significant matchups
        print("⚾ Updating all significant batter-pitcher matchups...")
        update_all_matchups(conn, min_pa=10, days_back=1095)
        
        # Update all pitcher tunneling  
        print("\n🌪️ Updating all pitcher tunneling analysis...")
        update_pitcher_tunneling_data(conn, lookback_days=60, min_pitches=100)
        
        print("\n✅ Weekly comprehensive update complete!")
        
    except Exception as e:
        print(f"❌ Error in weekly update: {e}")
        raise

def cleanup_old_analysis_data(conn, days_to_keep: int = 90):
    """Clean up old analysis data to keep database size manageable"""
    
    print(f"\n🧹 Cleaning up analysis data older than {days_to_keep} days")
    
    cutoff_date = (datetime.now() - timedelta(days=days_to_keep)).strftime('%Y-%m-%d')
    
    try:
        cur = conn.cursor()
        
        # Clean old tunneling data
        cur.execute("""
            DELETE FROM pitch_tunneling 
            WHERE game_date < %s
        """, (cutoff_date,))
        
        tunneling_deleted = cur.rowcount
        
        # Keep matchup data longer since it's more historical
        matchup_cutoff = (datetime.now() - timedelta(days=days_to_keep * 3)).strftime('%Y-%m-%d')
        
        cur.execute("""
            DELETE FROM batter_pitcher_matchups 
            WHERE last_updated < %s 
            AND sample_size_rating = 'INSUFFICIENT'
        """, (matchup_cutoff,))
        
        matchup_deleted = cur.rowcount
        
        conn.commit()
        
        print(f"   🗑️ Deleted {tunneling_deleted} old tunneling records")
        print(f"   🗑️ Deleted {matchup_deleted} insufficient matchup records")
        
    except Exception as e:
        print(f"   ❌ Error during cleanup: {e}")
        conn.rollback()

def main():
    """Test the matchup and tunneling integration"""
    
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Date to update (YYYY-MM-DD)")
    parser.add_argument("--weekly-update", action="store_true", help="Run weekly comprehensive update")
    parser.add_argument("--cleanup", action="store_true", help="Clean up old data")
    parser.add_argument("--force", action="store_true", help="Force update even with little data")
    
    args = parser.parse_args()
    
    # Connect to database
    config = require_config(require_database=True)
    dsn = config.PG_DSN
    if not dsn:
        print("❌ PG_DSN environment variable must be set")
        return
    
    try:
        conn = psycopg2.connect(dsn)
        
        if args.weekly_update:
            run_weekly_matchup_update(conn)
        elif args.cleanup:
            cleanup_old_analysis_data(conn)
        else:
            game_date = args.date or datetime.now().strftime('%Y-%m-%d')
            update_matchup_and_tunneling_data(conn, game_date, args.force)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()