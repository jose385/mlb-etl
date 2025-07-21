#!/usr/bin/env python3
"""
batter_pitcher_matchups.py - Advanced batter vs pitcher matchup analysis
Analyzes historical performance between specific batter-pitcher pairs for betting insights
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import psycopg2
from typing import Dict, List, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')

class BatterPitcherMatchupAnalyzer:
    """Analyzes specific batter vs pitcher historical matchups"""
    
    def __init__(self, conn):
        self.conn = conn
        
    def analyze_matchup(self, batter_id: int, pitcher_id: int, 
                       lookback_days: int = 1095) -> Dict:  # 3 years default
        """Comprehensive analysis of batter vs pitcher matchup"""
        
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=lookback_days)
        
        # Get historical matchup data
        query = """
        SELECT 
            s.game_date,
            s.game_pk,
            s.at_bat_number,
            s.pitch_number,
            s.batter,
            s.pitcher,
            s.events,
            s.description,
            s.balls,
            s.strikes,
            s.outs_when_up,
            s.inning,
            s.pitch_type,
            s.release_speed,
            s.launch_speed,
            s.launch_angle,
            s.hit_distance_sc,
            s.woba_value,
            s.estimated_woba_using_speedangle as xwoba,
            s.estimated_ba_using_speedangle as xba,
            s.plate_x,
            s.plate_z,
            s.zone,
            -- Runner context
            p.runner_on_1b,
            p.runner_on_2b,
            p.runner_on_3b,
            -- Game situation
            p.home_score,
            p.away_score,
            p.inning as pbp_inning
        FROM statcast s
        LEFT JOIN statsapi_playlog p ON s.game_pk = p.game_pk 
                                    AND s.at_bat_number = p.at_bat_index
        WHERE s.batter = %s 
        AND s.pitcher = %s
        AND s.game_date >= %s
        ORDER BY s.game_date DESC, s.at_bat_number, s.pitch_number
        """
        
        try:
            df = pd.read_sql(query, self.conn, params=[batter_id, pitcher_id, start_date])
            
            if df.empty:
                return {
                    "batter_id": batter_id,
                    "pitcher_id": pitcher_id,
                    "error": "No historical matchup data found",
                    "sample_size_rating": "INSUFFICIENT",
                    "betting_recommendation": "NO DATA - Avoid betting this matchup"
                }
            
            # Calculate comprehensive matchup metrics
            matchup_stats = self.calculate_matchup_statistics(df)
            
            # Analyze pitch-specific performance
            pitch_performance = self.analyze_pitch_type_performance(df)
            
            # Situational analysis
            situational_stats = self.analyze_situational_performance(df)
            
            # Recent form weighting
            recent_performance = self.calculate_recent_form_factor(df)
            
            # Calculate betting edge
            betting_analysis = self.calculate_betting_edge(
                matchup_stats, pitch_performance, situational_stats, recent_performance
            )
            
            return {
                "batter_id": batter_id,
                "pitcher_id": pitcher_id,
                "analysis_date": end_date,
                "data_period": f"{start_date} to {end_date}",
                
                # Core statistics
                **matchup_stats,
                
                # Pitch-specific performance
                "pitch_performance": pitch_performance,
                
                # Situational performance
                "situational_performance": situational_stats,
                
                # Recent form
                **recent_performance,
                
                # Betting analysis
                **betting_analysis
            }
            
        except Exception as e:
            return {
                "batter_id": batter_id,
                "pitcher_id": pitcher_id,
                "error": f"Database error: {e}",
                "betting_recommendation": "ERROR - Cannot analyze matchup"
            }
    
    def calculate_matchup_statistics(self, df: pd.DataFrame) -> Dict:
        """Calculate comprehensive matchup statistics"""
        
        # Group by at-bat to get plate appearance level stats
        ab_stats = df.groupby(['game_date', 'at_bat_number']).agg({
            'events': 'last',  # Final outcome of the AB
            'woba_value': 'mean',
            'xwoba': 'mean',
            'xba': 'mean',
            'launch_speed': 'max',  # Best contact in the AB
            'launch_angle': 'mean'
        }).reset_index()
        
        # Count plate appearances and outcomes
        total_pa = len(ab_stats)
        
        # Count specific outcomes
        hits = len(ab_stats[ab_stats['events'].isin(['single', 'double', 'triple', 'home_run'])])
        home_runs = len(ab_stats[ab_stats['events'] == 'home_run'])
        doubles = len(ab_stats[ab_stats['events'] == 'double'])
        triples = len(ab_stats[ab_stats['events'] == 'triple'])
        walks = len(ab_stats[ab_stats['events'].isin(['walk', 'intent_walk'])])
        hbp = len(ab_stats[ab_stats['events'] == 'hit_by_pitch'])
        strikeouts = len(ab_stats[ab_stats['events'].str.contains('strikeout', na=False)])
        
        # At bats (exclude walks, HBP, sac flies)
        non_ab_events = ['walk', 'intent_walk', 'hit_by_pitch', 'sac_fly', 'sac_bunt']
        at_bats = len(ab_stats[~ab_stats['events'].isin(non_ab_events)])
        
        # Calculate rate stats
        batting_avg = hits / at_bats if at_bats > 0 else 0
        on_base_pct = (hits + walks + hbp) / total_pa if total_pa > 0 else 0
        
        # Slugging calculation
        total_bases = (
            len(ab_stats[ab_stats['events'] == 'single']) +
            len(ab_stats[ab_stats['events'] == 'double']) * 2 +
            len(ab_stats[ab_stats['events'] == 'triple']) * 3 +
            len(ab_stats[ab_stats['events'] == 'home_run']) * 4
        )
        slugging_pct = total_bases / at_bats if at_bats > 0 else 0
        ops = on_base_pct + slugging_pct
        
        # Advanced metrics
        woba = ab_stats['woba_value'].mean() if ab_stats['woba_value'].notna().any() else 0
        xwoba = ab_stats['xwoba'].mean() if ab_stats['xwoba'].notna().any() else 0
        xba = ab_stats['xba'].mean() if ab_stats['xba'].notna().any() else 0
        
        # Contact quality
        contact_abs = ab_stats[ab_stats['launch_speed'].notna()]
        avg_exit_velo = contact_abs['launch_speed'].mean() if len(contact_abs) > 0 else 0
        max_exit_velo = contact_abs['launch_speed'].max() if len(contact_abs) > 0 else 0
        
        # Hard hit rate (95+ mph)
        hard_hits = len(contact_abs[contact_abs['launch_speed'] >= 95])
        hard_hit_rate = hard_hits / len(contact_abs) if len(contact_abs) > 0 else 0
        
        # Barrel rate calculation
        barrel_count = 0
        for _, ab in contact_abs.iterrows():
            if pd.notna(ab['launch_speed']) and pd.notna(ab['launch_angle']):
                if self.is_barrel(ab['launch_speed'], ab['launch_angle']):
                    barrel_count += 1
        barrel_rate = barrel_count / len(contact_abs) if len(contact_abs) > 0 else 0
        
        # Sample size assessment
        sample_size_rating = self.assess_sample_size(total_pa)
        confidence_level = self.calculate_confidence_level(total_pa, at_bats)
        
        return {
            "plate_appearances": total_pa,
            "at_bats": at_bats,
            "hits": hits,
            "home_runs": home_runs,
            "doubles": doubles,
            "triples": triples,
            "walks": walks,
            "hit_by_pitch": hbp,
            "strikeouts": strikeouts,
            
            "batting_avg": round(batting_avg, 3),
            "on_base_pct": round(on_base_pct, 3),
            "slugging_pct": round(slugging_pct, 3),
            "ops": round(ops, 3),
            "woba": round(woba, 3),
            "xwoba": round(xwoba, 3),
            "expected_batting_avg": round(xba, 3),
            
            "avg_exit_velocity": round(avg_exit_velo, 1),
            "max_exit_velocity": round(max_exit_velo, 1),
            "hard_hit_rate": round(hard_hit_rate, 3),
            "barrel_rate": round(barrel_rate, 3),
            
            "sample_size_rating": sample_size_rating,
            "confidence_level": round(confidence_level, 2)
        }
    
    def analyze_pitch_type_performance(self, df: pd.DataFrame) -> Dict:
        """Analyze performance against different pitch types"""
        
        pitch_performance = {}
        
        # Group pitches by type
        pitch_groups = {
            'fastball': ['FF', '4-Seam Fastball', 'FT', '2-Seam Fastball', 'SI', 'Sinker'],
            'breaking_ball': ['SL', 'Slider', 'CU', 'Curveball', 'KC', 'Knuckle Curve'],
            'offspeed': ['CH', 'Changeup', 'FS', 'Splitter', 'KN', 'Knuckleball']
        }
        
        for group_name, pitch_types in pitch_groups.items():
            group_pitches = df[df['pitch_type'].isin(pitch_types)]
            
            if len(group_pitches) > 5:  # Need minimum sample
                # Get at-bat level results for this pitch group
                ab_results = group_pitches.groupby(['game_date', 'at_bat_number']).agg({
                    'events': 'last',
                    'woba_value': 'mean'
                }).reset_index()
                
                hits = len(ab_results[ab_results['events'].isin(['single', 'double', 'triple', 'home_run'])])
                abs_group = len(ab_results[ab_results['events'].notna()])
                
                if abs_group > 0:
                    avg = hits / abs_group
                    woba = ab_results['woba_value'].mean() if ab_results['woba_value'].notna().any() else 0
                    
                    pitch_performance[f"{group_name}_avg"] = round(avg, 3)
                    pitch_performance[f"{group_name}_woba"] = round(woba, 3)
                    pitch_performance[f"{group_name}_sample"] = abs_group
        
        return pitch_performance
    
    def analyze_situational_performance(self, df: pd.DataFrame) -> Dict:
        """Analyze performance in different game situations"""
        
        situations = {}
        
        # Runners in scoring position
        risp_df = df[(df['runner_on_2b'].notna()) | (df['runner_on_3b'].notna())]
        if len(risp_df) > 0:
            risp_abs = risp_df.groupby(['game_date', 'at_bat_number'])['events'].last()
            risp_hits = len(risp_abs[risp_abs.isin(['single', 'double', 'triple', 'home_run'])])
            risp_total = len(risp_abs[risp_abs.notna()])
            situations['risp_avg'] = round(risp_hits / risp_total, 3) if risp_total > 0 else 0
            situations['risp_sample'] = risp_total
        
        # Two-strike performance
        two_strike_df = df[df['strikes'] >= 2]
        if len(two_strike_df) > 0:
            two_strike_abs = two_strike_df.groupby(['game_date', 'at_bat_number']).agg({
                'events': 'last',
                'woba_value': 'mean'
            }).reset_index()
            
            hits = len(two_strike_abs[two_strike_abs['events'].isin(['single', 'double', 'triple', 'home_run'])])
            total = len(two_strike_abs[two_strike_abs['events'].notna()])
            woba = two_strike_abs['woba_value'].mean() if two_strike_abs['woba_value'].notna().any() else 0
            
            situations['two_strike_avg'] = round(hits / total, 3) if total > 0 else 0
            situations['two_strike_woba'] = round(woba, 3)
            situations['two_strike_sample'] = total
        
        # Late and close situations (7th+ inning, game within 3 runs)
        if 'home_score' in df.columns and 'away_score' in df.columns:
            late_close_df = df[
                (df['inning'] >= 7) & 
                (abs(df['home_score'] - df['away_score']) <= 3)
            ]
            
            if len(late_close_df) > 0:
                lc_abs = late_close_df.groupby(['game_date', 'at_bat_number']).agg({
                    'events': 'last',
                    'woba_value': 'mean'
                }).reset_index()
                
                hits = len(lc_abs[lc_abs['events'].isin(['single', 'double', 'triple', 'home_run'])])
                total = len(lc_abs[lc_abs['events'].notna()])
                
                situations['late_close_avg'] = round(hits / total, 3) if total > 0 else 0
                situations['late_close_sample'] = total
        
        return situations
    
    def calculate_recent_form_factor(self, df: pd.DataFrame, 
                                   recent_cutoff_days: int = 30) -> Dict:
        """Calculate recent form weighting"""
        
        if df.empty:
            return {"recent_form_factor": 1.0, "recent_sample": 0}
        
        cutoff_date = datetime.now().date() - timedelta(days=recent_cutoff_days)
        
        recent_df = df[pd.to_datetime(df['game_date']).dt.date >= cutoff_date]
        
        if len(recent_df) == 0:
            return {"recent_form_factor": 1.0, "recent_sample": 0}
        
        # Calculate recent performance
        recent_abs = recent_df.groupby(['game_date', 'at_bat_number']).agg({
            'events': 'last',
            'woba_value': 'mean'
        }).reset_index()
        
        recent_hits = len(recent_abs[recent_abs['events'].isin(['single', 'double', 'triple', 'home_run'])])
        recent_total = len(recent_abs[recent_abs['events'].notna()])
        
        if recent_total == 0:
            return {"recent_form_factor": 1.0, "recent_sample": 0}
        
        recent_avg = recent_hits / recent_total
        
        # Calculate overall performance for comparison
        all_abs = df.groupby(['game_date', 'at_bat_number'])['events'].last()
        overall_hits = len(all_abs[all_abs.isin(['single', 'double', 'triple', 'home_run'])])
        overall_total = len(all_abs[all_abs.notna()])
        overall_avg = overall_hits / overall_total if overall_total > 0 else 0
        
        # Form factor: >1.0 means hot, <1.0 means cold
        if overall_avg > 0:
            form_factor = recent_avg / overall_avg
        else:
            form_factor = 1.0
        
        return {
            "recent_form_factor": round(form_factor, 3),
            "recent_sample": recent_total,
            "recent_avg": round(recent_avg, 3),
            "overall_avg": round(overall_avg, 3)
        }
    
    def calculate_betting_edge(self, matchup_stats: Dict, pitch_performance: Dict,
                              situational_stats: Dict, recent_performance: Dict) -> Dict:
        """Calculate betting edge strength and recommendations"""
        
        sample_size = matchup_stats["plate_appearances"]
        ops = matchup_stats["ops"]
        avg = matchup_stats["batting_avg"]
        recent_form = recent_performance.get("recent_form_factor", 1.0)
        
        # Edge strength factors
        edge_factors = []
        
        # Sample size factor
        if sample_size >= 50:
            edge_factors.append(("Large sample", 25))
        elif sample_size >= 20:
            edge_factors.append(("Good sample", 15))
        elif sample_size >= 10:
            edge_factors.append(("Medium sample", 8))
        else:
            edge_factors.append(("Small sample", 0))
        
        # Performance factor
        if ops >= 1.000:
            edge_factors.append(("Excellent performance", 30))
        elif ops >= 0.850:
            edge_factors.append(("Good performance", 20))
        elif ops >= 0.700:
            edge_factors.append(("Average performance", 10))
        elif ops <= 0.500:
            edge_factors.append(("Poor performance", 25))  # Good for pitching props
        else:
            edge_factors.append(("Below average", 15))
        
        # Recent form factor
        if recent_form >= 1.3:
            edge_factors.append(("Red hot recent form", 20))
        elif recent_form >= 1.1:
            edge_factors.append(("Good recent form", 10))
        elif recent_form <= 0.7:
            edge_factors.append(("Poor recent form", 15))
        
        # Situational factors
        risp_avg = situational_stats.get("risp_avg", 0)
        if risp_avg >= 0.350 and situational_stats.get("risp_sample", 0) >= 5:
            edge_factors.append(("Excellent w/ RISP", 15))
        elif risp_avg <= 0.150 and situational_stats.get("risp_sample", 0) >= 5:
            edge_factors.append(("Poor w/ RISP", 12))
        
        # Calculate total edge strength
        total_edge = sum(factor[1] for factor in edge_factors)
        
        # Generate betting recommendation
        betting_recommendation = self.generate_betting_recommendation(
            matchup_stats, edge_factors, total_edge
        )
        
        return {
            "betting_edge_strength": min(100, total_edge),
            "edge_factors": [f[0] for f in edge_factors],
            "betting_recommendation": betting_recommendation,
            "edge_description": self.create_edge_description(edge_factors, matchup_stats)
        }
    
    def generate_betting_recommendation(self, stats: Dict, factors: List, 
                                      edge_strength: float) -> str:
        """Generate specific betting recommendation"""
        
        sample_size = stats["plate_appearances"]
        ops = stats["ops"]
        avg = stats["batting_avg"]
        
        if sample_size < 10:
            return "AVOID - Insufficient sample size for reliable betting"
        
        if edge_strength >= 70:
            if ops >= 0.900:
                return "STRONG BET - Back batter props (hits, RBIs, total bases)"
            elif ops <= 0.500:
                return "STRONG BET - Back pitcher strikeout props, fade batter props"
            else:
                return "STRONG EDGE - Significant matchup advantage detected"
        elif edge_strength >= 50:
            if ops >= 0.800:
                return "MODERATE BET - Lean batter props OVER"
            elif ops <= 0.600:
                return "MODERATE BET - Lean pitcher props OVER, batter UNDER"
            else:
                return "MODERATE EDGE - Some matchup advantage"
        elif edge_strength >= 30:
            return "SLIGHT EDGE - Light betting opportunity"
        else:
            return "NO STRONG EDGE - Avoid matchup-specific bets"
    
    def create_edge_description(self, factors: List, stats: Dict) -> str:
        """Create detailed edge description"""
        
        descriptions = []
        
        if stats["plate_appearances"] >= 20:
            descriptions.append(f"Solid {stats['plate_appearances']} PA sample")
        
        if stats["ops"] >= 0.900:
            descriptions.append(f"Dominant {stats['ops']:.3f} OPS in matchup")
        elif stats["ops"] <= 0.500:
            descriptions.append(f"Struggles badly: {stats['ops']:.3f} OPS")
        
        if stats.get("hard_hit_rate", 0) >= 0.5:
            descriptions.append(f"Quality contact: {stats['hard_hit_rate']:.1%} hard hit rate")
        
        return "; ".join(descriptions) if descriptions else "Standard matchup"
    
    def is_barrel(self, exit_velocity: float, launch_angle: float) -> bool:
        """Determine if a batted ball is a 'barrel' (optimal contact)"""
        
        if pd.isna(exit_velocity) or pd.isna(launch_angle):
            return False
        
        # Barrel definition based on exit velocity and launch angle
        if exit_velocity >= 98 and 26 <= launch_angle <= 30:
            return True
        elif exit_velocity >= 99 and 24 <= launch_angle <= 33:
            return True
        elif exit_velocity >= 100 and 22 <= launch_angle <= 35:
            return True
        elif exit_velocity >= 101 and 20 <= launch_angle <= 37:
            return True
        elif exit_velocity >= 102 and 18 <= launch_angle <= 39:
            return True
        elif exit_velocity >= 103 and 16 <= launch_angle <= 41:
            return True
        
        return False
    
    def assess_sample_size(self, plate_appearances: int) -> str:
        """Assess the reliability of sample size"""
        
        if plate_appearances >= 50:
            return "LARGE"
        elif plate_appearances >= 20:
            return "MEDIUM"
        elif plate_appearances >= 10:
            return "SMALL"
        else:
            return "INSUFFICIENT"
    
    def calculate_confidence_level(self, total_pa: int, at_bats: int) -> float:
        """Calculate statistical confidence level"""
        
        if total_pa >= 50:
            return 0.95
        elif total_pa >= 30:
            return 0.85
        elif total_pa >= 20:
            return 0.75
        elif total_pa >= 10:
            return 0.60
        else:
            return 0.40

def update_all_matchups(conn, min_pa: int = 10, days_back: int = 1095):
    """Update all significant batter-pitcher matchups"""
    
    print(f"🔄 Updating all batter-pitcher matchups (min {min_pa} PA)...")
    
    # Find all matchups with sufficient plate appearances
    query = f"""
    SELECT 
        s.batter,
        s.pitcher,
        r1.person_full_name as batter_name,
        r2.person_full_name as pitcher_name,
        COUNT(*) as total_pitches,
        COUNT(DISTINCT CONCAT(s.game_date, '_', s.at_bat_number)) as plate_appearances
    FROM statcast s
    LEFT JOIN roster r1 ON s.batter = r1.person_id 
                        AND s.game_date = r1.game_date
    LEFT JOIN roster r2 ON s.pitcher = r2.person_id 
                        AND s.game_date = r2.game_date
    WHERE s.game_date >= CURRENT_DATE - INTERVAL '{days_back} days'
    GROUP BY s.batter, s.pitcher, r1.person_full_name, r2.person_full_name
    HAVING COUNT(DISTINCT CONCAT(s.game_date, '_', s.at_bat_number)) >= {min_pa}
    ORDER BY plate_appearances DESC
    LIMIT 500  -- Process top 500 matchups
    """
    
    try:
        matchups_df = pd.read_sql(query, conn)
        print(f"📊 Found {len(matchups_df)} significant matchups to analyze")
        
        analyzer = BatterPitcherMatchupAnalyzer(conn)
        updated_count = 0
        
        cur = conn.cursor()
        
        for _, matchup in matchups_df.iterrows():
            batter_id = matchup['batter']
            pitcher_id = matchup['pitcher']
            
            print(f"   Analyzing: {matchup['batter_name']} vs {matchup['pitcher_name']} ({matchup['plate_appearances']} PA)")
            
            # Analyze matchup
            analysis = analyzer.analyze_matchup(batter_id, pitcher_id, days_back)
            
            if 'error' in analysis:
                print(f"   ❌ Error: {analysis['error']}")
                continue
            
            # Insert/update in database
            upsert_sql = """
            INSERT INTO public.batter_pitcher_matchups (
                batter_id, pitcher_id, batter_name, pitcher_name,
                analysis_start_date, analysis_end_date, last_updated,
                plate_appearances, at_bats, hits, home_runs, doubles, triples,
                walks, hit_by_pitch, strikeouts,
                batting_avg, on_base_pct, slugging_pct, ops, woba, xwoba,
                expected_batting_avg,
                avg_exit_velocity, max_exit_velocity, hard_hit_rate, barrel_rate,
                risp_avg, recent_form_factor,
                sample_size_rating, confidence_level,
                betting_edge_strength, betting_recommendation, edge_description
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
                betting_recommendation = EXCLUDED.betting_recommendation
            """
            
            situational = analysis.get('situational_performance', {})
            
            cur.execute(upsert_sql, (
                batter_id, pitcher_id, 
                matchup.get('batter_name', ''), matchup.get('pitcher_name', ''),
                analysis.get('data_period', '').split(' to ')[0] if ' to ' in str(analysis.get('data_period', '')) else None,
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
            
            updated_count += 1
        
        conn.commit()
        print(f"✅ Updated {updated_count} matchups in database")
        
    except Exception as e:
        print(f"❌ Error updating matchups: {e}")
        conn.rollback()

def get_todays_matchup_edges(conn, game_date: str = None, min_edge_strength: int = 50) -> List[Dict]:
    """Get today's best matchup betting opportunities"""
    
    if game_date is None:
        game_date = datetime.now().strftime('%Y-%m-%d')
    
    print(f"🎯 Finding today's best matchup edges for {game_date}...")
    
    # Get today's probable pitchers and lineups
    query = f"""
    WITH todays_matchups AS (
        SELECT DISTINCT
            l.game_pk,
            l.person_id as batter_id,
            l.person_full_name as batter_name,
            l.batting_order,
            l.team_id,
            l.side as batting_team,
            -- Get opposing pitcher (simplified approach)
            l2.person_id as pitcher_id,
            l2.person_full_name as pitcher_name
        FROM lineup l
        JOIN lineup l2 ON l.game_pk = l2.game_pk 
                       AND l.team_id != l2.team_id
                       AND l2.position_code = '1'  -- Pitcher
        WHERE l.game_date = '{game_date}'
        AND l.batting_order <= 6  -- Focus on top 6 batters
    )
    SELECT 
        tm.*,
        bpm.plate_appearances,
        bpm.ops,
        bpm.batting_avg,
        bpm.betting_edge_strength,
        bpm.betting_recommendation,
        bpm.edge_description,
        bpm.sample_size_rating,
        bpm.confidence_level
    FROM todays_matchups tm
    JOIN batter_pitcher_matchups bpm ON tm.batter_id = bpm.batter_id 
                                     AND tm.pitcher_id = bpm.pitcher_id
    WHERE bpm.betting_edge_strength >= {min_edge_strength}
    ORDER BY bpm.betting_edge_strength DESC
    LIMIT 20  -- Top 20 edges
    """
    
    try:
        df = pd.read_sql(query, conn)
        
        if df.empty:
            return [{"message": f"No strong matchup edges found for {game_date}"}]
        
        results = []
        for _, row in df.iterrows():
            results.append({
                "game_pk": row['game_pk'],
                "batter_name": row['batter_name'],
                "pitcher_name": row['pitcher_name'],
                "batting_order": row['batting_order'],
                "plate_appearances": row['plate_appearances'],
                "matchup_ops": row['ops'],
                "matchup_avg": row['batting_avg'],
                "edge_strength": row['betting_edge_strength'],
                "betting_recommendation": row['betting_recommendation'],
                "edge_description": row['edge_description'],
                "sample_size_rating": row['sample_size_rating'],
                "confidence_level": row['confidence_level']
            })
        
        return results
        
    except Exception as e:
        return [{"error": f"Database error: {e}"}]

def print_matchup_report(matchup_edges: List[Dict]):
    """Print formatted matchup analysis report"""
    
    print(f"\n⚾ BATTER VS PITCHER MATCHUP REPORT")
    print("=" * 70)
    
    if not matchup_edges or (len(matchup_edges) == 1 and 'message' in matchup_edges[0]):
        print("📊 No strong matchup edges found today")
        return
    
    strong_edges = [e for e in matchup_edges if e.get('edge_strength', 0) >= 70]
    moderate_edges = [e for e in matchup_edges if 50 <= e.get('edge_strength', 0) < 70]
    
    if strong_edges:
        print(f"\n🚨 STRONG MATCHUP EDGES ({len(strong_edges)} found):")
        print("-" * 50)
        
        for edge in strong_edges:
            print(f"\n🎯 {edge['batter_name']} (#{edge['batting_order']}) vs {edge['pitcher_name']}")
            print(f"   📊 Historical: {edge['matchup_avg']:.3f} AVG, {edge['matchup_ops']:.3f} OPS ({edge['plate_appearances']} PA)")
            print(f"   ⭐ Edge Strength: {edge['edge_strength']}/100")
            print(f"   💰 BET: {edge['betting_recommendation']}")
            print(f"   📝 {edge['edge_description']}")
            print(f"   🎲 Confidence: {edge['sample_size_rating']} sample ({edge['confidence_level']:.0%})")
    
    if moderate_edges:
        print(f"\n📈 MODERATE MATCHUP EDGES ({len(moderate_edges)} found):")
        print("-" * 50)
        
        for edge in moderate_edges:
            print(f"\n• {edge['batter_name']} vs {edge['pitcher_name']}: {edge['betting_recommendation']}")
            print(f"  Historical: {edge['matchup_avg']:.3f} AVG in {edge['plate_appearances']} PA")

def main():
    """Test matchup analysis"""
    
    import os
    dsn = os.getenv("PG_DSN")
    if not dsn:
        print("❌ PG_DSN environment variable must be set")
        return
    
    try:
        conn = psycopg2.connect(dsn)
        
        print("⚾ Batter vs Pitcher Matchup Analysis")
        print("=" * 50)
        
        # Test individual matchup analysis
        analyzer = BatterPitcherMatchupAnalyzer(conn)
        
        # Example: Analyze a specific matchup (you'd replace with real player IDs)
        print("🔍 Testing individual matchup analysis...")
        result = analyzer.analyze_matchup(545361, 434378)  # Example IDs
        
        if 'error' not in result:
            print(f"📊 Sample matchup analysis:")
            print(f"   PA: {result.get('plate_appearances', 0)}")
            print(f"   OPS: {result.get('ops', 0):.3f}")
            print(f"   Edge Strength: {result.get('betting_edge_strength', 0)}")
            print(f"   Recommendation: {result.get('betting_recommendation', 'N/A')}")
        
        # Get today's best edges
        print(f"\n🎯 Getting today's best matchup edges...")
        edges = get_todays_matchup_edges(conn, min_edge_strength=30)
        print_matchup_report(edges)
        
        # Update matchups (uncomment to run full update)
        # print(f"\n🔄 Updating matchup database...")
        # update_all_matchups(conn, min_pa=15)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()