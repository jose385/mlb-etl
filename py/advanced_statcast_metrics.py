#!/usr/bin/env python3
"""
advanced_statcast_metrics.py - Add this to your py/ directory
Calculate advanced Statcast metrics for deeper betting insights
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import psycopg2
from typing import Dict, List, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')

class AdvancedStatcastAnalyzer:
    """Calculate advanced Statcast metrics beyond basic stats"""
    
    def __init__(self, conn):
        self.conn = conn
        
    def calculate_expected_stats(self, player_id: int, lookback_days: int = 30) -> Dict:
        """Calculate xBA, xwOBA, xSLG using Statcast data"""
        
        query = """
        SELECT 
            launch_speed,
            launch_angle,
            events,
            woba_value,
            estimated_woba_using_speedangle,
            estimated_ba_using_speedangle,
            babip_value,
            iso_value,
            hit_distance_sc,
            bb_type,
            game_date
        FROM statcast 
        WHERE batter = %s 
        AND game_date >= %s
        AND launch_speed IS NOT NULL 
        AND launch_angle IS NOT NULL
        ORDER BY game_date DESC
        """
        
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=lookback_days)
        
        try:
            df = pd.read_sql(query, self.conn, params=[player_id, start_date])
            
            if df.empty:
                return {"error": "No recent Statcast data"}
            
            # Calculate expected stats
            expected_ba = df['estimated_ba_using_speedangle'].mean() if df['estimated_ba_using_speedangle'].notna().any() else None
            expected_woba = df['estimated_woba_using_speedangle'].mean() if df['estimated_woba_using_speedangle'].notna().any() else None
            
            # Calculate actual stats for comparison
            hits = len(df[df['events'].isin(['single', 'double', 'triple', 'home_run'])])
            at_bats = len(df[df['events'].notna()])
            actual_ba = hits / at_bats if at_bats > 0 else 0
            
            actual_woba = df['woba_value'].mean() if df['woba_value'].notna().any() else None
            
            # Calculate differences (luck indicators)
            ba_diff = actual_ba - expected_ba if expected_ba else None
            woba_diff = actual_woba - expected_woba if actual_woba and expected_woba else None
            
            # Barrel rate calculation
            barrel_rate = self.calculate_barrel_rate(df)
            
            # Hard hit rate (95+ mph exit velocity)
            hard_hit_rate = len(df[df['launch_speed'] >= 95]) / len(df) if len(df) > 0 else 0
            
            return {
                "player_id": player_id,
                "games_analyzed": len(df['game_date'].unique()),
                "plate_appearances": len(df),
                
                # Expected vs Actual
                "expected_ba": round(expected_ba, 3) if expected_ba else None,
                "actual_ba": round(actual_ba, 3),
                "ba_difference": round(ba_diff, 3) if ba_diff else None,
                
                "expected_woba": round(expected_woba, 3) if expected_woba else None,
                "actual_woba": round(actual_woba, 3) if actual_woba else None,
                "woba_difference": round(woba_diff, 3) if woba_diff else None,
                
                # Quality of contact
                "barrel_rate": round(barrel_rate, 3),
                "hard_hit_rate": round(hard_hit_rate, 3),
                "avg_exit_velocity": round(df['launch_speed'].mean(), 1),
                "avg_launch_angle": round(df['launch_angle'].mean(), 1),
                
                # Betting insights
                "luck_factor": self.determine_luck_factor(ba_diff, woba_diff),
                "contact_quality": self.assess_contact_quality(barrel_rate, hard_hit_rate),
                "betting_recommendation": self.generate_xstats_betting_rec(ba_diff, woba_diff, barrel_rate)
            }
            
        except Exception as e:
            return {"error": f"Database error: {e}"}
    
    def calculate_barrel_rate(self, df: pd.DataFrame) -> float:
        """Calculate barrel rate using Statcast barrel definition"""
        
        if df.empty:
            return 0.0
        
        # Barrel definition: 98+ mph exit velocity with optimal launch angle (26-30 degrees)
        # or certain combinations of exit velocity and launch angle
        barrels = 0
        
        for _, row in df.iterrows():
            exit_vel = row.get('launch_speed', 0)
            launch_angle = row.get('launch_angle', 0)
            
            # Skip if missing data
            if pd.isna(exit_vel) or pd.isna(launch_angle):
                continue
            
            # Main barrel definition
            if exit_vel >= 98 and 26 <= launch_angle <= 30:
                barrels += 1
            # Additional barrel conditions based on exit velocity
            elif exit_vel >= 99 and 24 <= launch_angle <= 33:
                barrels += 1
            elif exit_vel >= 100 and 22 <= launch_angle <= 35:
                barrels += 1
            elif exit_vel >= 101 and 20 <= launch_angle <= 37:
                barrels += 1
            elif exit_vel >= 102 and 18 <= launch_angle <= 39:
                barrels += 1
            elif exit_vel >= 103 and 16 <= launch_angle <= 41:
                barrels += 1
        
        return barrels / len(df) if len(df) > 0 else 0.0
    
    def analyze_pitch_performance(self, pitcher_id: int, lookback_days: int = 30) -> Dict:
        """Analyze pitcher's stuff quality using Statcast"""
        
        query = """
        SELECT 
            pitch_type,
            release_speed,
            spin_rate,
            spin_axis,
            pfx_x,
            pfx_z,
            plate_x,
            plate_z,
            zone,
            description,
            events,
            launch_speed,
            launch_angle,
            estimated_woba_using_speedangle,
            game_date
        FROM statcast 
        WHERE pitcher = %s 
        AND game_date >= %s
        AND pitch_type IS NOT NULL
        ORDER BY game_date DESC
        """
        
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=lookback_days)
        
        try:
            df = pd.read_sql(query, self.conn, params=[pitcher_id, start_date])
            
            if df.empty:
                return {"error": "No recent pitcher data"}
            
            # Velocity analysis
            velocity_by_pitch = df.groupby('pitch_type')['release_speed'].agg(['mean', 'std']).round(1)
            
            # Spin rate analysis
            spin_by_pitch = df.groupby('pitch_type')['spin_rate'].mean().round(0)
            
            # Command analysis (strike zone control)
            strike_rate = len(df[df['description'].isin(['called_strike', 'swinging_strike', 'foul'])]) / len(df)
            zone_rate = len(df[df['zone'] <= 9]) / len(df)  # Pitches in strike zone
            
            # Swing and miss rate
            swings = df[df['description'].isin(['swinging_strike', 'foul', 'hit_into_play'])]
            whiff_rate = len(swings[swings['description'] == 'swinging_strike']) / len(swings) if len(swings) > 0 else 0
            
            # Contact quality allowed
            contact_df = df[df['launch_speed'].notna()]
            avg_exit_velo_allowed = contact_df['launch_speed'].mean() if not contact_df.empty else None
            hard_contact_rate = len(contact_df[contact_df['launch_speed'] >= 95]) / len(contact_df) if len(contact_df) > 0 else 0
            
            # Expected stats allowed
            expected_woba_against = df['estimated_woba_using_speedangle'].mean() if df['estimated_woba_using_speedangle'].notna().any() else None
            
            return {
                "pitcher_id": pitcher_id,
                "games_analyzed": len(df['game_date'].unique()),
                "pitches_thrown": len(df),
                
                # Velocity metrics
                "avg_velocity_by_pitch": velocity_by_pitch['mean'].to_dict(),
                "velocity_consistency": velocity_by_pitch['std'].to_dict(),
                
                # Stuff quality
                "avg_spin_rate_by_pitch": spin_by_pitch.to_dict(),
                "strike_rate": round(strike_rate, 3),
                "zone_rate": round(zone_rate, 3),
                "whiff_rate": round(whiff_rate, 3),
                
                # Contact management
                "avg_exit_velo_allowed": round(avg_exit_velo_allowed, 1) if avg_exit_velo_allowed else None,
                "hard_contact_rate_allowed": round(hard_contact_rate, 3),
                "expected_woba_against": round(expected_woba_against, 3) if expected_woba_against else None,
                
                # Performance assessment
                "stuff_grade": self.grade_pitcher_stuff(whiff_rate, avg_exit_velo_allowed, strike_rate),
                "betting_insight": self.generate_pitcher_stuff_insight(whiff_rate, hard_contact_rate, zone_rate)
            }
            
        except Exception as e:
            return {"error": f"Database error: {e}"}
    
    def analyze_situational_performance(self, player_id: int, situation_type: str = 'risp') -> Dict:
        """Analyze performance in specific situations"""
        
        # Define situation filters
        situation_filters = {
            'risp': "runner_on_2b IS NOT NULL OR runner_on_3b IS NOT NULL",
            'high_leverage': "inning >= 7 AND outs_when_up <= 1",
            'clutch': "inning >= 8 AND ABS(home_score - away_score) <= 2",
            'late_count': "balls >= 2 OR strikes >= 2"
        }
        
        if situation_type not in situation_filters:
            return {"error": f"Unknown situation type: {situation_type}"}
        
        # Get situational performance
        situation_query = f"""
        WITH situational_data AS (
            SELECT 
                events,
                woba_value,
                launch_speed,
                launch_angle,
                game_date
            FROM statcast s
            LEFT JOIN statsapi_playlog p ON s.game_pk = p.game_pk AND s.at_bat_number = p.at_bat_index
            WHERE s.batter = %s 
            AND s.game_date >= %s
            AND ({situation_filters[situation_type]})
        ),
        overall_data AS (
            SELECT 
                events,
                woba_value,
                launch_speed,
                launch_angle
            FROM statcast
            WHERE batter = %s 
            AND game_date >= %s
        )
        SELECT 'situational' as data_type, * FROM situational_data
        UNION ALL
        SELECT 'overall' as data_type, * FROM overall_data
        """
        
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=60)  # Longer lookback for situational stats
        
        try:
            df = pd.read_sql(situation_query, self.conn, params=[player_id, start_date, player_id, start_date])
            
            if df.empty:
                return {"error": "No data found"}
            
            situational_df = df[df['data_type'] == 'situational']
            overall_df = df[df['data_type'] == 'overall']
            
            # Calculate performance metrics
            def calc_metrics(data):
                if data.empty:
                    return {"avg": 0, "woba": 0, "exit_velo": 0, "pa": 0}
                
                hits = len(data[data['events'].isin(['single', 'double', 'triple', 'home_run'])])
                pa = len(data[data['events'].notna()])
                avg = hits / pa if pa > 0 else 0
                woba = data['woba_value'].mean() if data['woba_value'].notna().any() else 0
                exit_velo = data['launch_speed'].mean() if data['launch_speed'].notna().any() else 0
                
                return {"avg": avg, "woba": woba, "exit_velo": exit_velo, "pa": pa}
            
            situational_metrics = calc_metrics(situational_df)
            overall_metrics = calc_metrics(overall_df)
            
            # Calculate differences
            avg_diff = situational_metrics["avg"] - overall_metrics["avg"]
            woba_diff = situational_metrics["woba"] - overall_metrics["woba"]
            
            return {
                "player_id": player_id,
                "situation_type": situation_type,
                "situational_pa": situational_metrics["pa"],
                "overall_pa": overall_metrics["pa"],
                
                "situational_avg": round(situational_metrics["avg"], 3),
                "overall_avg": round(overall_metrics["avg"], 3),
                "avg_difference": round(avg_diff, 3),
                
                "situational_woba": round(situational_metrics["woba"], 3),
                "overall_woba": round(overall_metrics["woba"], 3),
                "woba_difference": round(woba_diff, 3),
                
                "performance_rating": self.rate_situational_performance(avg_diff, woba_diff),
                "betting_insight": self.generate_situational_betting_insight(situation_type, avg_diff, woba_diff, situational_metrics["pa"])
            }
            
        except Exception as e:
            return {"error": f"Database error: {e}"}
    
    def determine_luck_factor(self, ba_diff: Optional[float], woba_diff: Optional[float]) -> str:
        """Determine if player is running hot/cold based on expected stats"""
        
        if ba_diff is None or woba_diff is None:
            return "UNKNOWN"
        
        if ba_diff >= 0.050 and woba_diff >= 0.040:
            return "RUNNING HOT (due for regression)"
        elif ba_diff <= -0.050 and woba_diff <= -0.040:
            return "RUNNING COLD (due for positive regression)"
        elif ba_diff >= 0.030 or woba_diff >= 0.025:
            return "SLIGHTLY LUCKY"
        elif ba_diff <= -0.030 or woba_diff <= -0.025:
            return "SLIGHTLY UNLUCKY"
        else:
            return "PERFORMING AS EXPECTED"
    
    def assess_contact_quality(self, barrel_rate: float, hard_hit_rate: float) -> str:
        """Assess quality of contact"""
        
        if barrel_rate >= 0.15 and hard_hit_rate >= 0.45:
            return "ELITE CONTACT"
        elif barrel_rate >= 0.10 and hard_hit_rate >= 0.40:
            return "GOOD CONTACT"
        elif barrel_rate >= 0.06 and hard_hit_rate >= 0.35:
            return "AVERAGE CONTACT"
        elif barrel_rate <= 0.03 or hard_hit_rate <= 0.25:
            return "POOR CONTACT"
        else:
            return "BELOW AVERAGE CONTACT"
    
    def grade_pitcher_stuff(self, whiff_rate: float, avg_exit_velo: Optional[float], strike_rate: float) -> str:
        """Grade pitcher's overall stuff quality"""
        
        score = 0
        
        # Whiff rate component
        if whiff_rate >= 0.30:
            score += 30
        elif whiff_rate >= 0.25:
            score += 20
        elif whiff_rate >= 0.20:
            score += 10
        
        # Exit velocity component
        if avg_exit_velo and avg_exit_velo <= 87:
            score += 25
        elif avg_exit_velo and avg_exit_velo <= 89:
            score += 15
        elif avg_exit_velo and avg_exit_velo <= 91:
            score += 5
        
        # Strike rate component
        if strike_rate >= 0.68:
            score += 25
        elif strike_rate >= 0.65:
            score += 15
        elif strike_rate >= 0.62:
            score += 5
        
        if score >= 70:
            return "A+ (Elite stuff)"
        elif score >= 55:
            return "A (Great stuff)"
        elif score >= 40:
            return "B (Good stuff)"
        elif score >= 25:
            return "C (Average stuff)"
        else:
            return "D (Below average stuff)"
    
    def generate_xstats_betting_rec(self, ba_diff: Optional[float], woba_diff: Optional[float], barrel_rate: float) -> str:
        """Generate betting recommendation based on expected stats"""
        
        if ba_diff is None or woba_diff is None:
            return "Insufficient data for recommendation"
        
        if ba_diff >= 0.050:  # Running very hot
            return "FADE player props - due for negative regression"
        elif ba_diff <= -0.050:  # Running very cold
            return "BACK player props - due for positive regression"
        elif barrel_rate >= 0.15:  # Elite contact regardless of results
            return "BACK player props - elite contact quality"
        elif barrel_rate <= 0.03:  # Poor contact
            return "FADE player props - poor contact quality"
        else:
            return "No strong edge based on expected stats"
    
    def generate_pitcher_stuff_insight(self, whiff_rate: float, hard_contact_rate: float, zone_rate: float) -> str:
        """Generate pitcher betting insights based on stuff metrics"""
        
        if whiff_rate >= 0.28 and hard_contact_rate <= 0.35:
            return "BACK strikeout props and team UNDER - elite stuff"
        elif whiff_rate <= 0.18 or hard_contact_rate >= 0.50:
            return "FADE strikeout props and consider OVER - poor stuff"
        elif zone_rate <= 0.45:  # Poor command
            return "BACK opposing team props - poor command"
        else:
            return "No strong stuff-based edge"
    
    def generate_situational_betting_insight(self, situation: str, avg_diff: float, woba_diff: float, sample_size: int) -> str:
        """Generate betting insights for situational performance"""
        
        if sample_size < 20:
            return "Insufficient sample size for reliable insight"
        
        if situation == 'risp' and avg_diff >= 0.050:
            return "BACK RBI props - strong with RISP"
        elif situation == 'risp' and avg_diff <= -0.050:
            return "FADE RBI props - struggles with RISP"
        elif situation == 'high_leverage' and woba_diff >= 0.040:
            return "BACK clutch performance props"
        elif situation == 'high_leverage' and woba_diff <= -0.040:
            return "FADE late-inning props - struggles in pressure"
        else:
            return "No significant situational edge detected"
    
    def rate_situational_performance(self, avg_diff: float, woba_diff: float) -> str:
        """Rate situational performance"""
        
        if avg_diff >= 0.040 and woba_diff >= 0.030:
            return "CLUTCH PERFORMER"
        elif avg_diff <= -0.040 and woba_diff <= -0.030:
            return "PRESSURE STRUGGLES"
        elif avg_diff >= 0.020 or woba_diff >= 0.015:
            return "SLIGHT SITUATIONAL BOOST"
        elif avg_diff <= -0.020 or woba_diff <= -0.015:
            return "SLIGHT SITUATIONAL DROP"
        else:
            return "NEUTRAL SITUATIONAL IMPACT"

def main():
    """Test advanced Statcast analysis"""
    
    import os
    dsn = os.getenv("PG_DSN")
    if not dsn:
        print("❌ PG_DSN environment variable must be set")
        return
    
    try:
        conn = psycopg2.connect(dsn)
        
        print("🎯 Advanced Statcast Analysis Test")
        print("=" * 60)
        
        analyzer = AdvancedStatcastAnalyzer(conn)
        
        # Test expected stats analysis (example player ID)
        print("🔬 Testing expected stats analysis...")
        xstats = analyzer.calculate_expected_stats(545361, lookback_days=30)  # Example: Mike Trout
        
        if 'error' not in xstats:
            print(f"   📊 Expected BA: {xstats.get('expected_ba', 'N/A')}")
            print(f"   📊 Actual BA: {xstats.get('actual_ba', 'N/A')}")
            print(f"   🍀 Luck factor: {xstats.get('luck_factor', 'N/A')}")
            print(f"   ⚾ Contact quality: {xstats.get('contact_quality', 'N/A')}")
            print(f"   💰 Betting rec: {xstats.get('betting_recommendation', 'N/A')}")
        else:
            print(f"   ❌ {xstats['error']}")
        
        # Test pitcher stuff analysis
        print(f"\n🎯 Testing pitcher stuff analysis...")
        pitcher_analysis = analyzer.analyze_pitch_performance(434378, lookback_days=30)  # Example pitcher
        
        if 'error' not in pitcher_analysis:
            print(f"   ⚡ Whiff rate: {pitcher_analysis.get('whiff_rate', 'N/A')}")
            print(f"   🎯 Zone rate: {pitcher_analysis.get('zone_rate', 'N/A')}")
            print(f"   📊 Stuff grade: {pitcher_analysis.get('stuff_grade', 'N/A')}")
            print(f"   💰 Betting insight: {pitcher_analysis.get('betting_insight', 'N/A')}")
        else:
            print(f"   ❌ {pitcher_analysis['error']}")
        
        # Test situational analysis
        print(f"\n🎲 Testing situational analysis (RISP)...")
        risp_analysis = analyzer.analyze_situational_performance(545361, 'risp')
        
        if 'error' not in risp_analysis:
            print(f"   📊 RISP avg vs overall: {risp_analysis.get('avg_difference', 'N/A')}")
            print(f"   🎯 Performance rating: {risp_analysis.get('performance_rating', 'N/A')}")
            print(f"   💰 Betting insight: {risp_analysis.get('betting_insight', 'N/A')}")
        else:
            print(f"   ❌ {risp_analysis['error']}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()