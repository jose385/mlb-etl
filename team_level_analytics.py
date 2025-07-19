#!/usr/bin/env python3
"""
team_level_analytics.py - Add this to your py/ directory
Advanced team-level analytics for betting insights
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import psycopg2
from typing import Dict, List, Optional, Tuple
import json

class TeamAnalytics:
    """Comprehensive team-level analysis for betting"""
    
    def __init__(self, conn):
        self.conn = conn
        
    def analyze_team_vs_handedness(self, team_id: int, lookback_days: int = 30) -> Dict:
        """Analyze team performance vs LHP/RHP"""
        
        query = """
        WITH team_vs_handedness AS (
            SELECT 
                s.game_date,
                s.game_pk,
                s.p_throws,
                COUNT(CASE WHEN s.events IN ('single', 'double', 'triple', 'home_run') THEN 1 END) as hits,
                COUNT(CASE WHEN s.events = 'home_run' THEN 1 END) as home_runs,
                COUNT(CASE WHEN s.events LIKE '%strikeout%' THEN 1 END) as strikeouts,
                COUNT(CASE WHEN s.events IN ('walk', 'hit_by_pitch') THEN 1 END) as walks,
                COUNT(CASE WHEN s.events IS NOT NULL AND s.events != '' THEN 1 END) as at_bats,
                SUM(COALESCE(s.woba_value, 0)) as total_woba_value,
                COUNT(CASE WHEN s.woba_value IS NOT NULL THEN 1 END) as woba_opportunities,
                AVG(s.launch_speed) as avg_exit_velocity,
                COUNT(*) as total_pitches
            FROM statcast s
            JOIN roster r ON s.batter = r.person_id 
                        AND s.game_date = r.game_date 
                        AND r.team_id = %s
            WHERE s.game_date >= %s
            AND s.p_throws IS NOT NULL
            GROUP BY s.game_date, s.game_pk, s.p_throws
        )
        SELECT 
            p_throws,
            COUNT(*) as games,
            SUM(hits) as total_hits,
            SUM(at_bats) as total_at_bats,
            SUM(home_runs) as total_hrs,
            SUM(strikeouts) as total_strikeouts,
            SUM(walks) as total_walks,
            SUM(total_woba_value) as sum_woba_value,
            SUM(woba_opportunities) as sum_woba_opps,
            AVG(avg_exit_velocity) as avg_exit_velo,
            SUM(total_pitches) as pitches_faced
        FROM team_vs_handedness
        GROUP BY p_throws
        """
        
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=lookback_days)
        
        try:
            df = pd.read_sql(query, self.conn, params=[team_id, start_date])
            
            if df.empty:
                return {"error": "No team data found"}
            
            analysis = {"team_id": team_id}
            
            for _, row in df.iterrows():
                handedness = "vs_LHP" if row['p_throws'] == 'L' else "vs_RHP"
                
                avg = row['total_hits'] / row['total_at_bats'] if row['total_at_bats'] > 0 else 0
                woba = row['sum_woba_value'] / row['sum_woba_opps'] if row['sum_woba_opps'] > 0 else 0
                hr_rate = row['total_hrs'] / row['total_at_bats'] if row['total_at_bats'] > 0 else 0
                k_rate = row['total_strikeouts'] / row['total_at_bats'] if row['total_at_bats'] > 0 else 0
                bb_rate = row['total_walks'] / (row['total_at_bats'] + row['total_walks']) if (row['total_at_bats'] + row['total_walks']) > 0 else 0
                
                analysis[handedness] = {
                    "games": int(row['games']),
                    "avg": round(avg, 3),
                    "woba": round(woba, 3),
                    "hr_rate": round(hr_rate, 3),
                    "k_rate": round(k_rate, 3),
                    "bb_rate": round(bb_rate, 3),
                    "exit_velocity": round(row['avg_exit_velo'], 1) if row['avg_exit_velo'] else None
                }
            
            # Calculate splits advantage
            if "vs_LHP" in analysis and "vs_RHP" in analysis:
                lhp_woba = analysis["vs_LHP"]["woba"]
                rhp_woba = analysis["vs_RHP"]["woba"]
                
                analysis["platoon_advantage"] = self.determine_platoon_advantage(lhp_woba, rhp_woba)
                analysis["betting_insight"] = self.generate_handedness_betting_insight(
                    analysis["vs_LHP"], analysis["vs_RHP"]
                )
            
            return analysis
            
        except Exception as e:
            return {"error": f"Database error: {e}"}
    
    def analyze_team_vs_pitch_types(self, team_id: int, lookback_days: int = 30) -> Dict:
        """Analyze team performance against different pitch types"""
        
        query = """
        SELECT 
            s.pitch_type,
            COUNT(CASE WHEN s.events IN ('single', 'double', 'triple', 'home_run') THEN 1 END) as hits,
            COUNT(CASE WHEN s.events = 'home_run' THEN 1 END) as home_runs,
            COUNT(CASE WHEN s.events LIKE '%strikeout%' THEN 1 END) as strikeouts,
            COUNT(CASE WHEN s.events IS NOT NULL AND s.events != '' THEN 1 END) as at_bats,
            AVG(s.launch_speed) as avg_exit_velocity,
            SUM(COALESCE(s.woba_value, 0)) as total_woba_value,
            COUNT(CASE WHEN s.woba_value IS NOT NULL THEN 1 END) as woba_opportunities,
            COUNT(*) as pitches_seen
        FROM statcast s
        JOIN roster r ON s.batter = r.person_id 
                    AND s.game_date = r.game_date 
                    AND r.team_id = %s
        WHERE s.game_date >= %s
        AND s.pitch_type IS NOT NULL
        GROUP BY s.pitch_type
        HAVING COUNT(*) >= 20  -- Minimum sample size
        ORDER BY COUNT(*) DESC
        """
        
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=lookback_days)
        
        try:
            df = pd.read_sql(query, self.conn, params=[team_id, start_date])
            
            if df.empty:
                return {"error": "No pitch type data found"}
            
            pitch_analysis = {}
            
            for _, row in df.iterrows():
                pitch_type = row['pitch_type']
                
                avg = row['hits'] / row['at_bats'] if row['at_bats'] > 0 else 0
                woba = row['total_woba_value'] / row['woba_opportunities'] if row['woba_opportunities'] > 0 else 0
                k_rate = row['strikeouts'] / row['at_bats'] if row['at_bats'] > 0 else 0
                
                pitch_analysis[pitch_type] = {
                    "pitches_seen": int(row['pitches_seen']),
                    "avg": round(avg, 3),
                    "woba": round(woba, 3),
                    "hr_rate": round(row['home_runs'] / row['at_bats'] if row['at_bats'] > 0 else 0, 3),
                    "k_rate": round(k_rate, 3),
                    "exit_velocity": round(row['avg_exit_velocity'], 1) if row['avg_exit_velocity'] else None
                }
            
            # Identify strengths and weaknesses
            best_pitch = max(pitch_analysis.items(), key=lambda x: x[1]['woba'])
            worst_pitch = min(pitch_analysis.items(), key=lambda x: x[1]['woba'])
            
            return {
                "team_id": team_id,
                "pitch_type_performance": pitch_analysis,
                "strength": f"Best vs {best_pitch[0]} (wOBA: {best_pitch[1]['woba']})",
                "weakness": f"Worst vs {worst_pitch[0]} (wOBA: {worst_pitch[1]['woba']})",
                "betting_insight": self.generate_pitch_type_betting_insight(pitch_analysis)
            }
            
        except Exception as e:
            return {"error": f"Database error: {e}"}
    
    def analyze_lineup_protection(self, team_id: int, game_date: str) -> Dict:
        """Analyze lineup construction and protection"""
        
        query = """
        WITH lineup_with_stats AS (
            SELECT 
                l.batting_order,
                l.person_id,
                l.person_full_name,
                l.position_code,
                l.stats_batting_avg,
                l.stats_batting_obp,
                l.stats_batting_slg,
                l.stats_batting_ops,
                l.stats_batting_home_runs,
                l.stats_batting_rbi
            FROM lineup l
            WHERE l.team_id = %s 
            AND l.game_date = %s
            AND l.batting_order <= 9
            ORDER BY l.batting_order
        )
        SELECT * FROM lineup_with_stats
        """
        
        try:
            df = pd.read_sql(query, self.conn, params=[team_id, game_date])
            
            if df.empty:
                return {"error": "No lineup data found"}
            
            lineup_analysis = {
                "team_id": team_id,
                "game_date": game_date,
                "lineup_strength": {},
                "protection_analysis": {}
            }
            
            # Analyze lineup segments
            segments = {
                "top_order": df[df['batting_order'] <= 3],
                "middle_order": df[(df['batting_order'] >= 4) & (df['batting_order'] <= 6)],
                "bottom_order": df[df['batting_order'] >= 7]
            }
            
            for segment, players in segments.items():
                if not players.empty:
                    avg_ops = players['stats_batting_ops'].mean() if players['stats_batting_ops'].notna().any() else 0
                    avg_hr = players['stats_batting_home_runs'].mean() if players['stats_batting_home_runs'].notna().any() else 0
                    
                    lineup_analysis["lineup_strength"][segment] = {
                        "avg_ops": round(avg_ops, 3),
                        "avg_hr": round(avg_hr, 1),
                        "strength_rating": self.rate_lineup_segment(avg_ops, avg_hr, segment)
                    }
            
            # Analyze protection (players hitting behind power hitters)
            power_hitters = df[df['stats_batting_home_runs'] >= 15] if df['stats_batting_home_runs'].notna().any() else pd.DataFrame()
            
            for _, hitter in power_hitters.iterrows():
                order = hitter['batting_order']
                if order < 9:  # Has someone hitting behind them
                    protection = df[df['batting_order'] == order + 1]
                    if not protection.empty:
                        protector = protection.iloc[0]
                        protection_ops = protector['stats_batting_ops'] if pd.notna(protector['stats_batting_ops']) else 0
                        
                        lineup_analysis["protection_analysis"][hitter['person_full_name']] = {
                            "batting_order": int(order),
                            "protector": protector['person_full_name'],
                            "protector_ops": round(protection_ops, 3),
                            "protection_quality": self.rate_protection_quality(protection_ops)
                        }
            
            # Overall lineup assessment
            lineup_analysis["overall_assessment"] = self.assess_overall_lineup(lineup_analysis["lineup_strength"])
            lineup_analysis["betting_insight"] = self.generate_lineup_betting_insight(lineup_analysis)
            
            return lineup_analysis
            
        except Exception as e:
            return {"error": f"Database error: {e}"}
    
    def analyze_defensive_positioning(self, team_id: int, lookback_days: int = 15) -> Dict:
        """Analyze defensive positioning effectiveness"""
        
        query = """
        SELECT 
            s.if_fielding_alignment,
            s.of_fielding_alignment,
            s.bb_type,
            s.hit_location,
            s.events,
            s.launch_speed,
            s.launch_angle,
            COUNT(*) as balls_in_play
        FROM statcast s
        JOIN roster r ON s.pitcher = r.person_id 
                    AND s.game_date = r.game_date 
                    AND r.team_id = %s
        WHERE s.game_date >= %s
        AND s.bb_type IS NOT NULL
        AND s.if_fielding_alignment IS NOT NULL
        GROUP BY s.if_fielding_alignment, s.of_fielding_alignment, s.bb_type, 
                 s.hit_location, s.events, s.launch_speed, s.launch_angle
        """
        
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=lookback_days)
        
        try:
            df = pd.read_sql(query, self.conn, params=[team_id, start_date])
            
            if df.empty:
                return {"error": "No defensive positioning data"}
            
            # Analyze alignment effectiveness
            alignment_analysis = {}
            
            for alignment in df['if_fielding_alignment'].unique():
                if pd.isna(alignment):
                    continue
                    
                alignment_df = df[df['if_fielding_alignment'] == alignment]
                
                hits = len(alignment_df[alignment_df['events'].isin(['single', 'double', 'triple', 'home_run'])])
                total_bip = len(alignment_df)
                babip = hits / total_bip if total_bip > 0 else 0
                
                alignment_analysis[alignment] = {
                    "balls_in_play": total_bip,
                    "babip_allowed": round(babip, 3),
                    "effectiveness": self.rate_defensive_effectiveness(babip)
                }
            
            return {
                "team_id": team_id,
                "defensive_alignments": alignment_analysis,
                "overall_defense_rating": self.rate_overall_defense(alignment_analysis),
                "betting_insight": self.generate_defensive_betting_insight(alignment_analysis)
            }
            
        except Exception as e:
            return {"error": f"Database error: {e}"}
    
    def analyze_late_inning_performance(self, team_id: int, lookback_days: int = 30) -> Dict:
        """Analyze team performance in late innings (7+)"""
        
        query = """
        WITH late_inning_performance AS (
            SELECT 
                s.inning,
                s.inning_topbot,
                COUNT(CASE WHEN s.events IN ('single', 'double', 'triple', 'home_run') THEN 1 END) as hits,
                COUNT(CASE WHEN s.events = 'home_run' THEN 1 END) as home_runs,
                COUNT(CASE WHEN s.events LIKE '%strikeout%' THEN 1 END) as strikeouts,
                COUNT(CASE WHEN s.events IN ('walk', 'hit_by_pitch') THEN 1 END) as walks,
                COUNT(CASE WHEN s.events IS NOT NULL AND s.events != '' THEN 1 END) as at_bats,
                SUM(COALESCE(s.woba_value, 0)) as total_woba_value,
                COUNT(CASE WHEN s.woba_value IS NOT NULL THEN 1 END) as woba_opportunities
            FROM statcast s
            JOIN roster r ON s.batter = r.person_id 
                        AND s.game_date = r.game_date 
                        AND r.team_id = %s
            WHERE s.game_date >= %s
            AND s.inning >= 7
            GROUP BY s.inning, s.inning_topbot
        )
        SELECT 
            'late_innings' as period,
            SUM(hits) as total_hits,
            SUM(at_bats) as total_at_bats,
            SUM(home_runs) as total_hrs,
            SUM(strikeouts) as total_strikeouts,
            SUM(walks) as total_walks,
            SUM(total_woba_value) as sum_woba_value,
            SUM(woba_opportunities) as sum_woba_opps
        FROM late_inning_performance
        
        UNION ALL
        
        SELECT 
            'early_innings' as period,
            SUM(CASE WHEN s.events IN ('single', 'double', 'triple', 'home_run') THEN 1 END) as total_hits,
            SUM(CASE WHEN s.events IS NOT NULL AND s.events != '' THEN 1 END) as total_at_bats,
            SUM(CASE WHEN s.events = 'home_run' THEN 1 END) as total_hrs,
            SUM(CASE WHEN s.events LIKE '%strikeout%' THEN 1 END) as total_strikeouts,
            SUM(CASE WHEN s.events IN ('walk', 'hit_by_pitch') THEN 1 END) as total_walks,
            SUM(COALESCE(s.woba_value, 0)) as sum_woba_value,
            COUNT(CASE WHEN s.woba_value IS NOT NULL THEN 1 END) as sum_woba_opps
        FROM statcast s
        JOIN roster r ON s.batter = r.person_id 
                    AND s.game_date = r.game_date 
                    AND r.team_id = %s
        WHERE s.game_date >= %s
        AND s.inning <= 6
        """
        
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=lookback_days)
        
        try:
            df = pd.read_sql(query, self.conn, params=[team_id, start_date, team_id, start_date])
            
            if df.empty:
                return {"error": "No late inning data"}
            
            analysis = {"team_id": team_id}
            
            for _, row in df.iterrows():
                period = row['period']
                
                avg = row['total_hits'] / row['total_at_bats'] if row['total_at_bats'] > 0 else 0
                woba = row['sum_woba_value'] / row['sum_woba_opps'] if row['sum_woba_opps'] > 0 else 0
                k_rate = row['total_strikeouts'] / row['total_at_bats'] if row['total_at_bats'] > 0 else 0
                
                analysis[period] = {
                    "avg": round(avg, 3),
                    "woba": round(woba, 3),
                    "hr_rate": round(row['total_hrs'] / row['total_at_bats'] if row['total_at_bats'] > 0 else 0, 3),
                    "k_rate": round(k_rate, 3),
                    "at_bats": int(row['total_at_bats'])
                }
            
            # Calculate late-inning performance differential
            if "late_innings" in analysis and "early_innings" in analysis:
                late_woba = analysis["late_innings"]["woba"]
                early_woba = analysis["early_innings"]["woba"]
                woba_diff = late_woba - early_woba
                
                analysis["late_inning_differential"] = round(woba_diff, 3)
                analysis["late_inning_rating"] = self.rate_late_inning_performance(woba_diff)
                analysis["betting_insight"] = self.generate_late_inning_betting_insight(woba_diff, late_woba)
            
            return analysis
            
        except Exception as e:
            return {"error": f"Database error: {e}"}
    
    def get_comprehensive_team_matchup(self, home_team_id: int, away_team_id: int, game_date: str) -> Dict:
        """Complete team vs team matchup analysis"""
        
        print(f"🔍 Analyzing comprehensive matchup: Team {away_team_id} @ Team {home_team_id}")
        
        matchup_analysis = {
            "game_date": game_date,
            "home_team_id": home_team_id,
            "away_team_id": away_team_id,
            "analysis_timestamp": datetime.now().isoformat()
        }
        
        # Get handedness analysis for both teams
        home_handedness = self.analyze_team_vs_handedness(home_team_id)
        away_handedness = self.analyze_team_vs_handedness(away_team_id)
        
        # Get lineup analysis
        home_lineup = self.analyze_lineup_protection(home_team_id, game_date)
        away_lineup = self.analyze_lineup_protection(away_team_id, game_date)
        
        # Get late-inning performance
        home_late_innings = self.analyze_late_inning_performance(home_team_id)
        away_late_innings = self.analyze_late_inning_performance(away_team_id)
        
        matchup_analysis.update({
            "home_team_analysis": {
                "handedness_splits": home_handedness,
                "lineup_analysis": home_lineup,
                "late_inning_performance": home_late_innings
            },
            "away_team_analysis": {
                "handedness_splits": away_handedness,
                "lineup_analysis": away_lineup,
                "late_inning_performance": away_late_innings
            }
        })
        
        # Generate matchup insights
        matchup_analysis["matchup_advantages"] = self.determine_matchup_advantages(
            home_handedness, away_handedness, home_lineup, away_lineup
        )
        
        matchup_analysis["betting_recommendations"] = self.generate_matchup_betting_recommendations(
            matchup_analysis
        )
        
        return matchup_analysis
    
    # Helper methods for rating and insights
    def determine_platoon_advantage(self, lhp_woba: float, rhp_woba: float) -> str:
        """Determine platoon advantage"""
        diff = abs(lhp_woba - rhp_woba)
        
        if diff >= 0.050:
            return "SIGNIFICANT vs RHP" if rhp_woba > lhp_woba else "SIGNIFICANT vs LHP"
        elif diff >= 0.030:
            return "MODERATE vs RHP" if rhp_woba > lhp_woba else "MODERATE vs LHP"
        else:
            return "BALANCED"
    
    def rate_lineup_segment(self, avg_ops: float, avg_hr: float, segment: str) -> str:
        """Rate lineup segment strength"""
        if segment == "top_order":
            if avg_ops >= 0.800:
                return "ELITE"
            elif avg_ops >= 0.750:
                return "STRONG"
            else:
                return "AVERAGE"
        elif segment == "middle_order":
            if avg_ops >= 0.850 and avg_hr >= 20:
                return "ELITE POWER"
            elif avg_ops >= 0.800:
                return "STRONG"
            else:
                return "AVERAGE"
        else:  # bottom_order
            if avg_ops >= 0.700:
                return "SURPRISINGLY STRONG"
            else:
                return "TYPICAL BOTTOM"
    
    def rate_protection_quality(self, ops: float) -> str:
        """Rate lineup protection quality"""
        if ops >= 0.850:
            return "ELITE PROTECTION"
        elif ops >= 0.800:
            return "GOOD PROTECTION"
        elif ops >= 0.750:
            return "AVERAGE PROTECTION"
        else:
            return "POOR PROTECTION"
    
    def assess_overall_lineup(self, lineup_strength: Dict) -> str:
        """Assess overall lineup quality"""
        # This is a simplified assessment
        strengths = sum(1 for segment in lineup_strength.values() 
                       if segment.get("strength_rating") in ["ELITE", "STRONG", "ELITE POWER"])
        
        if strengths >= 2:
            return "DEEP LINEUP"
        elif strengths == 1:
            return "TOP-HEAVY LINEUP"
        else:
            return "WEAK LINEUP"
    
    def generate_handedness_betting_insight(self, vs_lhp: Dict, vs_rhp: Dict) -> str:
        """Generate betting insight based on handedness splits"""
        
        lhp_woba = vs_lhp.get("woba", 0)
        rhp_woba = vs_rhp.get("woba", 0)
        diff = abs(lhp_woba - rhp_woba)
        
        if diff >= 0.050:
            better_vs = "LHP" if lhp_woba > rhp_woba else "RHP"
            return f"STRONG EDGE vs {better_vs} - significant platoon advantage"
        elif diff >= 0.030:
            better_vs = "LHP" if lhp_woba > rhp_woba else "RHP"
            return f"MODERATE EDGE vs {better_vs}"
        else:
            return "No significant handedness edge"
    
    def generate_pitch_type_betting_insight(self, pitch_analysis: Dict) -> str:
        """Generate betting insight based on pitch type performance"""
        
        if not pitch_analysis:
            return "Insufficient pitch type data"
        
        # Find best and worst performance
        best_woba = max(pitch_analysis.values(), key=lambda x: x['woba'])['woba']
        worst_woba = min(pitch_analysis.values(), key=lambda x: x['woba'])['woba']
        
        if best_woba - worst_woba >= 0.100:
            return "SIGNIFICANT pitch type splits - research opposing starter's repertoire"
        else:
            return "Balanced against different pitch types"
    
    def rate_defensive_effectiveness(self, babip: float) -> str:
        """Rate defensive effectiveness"""
        if babip <= 0.280:
            return "ELITE DEFENSE"
        elif babip <= 0.300:
            return "GOOD DEFENSE"
        elif babip <= 0.320:
            return "AVERAGE DEFENSE"
        else:
            return "POOR DEFENSE"
    
    def rate_overall_defense(self, alignment_analysis: Dict) -> str:
        """Rate overall defensive performance"""
        if not alignment_analysis:
            return "UNKNOWN"
        
        avg_babip = np.mean([data['babip_allowed'] for data in alignment_analysis.values()])
        return self.rate_defensive_effectiveness(avg_babip)
    
    def generate_defensive_betting_insight(self, alignment_analysis: Dict) -> str:
        """Generate defensive betting insight"""
        
        if not alignment_analysis:
            return "Insufficient defensive data"
        
        avg_babip = np.mean([data['babip_allowed'] for data in alignment_analysis.values()])
        
        if avg_babip <= 0.280:
            return "BACK team UNDER - elite defense suppresses offense"
        elif avg_babip >= 0.320:
            return "BACK opponent OVER - poor defense allows extra hits"
        else:
            return "Defense has neutral impact"
    
    def rate_late_inning_performance(self, woba_diff: float) -> str:
        """Rate late-inning performance"""
        if woba_diff >= 0.030:
            return "CLUTCH PERFORMERS"
        elif woba_diff <= -0.030:
            return "LATE-INNING STRUGGLES"
        else:
            return "CONSISTENT THROUGHOUT"
    
    def generate_late_inning_betting_insight(self, woba_diff: float, late_woba: float) -> str:
        """Generate late-inning betting insight"""
        
        if woba_diff >= 0.040:
            return "BACK late-inning props - team performs better under pressure"
        elif woba_diff <= -0.040:
            return "FADE late-inning props - team struggles in high leverage"
        elif late_woba >= 0.350:
            return "Team stays productive late in games"
        else:
            return "No significant late-inning edge"
    
    def generate_lineup_betting_insight(self, lineup_analysis: Dict) -> str:
        """Generate lineup-based betting insight"""
        
        assessment = lineup_analysis.get("overall_assessment", "")
        
        if assessment == "DEEP LINEUP":
            return "BACK team total runs - deep lineup creates consistent offense"
        elif assessment == "TOP-HEAVY LINEUP":
            return "Monitor top-order performance - offense depends on key hitters"
        else:
            return "Consider fading team offensive props"
    
    def determine_matchup_advantages(self, home_hand: Dict, away_hand: Dict, 
                                   home_lineup: Dict, away_lineup: Dict) -> List[str]:
        """Determine key matchup advantages"""
        
        advantages = []
        
        # Check handedness advantages
        if 'error' not in home_hand and 'platoon_advantage' in home_hand:
            if 'SIGNIFICANT' in home_hand['platoon_advantage']:
                advantages.append(f"Home team has {home_hand['platoon_advantage']}")
        
        if 'error' not in away_hand and 'platoon_advantage' in away_hand:
            if 'SIGNIFICANT' in away_hand['platoon_advantage']:
                advantages.append(f"Away team has {away_hand['platoon_advantage']}")
        
        # Check lineup depth
        if 'error' not in home_lineup and home_lineup.get('overall_assessment') == 'DEEP LINEUP':
            advantages.append("Home team has deep offensive lineup")
        
        if 'error' not in away_lineup and away_lineup.get('overall_assessment') == 'DEEP LINEUP':
            advantages.append("Away team has deep offensive lineup")
        
        return advantages
    
    def generate_matchup_betting_recommendations(self, matchup_analysis: Dict) -> List[str]:
        """Generate betting recommendations based on complete matchup"""
        
        recommendations = []
        
        advantages = matchup_analysis.get("matchup_advantages", [])
        
        # Aggregate insights from individual analyses
        home_insights = []
        away_insights = []
        
        for analysis_type in ["handedness_splits", "lineup_analysis", "late_inning_performance"]:
            home_data = matchup_analysis["home_team_analysis"].get(analysis_type, {})
            away_data = matchup_analysis["away_team_analysis"].get(analysis_type, {})
            
            if 'betting_insight' in home_data and home_data['betting_insight'] != "No strong edge based on expected stats":
                home_insights.append(home_data['betting_insight'])
            
            if 'betting_insight' in away_data and away_data['betting_insight'] != "No strong edge based on expected stats":
                away_insights.append(away_data['betting_insight'])
        
        # Synthesize recommendations
        if len(advantages) >= 2:
            recommendations.append("STRONG MATCHUP EDGE DETECTED - Multiple factors favor one side")
        
        if home_insights:
            recommendations.extend([f"Home team: {insight}" for insight in home_insights[:2]])
        
        if away_insights:
            recommendations.extend([f"Away team: {insight}" for insight in away_insights[:2]])
        
        if not recommendations:
            recommendations.append("Balanced matchup - look for other edges (weather, umpire, etc.)")
        
        return recommendations

def main():
    """Test team analytics"""
    
    import os
    dsn = os.getenv("PG_DSN")
    if not dsn:
        print("❌ PG_DSN environment variable must be set")
        return
    
    try:
        conn = psycopg2.connect(dsn)
        
        print("🏟️ Team-Level Analytics Test")
        print("=" * 60)
        
        analyzer = TeamAnalytics(conn)
        
        # Test handedness analysis
        print("🔄 Testing handedness analysis...")
        handedness = analyzer.analyze_team_vs_handedness(147, lookback_days=30)  # Example team
        
        if 'error' not in handedness:
            print(f"   📊 vs LHP: {handedness.get('vs_LHP', {}).get('woba', 'N/A')}")
            print(f"   📊 vs RHP: {handedness.get('vs_RHP', {}).get('woba', 'N/A')}")
            print(f"   🎯 Advantage: {handedness.get('platoon_advantage', 'N/A')}")
            print(f"   💰 Insight: {handedness.get('betting_insight', 'N/A')}")
        else:
            print(f"   ❌ {handedness['error']}")
        
        # Test lineup analysis
        print(f"\n👥 Testing lineup analysis...")
        lineup = analyzer.analyze_lineup_protection(147, "2024-04-15")  # Example
        
        if 'error' not in lineup:
            assessment = lineup.get('overall_assessment', 'N/A')
            print(f"   📊 Overall lineup: {assessment}")
            print(f"   💰 Insight: {lineup.get('betting_insight', 'N/A')}")
        else:
            print(f"   ❌ {lineup['error']}")
        
        # Test late-inning performance
        print(f"\n🕐 Testing late-inning performance...")
        late_innings = analyzer.analyze_late_inning_performance(147)
        
        if 'error' not in late_innings:
            rating = late_innings.get('late_inning_rating', 'N/A')
            diff = late_innings.get('late_inning_differential', 'N/A')
            print(f"   📊 Late-inning rating: {rating}")
            print(f"   📈 Performance diff: {diff}")
            print(f"   💰 Insight: {late_innings.get('betting_insight', 'N/A')}")
        else:
            print(f"   ❌ {late_innings['error']}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()