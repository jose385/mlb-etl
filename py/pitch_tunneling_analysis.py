#!/usr/bin/env python3
"""
pitch_tunneling_analysis.py - Advanced pitch tunneling analysis for MLB
Analyzes how well different pitch types "tunnel" together (look the same initially)
This creates deception and affects betting outcomes for strikeout props
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import psycopg2
from typing import Dict, List, Optional, Tuple
import math
from sklearn.metrics.pairwise import euclidean_distances
import warnings
warnings.filterwarnings('ignore')
try:
    from py.imports import setup_imports
    setup_imports()
except ImportError:
    pass

from py.config import require_config, get_config
class PitchTunnelingAnalyzer:
    """Analyzes pitch tunneling effectiveness for deception and betting insights"""
    
    def __init__(self, conn):
        self.conn = conn
        
        # Tunnel measurement constants
        self.TUNNEL_POINT_DISTANCE = 175  # Distance from plate where tunneling is measured (inches)
        self.HOME_PLATE_DISTANCE = 720    # Total distance from mound to plate (inches)
        
    def analyze_pitcher_tunneling(self, pitcher_id: int, lookback_days: int = 60) -> Dict:
        """Comprehensive tunneling analysis for a specific pitcher"""
        
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=lookback_days)
        
        # Get pitcher's recent Statcast data
        query = """
        SELECT 
            s.game_date,
            s.game_pk,
            s.pitcher,
            s.pitch_type,
            s.pitch_name,
            s.release_speed,
            s.release_pos_x,
            s.release_pos_y,
            s.release_pos_z,
            s.pfx_x,
            s.pfx_z,
            s.vx0,
            s.vy0,
            s.vz0,
            s.ax,
            s.ay,
            s.az,
            s.plate_x,
            s.plate_z,
            s.sz_top,
            s.sz_bot,
            s.zone,
            s.description,
            s.events,
            s.balls,
            s.strikes,
            s.at_bat_number,
            s.pitch_number
        FROM statcast s
        WHERE s.pitcher = %s
        AND s.game_date >= %s
        AND s.game_date <= %s
        AND s.pitch_type IS NOT NULL
        AND s.release_pos_x IS NOT NULL
        AND s.release_pos_y IS NOT NULL
        AND s.release_pos_z IS NOT NULL
        ORDER BY s.game_date DESC, s.at_bat_number, s.pitch_number
        """
        
        try:
            df = pd.read_sql(query, self.conn, params=[pitcher_id, start_date, end_date])
            
            if df.empty:
                return {
                    "pitcher_id": pitcher_id,
                    "error": "No recent pitching data found",
                    "betting_recommendation": "NO DATA"
                }
            
            # Group by pitch type and analyze combinations
            pitch_types = df['pitch_type'].value_counts()
            
            # Only analyze pitch types with sufficient sample size
            significant_pitches = pitch_types[pitch_types >= 20].index.tolist()
            
            if len(significant_pitches) < 2:
                return {
                    "pitcher_id": pitcher_id,
                    "error": "Insufficient pitch type variety for tunneling analysis",
                    "betting_recommendation": "INSUFFICIENT REPERTOIRE"
                }
            
            # Analyze all significant pitch combinations
            tunnel_combinations = {}
            best_tunnels = []
            
            for i, pitch1 in enumerate(significant_pitches):
                for pitch2 in significant_pitches[i+1:]:
                    tunnel_analysis = self.analyze_pitch_pair_tunneling(df, pitch1, pitch2)
                    
                    if tunnel_analysis['tunnel_quality_score'] > 0:
                        combination_key = f"{pitch1}_{pitch2}"
                        tunnel_combinations[combination_key] = tunnel_analysis
                        
                        if tunnel_analysis['tunnel_quality_score'] >= 70:
                            best_tunnels.append((pitch1, pitch2, tunnel_analysis))
            
            # Overall pitcher tunneling assessment
            overall_assessment = self.assess_overall_tunneling(tunnel_combinations, df)
            
            # Betting implications
            betting_analysis = self.calculate_tunneling_betting_impact(
                tunnel_combinations, best_tunnels, overall_assessment, df
            )
            
            return {
                "pitcher_id": pitcher_id,
                "analysis_date": end_date,
                "analysis_period": f"{start_date} to {end_date}",
                "total_pitches": len(df),
                "pitch_types_analyzed": significant_pitches,
                "pitch_type_counts": pitch_types.to_dict(),
                "tunnel_combinations": tunnel_combinations,
                "best_tunnels": [(t[0], t[1], t[2]['tunnel_quality_score']) for t in best_tunnels],
                "overall_tunneling_grade": overall_assessment['grade'],
                "deception_score": overall_assessment['deception_score'],
                "average_tunnel_quality": overall_assessment['avg_tunnel_quality'],
                **betting_analysis
            }
            
        except Exception as e:
            return {
                "pitcher_id": pitcher_id,
                "error": f"Database error: {e}",
                "betting_recommendation": "ERROR"
            }
    
    def analyze_pitch_pair_tunneling(self, df: pd.DataFrame, pitch_type_1: str, 
                                   pitch_type_2: str) -> Dict:
        """Analyze tunneling between two specific pitch types"""
        
        # Filter data for each pitch type
        pitch1_data = df[df['pitch_type'] == pitch_type_1].copy()
        pitch2_data = df[df['pitch_type'] == pitch_type_2].copy()
        
        if len(pitch1_data) < 10 or len(pitch2_data) < 10:
            return {"tunnel_quality_score": 0, "error": "Insufficient data for analysis"}
        
        # Calculate release point similarity
        release_similarity = self.calculate_release_point_similarity(pitch1_data, pitch2_data)
        
        # Calculate tunnel point trajectory similarity
        tunnel_similarity = self.calculate_tunnel_point_similarity(pitch1_data, pitch2_data)
        
        # Calculate movement differential (at the plate)
        movement_analysis = self.calculate_movement_differential(pitch1_data, pitch2_data)
        
        # Calculate velocity differential
        velocity_analysis = self.calculate_velocity_differential(pitch1_data, pitch2_data)
        
        # Usage and sequence analysis
        usage_analysis = self.analyze_pitch_usage_patterns(df, pitch_type_1, pitch_type_2)
        
        # Effectiveness analysis
        effectiveness = self.analyze_tunneling_effectiveness(df, pitch1_data, pitch2_data, 
                                                           pitch_type_1, pitch_type_2)
        
        # Calculate overall tunnel quality score
        tunnel_quality = self.calculate_tunnel_quality_score(
            release_similarity, tunnel_similarity, movement_analysis, 
            velocity_analysis, effectiveness
        )
        
        return {
            "pitch_type_1": pitch_type_1,
            "pitch_type_2": pitch_type_2,
            "pitch_1_count": len(pitch1_data),
            "pitch_2_count": len(pitch2_data),
            
            # Release point analysis
            "release_point_diff_x": release_similarity['horizontal_diff'],
            "release_point_diff_y": release_similarity['vertical_diff'], 
            "release_point_diff_z": release_similarity['depth_diff'],
            "release_point_similarity": release_similarity['similarity_score'],
            
            # Tunnel analysis
            "tunnel_break_distance": tunnel_similarity['break_distance'],
            "tunnel_quality_score": tunnel_quality,
            
            # Movement analysis
            "horizontal_break_diff": movement_analysis['horizontal_diff'],
            "vertical_break_diff": movement_analysis['vertical_diff'],
            "movement_contrast": movement_analysis['contrast_score'],
            
            # Velocity analysis
            "velocity_diff": velocity_analysis['velocity_diff'],
            "velocity_similarity": velocity_analysis['similarity_score'],
            
            # Usage patterns
            **usage_analysis,
            
            # Effectiveness
            **effectiveness
        }
    
    def calculate_release_point_similarity(self, pitch1_data: pd.DataFrame, 
                                         pitch2_data: pd.DataFrame) -> Dict:
        """Calculate how similar release points are between pitch types"""
        
        # Average release points for each pitch
        pitch1_release = {
            'x': pitch1_data['release_pos_x'].mean(),
            'y': pitch1_data['release_pos_y'].mean(), 
            'z': pitch1_data['release_pos_z'].mean()
        }
        
        pitch2_release = {
            'x': pitch2_data['release_pos_x'].mean(),
            'y': pitch2_data['release_pos_y'].mean(),
            'z': pitch2_data['release_pos_z'].mean()
        }
        
        # Calculate differences (in inches)
        horizontal_diff = abs(pitch1_release['x'] - pitch2_release['x'])
        vertical_diff = abs(pitch1_release['y'] - pitch2_release['y'])
        depth_diff = abs(pitch1_release['z'] - pitch2_release['z'])
        
        # Calculate 3D distance
        total_distance = math.sqrt(horizontal_diff**2 + vertical_diff**2 + depth_diff**2)
        
        # Convert to similarity score (0-100, higher is better)
        # Perfect similarity = 0 distance, good tunneling typically < 3 inches
        similarity_score = max(0, 100 - (total_distance * 20))
        
        return {
            'horizontal_diff': round(horizontal_diff, 2),
            'vertical_diff': round(vertical_diff, 2),
            'depth_diff': round(depth_diff, 2),
            'total_distance': round(total_distance, 2),
            'similarity_score': round(similarity_score, 1)
        }
    
    def calculate_tunnel_point_similarity(self, pitch1_data: pd.DataFrame, 
                                        pitch2_data: pd.DataFrame) -> Dict:
        """Calculate where pitches start to diverge (tunnel break point)"""
        
        # For each pitch, calculate trajectory at various points
        break_distances = []
        
        # Check trajectory similarity at multiple points approaching the plate
        for distance_from_plate in [200, 175, 150, 125, 100]:  # inches from plate
            
            # Calculate projected position at this distance for both pitches
            pitch1_positions = []
            pitch2_positions = []
            
            for _, pitch in pitch1_data.iterrows():
                if pd.notna(pitch['vx0']) and pd.notna(pitch['vy0']) and pd.notna(pitch['vz0']):
                    pos = self.calculate_pitch_position_at_distance(pitch, distance_from_plate)
                    if pos:
                        pitch1_positions.append(pos)
            
            for _, pitch in pitch2_data.iterrows():
                if pd.notna(pitch['vx0']) and pd.notna(pitch['vy0']) and pd.notna(pitch['vz0']):
                    pos = self.calculate_pitch_position_at_distance(pitch, distance_from_plate)
                    if pos:
                        pitch2_positions.append(pos)
            
            if len(pitch1_positions) > 5 and len(pitch2_positions) > 5:
                # Calculate average positions
                avg_pitch1 = np.mean(pitch1_positions, axis=0)
                avg_pitch2 = np.mean(pitch2_positions, axis=0)
                
                # Calculate distance between average trajectories
                trajectory_distance = np.linalg.norm(avg_pitch1 - avg_pitch2)
                
                # If trajectories are significantly different, this is the break point
                if trajectory_distance > 2.0:  # 2 inches separation
                    break_distances.append(distance_from_plate)
        
        # Tunnel break distance (where they start to separate)
        if break_distances:
            tunnel_break = max(break_distances)  # Furthest point they're still similar
        else:
            tunnel_break = self.TUNNEL_POINT_DISTANCE  # Default tunnel point
        
        return {
            'break_distance': tunnel_break,
            'tunnel_depth': self.HOME_PLATE_DISTANCE - tunnel_break  # How deep the tunnel goes
        }
    
    def calculate_pitch_position_at_distance(self, pitch_data: pd.Series, 
                                           distance_from_plate: float) -> Optional[np.ndarray]:
        """Calculate where a pitch will be at a specific distance from the plate"""
        
        try:
            # Initial position and velocity
            x0, y0, z0 = pitch_data['release_pos_x'], pitch_data['release_pos_y'], pitch_data['release_pos_z']
            vx0, vy0, vz0 = pitch_data['vx0'], pitch_data['vy0'], pitch_data['vz0']
            ax, ay, az = pitch_data['ax'], pitch_data['ay'], pitch_data['az']
            
            # Time to reach the distance (assuming constant velocity in y direction initially)
            distance_to_travel = self.HOME_PLATE_DISTANCE - distance_from_plate
            t = distance_to_travel / abs(vy0) if vy0 != 0 else 0
            
            if t <= 0:
                return None
            
            # Calculate position using kinematic equations
            x = x0 + vx0 * t + 0.5 * ax * t**2
            y = y0 + vy0 * t + 0.5 * ay * t**2
            z = z0 + vz0 * t + 0.5 * az * t**2
            
            return np.array([x, y, z])
            
        except (ValueError, TypeError):
            return None
    
    def calculate_movement_differential(self, pitch1_data: pd.DataFrame, 
                                     pitch2_data: pd.DataFrame) -> Dict:
        """Calculate how different the final movement is between pitches"""
        
        # Average movement for each pitch type
        pitch1_movement = {
            'horizontal': pitch1_data['pfx_x'].mean(),
            'vertical': pitch1_data['pfx_z'].mean()
        }
        
        pitch2_movement = {
            'horizontal': pitch2_data['pfx_x'].mean(), 
            'vertical': pitch2_data['pfx_z'].mean()
        }
        
        # Calculate differences
        horizontal_diff = abs(pitch1_movement['horizontal'] - pitch2_movement['horizontal'])
        vertical_diff = abs(pitch1_movement['vertical'] - pitch2_movement['vertical'])
        
        # Total movement contrast (higher = better for tunneling deception)
        total_movement_diff = math.sqrt(horizontal_diff**2 + vertical_diff**2)
        
        # Convert to contrast score (0-100, higher = more deceptive)
        # Good tunneling pairs have significant movement differences (8+ inches)
        contrast_score = min(100, total_movement_diff * 8)
        
        return {
            'horizontal_diff': round(horizontal_diff, 2),
            'vertical_diff': round(vertical_diff, 2), 
            'total_movement_diff': round(total_movement_diff, 2),
            'contrast_score': round(contrast_score, 1)
        }
    
    def calculate_velocity_differential(self, pitch1_data: pd.DataFrame, 
                                      pitch2_data: pd.DataFrame) -> Dict:
        """Calculate velocity similarity/difference between pitches"""
        
        avg_velo1 = pitch1_data['release_speed'].mean()
        avg_velo2 = pitch2_data['release_speed'].mean()
        
        velocity_diff = abs(avg_velo1 - avg_velo2)
        
        # Similarity score (0-100, higher = more similar out of hand)
        # Good tunneling: similar velocities initially (within 3-5 mph)
        if velocity_diff <= 2:
            similarity_score = 100
        elif velocity_diff <= 5:
            similarity_score = 90 - (velocity_diff - 2) * 10
        elif velocity_diff <= 10:
            similarity_score = 60 - (velocity_diff - 5) * 8
        else:
            similarity_score = max(0, 20 - (velocity_diff - 10) * 2)
        
        return {
            'pitch1_avg_velocity': round(avg_velo1, 1),
            'pitch2_avg_velocity': round(avg_velo2, 1),
            'velocity_diff': round(velocity_diff, 1),
            'similarity_score': round(similarity_score, 1)
        }
    
    def analyze_pitch_usage_patterns(self, df: pd.DataFrame, pitch_type_1: str, 
                                   pitch_type_2: str) -> Dict:
        """Analyze how often pitches are used and sequenced together"""
        
        total_pitches = len(df)
        pitch1_count = len(df[df['pitch_type'] == pitch_type_1])
        pitch2_count = len(df[df['pitch_type'] == pitch_type_2])
        
        # Usage rates
        pitch1_usage = pitch1_count / total_pitches
        pitch2_usage = pitch2_count / total_pitches
        
        # Sequence analysis - how often they're thrown back-to-back
        df_sorted = df.sort_values(['game_date', 'at_bat_number', 'pitch_number'])
        df_sorted['next_pitch_type'] = df_sorted['pitch_type'].shift(-1)
        
        # Count sequences
        p1_to_p2 = len(df_sorted[(df_sorted['pitch_type'] == pitch_type_1) & 
                                (df_sorted['next_pitch_type'] == pitch_type_2)])
        p2_to_p1 = len(df_sorted[(df_sorted['pitch_type'] == pitch_type_2) & 
                                (df_sorted['next_pitch_type'] == pitch_type_1)])
        
        total_sequences = p1_to_p2 + p2_to_p1
        sequence_frequency = total_sequences / (total_pitches - 1) if total_pitches > 1 else 0
        
        return {
            'pitch_1_usage_rate': round(pitch1_usage, 3),
            'pitch_2_usage_rate': round(pitch2_usage, 3),
            'sequence_frequency': round(sequence_frequency, 3),
            'total_sequences': total_sequences
        }
    
    def analyze_tunneling_effectiveness(self, df: pd.DataFrame, pitch1_data: pd.DataFrame,
                                      pitch2_data: pd.DataFrame, pitch_type_1: str, 
                                      pitch_type_2: str) -> Dict:
        """Analyze how effective the tunneling is at generating whiffs/strikes"""
        
        # Calculate whiff rates for each pitch
        pitch1_swings = pitch1_data[pitch1_data['description'].isin(['swinging_strike', 'foul', 'hit_into_play'])]
        pitch1_whiffs = len(pitch1_data[pitch1_data['description'] == 'swinging_strike'])
        pitch1_whiff_rate = pitch1_whiffs / len(pitch1_swings) if len(pitch1_swings) > 0 else 0
        
        pitch2_swings = pitch2_data[pitch2_data['description'].isin(['swinging_strike', 'foul', 'hit_into_play'])]
        pitch2_whiffs = len(pitch2_data[pitch2_data['description'] == 'swinging_strike'])
        pitch2_whiff_rate = pitch2_whiffs / len(pitch2_swings) if len(pitch2_swings) > 0 else 0
        
        # Chase rate (swings on pitches outside the zone)
        pitch1_outside = pitch1_data[pitch1_data['zone'] > 9]  # Zones 11-14 are outside
        pitch1_chases = len(pitch1_outside[pitch1_outside['description'].isin(['swinging_strike', 'foul', 'hit_into_play'])])
        pitch1_chase_rate = pitch1_chases / len(pitch1_outside) if len(pitch1_outside) > 0 else 0
        
        pitch2_outside = pitch2_data[pitch2_data['zone'] > 9]
        pitch2_chases = len(pitch2_outside[pitch2_outside['description'].isin(['swinging_strike', 'foul', 'hit_into_play'])])
        pitch2_chase_rate = pitch2_chases / len(pitch2_outside) if len(pitch2_outside) > 0 else 0
        
        # Called strike rate (strikes that weren't swung at)
        pitch1_called_strikes = len(pitch1_data[pitch1_data['description'] == 'called_strike'])
        pitch1_called_balls = len(pitch1_data[pitch1_data['description'] == 'ball'])
        pitch1_called_strike_rate = pitch1_called_strikes / (pitch1_called_strikes + pitch1_called_balls) if (pitch1_called_strikes + pitch1_called_balls) > 0 else 0
        
        pitch2_called_strikes = len(pitch2_data[pitch2_data['description'] == 'called_strike'])  
        pitch2_called_balls = len(pitch2_data[pitch2_data['description'] == 'ball'])
        pitch2_called_strike_rate = pitch2_called_strikes / (pitch2_called_strikes + pitch2_called_balls) if (pitch2_called_strikes + pitch2_called_balls) > 0 else 0
        
        return {
            'pitch1_whiff_rate': round(pitch1_whiff_rate, 3),
            'pitch2_whiff_rate': round(pitch2_whiff_rate, 3),
            'pitch1_chase_rate': round(pitch1_chase_rate, 3),
            'pitch2_chase_rate': round(pitch2_chase_rate, 3),
            'pitch1_called_strike_rate': round(pitch1_called_strike_rate, 3),
            'pitch2_called_strike_rate': round(pitch2_called_strike_rate, 3),
            'combined_whiff_rate': round((pitch1_whiff_rate + pitch2_whiff_rate) / 2, 3)
        }
    
    def calculate_tunnel_quality_score(self, release_sim: Dict, tunnel_sim: Dict, 
                                     movement: Dict, velocity: Dict, effectiveness: Dict) -> float:
        """Calculate overall tunnel quality score (0-100)"""
        
        # Weight different factors
        release_weight = 0.30    # 30% - release point similarity
        movement_weight = 0.25   # 25% - movement differential  
        velocity_weight = 0.20   # 20% - velocity similarity
        effectiveness_weight = 0.25  # 25% - actual effectiveness
        
        # Component scores
        release_score = release_sim['similarity_score']
        movement_score = movement['contrast_score']
        velocity_score = velocity['similarity_score']
        
        # Effectiveness score (based on whiff rates)
        combined_whiff = effectiveness['combined_whiff_rate']
        effectiveness_score = min(100, combined_whiff * 400)  # 25% whiff rate = 100 score
        
        # Calculate weighted total
        total_score = (
            release_score * release_weight +
            movement_score * movement_weight +
            velocity_score * velocity_weight +
            effectiveness_score * effectiveness_weight
        )
        
        return round(total_score, 1)
    
    def assess_overall_tunneling(self, tunnel_combinations: Dict, df: pd.DataFrame) -> Dict:
        """Assess pitcher's overall tunneling ability"""
        
        if not tunnel_combinations:
            return {
                'grade': 'F',
                'deception_score': 0,
                'avg_tunnel_quality': 0,
                'strengths': [],
                'weaknesses': ['No effective pitch tunneling detected']
            }
        
        # Calculate average tunnel quality
        quality_scores = [combo['tunnel_quality_score'] for combo in tunnel_combinations.values()]
        avg_quality = np.mean(quality_scores)
        
        # Count high-quality tunnels
        excellent_tunnels = [score for score in quality_scores if score >= 80]
        good_tunnels = [score for score in quality_scores if 60 <= score < 80]
        
        # Overall deception score
        deception_score = min(100, avg_quality + len(excellent_tunnels) * 10)
        
        # Grade assignment
        if avg_quality >= 75 and len(excellent_tunnels) >= 2:
            grade = 'A+'
        elif avg_quality >= 70:
            grade = 'A'
        elif avg_quality >= 60:
            grade = 'B'  
        elif avg_quality >= 50:
            grade = 'C'
        elif avg_quality >= 35:
            grade = 'D'
        else:
            grade = 'F'
        
        # Identify strengths and weaknesses
        strengths = []
        weaknesses = []
        
        if len(excellent_tunnels) >= 2:
            strengths.append(f"Multiple elite tunnel pairs ({len(excellent_tunnels)})")
        elif len(excellent_tunnels) == 1:
            strengths.append("One elite tunnel pair")
        
        if len(good_tunnels) >= 2:
            strengths.append(f"Good secondary tunneling ({len(good_tunnels)} pairs)")
        
        if avg_quality < 40:
            weaknesses.append("Poor overall tunnel quality")
        
        if len(quality_scores) < 3:
            weaknesses.append("Limited pitch repertoire for tunneling")
        
        return {
            'grade': grade,
            'deception_score': round(deception_score, 1),
            'avg_tunnel_quality': round(avg_quality, 1),
            'excellent_tunnels': len(excellent_tunnels),
            'good_tunnels': len(good_tunnels),
            'total_combinations': len(tunnel_combinations),
            'strengths': strengths,
            'weaknesses': weaknesses
        }
    
    def calculate_tunneling_betting_impact(self, tunnel_combinations: Dict, best_tunnels: List,
                                         assessment: Dict, df: pd.DataFrame) -> Dict:
        """Calculate how tunneling affects betting outcomes"""
        
        # Overall strikeout rate for this pitcher
        total_abs = len(df.groupby(['game_date', 'at_bat_number'])['events'].last())
        strikeouts = len(df[df['events'].str.contains('strikeout', na=False)])
        overall_k_rate = strikeouts / total_abs if total_abs > 0 else 0
        
        # Calculate impact on strikeout props
        tunnel_grade = assessment['grade']
        deception_score = assessment['deception_score']
        
        # Estimate strikeout prop impact
        if deception_score >= 80:
            k_prop_impact = 1.15  # 15% boost to strikeout expectation
            impact_description = "Elite tunneling significantly boosts strikeout potential"
        elif deception_score >= 60:
            k_prop_impact = 1.08  # 8% boost
            impact_description = "Good tunneling provides moderate strikeout boost"
        elif deception_score >= 40:
            k_prop_impact = 1.02  # 2% boost  
            impact_description = "Average tunneling provides slight strikeout boost"
        else:
            k_prop_impact = 0.95  # 5% penalty for poor tunneling
            impact_description = "Poor tunneling may hurt strikeout potential"
        
        # Generate specific betting insights
        betting_insights = []
        
        if len(best_tunnels) >= 2:
            betting_insights.append("BACK strikeout props - Multiple elite tunnel pairs")
        elif len(best_tunnels) == 1:
            betting_insights.append("LEAN strikeout props - One strong tunnel pair")
        
        if deception_score >= 75:
            betting_insights.append("High deception pitcher - opponent may struggle")
        
        # Overall recommendation
        if deception_score >= 70 and overall_k_rate >= 0.20:
            betting_recommendation = "STRONG BET - Back strikeout props"
        elif deception_score >= 50:
            betting_recommendation = "MODERATE BET - Lean strikeout props OVER"  
        elif deception_score <= 30:
            betting_recommendation = "FADE - Avoid or bet strikeout props UNDER"
        else:
            betting_recommendation = "NEUTRAL - No strong tunneling edge"
        
        return {
            'overall_k_rate': round(overall_k_rate, 3),
            'strikeout_prop_impact': round(k_prop_impact, 2),
            'impact_description': impact_description,
            'betting_insights': betting_insights,
            'betting_recommendation': betting_recommendation,
            'confidence_level': 'HIGH' if len(df) >= 100 else 'MEDIUM' if len(df) >= 50 else 'LOW'
        }

def update_pitcher_tunneling_data(conn, lookback_days: int = 60, min_pitches: int = 100):
    """Update tunneling analysis for all active pitchers"""
    
    print(f"🌪️ Updating pitch tunneling analysis for active pitchers...")
    
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=lookback_days)
    
    # Find pitchers with sufficient recent data
    query = f"""
    SELECT 
        s.pitcher,
        r.person_full_name as pitcher_name,
        COUNT(*) as total_pitches,
        COUNT(DISTINCT s.pitch_type) as pitch_varieties
    FROM statcast s
    LEFT JOIN roster r ON s.pitcher = r.person_id 
                       AND s.game_date = r.game_date
    WHERE s.game_date >= '{start_date}'
    AND s.pitch_type IS NOT NULL
    AND s.release_pos_x IS NOT NULL
    AND s.release_pos_y IS NOT NULL
    AND s.release_pos_z IS NOT NULL
    GROUP BY s.pitcher, r.person_full_name
    HAVING COUNT(*) >= {min_pitches}
    AND COUNT(DISTINCT s.pitch_type) >= 3  -- Need variety for tunneling
    ORDER BY total_pitches DESC
    LIMIT 100  -- Top 100 most active pitchers
    """
    
    try:
        pitchers_df = pd.read_sql(query, conn)
        print(f"📊 Found {len(pitchers_df)} pitchers with sufficient data for tunneling analysis")
        
        analyzer = PitchTunnelingAnalyzer(conn)
        updated_count = 0
        
        cur = conn.cursor()
        
        for _, pitcher in pitchers_df.iterrows():
            pitcher_id = pitcher['pitcher']
            pitcher_name = pitcher.get('pitcher_name', '')
            
            print(f"   🌪️ Analyzing: {pitcher_name} ({pitcher['total_pitches']} pitches)")
            
            # Analyze pitcher's tunneling
            analysis = analyzer.analyze_pitcher_tunneling(pitcher_id, lookback_days)
            
            if 'error' in analysis:
                print(f"   ❌ Error: {analysis['error']}")
                continue
            
            # Insert tunneling combinations into database
            for combo_name, combo_data in analysis.get('tunnel_combinations', {}).items():
                pitch_types = combo_name.split('_')
                if len(pitch_types) == 2:
                    
                    # Insert/update tunneling record
                    upsert_sql = """
                    INSERT INTO public.pitch_tunneling (
                        pitcher_id, pitcher_name, game_date,
                        pitch_type_1, pitch_type_2,
                        release_point_diff_x, release_point_diff_y, release_point_diff_z,
                        release_point_similarity,
                        tunnel_break_distance, tunnel_quality_score,
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
                        strikeout_prop_impact = EXCLUDED.strikeout_prop_impact
                    """
                    
                    cur.execute(upsert_sql, (
                        pitcher_id, pitcher_name, end_date,
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
                        combo_data.get('pitch2_whiff_rate', 0) - combo_data.get('pitch1_whiff_rate', 0),  # Improvement
                        combo_data.get('pitch2_chase_rate', 0) - combo_data.get('pitch1_chase_rate', 0),
                        combo_data.get('pitch_1_count', 0),
                        combo_data.get('pitch_2_count', 0), 
                        combo_data.get('total_sequences', 0),
                        analysis.get('confidence_level', 'MEDIUM'),
                        analysis.get('strikeout_prop_impact', 1.0),
                        analysis.get('betting_recommendation', '')
                    ))
                    
                    updated_count += 1
        
        conn.commit()
        print(f"✅ Updated {updated_count} tunneling combinations in database")
        
    except Exception as e:
        print(f"❌ Error updating tunneling data: {e}")
        conn.rollback()

def get_todays_tunneling_edges(conn, game_date: str = None, min_tunnel_quality: int = 60) -> List[Dict]:
    """Get today's best tunneling-based betting opportunities"""
    
    if game_date is None:
        game_date = datetime.now().strftime('%Y-%m-%d')
    
    print(f"🌪️ Finding today's best tunneling edges for {game_date}...")
    
    # Get today's probable pitchers with good tunneling
    query = f"""
    WITH todays_pitchers AS (
        SELECT DISTINCT
            l.game_pk,
            l.person_id as pitcher_id,
            l.person_full_name as pitcher_name,
            l.team_id,
            l.side as pitching_team
        FROM lineup l
        WHERE l.game_date = '{game_date}'
        AND l.position_code = '1'  -- Pitchers only
    ),
    pitcher_tunneling AS (
        SELECT 
            tp.*,
            AVG(pt.tunnel_quality_score) as avg_tunnel_quality,
            MAX(pt.tunnel_quality_score) as best_tunnel_quality,
            COUNT(*) as tunnel_pairs,
            AVG(pt.strikeout_prop_impact) as avg_k_impact,
            STRING_AGG(pt.betting_insight, '; ') as betting_insights
        FROM todays_pitchers tp
        JOIN pitch_tunneling pt ON tp.pitcher_id = pt.pitcher_id
        WHERE pt.game_date >= CURRENT_DATE - INTERVAL '60 days'
        AND pt.tunnel_quality_score >= {min_tunnel_quality}
        GROUP BY tp.game_pk, tp.pitcher_id, tp.pitcher_name, tp.team_id, tp.pitching_team
        HAVING AVG(pt.tunnel_quality_score) >= {min_tunnel_quality}
    )
    SELECT * FROM pitcher_tunneling
    ORDER BY avg_tunnel_quality DESC, best_tunnel_quality DESC
    LIMIT 10  -- Top 10 tunneling opportunities
    """
    
    try:
        df = pd.read_sql(query, conn)
        
        if df.empty:
            return [{"message": f"No strong tunneling edges found for {game_date}"}]
        
        results = []
        for _, row in df.iterrows():
            results.append({
                "game_pk": row['game_pk'],
                "pitcher_name": row['pitcher_name'],
                "pitching_team": row['pitching_team'],
                "avg_tunnel_quality": round(row['avg_tunnel_quality'], 1),
                "best_tunnel_quality": round(row['best_tunnel_quality'], 1),
                "tunnel_pairs": int(row['tunnel_pairs']),
                "strikeout_prop_impact": round(row['avg_k_impact'], 2),
                "betting_insights": row['betting_insights'],
                "tunneling_grade": (
                    'A+' if row['avg_tunnel_quality'] >= 80 else
                    'A' if row['avg_tunnel_quality'] >= 70 else
                    'B' if row['avg_tunnel_quality'] >= 60 else 'C'
                )
            })
        
        return results
        
    except Exception as e:
        return [{"error": f"Database error: {e}"}]

def print_tunneling_report(tunneling_edges: List[Dict]):
    """Print formatted tunneling analysis report"""
    
    print(f"\n🌪️ PITCH TUNNELING ANALYSIS REPORT")
    print("=" * 70)
    
    if not tunneling_edges or (len(tunneling_edges) == 1 and 'message' in tunneling_edges[0]):
        print("📊 No strong tunneling edges found today")
        return
    
    elite_tunnelers = [e for e in tunneling_edges if e.get('avg_tunnel_quality', 0) >= 80]
    good_tunnelers = [e for e in tunneling_edges if 60 <= e.get('avg_tunnel_quality', 0) < 80]
    
    if elite_tunnelers:
        print(f"\n🌟 ELITE TUNNELING PITCHERS ({len(elite_tunnelers)} found):")
        print("-" * 60)
        
        for pitcher in elite_tunnelers:
            print(f"\n⭐ {pitcher['pitcher_name']} ({pitcher['pitching_team']})")
            print(f"   🎯 Tunnel Quality: {pitcher['avg_tunnel_quality']}/100 (Grade: {pitcher['tunneling_grade']})")
            print(f"   🌪️ Best Tunnel: {pitcher['best_tunnel_quality']}/100")
            print(f"   📊 Tunnel Pairs: {pitcher['tunnel_pairs']}")
            print(f"   ⚡ K Prop Impact: {pitcher['strikeout_prop_impact']:.2f}x")
            print(f"   💰 BETTING: {pitcher['betting_insights']}")
    
    if good_tunnelers:
        print(f"\n📈 GOOD TUNNELING PITCHERS ({len(good_tunnelers)} found):")
        print("-" * 60)
        
        for pitcher in good_tunnelers:
            print(f"\n• {pitcher['pitcher_name']}: {pitcher['avg_tunnel_quality']}/100 quality")
            print(f"  Impact: {pitcher['strikeout_prop_impact']:.2f}x strikeout boost")

def main():
    """Test pitch tunneling analysis"""
    
    import os
    config = require_config(require_database=True)
    dsn = config.PG_DSN
    if not dsn:
        print("❌ PG_DSN environment variable must be set")
        return
    
    try:
        conn = psycopg2.connect(dsn)
        
        print("🌪️ Pitch Tunneling Analysis")
        print("=" * 50)
        
        # Test individual pitcher analysis
        analyzer = PitchTunnelingAnalyzer(conn)
        
        # Example: Analyze a specific pitcher (replace with real pitcher ID)
        print("🔍 Testing individual pitcher tunneling analysis...")
        result = analyzer.analyze_pitcher_tunneling(434378, lookback_days=60)  # Example pitcher
        
        if 'error' not in result:
            print(f"📊 Sample tunneling analysis:")
            print(f"   Pitcher ID: {result.get('pitcher_id')}")
            print(f"   Overall Grade: {result.get('overall_tunneling_grade', 'N/A')}")
            print(f"   Deception Score: {result.get('deception_score', 0)}/100")
            print(f"   Best Tunnels: {result.get('best_tunnels', [])}")
            print(f"   Betting Rec: {result.get('betting_recommendation', 'N/A')}")
        else:
            print(f"   ❌ {result['error']}")
        
        # Get today's best tunneling edges
        print(f"\n🎯 Getting today's best tunneling edges...")
        edges = get_todays_tunneling_edges(conn, min_tunnel_quality=50)
        print_tunneling_report(edges)
        
        # Update tunneling database (uncomment to run full update)
        # print(f"\n🔄 Updating tunneling database...")
        # update_pitcher_tunneling_data(conn, lookback_days=60)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()