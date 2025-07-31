#!/usr/bin/env python3
"""
enhanced_betting_data_retrieval.py - Updated for enhanced schema
Uses pre-calculated recent_stats and enhanced game_info tables
Works with the 9-table enhanced schema
"""

import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging

class EnhancedBettingDataRetriever:
    """
    Enhanced betting data retriever for 9-table schema
    Uses pre-calculated stats and enhanced game context
    """
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.logger = logging.getLogger(__name__)
    
    def get_connection(self):
        """Get database connection with proper error handling"""
        try:
            return psycopg2.connect(self.connection_string)
        except Exception as e:
            self.logger.error(f"Database connection failed: {e}")
            raise
    
    def get_game_context(self, game_pk: int) -> Dict:
        """NEW: Get comprehensive game context from game_info table"""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                query = """
                SELECT gi.*, vf.run_factor, vf.hr_factor, vf.pitcher_friendly_score,
                       vf.over_under_tendency, vf.dome_stadium, vf.elevation_feet
                FROM game_info gi
                LEFT JOIN venue_factors vf ON gi.venue_name = vf.venue_name
                WHERE gi.game_pk = %s
                """
                
                cur.execute(query, (game_pk,))
                result = cur.fetchone()
                
                if result:
                    return {
                        'game_pk': result['game_pk'],
                        'game_date': result['game_date'],
                        'home_team': result['home_team'],
                        'away_team': result['away_team'],
                        'venue_name': result['venue_name'],
                        'home_starting_pitcher': result['home_starting_pitcher'],
                        'away_starting_pitcher': result['away_starting_pitcher'],
                        'home_starter_name': result['home_starter_name'],
                        'away_starter_name': result['away_starter_name'],
                        'series_game_number': result['series_game_number'],
                        'game_status': result['game_status'],
                        
                        # Venue factors
                        'run_factor': result['run_factor'] or 1.0,
                        'hr_factor': result['hr_factor'] or 1.0,
                        'pitcher_friendly_score': result['pitcher_friendly_score'] or 5,
                        'over_under_tendency': result['over_under_tendency'] or 0.5,
                        'dome_stadium': result['dome_stadium'] or False,
                        'elevation_feet': result['elevation_feet'] or 0,
                        
                        # Final scores if available
                        'home_score': result['home_score'],
                        'away_score': result['away_score'],
                        'winning_team': result['winning_team'],
                    }
                else:
                    return {'game_pk': game_pk, 'error': 'Game context not found'}
    
    def get_recent_pitcher_stats(self, pitcher_id: int, target_date: str, stat_type: str = 'pitching_5starts') -> Dict:
        """ENHANCED: Get pre-calculated recent pitcher stats"""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                query = """
                SELECT * FROM recent_stats 
                WHERE player_id = %s 
                  AND stat_type = %s
                  AND stat_date <= %s
                ORDER BY stat_date DESC
                LIMIT 1
                """
                
                cur.execute(query, (pitcher_id, stat_type, target_date))
                result = cur.fetchone()
                
                if result:
                    return {
                        'pitcher_id': pitcher_id,
                        'stat_date': result['stat_date'],
                        'games_played': result['games_played'],
                        'era': result['era'],
                        'whip': result['whip'],
                        'strikeouts_per_9': result['strikeouts_per_9'],
                        'walks_per_9': result['walks_per_9'],
                        'hits_allowed': result['hits_allowed'],
                        'runs_allowed': result['runs_allowed'],
                        'quality_starts': result['quality_starts'],
                        'hot_streak': result['hot_streak'],
                        'cold_streak': result['cold_streak'],
                        'workload_score': result['workload_score'],
                        'date_range_start': result['date_range_start'],
                        'date_range_end': result['date_range_end'],
                        
                        # Form assessment
                        'recent_form': (
                            'RED_HOT' if result['hot_streak'] and result['era'] <= 2.50 else
                            'HOT' if result['hot_streak'] or result['era'] <= 3.00 else
                            'COLD' if result['cold_streak'] or result['era'] >= 5.50 else
                            'STRUGGLING' if result['era'] >= 4.50 else
                            'GOOD'
                        ),
                        'reliability': 'HIGH' if result['games_played'] >= 5 else 'MEDIUM' if result['games_played'] >= 3 else 'LOW'
                    }
                else:
                    return {'pitcher_id': pitcher_id, 'error': 'No recent stats found'}
    
    def get_recent_team_stats(self, team: str, target_date: str, stat_type: str = 'batting_15d') -> Dict:
        """ENHANCED: Get pre-calculated team stats from recent_stats"""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                # First get team players, then aggregate their recent stats
                query = """
                WITH team_players AS (
                    SELECT DISTINCT r.person_id
                    FROM rosters r
                    JOIN game_info gi ON r.game_date = gi.game_date
                    WHERE gi.home_team = %s OR gi.away_team = %s
                      AND r.game_date <= %s
                      AND r.game_date >= %s
                    LIMIT 25  -- Typical roster size
                ),
                team_recent_stats AS (
                    SELECT rs.*
                    FROM recent_stats rs
                    JOIN team_players tp ON rs.player_id = tp.person_id
                    WHERE rs.stat_type = %s
                      AND rs.stat_date <= %s
                    ORDER BY rs.stat_date DESC
                )
                SELECT 
                    COUNT(*) as players_with_stats,
                    AVG(rs.ops) as avg_ops,
                    AVG(rs.batting_avg) as avg_batting_avg,
                    AVG(rs.on_base_pct) as avg_obp,
                    AVG(rs.slugging_pct) as avg_slg,
                    SUM(rs.home_runs) as total_hrs,
                    SUM(rs.rbis) as total_rbis,
                    COUNT(CASE WHEN rs.hot_streak THEN 1 END) as hot_players,
                    COUNT(CASE WHEN rs.cold_streak THEN 1 END) as cold_players,
                    AVG(rs.games_played) as avg_games
                FROM team_recent_stats rs
                """
                
                target_date_obj = datetime.strptime(target_date, '%Y-%m-%d').date()
                lookback_date = target_date_obj - timedelta(days=30)
                
                cur.execute(query, (team, team, target_date, lookback_date, stat_type, target_date))
                result = cur.fetchone()
                
                if result and result['players_with_stats'] > 0:
                    avg_ops = result['avg_ops'] or 0.700
                    hot_pct = result['hot_players'] / result['players_with_stats']
                    cold_pct = result['cold_players'] / result['players_with_stats']
                    
                    return {
                        'team': team,
                        'stat_type': stat_type,
                        'players_analyzed': result['players_with_stats'],
                        'avg_ops': round(avg_ops, 3),
                        'avg_batting_avg': round(result['avg_batting_avg'] or 0.250, 3),
                        'avg_obp': round(result['avg_obp'] or 0.320, 3),
                        'avg_slg': round(result['avg_slg'] or 0.400, 3),
                        'total_home_runs': result['total_hrs'] or 0,
                        'total_rbis': result['total_rbis'] or 0,
                        'hot_players': result['hot_players'] or 0,
                        'cold_players': result['cold_players'] or 0,
                        'avg_games': round(result['avg_games'] or 0, 1),
                        
                        # Team form assessment
                        'offensive_form': (
                            'RED_HOT' if hot_pct >= 0.4 and avg_ops >= 0.800 else
                            'HOT' if hot_pct >= 0.3 or avg_ops >= 0.750 else
                            'COLD' if cold_pct >= 0.4 or avg_ops <= 0.650 else
                            'STRUGGLING' if avg_ops <= 0.680 else
                            'AVERAGE'
                        ),
                        'hot_player_pct': round(hot_pct, 3),
                        'cold_player_pct': round(cold_pct, 3)
                    }
                else:
                    return {'team': team, 'error': 'No recent team stats found'}
    
    def get_enhanced_weather_impact(self, game_pk: int) -> Dict:
        """ENHANCED: Weather impact with venue factors"""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                query = """
                SELECT w.*, vf.run_factor, vf.hr_factor, vf.dome_stadium, 
                       vf.elevation_feet, vf.pitcher_friendly_score
                FROM weather w
                JOIN game_info gi ON w.game_pk = gi.game_pk
                LEFT JOIN venue_factors vf ON gi.venue_name = vf.venue_name
                WHERE w.game_pk = %s
                """
                
                cur.execute(query, (game_pk,))
                result = cur.fetchone()
                
                if result:
                    temp = result['temperature_f'] or 72
                    wind_speed = result['wind_speed_mph'] or 0
                    elevation = result['elevation_feet'] or 0
                    run_factor = result['run_factor'] or 1.0
                    hr_factor = result['hr_factor'] or 1.0
                    dome = result['dome_stadium'] or False
                    
                    # Enhanced impact calculation
                    temp_impact = 0
                    if not dome:  # Weather doesn't matter in domes
                        if temp >= 85:
                            temp_impact = 0.15
                        elif temp >= 80:
                            temp_impact = 0.10
                        elif temp <= 45:
                            temp_impact = -0.15
                        elif temp <= 55:
                            temp_impact = -0.08
                    
                    # Elevation impact (Coors Field effect)
                    elevation_impact = min(0.20, elevation / 25000) if elevation > 3000 else 0
                    
                    # Wind impact
                    wind_impact = 0
                    if not dome and wind_speed >= 10:
                        wind_direction = result['wind_direction_deg'] or 0
                        if 180 <= wind_direction <= 270:  # Wind helping HRs
                            wind_impact = min(0.15, wind_speed / 100)
                        else:  # Wind hurting HRs
                            wind_impact = -min(0.10, wind_speed / 120)
                    
                    # Combined impact
                    total_impact = temp_impact + elevation_impact + wind_impact
                    final_factor = run_factor * (1 + total_impact)
                    
                    # Generate recommendation
                    if final_factor >= 1.15:
                        impact = "STRONG OVER"
                        confidence = "HIGH"
                    elif final_factor >= 1.08:
                        impact = "OVER LEAN"
                        confidence = "MEDIUM"
                    elif final_factor <= 0.85:
                        impact = "STRONG UNDER"
                        confidence = "HIGH"
                    elif final_factor <= 0.92:
                        impact = "UNDER LEAN"
                        confidence = "MEDIUM"
                    else:
                        impact = "NEUTRAL"
                        confidence = "LOW"
                    
                    return {
                        'game_pk': game_pk,
                        'venue_name': result['venue_name'],
                        'temperature_f': temp,
                        'wind_speed_mph': wind_speed,
                        'elevation_feet': elevation,
                        'dome_stadium': dome,
                        'run_factor': run_factor,
                        'hr_factor': hr_factor,
                        'final_factor': round(final_factor, 3),
                        'impact': impact,
                        'confidence': confidence,
                        'reason': f"Temp: {temp}°F, Wind: {wind_speed}mph, Elevation: {elevation}ft, Park factor: {run_factor:.2f}"
                    }
                else:
                    return {'game_pk': game_pk, 'error': 'No weather data found'}
    
    def get_enhanced_umpire_impact(self, game_pk: int) -> Dict:
        """ENHANCED: Umpire impact using pre-calculated metrics"""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                query = """
                SELECT umpire_name, position, avg_total_runs_in_games, 
                       over_under_record, sample_size, pitcher_friendly_score,
                       strike_rate_overall, avg_game_length_minutes
                FROM umpires 
                WHERE game_pk = %s AND position = 'Home Plate'
                """
                
                cur.execute(query, (game_pk,))
                result = cur.fetchone()
                
                if result:
                    umpire_name = result['umpire_name']
                    avg_runs = result['avg_total_runs_in_games'] or 8.5
                    over_pct = result['over_under_record'] or 0.5
                    sample_size = result['sample_size'] or 0
                    pitcher_friendly = result['pitcher_friendly_score'] or 50
                    
                    # Confidence based on sample size
                    if sample_size >= 50:
                        confidence = "HIGH"
                    elif sample_size >= 25:
                        confidence = "MEDIUM"
                    else:
                        confidence = "LOW"
                    
                    # Impact assessment
                    mlb_average_runs = 8.5
                    runs_diff = avg_runs - mlb_average_runs
                    
                    if runs_diff >= 1.0 or over_pct >= 0.60:
                        impact = "STRONG OVER"
                    elif runs_diff >= 0.5 or over_pct >= 0.55:
                        impact = "OVER LEAN"
                    elif runs_diff <= -1.0 or over_pct <= 0.40:
                        impact = "STRONG UNDER"
                    elif runs_diff <= -0.5 or over_pct <= 0.45:
                        impact = "UNDER LEAN"
                    else:
                        impact = "NEUTRAL"
                    
                    return {
                        'game_pk': game_pk,
                        'umpire_name': umpire_name,
                        'avg_runs': round(avg_runs, 1),
                        'over_under_record': round(over_pct, 3),
                        'sample_size': sample_size,
                        'pitcher_friendly_score': pitcher_friendly,
                        'impact': impact,
                        'confidence': confidence,
                        'reason': f"{umpire_name}: {avg_runs:.1f} runs/game, {over_pct:.1%} OVER rate ({sample_size} games)"
                    }
                else:
                    return {'game_pk': game_pk, 'error': 'No umpire data found'}
    
    def get_comprehensive_game_analysis(self, game_pk: int) -> Dict:
        """ENHANCED: Complete analysis using all enhanced data sources"""
        try:
            # Get all enhanced data
            game_context = self.get_game_context(game_pk)
            
            if 'error' in game_context:
                return game_context
            
            # Get pitcher analysis
            home_pitcher_stats = {}
            away_pitcher_stats = {}
            
            if game_context['home_starting_pitcher']:
                home_pitcher_stats = self.get_recent_pitcher_stats(
                    game_context['home_starting_pitcher'], 
                    str(game_context['game_date'])
                )
            
            if game_context['away_starting_pitcher']:
                away_pitcher_stats = self.get_recent_pitcher_stats(
                    game_context['away_starting_pitcher'], 
                    str(game_context['game_date'])
                )
            
            # Get team analysis
            home_team_stats = self.get_recent_team_stats(
                game_context['home_team'], 
                str(game_context['game_date'])
            )
            
            away_team_stats = self.get_recent_team_stats(
                game_context['away_team'], 
                str(game_context['game_date'])
            )
            
            # Get weather and umpire analysis
            weather_analysis = self.get_enhanced_weather_impact(game_pk)
            umpire_analysis = self.get_enhanced_umpire_impact(game_pk)
            
            return {
                'game_pk': game_pk,
                'game_context': game_context,
                'home_pitcher': home_pitcher_stats,
                'away_pitcher': away_pitcher_stats,
                'home_team': home_team_stats,
                'away_team': away_team_stats,
                'weather': weather_analysis,
                'umpire': umpire_analysis,
                'analysis_quality': {
                    'game_context_available': True,
                    'pitcher_data_quality': (
                        'EXCELLENT' if 'error' not in home_pitcher_stats and 'error' not in away_pitcher_stats else
                        'GOOD' if 'error' not in home_pitcher_stats or 'error' not in away_pitcher_stats else
                        'POOR'
                    ),
                    'team_data_quality': (
                        'EXCELLENT' if 'error' not in home_team_stats and 'error' not in away_team_stats else
                        'GOOD' if 'error' not in home_team_stats or 'error' not in away_team_stats else
                        'POOR'
                    ),
                    'weather_available': 'error' not in weather_analysis,
                    'umpire_available': 'error' not in umpire_analysis
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error in comprehensive analysis for game {game_pk}: {e}")
            return {'game_pk': game_pk, 'error': str(e)}


# Enhanced usage example
def get_enhanced_game_analysis(connection_string: str, game_pk: int):
    """Get comprehensive enhanced analysis for a game"""
    retriever = EnhancedBettingDataRetriever(connection_string)
    return retriever.get_comprehensive_game_analysis(game_pk)


if __name__ == "__main__":
    # Example usage
    CONNECTION_STRING = "postgresql://user:password@localhost/mlb_betting"
    
    # Get enhanced analysis for a game
    analysis = get_enhanced_game_analysis(CONNECTION_STRING, 123456)
    
    print("Enhanced Betting Analysis:")
    print(f"Game: {analysis['game_context']['home_team']} vs {analysis['game_context']['away_team']}")
    print(f"Weather Impact: {analysis['weather']['impact']}")
    print(f"Umpire Impact: {analysis['umpire']['impact']}")
    print(f"Data Quality: {analysis['analysis_quality']}")