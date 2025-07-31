"""
Betting Data Retrieval Module
Simple, focused data retrieval for MLB betting analysis
Gets the essential information for OVER/UNDER and moneyline decisions
"""

import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging

class BettingDataRetriever:
    """
    Retrieves essential betting data from simplified database schema
    Focuses on the 4 key factors that move betting lines
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
    
    def get_team_trends(self, team: str, target_date: str, games_back: int = 10) -> Dict:
        """
        Get team offensive/defensive trends over last N games
        Critical for betting - shows team form and momentum
        """
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                query = """
                WITH team_games AS (
                    SELECT DISTINCT 
                        game_date,
                        game_pk,
                        home_team,
                        away_team,
                        CASE 
                            WHEN home_team = %s THEN 'home'
                            WHEN away_team = %s THEN 'away'
                        END as team_side
                    FROM games 
                    WHERE (home_team = %s OR away_team = %s)
                        AND game_date < %s
                        AND game_date >= %s - INTERVAL '%s days'
                    ORDER BY game_date DESC
                    LIMIT %s
                ),
                game_scores AS (
                    SELECT 
                        tg.game_pk,
                        tg.game_date,
                        tg.team_side,
                        -- Get final scores from last play of game
                        MAX(CASE WHEN tg.team_side = 'home' THEN pb.home_score ELSE pb.away_score END) as team_runs,
                        MAX(CASE WHEN tg.team_side = 'home' THEN pb.away_score ELSE pb.home_score END) as opponent_runs,
                        MAX(pb.home_score + pb.away_score) as total_runs
                    FROM team_games tg
                    JOIN play_by_play pb ON tg.game_pk = pb.game_pk
                    GROUP BY tg.game_pk, tg.game_date, tg.team_side
                )
                SELECT 
                    COUNT(*) as games_played,
                    SUM(CASE WHEN team_runs > opponent_runs THEN 1 ELSE 0 END) as wins,
                    SUM(CASE WHEN team_runs < opponent_runs THEN 1 ELSE 0 END) as losses,
                    ROUND(AVG(team_runs), 2) as avg_runs_scored,
                    ROUND(AVG(opponent_runs), 2) as avg_runs_allowed,
                    ROUND(AVG(total_runs), 2) as avg_total_runs,
                    SUM(CASE WHEN total_runs >= 9 THEN 1 ELSE 0 END) as high_scoring_games,
                    ROUND(AVG(CASE WHEN team_runs > opponent_runs THEN team_runs ELSE NULL END), 2) as avg_runs_in_wins,
                    ROUND(AVG(CASE WHEN team_runs < opponent_runs THEN team_runs ELSE NULL END), 2) as avg_runs_in_losses
                FROM game_scores
                """
                
                target_date_obj = datetime.strptime(target_date, '%Y-%m-%d').date()
                lookback_date = target_date_obj - timedelta(days=30)  # Wider search window
                
                cur.execute(query, (team, team, team, team, target_date, lookback_date, games_back * 7, games_back))
                result = cur.fetchone()
                
                if result and result['games_played'] > 0:
                    return {
                        'team': team,
                        'games_analyzed': result['games_played'],
                        'record': f"{result['wins']}-{result['losses']}",
                        'win_pct': round(result['wins'] / result['games_played'], 3),
                        'avg_runs_scored': result['avg_runs_scored'],
                        'avg_runs_allowed': result['avg_runs_allowed'],
                        'avg_total_runs': result['avg_total_runs'],
                        'offensive_form': 'HOT' if result['avg_runs_scored'] >= 5.0 else 'COLD' if result['avg_runs_scored'] <= 3.5 else 'AVERAGE',
                        'over_tendency': result['high_scoring_games'] / result['games_played'] if result['games_played'] > 0 else 0,
                        'recent_form': 'STRONG' if result['wins'] / result['games_played'] >= 0.7 else 'WEAK' if result['wins'] / result['games_played'] <= 0.3 else 'AVERAGE'
                    }
                else:
                    return {'team': team, 'games_analyzed': 0, 'error': 'Insufficient data'}
    
    def get_pitcher_trends(self, pitcher_id: int, target_date: str, starts_back: int = 5) -> Dict:
        """
        Get starting pitcher recent performance trends
        Focus on ERA, WHIP, strikeouts over last 5 starts
        """
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                query = """
                WITH pitcher_starts AS (
                    SELECT DISTINCT 
                        game_date,
                        game_pk,
                        pitcher,
                        home_team,
                        away_team
                    FROM games 
                    WHERE pitcher = %s 
                        AND game_date < %s
                        AND inning <= 1  -- Starting pitcher
                    ORDER BY game_date DESC
                    LIMIT %s
                ),
                start_stats AS (
                    SELECT 
                        ps.game_pk,
                        ps.game_date,
                        -- Calculate basic stats per start
                        COUNT(*) as pitches_thrown,
                        COUNT(CASE WHEN events IN ('single', 'double', 'triple', 'home_run') THEN 1 END) as hits_allowed,
                        COUNT(CASE WHEN events = 'strikeout' THEN 1 END) as strikeouts,
                        COUNT(CASE WHEN events = 'walk' THEN 1 END) as walks,
                        COUNT(CASE WHEN events = 'home_run' THEN 1 END) as home_runs_allowed,
                        MAX(inning) as innings_pitched_approx,
                        -- Get runs from play-by-play
                        (SELECT COUNT(*) 
                         FROM play_by_play pb 
                         WHERE pb.game_pk = ps.game_pk 
                           AND pb.pitcher = ps.pitcher 
                           AND pb.is_scoring_play = true) as earned_runs
                    FROM pitcher_starts ps
                    JOIN games g ON ps.game_pk = g.game_pk AND ps.pitcher = g.pitcher
                    GROUP BY ps.game_pk, ps.game_date, ps.pitcher
                )
                SELECT 
                    COUNT(*) as starts_analyzed,
                    ROUND(AVG(hits_allowed), 2) as avg_hits_per_start,
                    ROUND(AVG(strikeouts), 2) as avg_strikeouts_per_start,
                    ROUND(AVG(walks), 2) as avg_walks_per_start,
                    ROUND(AVG(home_runs_allowed), 2) as avg_hr_allowed_per_start,
                    ROUND(AVG(earned_runs), 2) as avg_earned_runs_per_start,
                    ROUND(AVG(innings_pitched_approx), 1) as avg_innings_per_start,
                    -- Calculate ERA approximation (earned runs per 9 innings)
                    ROUND(
                        CASE 
                            WHEN AVG(innings_pitched_approx) > 0 
                            THEN (AVG(earned_runs) * 9.0) / AVG(innings_pitched_approx)
                            ELSE 0 
                        END, 2
                    ) as estimated_era,
                    -- Calculate WHIP approximation ((walks + hits) / innings)
                    ROUND(
                        CASE 
                            WHEN AVG(innings_pitched_approx) > 0 
                            THEN (AVG(walks) + AVG(hits_allowed)) / AVG(innings_pitched_approx)
                            ELSE 0 
                        END, 2
                    ) as estimated_whip,
                    -- Recent form indicators
                    SUM(CASE WHEN hits_allowed <= 4 THEN 1 ELSE 0 END) as quality_starts,
                    SUM(CASE WHEN strikeouts >= 6 THEN 1 ELSE 0 END) as strong_strikeout_games
                FROM start_stats
                """
                
                cur.execute(query, (pitcher_id, target_date, starts_back))
                result = cur.fetchone()
                
                if result and result['starts_analyzed'] > 0:
                    era = result['estimated_era'] or 0
                    whip = result['estimated_whip'] or 0
                    
                    return {
                        'pitcher_id': pitcher_id,
                        'starts_analyzed': result['starts_analyzed'],
                        'avg_hits_per_start': result['avg_hits_per_start'],
                        'avg_strikeouts_per_start': result['avg_strikeouts_per_start'],
                        'avg_walks_per_start': result['avg_walks_per_start'],
                        'estimated_era': era,
                        'estimated_whip': whip,
                        'avg_innings_per_start': result['avg_innings_per_start'],
                        'quality_starts': result['quality_starts'],
                        'recent_form': (
                            'EXCELLENT' if era <= 3.00 and whip <= 1.10 else
                            'GOOD' if era <= 4.00 and whip <= 1.30 else
                            'POOR' if era >= 5.50 or whip >= 1.50 else
                            'AVERAGE'
                        ),
                        'strikeout_rate': 'HIGH' if result['avg_strikeouts_per_start'] >= 7 else 'LOW' if result['avg_strikeouts_per_start'] <= 4 else 'AVERAGE',
                        'control': 'GOOD' if result['avg_walks_per_start'] <= 2.5 else 'POOR'
                    }
                else:
                    return {'pitcher_id': pitcher_id, 'starts_analyzed': 0, 'error': 'Insufficient data'}
    
    def get_weather_impact(self, game_pk: int) -> Dict:
        """
        Get weather conditions and calculate OVER/UNDER impact
        Temperature + wind = major factors in baseball betting
        """
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                query = """
                SELECT 
                    game_pk,
                    venue_name,
                    home_team,
                    temperature_f,
                    humidity_pct,
                    wind_speed_mph,
                    wind_direction_deg,
                    wind_x_component,
                    wind_y_component,
                    hr_distance_factor_ft,
                    over_under_lean,
                    weather_impact_score,
                    park_factor
                FROM weather 
                WHERE game_pk = %s
                """
                
                cur.execute(query, (game_pk,))
                result = cur.fetchone()
                
                if result:
                    temp = result['temperature_f'] or 75
                    wind_speed = result['wind_speed_mph'] or 0
                    park_factor = result['park_factor'] or 1.0
                    
                    # Calculate manual impact if not pre-calculated
                    if not result['over_under_lean']:
                        temp_impact = 'OVER' if temp >= 80 else 'UNDER' if temp <= 60 else 'NEUTRAL'
                        wind_impact = 'OVER' if wind_speed >= 15 and result['wind_y_component'] > 0 else 'UNDER' if wind_speed >= 15 else 'NEUTRAL'
                        overall_lean = temp_impact if temp_impact != 'NEUTRAL' else wind_impact
                    else:
                        overall_lean = result['over_under_lean']
                    
                    return {
                        'game_pk': game_pk,
                        'venue': result['venue_name'],
                        'temperature_f': temp,
                        'humidity_pct': result['humidity_pct'],
                        'wind_speed_mph': wind_speed,
                        'wind_direction': result['wind_direction_deg'],
                        'park_factor': park_factor,
                        'hr_distance_change': result['hr_distance_factor_ft'],
                        'over_under_lean': overall_lean,
                        'impact_strength': (
                            'STRONG' if abs(result['weather_impact_score'] or 0) >= 15 else
                            'MODERATE' if abs(result['weather_impact_score'] or 0) >= 8 else
                            'WEAK'
                        ),
                        'weather_summary': f"{temp}°F, {wind_speed}mph wind → {overall_lean} lean"
                    }
                else:
                    return {'game_pk': game_pk, 'error': 'No weather data found'}
    
    def get_historical_matchups(self, pitcher_id: int, opposing_team: str, years_back: int = 2) -> Dict:
        """
        Get pitcher performance vs specific opposing team
        Shows if pitcher has historically dominated or struggled vs this team
        """
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                query = """
                WITH matchup_games AS (
                    SELECT DISTINCT 
                        g.game_date,
                        g.game_pk,
                        g.pitcher,
                        g.home_team,
                        g.away_team,
                        CASE 
                            WHEN g.home_team = %s THEN g.away_team
                            WHEN g.away_team = %s THEN g.home_team
                        END as opposing_team
                    FROM games g
                    WHERE g.pitcher = %s 
                        AND (g.home_team = %s OR g.away_team = %s)
                        AND g.game_date >= CURRENT_DATE - INTERVAL '%s years'
                        AND g.inning <= 3  -- Focus on starts, not relief appearances
                ),
                matchup_stats AS (
                    SELECT 
                        mg.game_pk,
                        mg.game_date,
                        -- Calculate performance in this matchup
                        COUNT(*) as pitches_thrown,
                        COUNT(CASE WHEN g.events IN ('single', 'double', 'triple', 'home_run') THEN 1 END) as hits_allowed,
                        COUNT(CASE WHEN g.events = 'strikeout' THEN 1 END) as strikeouts,
                        COUNT(CASE WHEN g.events = 'walk' THEN 1 END) as walks,
                        COUNT(CASE WHEN g.events = 'home_run' THEN 1 END) as home_runs_allowed,
                        MAX(g.inning) as max_inning_pitched,
                        -- Get runs allowed from play-by-play
                        (SELECT COUNT(*) 
                         FROM play_by_play pb 
                         WHERE pb.game_pk = mg.game_pk 
                           AND pb.pitcher = mg.pitcher 
                           AND pb.batting_team = %s
                           AND pb.is_scoring_play = true) as runs_allowed
                    FROM matchup_games mg
                    JOIN games g ON mg.game_pk = g.game_pk AND mg.pitcher = g.pitcher
                    GROUP BY mg.game_pk, mg.game_date, mg.pitcher
                )
                SELECT 
                    COUNT(*) as games_faced,
                    ROUND(AVG(hits_allowed), 2) as avg_hits_allowed,
                    ROUND(AVG(strikeouts), 2) as avg_strikeouts,
                    ROUND(AVG(walks), 2) as avg_walks,
                    ROUND(AVG(runs_allowed), 2) as avg_runs_allowed,
                    ROUND(AVG(home_runs_allowed), 2) as avg_hr_allowed,
                    ROUND(AVG(max_inning_pitched), 1) as avg_innings,
                    -- Calculate matchup ERA
                    ROUND(
                        CASE 
                            WHEN AVG(max_inning_pitched) > 0 
                            THEN (AVG(runs_allowed) * 9.0) / AVG(max_inning_pitched)
                            ELSE 0 
                        END, 2
                    ) as matchup_era,
                    -- Success rate
                    SUM(CASE WHEN runs_allowed <= 2 THEN 1 ELSE 0 END) as strong_outings,
                    SUM(CASE WHEN runs_allowed >= 5 THEN 1 ELSE 0 END) as poor_outings
                FROM matchup_stats
                """
                
                cur.execute(query, (opposing_team, opposing_team, pitcher_id, opposing_team, opposing_team, years_back, opposing_team))
                result = cur.fetchone()
                
                if result and result['games_faced'] > 0:
                    era = result['matchup_era'] or 0
                    strong_pct = result['strong_outings'] / result['games_faced']
                    poor_pct = result['poor_outings'] / result['games_faced']
                    
                    return {
                        'pitcher_id': pitcher_id,
                        'opposing_team': opposing_team,
                        'games_faced': result['games_faced'],
                        'avg_hits_allowed': result['avg_hits_allowed'],
                        'avg_strikeouts': result['avg_strikeouts'],
                        'avg_runs_allowed': result['avg_runs_allowed'],
                        'matchup_era': era,
                        'avg_innings': result['avg_innings'],
                        'strong_outings_pct': round(strong_pct, 3),
                        'poor_outings_pct': round(poor_pct, 3),
                        'historical_performance': (
                            'DOMINATES' if era <= 3.00 and strong_pct >= 0.6 else
                            'STRUGGLES' if era >= 5.50 or poor_pct >= 0.5 else
                            'MIXED' if result['games_faced'] >= 3 else
                            'LIMITED_SAMPLE'
                        ),
                        'sample_size': 'RELIABLE' if result['games_faced'] >= 5 else 'SMALL'
                    }
                else:
                    return {
                        'pitcher_id': pitcher_id, 
                        'opposing_team': opposing_team, 
                        'games_faced': 0, 
                        'historical_performance': 'NO_DATA'
                    }
    
    def get_all_betting_factors(self, game_pk: int, home_pitcher_id: int, away_pitcher_id: int, 
                               home_team: str, away_team: str, game_date: str) -> Dict:
        """
        Get all essential betting factors for a game in one call
        Combines team trends, pitcher trends, weather, and historical matchups
        """
        try:
            # Get all factors
            home_trends = self.get_team_trends(home_team, game_date)
            away_trends = self.get_team_trends(away_team, game_date)
            home_pitcher_trends = self.get_pitcher_trends(home_pitcher_id, game_date)
            away_pitcher_trends = self.get_pitcher_trends(away_pitcher_id, game_date)
            weather = self.get_weather_impact(game_pk)
            home_pitcher_vs_away = self.get_historical_matchups(home_pitcher_id, away_team)
            away_pitcher_vs_home = self.get_historical_matchups(away_pitcher_id, home_team)
            
            return {
                'game_pk': game_pk,
                'game_date': game_date,
                'teams': {
                    'home': home_team,
                    'away': away_team
                },
                'team_trends': {
                    'home': home_trends,
                    'away': away_trends
                },
                'pitcher_trends': {
                    'home': home_pitcher_trends,
                    'away': away_pitcher_trends
                },
                'weather': weather,
                'historical_matchups': {
                    'home_pitcher_vs_away_team': home_pitcher_vs_away,
                    'away_pitcher_vs_home_team': away_pitcher_vs_home
                },
                'data_quality': {
                    'home_team_data': home_trends.get('games_analyzed', 0),
                    'away_team_data': away_trends.get('games_analyzed', 0),
                    'home_pitcher_data': home_pitcher_trends.get('starts_analyzed', 0),
                    'away_pitcher_data': away_pitcher_trends.get('starts_analyzed', 0),
                    'weather_available': 'error' not in weather,
                    'sufficient_data': all([
                        home_trends.get('games_analyzed', 0) >= 5,
                        away_trends.get('games_analyzed', 0) >= 5,
                        home_pitcher_trends.get('starts_analyzed', 0) >= 3,
                        away_pitcher_trends.get('starts_analyzed', 0) >= 3
                    ])
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error getting betting factors for game {game_pk}: {e}")
            return {
                'game_pk': game_pk,
                'error': str(e),
                'data_quality': {'sufficient_data': False}
            }


# Example usage function
def get_game_analysis(connection_string: str, game_pk: int, home_pitcher_id: int, 
                     away_pitcher_id: int, home_team: str, away_team: str, game_date: str):
    """
    Example function showing how to use the data retriever
    Returns all essential betting factors for analysis
    """
    retriever = BettingDataRetriever(connection_string)
    return retriever.get_all_betting_factors(
        game_pk, home_pitcher_id, away_pitcher_id, home_team, away_team, game_date
    )


if __name__ == "__main__":
    # Example usage
    CONNECTION_STRING = "postgresql://user:password@localhost/mlb_betting"
    
    # Get betting factors for a specific game
    betting_data = get_game_analysis(
        CONNECTION_STRING,
        game_pk=123456,
        home_pitcher_id=543037,  # Example pitcher ID
        away_pitcher_id=592789,  # Example pitcher ID  
        home_team="NYY",
        away_team="BOS",
        game_date="2024-07-30"
    )
    
    print("Betting Analysis Data:")
    print(f"Data Quality: {betting_data['data_quality']}")
    print(f"Weather: {betting_data['weather'].get('weather_summary', 'N/A')}")
    print(f"Home Team Form: {betting_data['team_trends']['home'].get('recent_form', 'N/A')}")
    print(f"Away Team Form: {betting_data['team_trends']['away'].get('recent_form', 'N/A')}")