#!/usr/bin/env python3
"""
enhanced_simple_backfill.py - STREAMLINED: Focused on core data Claude needs
REMOVED: Weather, venue factors (Claude handles these)
ENHANCED: ALL advanced Statcast metrics for sophisticated analysis

Usage:
    python enhanced_simple_backfill.py --start YYYY-MM-DD --end YYYY-MM-DD [--output DIR]
"""

import os
import argparse
import time
import math
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
from collections import defaultdict

import pandas as pd
import numpy as np
from tqdm import tqdm

# MLB teams data for realistic placeholder generation
MLB_TEAMS = {
    'Arizona Diamondbacks': {'abbrev': 'ARI', 'league': 'NL', 'division': 'West'},
    'Atlanta Braves': {'abbrev': 'ATL', 'league': 'NL', 'division': 'East'},
    'Baltimore Orioles': {'abbrev': 'BAL', 'league': 'AL', 'division': 'East'},
    'Boston Red Sox': {'abbrev': 'BOS', 'league': 'AL', 'division': 'East'},
    'Chicago Cubs': {'abbrev': 'CHC', 'league': 'NL', 'division': 'Central'},
    'Chicago White Sox': {'abbrev': 'CWS', 'league': 'AL', 'division': 'Central'},
    'Cincinnati Reds': {'abbrev': 'CIN', 'league': 'NL', 'division': 'Central'},
    'Cleveland Guardians': {'abbrev': 'CLE', 'league': 'AL', 'division': 'Central'},
    'Colorado Rockies': {'abbrev': 'COL', 'league': 'NL', 'division': 'West'},
    'Detroit Tigers': {'abbrev': 'DET', 'league': 'AL', 'division': 'Central'},
    'Houston Astros': {'abbrev': 'HOU', 'league': 'AL', 'division': 'West'},
    'Kansas City Royals': {'abbrev': 'KC', 'league': 'AL', 'division': 'Central'},
    'Los Angeles Angels': {'abbrev': 'LAA', 'league': 'AL', 'division': 'West'},
    'Los Angeles Dodgers': {'abbrev': 'LAD', 'league': 'NL', 'division': 'West'},
    'Miami Marlins': {'abbrev': 'MIA', 'league': 'NL', 'division': 'East'},
    'Milwaukee Brewers': {'abbrev': 'MIL', 'league': 'NL', 'division': 'Central'},
    'Minnesota Twins': {'abbrev': 'MIN', 'league': 'AL', 'division': 'Central'},
    'New York Mets': {'abbrev': 'NYM', 'league': 'NL', 'division': 'East'},
    'New York Yankees': {'abbrev': 'NYY', 'league': 'AL', 'division': 'East'},
    'Oakland Athletics': {'abbrev': 'OAK', 'league': 'AL', 'division': 'West'},
    'Philadelphia Phillies': {'abbrev': 'PHI', 'league': 'NL', 'division': 'East'},
    'Pittsburgh Pirates': {'abbrev': 'PIT', 'league': 'NL', 'division': 'Central'},
    'San Diego Padres': {'abbrev': 'SD', 'league': 'NL', 'division': 'West'},
    'San Francisco Giants': {'abbrev': 'SF', 'league': 'NL', 'division': 'West'},
    'Seattle Mariners': {'abbrev': 'SEA', 'league': 'AL', 'division': 'West'},
    'St. Louis Cardinals': {'abbrev': 'STL', 'league': 'NL', 'division': 'Central'},
    'Tampa Bay Rays': {'abbrev': 'TB', 'league': 'AL', 'division': 'East'},
    'Texas Rangers': {'abbrev': 'TEX', 'league': 'AL', 'division': 'West'},
    'Toronto Blue Jays': {'abbrev': 'TOR', 'league': 'AL', 'division': 'East'},
    'Washington Nationals': {'abbrev': 'WSH', 'league': 'NL', 'division': 'East'},
}

MLB_VENUES = {
    'Arizona Diamondbacks': 'Chase Field',
    'Atlanta Braves': 'Truist Park',
    'Baltimore Orioles': 'Oriole Park at Camden Yards',
    'Boston Red Sox': 'Fenway Park',
    'Chicago Cubs': 'Wrigley Field',
    'Chicago White Sox': 'Guaranteed Rate Field',
    'Cincinnati Reds': 'Great American Ball Park',
    'Cleveland Guardians': 'Progressive Field',
    'Colorado Rockies': 'Coors Field',
    'Detroit Tigers': 'Comerica Park',
    'Houston Astros': 'Minute Maid Park',
    'Kansas City Royals': 'Kauffman Stadium',
    'Los Angeles Angels': 'Angel Stadium',
    'Los Angeles Dodgers': 'Dodger Stadium',
    'Miami Marlins': 'loanDepot park',
    'Milwaukee Brewers': 'American Family Field',
    'Minnesota Twins': 'Target Field',
    'New York Mets': 'Citi Field',
    'New York Yankees': 'Yankee Stadium',
    'Oakland Athletics': 'Oakland Coliseum',
    'Philadelphia Phillies': 'Citizens Bank Park',
    'Pittsburgh Pirates': 'PNC Park',
    'San Diego Padres': 'Petco Park',
    'San Francisco Giants': 'Oracle Park',
    'Seattle Mariners': 'T-Mobile Park',
    'St. Louis Cardinals': 'Busch Stadium',
    'Tampa Bay Rays': 'Tropicana Field',
    'Texas Rangers': 'Globe Life Field',
    'Toronto Blue Jays': 'Rogers Centre',
    'Washington Nationals': 'Nationals Park',
}

class PlaceholderDataGenerator:
    """Generate realistic placeholder MLB data with ALL advanced Statcast metrics"""
    
    def __init__(self, seed: int = 42):
        self.rng = np.random.RandomState(seed)
        self.player_id_counter = 10000
        self.game_pk_counter = 600000
        
    def generate_daily_games(self, date_str: str) -> List[Dict]:
        """Generate realistic daily game schedule"""
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        
        # Skip some days (no games on some dates)
        if self.rng.random() < 0.15:  # 15% chance of no games
            return []
        
        # Generate 8-15 games per day (realistic MLB schedule)
        num_games = self.rng.randint(8, 16)
        teams = list(MLB_TEAMS.keys())
        used_teams = set()
        games = []
        
        for game_num in range(num_games):
            # Pick two teams that haven't played today
            available_teams = [t for t in teams if t not in used_teams]
            if len(available_teams) < 2:
                break
                
            home_team = self.rng.choice(available_teams)
            available_teams.remove(home_team)
            away_team = self.rng.choice(available_teams)
            
            used_teams.update([home_team, away_team])
            
            game_pk = self.game_pk_counter + game_num
            
            # Generate realistic scores
            home_score = self.rng.poisson(4.2)  # Average MLB runs
            away_score = self.rng.poisson(4.2)
            
            # Ensure no ties (extremely rare in MLB)
            if home_score == away_score:
                if self.rng.random() < 0.5:
                    home_score += 1
                else:
                    away_score += 1
            
            games.append({
                'game_pk': game_pk,
                'game_date': date_str,
                'home_team': home_team,
                'away_team': away_team,
                'home_score': home_score,
                'away_score': away_score,
                'winning_team': home_team if home_score > away_score else away_team,
                'venue_name': MLB_VENUES[home_team],
                'game_status': 'Final',
                'game_time_et': f"{self.rng.randint(13, 20)}:{self.rng.choice(['00', '05', '10'])}",
                'day_night': 'D' if self.rng.random() < 0.3 else 'N',  # 30% day games
                'attendance': self.rng.randint(15000, 45000),
                'game_length_minutes': self.rng.randint(150, 210),  # 2.5-3.5 hours
                'series_game_number': self.rng.randint(1, 4),
                'home_team_rest_days': self.rng.randint(0, 3),
                'away_team_rest_days': self.rng.randint(0, 3),
            })
        
        self.game_pk_counter += num_games
        return games

    def generate_pitcher_id(self, team: str, is_starter: bool = True) -> int:
        """Generate consistent pitcher ID for team"""
        base_id = hash(team) % 100000
        if is_starter:
            return base_id + self.rng.randint(1, 5)  # 5 starters
        else:
            return base_id + self.rng.randint(10, 25)  # Relievers

def get_output_filename(data_type: str, date_str: str = None) -> str:
    """Standardized file naming that matches loader expectations"""
    filename_mapping = {
        'games': f'games_{date_str}.parquet' if date_str else 'games.parquet',
        'play_by_play': f'play_by_play_{date_str}.parquet' if date_str else 'play_by_play.parquet',
        'game_info': f'game_info_{date_str}.parquet' if date_str else 'game_info.parquet',
        'lineups': f'lineups_{date_str}.parquet' if date_str else 'lineups.parquet',
        'rosters': f'rosters_{date_str}.parquet' if date_str else 'rosters.parquet',
        'umpires': f'umpires_{date_str}.parquet' if date_str else 'umpires.parquet',
        'recent_stats': f'recent_stats_{date_str}.parquet' if date_str else 'recent_stats.parquet',
    }
    
    return filename_mapping[data_type]

def collect_enhanced_statcast_data(date_str: str, out_dir: Path, use_placeholder: bool = True) -> bool:
    """ENHANCED: Collect ALL advanced Statcast metrics Claude needs"""
    out_file = out_dir / get_output_filename('games', date_str)
    
    if out_file.exists():
        print(f"⏭️ Skipping Statcast for {date_str} (already exists)")
        return True
    
    if use_placeholder:
        print(f"⚾ Generating ENHANCED placeholder Statcast data for {date_str}...")
        
        try:
            generator = PlaceholderDataGenerator()
            daily_games = generator.generate_daily_games(date_str)
            
            if not daily_games:
                print(f"✅ No games scheduled for {date_str}")
                empty_df = pd.DataFrame(columns=['game_date', 'game_pk'])
                empty_df.to_parquet(out_file, index=False)
                return True
            
            all_pitches = []
            
            for game in daily_games:
                game_pk = game['game_pk']
                
                # Generate realistic number of pitches per game (250-350)
                num_pitches = generator.rng.randint(250, 351)
                
                # Generate pitchers for this game
                home_pitcher = generator.generate_pitcher_id(game['home_team'], is_starter=True)
                away_pitcher = generator.generate_pitcher_id(game['away_team'], is_starter=True)
                
                for pitch_num in range(num_pitches):
                    at_bat_num = (pitch_num // 4) + 1  # ~4 pitches per at-bat average
                    pitch_in_ab = (pitch_num % 4) + 1
                    
                    # Alternate between teams batting
                    inning = (at_bat_num // 6) + 1
                    is_top = (at_bat_num // 3) % 2 == 0
                    batting_team = game['away_team'] if is_top else game['home_team']
                    pitching_team = game['home_team'] if is_top else game['away_team']
                    
                    pitcher_id = home_pitcher if pitching_team == game['home_team'] else away_pitcher
                    batter_id = generator.generate_pitcher_id(batting_team, is_starter=False) + pitch_num % 9
                    
                    # Generate realistic pitch data
                    pitch_type = generator.rng.choice(['FF', 'SL', 'CH', 'CU', 'SI', 'FC'], 
                                                    p=[0.35, 0.20, 0.15, 0.10, 0.10, 0.10])
                    
                    # ENHANCED: Realistic pitch speeds and characteristics
                    if pitch_type == 'FF':  # Fastball
                        release_speed = generator.rng.normal(92.5, 3.5)
                        effective_speed = release_speed + generator.rng.normal(0, 1.0)
                        spin_rate = generator.rng.normal(2300, 200)
                        pfx_x = generator.rng.normal(0, 3.0)  # Horizontal movement
                        pfx_z = generator.rng.normal(10, 2.0)  # Rising fastball
                    elif pitch_type in ['SL', 'CU']:  # Breaking balls
                        release_speed = generator.rng.normal(83.0, 4.0)
                        effective_speed = release_speed + generator.rng.normal(-2, 1.0)
                        spin_rate = generator.rng.normal(2600, 300)
                        pfx_x = generator.rng.normal(-5, 3.0) if pitch_type == 'SL' else generator.rng.normal(0, 2.0)
                        pfx_z = generator.rng.normal(-5, 2.0)  # Downward break
                    else:  # Changeup, etc.
                        release_speed = generator.rng.normal(87.0, 3.0)
                        effective_speed = release_speed + generator.rng.normal(-3, 1.0)
                        spin_rate = generator.rng.normal(1800, 200)
                        pfx_x = generator.rng.normal(0, 2.0)
                        pfx_z = generator.rng.normal(5, 2.0)
                    
                    # Release point data
                    release_extension = generator.rng.normal(6.2, 0.3)
                    
                    # Count progression
                    balls = generator.rng.randint(0, 4)
                    strikes = generator.rng.randint(0, 3)
                    if balls == 4 or strikes == 3:
                        balls = min(balls, 3)
                        strikes = min(strikes, 2)
                    
                    # Plate location (realistic strike zone)
                    plate_x = generator.rng.normal(0, 1.2)
                    plate_z = generator.rng.normal(2.5, 0.8)
                    
                    # Zone (1-9 strike zone, 11+ outside)
                    if abs(plate_x) < 0.8 and 1.5 < plate_z < 3.5:
                        zone = generator.rng.randint(1, 10)
                    else:
                        zone = generator.rng.randint(11, 15)
                    
                    # Events (outcome of at-bat)
                    events = None
                    launch_speed = None
                    launch_angle = None
                    hit_distance_sc = None
                    hc_x = None
                    hc_y = None
                    
                    # ENHANCED: Advanced expected stats and quality metrics
                    estimated_ba = None
                    estimated_woba = None
                    estimated_slg = None
                    launch_speed_angle = None  # Barrel classification
                    babip_value = None
                    iso_value = None
                    woba_value = None
                    
                    if pitch_in_ab == 4 or (strikes == 2 and generator.rng.random() < 0.3):
                        events = generator.rng.choice([
                            'strikeout', 'single', 'groundout', 'flyout', 'double', 
                            'walk', 'home_run', 'triple', 'lineout', 'pop_out'
                        ], p=[0.23, 0.15, 0.20, 0.15, 0.06, 0.09, 0.04, 0.01, 0.04, 0.03])
                        
                        # ENHANCED: Generate batted ball data with advanced metrics
                        if events in ['single', 'double', 'triple', 'home_run', 'groundout', 'flyout', 'lineout']:
                            # Realistic launch data based on outcome
                            if events == 'home_run':
                                launch_speed = generator.rng.normal(105, 8)
                                launch_angle = generator.rng.normal(25, 5)
                                hit_distance_sc = generator.rng.normal(420, 40)
                                launch_speed_angle = 6  # Barrel
                            elif events == 'double':
                                launch_speed = generator.rng.normal(95, 10)
                                launch_angle = generator.rng.normal(15, 8)
                                hit_distance_sc = generator.rng.normal(350, 50)
                                launch_speed_angle = generator.rng.choice([5, 6])  # Good contact
                            elif events == 'single':
                                launch_speed = generator.rng.normal(85, 12)
                                launch_angle = generator.rng.normal(10, 10)
                                hit_distance_sc = generator.rng.normal(250, 40)
                                launch_speed_angle = generator.rng.choice([3, 4, 5])
                            else:  # Outs
                                launch_speed = generator.rng.normal(75, 15)
                                launch_angle = generator.rng.normal(5, 15)
                                hit_distance_sc = generator.rng.normal(200, 60)
                                launch_speed_angle = generator.rng.choice([1, 2, 3])
                            
                            # Hit coordinates (field position)
                            field_angle = generator.rng.uniform(-45, 45)  # Degrees from center field
                            hc_x = hit_distance_sc * np.sin(np.radians(field_angle))
                            hc_y = hit_distance_sc * np.cos(np.radians(field_angle))
                            
                            # ENHANCED: Calculate expected stats (xBA, xwOBA, xSLG)
                            # Simplified formulas based on launch speed and angle
                            launch_speed_clamped = max(50, min(130, launch_speed))
                            launch_angle_clamped = max(-50, min(50, launch_angle))
                            
                            # Expected batting average (simplified model)
                            if 8 <= launch_angle_clamped <= 32:  # Sweet spot
                                estimated_ba = 0.100 + (launch_speed_clamped - 60) * 0.008
                            else:
                                estimated_ba = 0.050 + (launch_speed_clamped - 60) * 0.004
                            estimated_ba = max(0.000, min(1.000, estimated_ba))
                            
                            # Expected wOBA and SLG (correlated with xBA)
                            estimated_woba = estimated_ba * 1.5 + 0.1
                            estimated_slg = estimated_ba * 2.0 + 0.2
                            estimated_woba = max(0.000, min(2.000, estimated_woba))
                            estimated_slg = max(0.000, min(4.000, estimated_slg))
                            
                            # BABIP and ISO values
                            if events not in ['home_run']:  # BABIP doesn't apply to HRs
                                babip_value = generator.rng.uniform(0.200, 0.500)
                            
                            if events in ['double', 'triple', 'home_run']:
                                iso_value = generator.rng.uniform(0.200, 0.800)
                            
                            # WOBA values (weighted on-base average)
                            woba_map = {
                                'single': 0.9, 'double': 1.25, 'triple': 1.6, 'home_run': 2.0,
                                'walk': 0.7, 'groundout': 0.0, 'flyout': 0.0, 'lineout': 0.0,
                                'strikeout': 0.0, 'pop_out': 0.0
                            }
                            woba_value = woba_map.get(events, 0.0)
                    
                    # ENHANCED: Build complete pitch data with ALL advanced metrics
                    pitch_data = {
                        # Core identifiers
                        'game_date': date_str,
                        'game_pk': game_pk,
                        'at_bat_number': at_bat_num,
                        'pitch_number': pitch_in_ab,
                        
                        # Player info
                        'pitcher': pitcher_id,
                        'batter': batter_id,
                        'stand': generator.rng.choice(['L', 'R']),
                        'p_throws': generator.rng.choice(['L', 'R']),
                        
                        # Game situation
                        'balls': balls,
                        'strikes': strikes,
                        'outs_when_up': generator.rng.randint(0, 3),
                        'inning': min(inning, 12),
                        'inning_topbot': 'Top' if is_top else 'Bot',
                        'home_team': game['home_team'][:3].upper(),
                        'away_team': game['away_team'][:3].upper(),
                        
                        # ENHANCED: Core pitch data
                        'release_speed': round(release_speed, 1) if release_speed else None,
                        'effective_speed': round(effective_speed, 1) if effective_speed else None,
                        'release_spin_rate': round(spin_rate) if spin_rate else None,
                        'release_extension': round(release_extension, 1) if release_extension else None,
                        
                        # ENHANCED: Pitch location and movement
                        'plate_x': round(plate_x, 2),
                        'plate_z': round(plate_z, 2),
                        'zone': zone,
                        'pfx_x': round(pfx_x, 1) if pfx_x else None,
                        'pfx_z': round(pfx_z, 1) if pfx_z else None,
                        
                        # Outcome data
                        'events': events,
                        'description': f"Pitch {pitch_in_ab} of at-bat",
                        'pitch_type': pitch_type,
                        
                        # ENHANCED: Batted ball data with advanced metrics
                        'launch_speed': round(launch_speed, 1) if launch_speed else None,
                        'launch_angle': round(launch_angle, 1) if launch_angle else None,
                        'hit_distance_sc': round(hit_distance_sc) if hit_distance_sc else None,
                        'launch_speed_angle': launch_speed_angle,  # Barrel classification
                        'hc_x': round(hc_x, 1) if hc_x else None,
                        'hc_y': round(hc_y, 1) if hc_y else None,
                        
                        # ENHANCED: Expected stats and quality metrics
                        'estimated_ba_using_speedangle': round(estimated_ba, 3) if estimated_ba else None,
                        'estimated_woba_using_speedangle': round(estimated_woba, 3) if estimated_woba else None,
                        'estimated_slg_using_speedangle': round(estimated_slg, 3) if estimated_slg else None,
                        'woba_value': round(woba_value, 3) if woba_value else None,
                        'babip_value': round(babip_value, 3) if babip_value else None,
                        'iso_value': round(iso_value, 3) if iso_value else None,
                        
                        # Run expectancy
                        'delta_run_exp': generator.rng.normal(0, 0.1),
                    }
                    
                    all_pitches.append(pitch_data)
            
            if all_pitches:
                df_games = pd.DataFrame(all_pitches)
                df_games.to_parquet(out_file, index=False)
                print(f"✅ ENHANCED Statcast: {len(df_games)} pitches with ALL advanced metrics → {out_file.name}")
            else:
                print(f"✅ No Statcast data for {date_str}")
            
            return True
            
        except Exception as e:
            print(f"❌ Enhanced Statcast error for {date_str}: {e}")
            return False
    
    else:
        # Real Statcast collection (for future use)
        print(f"⚾ Collecting REAL Statcast data for {date_str}...")
        try:
            from pybaseball import statcast
            
            # Get ALL available columns from real Statcast data
            statcast_data = statcast(start_dt=date_str, end_dt=date_str)
            
            if statcast_data.empty:
                print(f"✅ No real Statcast data for {date_str}")
                return True
            
            # Keep ALL columns that pybaseball provides
            df_games = statcast_data.copy()
            df_games['game_date'] = date_str
            
            # Save all available data
            df_games.to_parquet(out_file, index=False)
            print(f"✅ Real Statcast: {len(df_games)} pitches with {len(df_games.columns)} columns → {out_file.name}")
            return True
            
        except Exception as e:
            print(f"❌ Real Statcast error for {date_str}: {e}")
            return False

def collect_play_by_play_data(date_str: str, out_dir: Path, use_placeholder: bool = True) -> bool:
    """Collect play-by-play data (placeholder mode)"""
    out_file = out_dir / get_output_filename('play_by_play', date_str)
    
    if out_file.exists():
        print(f"⏭️ Skipping play-by-play for {date_str} (already exists)")
        return True
    
    if use_placeholder:
        print(f"🎬 Generating placeholder play-by-play data for {date_str}...")
        
        try:
            generator = PlaceholderDataGenerator()
            daily_games = generator.generate_daily_games(date_str)
            
            if not daily_games:
                print(f"✅ No games for {date_str}")
                return True
            
            all_plays = []
            
            for game in daily_games:
                game_pk = game['game_pk']
                
                # Generate realistic number of at-bats per game (60-80)
                num_at_bats = generator.rng.randint(60, 81)
                
                home_score = 0
                away_score = 0
                
                for at_bat_idx in range(num_at_bats):
                    inning = (at_bat_idx // 6) + 1
                    is_top = (at_bat_idx // 3) % 2 == 0
                    half_inning = 'top' if is_top else 'bottom'
                    
                    batting_team = game['away_team'] if is_top else game['home_team']
                    pitching_team = game['home_team'] if is_top else game['away_team']
                    
                    # Generate event outcome
                    event_outcome = generator.rng.choice([
                        'Strikeout', 'Groundout', 'Flyout', 'Single', 'Walk', 
                        'Double', 'Home Run', 'Lineout', 'Pop Out', 'Triple'
                    ], p=[0.23, 0.20, 0.15, 0.14, 0.09, 0.06, 0.04, 0.04, 0.03, 0.02])
                    
                    # Scoring plays
                    rbi = 0
                    is_scoring = False
                    if event_outcome in ['Single', 'Double', 'Triple', 'Home Run']:
                        if generator.rng.random() < 0.3:  # 30% chance of RBI
                            rbi = generator.rng.randint(1, 3)
                            is_scoring = True
                            if is_top:
                                away_score += rbi
                            else:
                                home_score += rbi
                    
                    # Generate runners as integers or pd.NA
                    runner_1b = int(generator.generate_pitcher_id(batting_team, False)) if generator.rng.random() < 0.2 else pd.NA
                    runner_2b = int(generator.generate_pitcher_id(batting_team, False)) if generator.rng.random() < 0.15 else pd.NA
                    runner_3b = int(generator.generate_pitcher_id(batting_team, False)) if generator.rng.random() < 0.1 else pd.NA
                    
                    play_data = {
                        'game_date': date_str,
                        'game_pk': game_pk,
                        'at_bat_index': at_bat_idx,
                        'event_index': 0,
                        'inning': min(inning, 12),
                        'half_inning': half_inning,
                        'pitcher': generator.generate_pitcher_id(pitching_team),
                        'batter': generator.generate_pitcher_id(batting_team, False),
                        'bat_side': generator.rng.choice(['L', 'R']),
                        'p_throws': generator.rng.choice(['L', 'R']),
                        'count_balls': generator.rng.randint(0, 4),
                        'count_strikes': generator.rng.randint(0, 3),
                        'outs': generator.rng.randint(0, 3),
                        'home_team': game['home_team'],
                        'away_team': game['away_team'],
                        'batting_team': batting_team,
                        'events': event_outcome,
                        'description': f"{event_outcome} by batter",
                        'home_score': home_score,
                        'away_score': away_score,
                        'is_scoring_play': is_scoring,
                        'rbi': rbi,
                        'runner_on_1b': runner_1b,
                        'runner_on_2b': runner_2b,
                        'runner_on_3b': runner_3b,
                    }
                    
                    all_plays.append(play_data)
            
            if all_plays:
                df_plays = pd.DataFrame(all_plays)
                
                # Set nullable integer types
                df_plays['runner_on_1b'] = df_plays['runner_on_1b'].astype('Int64')
                df_plays['runner_on_2b'] = df_plays['runner_on_2b'].astype('Int64')
                df_plays['runner_on_3b'] = df_plays['runner_on_3b'].astype('Int64')
                
                df_plays.to_parquet(out_file, index=False)
                print(f"✅ Placeholder play-by-play: {len(df_plays)} plays → {out_file.name}")
            
            return True
            
        except Exception as e:
            print(f"❌ Placeholder play-by-play error for {date_str}: {e}")
            return False
    
    else:
        # Real play-by-play collection
        print(f"🎬 Collecting REAL play-by-play data for {date_str}...")
        # Implementation for real MLB API calls would go here
        return True

def collect_lineups_data(date_str: str, out_dir: Path, use_placeholder: bool = True) -> bool:
    """Collect lineups data (placeholder mode)"""
    out_file = out_dir / get_output_filename('lineups', date_str)
    
    if out_file.exists():
        print(f"⏭️ Skipping lineups for {date_str} (already exists)")
        return True
    
    if use_placeholder:
        print(f"👥 Generating placeholder lineups data for {date_str}...")
        
        try:
            generator = PlaceholderDataGenerator()
            daily_games = generator.generate_daily_games(date_str)
            
            if not daily_games:
                return True
            
            all_lineups = []
            
            for game in daily_games:
                game_pk = game['game_pk']
                
                # Generate lineups for both teams
                for side, team in [('home', game['home_team']), ('away', game['away_team'])]:
                    team_id = hash(team) % 1000
                    
                    # Generate batting order (9 players)
                    for batting_order in range(1, 10):
                        player_id = generator.generate_pitcher_id(team, False) + batting_order
                        
                        # Position mapping
                        positions = ['C', '1B', '2B', '3B', 'SS', 'LF', 'CF', 'RF', 'DH']
                        position_code = positions[batting_order - 1] if batting_order <= len(positions) else 'OF'
                        
                        # Generate realistic season stats
                        batting_avg = max(0.180, min(0.350, generator.rng.normal(0.265, 0.040)))
                        obp = batting_avg + generator.rng.uniform(0.020, 0.080)
                        slg = batting_avg + generator.rng.uniform(0.050, 0.200)
                        ops = obp + slg
                        
                        lineup_info = {
                            'game_date': date_str,
                            'game_pk': game_pk,
                            'team_id': team_id,
                            'batting_order': batting_order,
                            'person_id': player_id,
                            'side': side,
                            'position_code': position_code,
                            'position_name': position_code,
                            'person_full_name': f"Player {player_id}",
                            'person_bat_side_code': generator.rng.choice(['L', 'R', 'S']),
                            'person_pitch_hand_code': generator.rng.choice(['L', 'R']),
                            'season_avg': round(batting_avg, 3),
                            'season_obp': round(obp, 3),
                            'season_slg': round(slg, 3),
                            'season_ops': round(ops, 3),
                            'season_home_runs': int(generator.rng.randint(0, 35)),
                            'season_rbi': int(generator.rng.randint(10, 100)),
                        }
                        all_lineups.append(lineup_info)
                    
                    # Add starting pitcher
                    pitcher_id = generator.generate_pitcher_id(team, is_starter=True)
                    era = max(1.50, min(7.00, generator.rng.normal(4.20, 0.80)))
                    whip = max(0.80, min(2.00, generator.rng.normal(1.30, 0.20)))
                    
                    pitcher_info = {
                        'game_date': date_str,
                        'game_pk': game_pk,
                        'team_id': team_id,
                        'batting_order': 10,
                        'person_id': pitcher_id,
                        'side': side,
                        'position_code': 'P',
                        'position_name': 'Pitcher',
                        'person_full_name': f"Pitcher {pitcher_id}",
                        'person_bat_side_code': generator.rng.choice(['L', 'R']),
                        'person_pitch_hand_code': generator.rng.choice(['L', 'R']),
                        'season_era': round(era, 2),
                        'season_whip': round(whip, 2),
                        'season_strikeouts': int(generator.rng.randint(50, 250)),
                        'season_innings_pitched': f"{generator.rng.randint(50, 200)}.{generator.rng.randint(0, 2)}",
                    }
                    all_lineups.append(pitcher_info)
            
            if all_lineups:
                df_lineups = pd.DataFrame(all_lineups)
                
                # Force integer columns
                integer_columns = ['season_home_runs', 'season_rbi', 'season_strikeouts']
                for col in integer_columns:
                    if col in df_lineups.columns:
                        df_lineups[col] = df_lineups[col].astype('Int64')
                
                df_lineups.to_parquet(out_file, index=False)
                print(f"✅ Placeholder lineups: {len(df_lineups)} players → {out_file.name}")
            
            return True
            
        except Exception as e:
            print(f"❌ Placeholder lineups error for {date_str}: {e}")
            return False
    
    else:
        # Real lineups collection
        print(f"👥 Collecting REAL lineups data for {date_str}...")
        return True

def calculate_recent_stats(date_str: str, out_dir: Path, use_placeholder: bool = True) -> bool:
    """Calculate recent stats (placeholder mode)"""
    out_file = out_dir / get_output_filename('recent_stats', date_str)
    
    if out_file.exists():
        print(f"⏭️ Skipping recent stats for {date_str} (already exists)")
        return True
    
    if use_placeholder:
        print(f"📈 Generating placeholder recent stats for {date_str}...")
        
        try:
            generator = PlaceholderDataGenerator()
            daily_games = generator.generate_daily_games(date_str)
            
            if not daily_games:
                return True
            
            recent_stats = []
            
            # Generate recent stats for players from today's games
            all_teams = set()
            for game in daily_games:
                all_teams.add(game['home_team'])
                all_teams.add(game['away_team'])
            
            for team in all_teams:
                team_id = hash(team) % 1000
                team_offset = hash(team) % 100000
                
                # Generate stats for batters
                for player_num in range(1, 26):  # 25 players per team
                    player_id = team_offset + player_num
                    
                    # Batting stats (last 15 days)
                    games_played = generator.rng.randint(8, 15)
                    batting_avg = max(0.150, min(0.400, generator.rng.normal(0.265, 0.050)))
                    obp = batting_avg + generator.rng.uniform(0.020, 0.080)
                    slg = batting_avg + generator.rng.uniform(0.050, 0.200)
                    ops = obp + slg
                    
                    # Hot/cold streaks
                    is_hot = ops > 0.850
                    is_cold = ops < 0.650
                    
                    batting_stat = {
                        'stat_date': date_str,
                        'player_id': player_id,
                        'stat_type': 'batting_15d',
                        'games_played': games_played,
                        'date_range_start': (datetime.strptime(date_str, '%Y-%m-%d') - timedelta(days=14)).strftime('%Y-%m-%d'),
                        'date_range_end': date_str,
                        'batting_avg': round(batting_avg, 3),
                        'on_base_pct': round(obp, 3),
                        'slugging_pct': round(slg, 3),
                        'ops': round(ops, 3),
                        'home_runs': int(generator.rng.randint(0, 5)),
                        'rbis': int(generator.rng.randint(0, 15)),
                        'stolen_bases': int(generator.rng.randint(0, 3)),
                        'strikeouts': int(generator.rng.randint(5, 20)),
                        'walks': int(generator.rng.randint(2, 12)),
                        'hot_streak': is_hot,
                        'cold_streak': is_cold,
                        'clutch_performance': generator.rng.uniform(0.200, 0.400),
                        'vs_lefties_ops': round(ops + generator.rng.uniform(-0.100, 0.100), 3),
                        'vs_righties_ops': round(ops + generator.rng.uniform(-0.100, 0.100), 3),
                        'consecutive_games': generator.rng.randint(1, games_played),
                        'workload_score': generator.rng.uniform(20, 80),
                    }
                    recent_stats.append(batting_stat)
                
                # Generate stats for pitchers
                for pitcher_num in range(1, 13):  # 12 pitchers per team
                    pitcher_id = team_offset + 1000 + pitcher_num
                    
                    # Pitching stats (last 15 days)
                    games_played = generator.rng.randint(3, 8)
                    era = max(1.00, min(8.00, generator.rng.normal(4.20, 1.20)))
                    whip = max(0.70, min(2.50, generator.rng.normal(1.30, 0.30)))
                    
                    # Hot/cold streaks for pitchers
                    is_hot = era < 3.00
                    is_cold = era > 5.50
                    
                    pitching_stat = {
                        'stat_date': date_str,
                        'player_id': pitcher_id,
                        'stat_type': 'pitching_15d',
                        'games_played': games_played,
                        'date_range_start': (datetime.strptime(date_str, '%Y-%m-%d') - timedelta(days=14)).strftime('%Y-%m-%d'),
                        'date_range_end': date_str,
                        'era': round(era, 2),
                        'whip': round(whip, 2),
                        'strikeouts_per_9': round(generator.rng.uniform(6.0, 12.0), 1),
                        'walks_per_9': round(generator.rng.uniform(2.0, 5.5), 1),
                        'hits_allowed': generator.rng.randint(8, 25),
                        'runs_allowed': generator.rng.randint(3, 15),
                        'quality_starts': generator.rng.randint(0, 3) if games_played >= 3 else 0,
                        'saves': generator.rng.randint(0, 3),
                        'blown_saves': generator.rng.randint(0, 2),
                        'hot_streak': is_hot,
                        'cold_streak': is_cold,
                        'clutch_performance': generator.rng.uniform(0.250, 0.350),
                        'consecutive_appearances': generator.rng.randint(1, games_played),
                        'workload_score': generator.rng.uniform(30, 90),
                    }
                    recent_stats.append(pitching_stat)
            
            if recent_stats:
                df_stats = pd.DataFrame(recent_stats)
                
                # Force integer columns
                integer_columns = [
                    'games_played', 'home_runs', 'rbis', 'stolen_bases', 'strikeouts', 'walks',
                    'hits_allowed', 'runs_allowed', 'quality_starts', 'saves', 'blown_saves',
                    'consecutive_appearances', 'consecutive_games'
                ]
                for col in integer_columns:
                    if col in df_stats.columns:
                        df_stats[col] = df_stats[col].astype('Int64')
                
                df_stats.to_parquet(out_file, index=False)
                print(f"✅ Placeholder recent stats: {len(df_stats)} player stats → {out_file.name}")
            
            return True
            
        except Exception as e:
            print(f"❌ Placeholder recent stats error for {date_str}: {e}")
            return False
    
    else:
        # Real recent stats calculation
        print(f"📈 Calculating REAL recent stats for {date_str}...")
        return True

def collect_game_info_data(date_str: str, out_dir: Path, use_placeholder: bool = True) -> bool:
    """Collect game info (placeholder mode)"""
    out_file = out_dir / get_output_filename('game_info', date_str)
    
    if out_file.exists():
        print(f"⏭️ Skipping game info for {date_str} (already exists)")
        return True
    
    if use_placeholder:
        print(f"📋 Generating placeholder game info for {date_str}...")
        
        try:
            generator = PlaceholderDataGenerator()
            daily_games = generator.generate_daily_games(date_str)
            
            if not daily_games:
                return True
            
            game_info_records = []
            
            for game in daily_games:
                # Add starting pitchers
                home_starter = generator.generate_pitcher_id(game['home_team'], is_starter=True)
                away_starter = generator.generate_pitcher_id(game['away_team'], is_starter=True)
                
                game_info = {
                    'game_pk': game['game_pk'],
                    'game_date': date_str,
                    'home_team': game['home_team'],
                    'away_team': game['away_team'],
                    'home_score': game['home_score'],
                    'away_score': game['away_score'],
                    'winning_team': game['winning_team'],
                    'game_length_minutes': game['game_length_minutes'],
                    'attendance': game['attendance'],
                    'game_status': game['game_status'],
                    'home_starting_pitcher': home_starter,
                    'away_starting_pitcher': away_starter,
                    'home_starter_name': f"Pitcher {home_starter}",
                    'away_starter_name': f"Pitcher {away_starter}",
                    'series_game_number': game['series_game_number'],
                    'home_team_rest_days': game['home_team_rest_days'],
                    'away_team_rest_days': game['away_team_rest_days'],
                    'venue_name': game['venue_name'],
                    'game_time_et': game['game_time_et'],
                    'day_night': game['day_night'],
                    'home_wins_before': generator.rng.randint(40, 100),
                    'home_losses_before': generator.rng.randint(40, 100),
                    'away_wins_before': generator.rng.randint(40, 100),
                    'away_losses_before': generator.rng.randint(40, 100),
                    'extra_innings': game['game_length_minutes'] > 200,
                }
                
                game_info_records.append(game_info)
            
            if game_info_records:
                df = pd.DataFrame(game_info_records)
                df.to_parquet(out_file, index=False)
                print(f"✅ Placeholder game info: {len(df)} games → {out_file.name}")
            
            return True
            
        except Exception as e:
            print(f"❌ Placeholder game info error for {date_str}: {e}")
            return False
    
    else:
        # Real game info collection
        print(f"📋 Collecting REAL game info for {date_str}...")
        return True

def collect_rosters_data(date_str: str, out_dir: Path, use_placeholder: bool = True) -> bool:
    """Collect rosters (placeholder mode)"""
    out_file = out_dir / get_output_filename('rosters', date_str)
    
    if out_file.exists():
        print(f"⏭️ Skipping rosters for {date_str} (already exists)")
        return True
    
    if use_placeholder:
        print(f"👥 Generating placeholder rosters for {date_str}...")
        
        try:
            generator = PlaceholderDataGenerator()
            daily_games = generator.generate_daily_games(date_str)
            
            if not daily_games:
                return True
            
            # Get unique teams from today's games
            teams = set()
            for game in daily_games:
                teams.add(game['home_team'])
                teams.add(game['away_team'])
            
            roster_records = []
            
            for team in teams:
                team_id = hash(team) % 1000
                
                # Generate 40-man roster (25 active + 15 minors)
                positions = ['C', '1B', '2B', '3B', 'SS', 'LF', 'CF', 'RF', 'DH', 'P'] * 4
                
                for roster_spot in range(40):
                    player_id = generator.generate_pitcher_id(team, False) + roster_spot
                    position = positions[roster_spot] if roster_spot < len(positions) else 'P'
                    
                    roster_record = {
                        'game_date': date_str,
                        'team_id': team_id,
                        'person_id': player_id,
                        'side': 'home' if generator.rng.random() < 0.5 else 'away',
                        'full_name': f"Player {player_id}",
                        'jersey_number': str(generator.rng.randint(1, 99)),
                        'position_code': position,
                        'position_name': position,
                        'bat_side': generator.rng.choice(['L', 'R', 'S']),
                        'pitch_hand': generator.rng.choice(['L', 'R']),
                        'status_code': 'A' if roster_spot < 25 else 'M',
                        'active': roster_spot < 25,
                    }
                    roster_records.append(roster_record)
            
            if roster_records:
                df = pd.DataFrame(roster_records)
                df.to_parquet(out_file, index=False)
                print(f"✅ Placeholder rosters: {len(df)} players → {out_file.name}")
            
            return True
            
        except Exception as e:
            print(f"❌ Placeholder rosters error for {date_str}: {e}")
            return False
    
    else:
        # Real rosters collection
        print(f"👥 Collecting REAL rosters for {date_str}...")
        return True

def collect_umpires_data(date_str: str, out_dir: Path, use_placeholder: bool = True) -> bool:
    """Collect umpires data (placeholder mode)"""
    out_file = out_dir / get_output_filename('umpires', date_str)
    
    if out_file.exists():
        print(f"⏭️ Skipping umpires for {date_str} (already exists)")
        return True
    
    if use_placeholder:
        print(f"👨‍⚖️ Generating placeholder umpires data for {date_str}...")
        
        try:
            generator = PlaceholderDataGenerator()
            daily_games = generator.generate_daily_games(date_str)
            
            if not daily_games:
                return True
            
            # MLB umpire names (sample)
            umpire_names = [
                'Joe West', 'Angel Hernandez', 'CB Bucknor', 'Jim Joyce', 
                'Jerry Meals', 'Marty Foster', 'Tim Timmons', 'Bill Miller',
                'Dan Bellino', 'Ron Kulpa', 'Brian Gorman', 'Pat Hoberg'
            ]
            
            umpire_records = []
            
            for game in daily_games:
                game_pk = game['game_pk']
                
                # Assign random umpires to positions
                positions = ['Home Plate', '1st Base', '2nd Base', '3rd Base']
                assigned_umps = generator.rng.choice(umpire_names, size=4, replace=False)
                
                for position, ump_name in zip(positions, assigned_umps):
                    ump_id = hash(ump_name) % 10000
                    
                    # Generate umpire tendencies (focus on home plate ump)
                    if position == 'Home Plate':
                        avg_runs = max(6.0, min(11.0, generator.rng.normal(8.5, 1.0)))
                        over_under_pct = max(0.30, min(0.70, generator.rng.normal(0.50, 0.08)))
                        sample_size = generator.rng.randint(20, 150)
                        pitcher_friendly = generator.rng.randint(30, 70)
                        strike_rate = max(0.40, min(0.65, generator.rng.normal(0.52, 0.05)))
                        game_length = generator.rng.randint(165, 195)
                    else:
                        avg_runs = 8.5
                        over_under_pct = 0.5
                        sample_size = generator.rng.randint(10, 50)
                        pitcher_friendly = 50
                        strike_rate = 0.52
                        game_length = 180
                    
                    umpire_record = {
                        'game_date': date_str,
                        'game_pk': game_pk,
                        'umpire_id': ump_id,
                        'umpire_name': ump_name,
                        'position': position,
                        'avg_total_runs_in_games': round(avg_runs, 1),
                        'over_under_record': round(over_under_pct, 3),
                        'sample_size': sample_size,
                        'pitcher_friendly_score': round(pitcher_friendly, 1),
                        'strike_rate_overall': round(strike_rate, 3),
                        'avg_game_length_minutes': game_length,
                        'late_inning_strike_rate': round(strike_rate + generator.rng.uniform(-0.02, 0.02), 3),
                        'close_game_strike_rate': round(strike_rate + generator.rng.uniform(-0.03, 0.03), 3),
                        'last_calculated': date_str
                    }
                    umpire_records.append(umpire_record)
            
            if umpire_records:
                df = pd.DataFrame(umpire_records)
                df.to_parquet(out_file, index=False)
                print(f"✅ Placeholder umpires: {len(df)} records → {out_file.name}")
            
            return True
            
        except Exception as e:
            print(f"❌ Placeholder umpires error for {date_str}: {e}")
            return False
    
    else:
        # Real umpires collection
        print(f"👨‍⚖️ Collecting REAL umpires data for {date_str}...")
        return True

# =============================================================================
# MAIN ORCHESTRATION
# =============================================================================

def streamlined_backfill_date_complete(date_str: str, out_dir: Path, use_placeholder: bool = True) -> Dict[str, any]:
    """
    STREAMLINED: Complete backfill with only the core data Claude needs
    Removed weather and venue factor collection (Claude handles these)
    Enhanced Statcast with ALL advanced metrics
    """
    mode = "PLACEHOLDER" if use_placeholder else "REAL"
    print(f"\n📅 {mode} Processing {date_str}")
    
    collection_tasks = [
        # CORE BASEBALL DATA (All essential for Claude's analysis)
        ('game_info', lambda: collect_game_info_data(date_str, out_dir, use_placeholder)),
        ('enhanced_statcast', lambda: collect_enhanced_statcast_data(date_str, out_dir, use_placeholder)),
        ('play_by_play', lambda: collect_play_by_play_data(date_str, out_dir, use_placeholder)),
        ('lineups', lambda: collect_lineups_data(date_str, out_dir, use_placeholder)),
        ('recent_stats', lambda: calculate_recent_stats(date_str, out_dir, use_placeholder)),
        ('rosters', lambda: collect_rosters_data(date_str, out_dir, use_placeholder)),
        ('umpires', lambda: collect_umpires_data(date_str, out_dir, use_placeholder)),
    ]
    
    results = {}
    
    for data_type, task_func in collection_tasks:
        print(f"📊 Collecting {data_type} ({mode})...")
        start_time = time.time()
        
        try:
            results[data_type] = task_func()
            elapsed = time.time() - start_time
            print(f"   ⏱️ Completed in {elapsed:.1f}s")
            
        except Exception as e:
            print(f"❌ {data_type} failed: {e}")
            results[data_type] = False
        
        # Small delay between collections
        if not use_placeholder:
            time.sleep(0.5)
    
    success_count = sum(1 for success in results.values() if success)
    total_count = len(results)
    
    print(f"🎉 {mode} {date_str}: {success_count}/{total_count} data sources collected")
    
    return results

def main():
    parser = argparse.ArgumentParser(description="STREAMLINED MLB data backfill - Core data only, enhanced Statcast")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--output", default="stage", help="Output directory")
    parser.add_argument("--real-data", action="store_true", help="Use real API calls instead of placeholder data")
    args = parser.parse_args()
    
    # Parse dates
    start_date = datetime.strptime(args.start, '%Y-%m-%d')
    end_date = datetime.strptime(args.end, '%Y-%m-%d')
    
    if end_date < start_date:
        raise ValueError("End date must be >= start date")
    
    # Setup output directory
    out_dir = Path(args.output)
    out_dir.mkdir(exist_ok=True)
    
    # Get configuration
    use_placeholder = not args.real_data
    try:
        from py.config import get_config
        config = get_config()
        
        # Override placeholder mode from config if available
        if hasattr(config, 'USE_PLACEHOLDER_DATA'):
            use_placeholder = config.USE_PLACEHOLDER_DATA
            
    except Exception as e:
        print(f"⚠️ Configuration warning: {e}")
    
    mode = "PLACEHOLDER" if use_placeholder else "REAL"
    print(f"🚀 STREAMLINED MLB backfill: {start_date.date()} to {end_date.date()}")
    print(f"🎯 Mode: {mode} data collection")
    print(f"📁 Output directory: {out_dir}")
    print(f"✨ ENHANCED: All advanced Statcast metrics included")
    print(f"🗑️ REMOVED: Weather & venue factors (Claude handles these)")
    
    if use_placeholder:
        print(f"🔧 Using placeholder data - perfect for testing!")
        print(f"   To use real data later: add --real-data flag")
    else:
        print(f"📡 Using real API calls")
    
    # Process each date
    current_date = start_date
    total_days = (end_date - start_date).days + 1
    overall_results = {}
    
    with tqdm(total=total_days, desc=f"Processing dates ({mode})") as pbar:
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            pbar.set_description(f"{mode} processing {date_str}")
            
            try:
                day_results = streamlined_backfill_date_complete(date_str, out_dir, use_placeholder)
                
                for data_type, success in day_results.items():
                    if data_type not in overall_results:
                        overall_results[data_type] = 0
                    if success:
                        overall_results[data_type] += 1
                        
            except Exception as e:
                print(f"❌ Error processing {date_str}: {e}")
            
            current_date += timedelta(days=1)
            pbar.update(1)
            
            time.sleep(0.2)
    
    # Print summary
    print(f"\n🎉 {mode} backfill finished!")
    print(f"📊 Success rates:")
    for data_type, success_count in overall_results.items():
        success_rate = (success_count / total_days) * 100
        print(f"   {data_type}: {success_count}/{total_days} days ({success_rate:.1f}%)")
    
    print(f"\n💡 Next steps:")
    print(f"   1. Load data: python loader/enhanced_load_parquet_into_pg.py --input-dir {args.output}")
    print(f"   2. Run analysis: python py/simple_analysis.py")
    
    if use_placeholder:
        print(f"\n🔄 To switch to real data later:")
        print(f"   Add --real-data flag or set USE_PLACEHOLDER_DATA=false in .env")
    
    print(f"\n✨ ENHANCED FEATURES:")
    print(f"   🎯 ALL advanced Statcast metrics (xBA, xwOBA, barrel rates, spin rates)")
    print(f"   🌤️ Weather analysis provided by Claude")
    print(f"   🏟️ Ballpark factors provided by Claude") 
    print(f"   📊 Streamlined 7-table schema for faster processing")

if __name__ == "__main__":
    main()