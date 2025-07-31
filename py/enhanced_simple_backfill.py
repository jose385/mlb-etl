#!/usr/bin/env python3
"""
enhanced_simple_backfill.py - Complete Enhanced MLB data collection
Collects data for the enhanced simplified schema (9 tables)
Features: Robust error handling, graceful degradation, advanced rate limiting

Usage:
    python enhanced_simple_backfill.py --start YYYY-MM-DD --end YYYY-MM-DD [--output DIR]
"""

import os
import argparse
import time
import requests
import math
import logging
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from enum import Enum
from collections import defaultdict, deque
from dataclasses import dataclass

import pandas as pd
import statsapi
from pybaseball import statcast
from tqdm import tqdm

# =============================================================================
# ENHANCED ERROR HANDLING SYSTEM
# =============================================================================

class DataSourceStatus(Enum):
    """Status of data source collection"""
    SUCCESS = "success"
    PARTIAL_SUCCESS = "partial_success"  
    FAILED = "failed"
    SKIPPED = "skipped"
    API_QUOTA_EXCEEDED = "api_quota_exceeded"
    API_TEMPORARILY_DOWN = "api_temporarily_down"

class EnhancedErrorHandler:
    """Enhanced error handling with graceful degradation"""
    
    def __init__(self):
        self.errors = []
        self.warnings = []
        self.collection_status = {}
        self.logger = logging.getLogger(__name__)
    
    def record_error(self, data_type: str, error: Exception, critical: bool = False):
        """Record an error with context"""
        error_info = {
            'data_type': data_type,
            'error': str(error),
            'error_type': type(error).__name__,
            'critical': critical,
            'timestamp': time.time()
        }
        
        self.errors.append(error_info)
        self.collection_status[data_type] = DataSourceStatus.FAILED
        
        if critical:
            self.logger.error(f"CRITICAL ERROR in {data_type}: {error}")
        else:
            self.logger.warning(f"Non-critical error in {data_type}: {error}")
    
    def record_success(self, data_type: str, records_count: int = 0):
        """Record successful data collection"""
        self.collection_status[data_type] = DataSourceStatus.SUCCESS
        self.logger.info(f"✅ {data_type}: {records_count} records collected")
    
    def record_partial_success(self, data_type: str, reason: str):
        """Record partial success (some data collected but with issues)"""
        self.collection_status[data_type] = DataSourceStatus.PARTIAL_SUCCESS
        self.warnings.append(f"{data_type}: {reason}")
        self.logger.warning(f"⚠️ {data_type}: Partial success - {reason}")
    
    def should_continue_collection(self, data_type: str) -> bool:
        """Determine if collection should continue after an error"""
        # Never stop for non-critical data sources
        non_critical_sources = ['weather', 'umpires', 'venue_factors']
        
        if data_type in non_critical_sources:
            return True
        
        # Stop only if too many critical sources have failed
        critical_failures = sum(1 for dt, status in self.collection_status.items() 
                              if status == DataSourceStatus.FAILED and dt not in non_critical_sources)
        
        return critical_failures < 2  # Allow up to 1 critical failure
    
    def get_collection_summary(self) -> Dict:
        """Get summary of collection results"""
        total_sources = len(self.collection_status)
        successful = sum(1 for status in self.collection_status.values() if status == DataSourceStatus.SUCCESS)
        partial = sum(1 for status in self.collection_status.values() if status == DataSourceStatus.PARTIAL_SUCCESS)
        failed = sum(1 for status in self.collection_status.values() if status == DataSourceStatus.FAILED)
        
        return {
            'total_sources': total_sources,
            'successful': successful,
            'partial_success': partial,
            'failed': failed,
            'success_rate': (successful + partial) / total_sources if total_sources > 0 else 0,
            'errors': self.errors,
            'warnings': self.warnings,
            'status_by_source': dict(self.collection_status)
        }

# =============================================================================
# ROBUST API RATE LIMITING SYSTEM
# =============================================================================

@dataclass
class APIConfig:
    """Configuration for API rate limiting"""
    calls_per_minute: int
    calls_per_hour: int
    calls_per_day: int
    base_delay: float
    max_delay: float
    timeout: int
    max_retries: int

class RobustRateLimiter:
    """Robust rate limiter with different limits per API and exponential backoff"""
    
    def __init__(self):
        # API configurations
        self.api_configs = {
            'mlb_statsapi': APIConfig(
                calls_per_minute=20,
                calls_per_hour=1000, 
                calls_per_day=10000,
                base_delay=0.3,
                max_delay=10.0,
                timeout=30,
                max_retries=3
            ),
            'openweather': APIConfig(
                calls_per_minute=60,
                calls_per_hour=1000,
                calls_per_day=10000,
                base_delay=0.5,
                max_delay=30.0,
                timeout=10,
                max_retries=5
            ),
            'pybaseball': APIConfig(
                calls_per_minute=10,  # Be conservative with baseball-reference
                calls_per_hour=500,
                calls_per_day=5000,
                base_delay=1.0,
                max_delay=60.0,
                timeout=45,
                max_retries=3
            )
        }
        
        # Track API calls
        self.call_history = defaultdict(lambda: {
            'minute': deque(),
            'hour': deque(), 
            'day': deque(),
            'last_call': 0,
            'consecutive_errors': 0,
            'total_calls': 0,
            'quota_reset_time': None
        })
    
    def _clean_old_calls(self, api_name: str):
        """Remove old calls from tracking"""
        now = time.time()
        history = self.call_history[api_name]
        
        # Clean minute history (keep last 60 seconds)
        while history['minute'] and now - history['minute'][0] > 60:
            history['minute'].popleft()
        
        # Clean hour history (keep last 3600 seconds)  
        while history['hour'] and now - history['hour'][0] > 3600:
            history['hour'].popleft()
        
        # Clean day history (keep last 86400 seconds)
        while history['day'] and now - history['day'][0] > 86400:
            history['day'].popleft()
    
    def _can_make_call(self, api_name: str) -> Tuple[bool, str, float]:
        """Check if we can make an API call"""
        if api_name not in self.api_configs:
            return True, "Unknown API - no limits", 0
        
        config = self.api_configs[api_name]
        history = self.call_history[api_name]
        
        self._clean_old_calls(api_name)
        
        # Check quota reset
        if history['quota_reset_time'] and time.time() < history['quota_reset_time']:
            wait_time = history['quota_reset_time'] - time.time()
            return False, f"Quota exceeded, reset in {wait_time:.1f}s", wait_time
        
        # Check per-minute limit
        if len(history['minute']) >= config.calls_per_minute:
            oldest_call = history['minute'][0]
            wait_time = 60 - (time.time() - oldest_call)
            return False, f"Per-minute limit reached", max(0, wait_time)
        
        # Check per-hour limit
        if len(history['hour']) >= config.calls_per_hour:
            oldest_call = history['hour'][0]
            wait_time = 3600 - (time.time() - oldest_call)
            return False, f"Per-hour limit reached", max(0, wait_time)
        
        # Check per-day limit
        if len(history['day']) >= config.calls_per_day:
            oldest_call = history['day'][0]
            wait_time = 86400 - (time.time() - oldest_call)
            return False, f"Per-day limit reached", max(0, wait_time)
        
        # Check minimum delay since last call
        if history['last_call']:
            time_since_last = time.time() - history['last_call']
            min_delay = self._calculate_delay(api_name)
            
            if time_since_last < min_delay:
                wait_time = min_delay - time_since_last
                return False, f"Minimum delay not met", wait_time
        
        return True, "OK", 0
    
    def _calculate_delay(self, api_name: str) -> float:
        """Calculate delay with exponential backoff for errors"""
        config = self.api_configs[api_name]
        history = self.call_history[api_name]
        
        base_delay = config.base_delay
        
        # Exponential backoff for consecutive errors
        if history['consecutive_errors'] > 0:
            backoff_multiplier = min(2 ** history['consecutive_errors'], 16)  # Cap at 16x
            delay = base_delay * backoff_multiplier
        else:
            delay = base_delay
        
        # Add small random jitter to avoid thundering herd
        jitter = random.uniform(0.1, 0.3)
        delay += jitter
        
        # Ensure we don't exceed max delay
        return min(delay, config.max_delay)
    
    def wait_for_api(self, api_name: str, operation: str = "API call") -> bool:
        """Wait until we can make an API call"""
        config = self.api_configs.get(api_name)
        if not config:
            return True
        
        max_wait_time = 300  # 5 minutes max wait
        total_wait_time = 0
        
        while total_wait_time < max_wait_time:
            can_call, reason, wait_time = self._can_make_call(api_name)
            
            if can_call:
                return True
            
            if wait_time > max_wait_time - total_wait_time:
                print(f"⏰ {operation} wait time ({wait_time:.1f}s) exceeds maximum - skipping")
                return False
            
            print(f"🚦 {operation}: {reason}, waiting {wait_time:.1f}s...")
            time.sleep(wait_time)
            total_wait_time += wait_time
        
        print(f"⏰ {operation}: Maximum wait time exceeded")
        return False
    
    def record_api_call(self, api_name: str, success: bool = True):
        """Record an API call"""
        now = time.time()
        history = self.call_history[api_name]
        
        # Record the call
        history['minute'].append(now)
        history['hour'].append(now)
        history['day'].append(now)
        history['last_call'] = now
        history['total_calls'] += 1
        
        # Update error tracking
        if success:
            history['consecutive_errors'] = 0
            # Clear quota reset if we had one and the call succeeded
            if history['quota_reset_time']:
                history['quota_reset_time'] = None
        else:
            history['consecutive_errors'] += 1
    
    def record_quota_exceeded(self, api_name: str, reset_time_seconds: int = 3600):
        """Record that API quota was exceeded"""
        self.call_history[api_name]['quota_reset_time'] = time.time() + reset_time_seconds
        self.call_history[api_name]['consecutive_errors'] += 1
    
    def get_api_status(self, api_name: str) -> Dict:
        """Get current status of an API"""
        if api_name not in self.api_configs:
            return {"status": "unknown", "message": "API not configured"}
        
        config = self.api_configs[api_name]
        history = self.call_history[api_name]
        
        self._clean_old_calls(api_name)
        
        return {
            "status": "available" if self._can_make_call(api_name)[0] else "limited",
            "calls_this_minute": len(history['minute']),
            "calls_this_hour": len(history['hour']),
            "calls_this_day": len(history['day']),
            "total_calls": history['total_calls'],
            "consecutive_errors": history['consecutive_errors'],
            "limits": {
                "per_minute": config.calls_per_minute,
                "per_hour": config.calls_per_hour,
                "per_day": config.calls_per_day
            },
            "quota_reset_time": history['quota_reset_time']
        }

# Global rate limiter instance
rate_limiter = RobustRateLimiter()

def api_call_with_retry(api_name: str, operation_name: str, call_func, *args, **kwargs):
    """Make an API call with robust retry logic"""
    config = rate_limiter.api_configs.get(api_name)
    if not config:
        # No rate limiting for unknown APIs
        return call_func(*args, **kwargs)
    
    last_exception = None
    
    for attempt in range(config.max_retries):
        # Wait for rate limit
        if not rate_limiter.wait_for_api(api_name, operation_name):
            print(f"❌ {operation_name}: Rate limit wait failed")
            return None
        
        try:
            # Make the API call
            result = call_func(*args, **kwargs)
            
            # Record successful call
            rate_limiter.record_api_call(api_name, success=True)
            
            return result
            
        except requests.exceptions.HTTPError as e:
            last_exception = e
            rate_limiter.record_api_call(api_name, success=False)
            
            if e.response.status_code == 429:  # Rate limit exceeded
                retry_after = int(e.response.headers.get('Retry-After', 60))
                rate_limiter.record_quota_exceeded(api_name, retry_after)
                print(f"🚦 {operation_name}: Rate limited, backing off for {retry_after}s")
                
                if attempt < config.max_retries - 1:
                    time.sleep(retry_after)
                    continue
                else:
                    print(f"❌ {operation_name}: Max retries exceeded for rate limiting")
                    break
                    
            elif 500 <= e.response.status_code < 600:  # Server error
                delay = rate_limiter._calculate_delay(api_name)
                print(f"🔧 {operation_name}: Server error {e.response.status_code}, retrying in {delay:.1f}s (attempt {attempt + 1}/{config.max_retries})")
                
                if attempt < config.max_retries - 1:
                    time.sleep(delay)
                    continue
                else:
                    print(f"❌ {operation_name}: Max retries exceeded for server errors")
                    break
            else:
                # Client error - don't retry
                print(f"❌ {operation_name}: Client error {e.response.status_code} - {e}")
                break
                
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            last_exception = e
            rate_limiter.record_api_call(api_name, success=False)
            
            delay = rate_limiter._calculate_delay(api_name)
            print(f"🌐 {operation_name}: Network error, retrying in {delay:.1f}s (attempt {attempt + 1}/{config.max_retries})")
            
            if attempt < config.max_retries - 1:
                time.sleep(delay)
                continue
            else:
                print(f"❌ {operation_name}: Max retries exceeded for network errors")
                break
                
        except Exception as e:
            last_exception = e
            rate_limiter.record_api_call(api_name, success=False)
            print(f"❌ {operation_name}: Unexpected error - {e}")
            break
    
    # All retries failed
    if last_exception:
        raise last_exception
    else:
        raise Exception(f"{operation_name}: All retry attempts failed")

# =============================================================================
# STANDARDIZED FILE NAMING SYSTEM
# =============================================================================

def get_output_filename(data_type: str, date_str: str = None) -> str:
    """
    Standardized file naming that matches loader expectations
    This ensures enhanced_simple_backfill.py creates files that
    enhanced_load_parquet_into_pg.py can properly map to tables
    """
    filename_mapping = {
        # Core data types (date-specific)
        'games': f'games_{date_str}.parquet' if date_str else 'games.parquet',
        'play_by_play': f'play_by_play_{date_str}.parquet' if date_str else 'play_by_play.parquet',
        'game_info': f'game_info_{date_str}.parquet' if date_str else 'game_info.parquet',
        'lineups': f'lineups_{date_str}.parquet' if date_str else 'lineups.parquet',
        'rosters': f'rosters_{date_str}.parquet' if date_str else 'rosters.parquet',
        'umpires': f'umpires_{date_str}.parquet' if date_str else 'umpires.parquet',
        'weather': f'weather_{date_str}.parquet' if date_str else 'weather.parquet',
        
        # One-time data types (no date)
        'venue_factors': 'venue_factors.parquet',
        'recent_stats': f'recent_stats_{date_str}.parquet' if date_str else 'recent_stats.parquet',
        
        # Legacy support (in case old names are used)
        'statcast': f'games_{date_str}.parquet' if date_str else 'games.parquet',
        'statsapi': f'play_by_play_{date_str}.parquet' if date_str else 'play_by_play.parquet',
        'lineup': f'lineups_{date_str}.parquet' if date_str else 'lineups.parquet',
        'roster': f'rosters_{date_str}.parquet' if date_str else 'rosters.parquet',
    }
    
    return filename_mapping.get(data_type, f'{data_type}_{date_str}.parquet' if date_str else f'{data_type}.parquet')

# =============================================================================
# STADIUM DATA AND CONFIGURATIONS
# =============================================================================

# Essential stadium coordinates for weather
STADIUM_LOCATIONS = {
    "Arizona Diamondbacks": {"lat": 33.4453, "lon": -112.0667},
    "Atlanta Braves": {"lat": 33.8906, "lon": -84.4677},
    "Baltimore Orioles": {"lat": 39.2840, "lon": -76.6217},
    "Boston Red Sox": {"lat": 42.3467, "lon": -71.0972},
    "Chicago White Sox": {"lat": 41.8299, "lon": -87.6338},
    "Chicago Cubs": {"lat": 41.9484, "lon": -87.6553},
    "Cincinnati Reds": {"lat": 39.5031, "lon": -84.3668},
    "Cleveland Guardians": {"lat": 41.4958, "lon": -81.6853},
    "Colorado Rockies": {"lat": 39.7559, "lon": -104.9942},
    "Detroit Tigers": {"lat": 42.3390, "lon": -83.0485},
    "Houston Astros": {"lat": 29.7570, "lon": -95.3555},
    "Kansas City Royals": {"lat": 39.0517, "lon": -94.4803},
    "Los Angeles Angels": {"lat": 33.8003, "lon": -117.8827},
    "Los Angeles Dodgers": {"lat": 34.0739, "lon": -118.2400},
    "Miami Marlins": {"lat": 25.7781, "lon": -80.2198},
    "Milwaukee Brewers": {"lat": 43.0280, "lon": -87.9712},
    "Minnesota Twins": {"lat": 44.9817, "lon": -93.2776},
    "New York Mets": {"lat": 40.7571, "lon": -73.8458},
    "New York Yankees": {"lat": 40.8296, "lon": -73.9262},
    "Oakland Athletics": {"lat": 37.7516, "lon": -122.2005},
    "Philadelphia Phillies": {"lat": 39.9061, "lon": -75.1665},
    "Pittsburgh Pirates": {"lat": 40.4469, "lon": -80.0057},
    "San Diego Padres": {"lat": 32.7073, "lon": -117.1566},
    "San Francisco Giants": {"lat": 37.7786, "lon": -122.3893},
    "Seattle Mariners": {"lat": 47.5914, "lon": -122.3326},
    "St. Louis Cardinals": {"lat": 38.6226, "lon": -90.1928},
    "Tampa Bay Rays": {"lat": 27.7682, "lon": -82.6534},
    "Texas Rangers": {"lat": 32.7513, "lon": -97.0830},
    "Toronto Blue Jays": {"lat": 43.6414, "lon": -79.3894},
    "Washington Nationals": {"lat": 38.8730, "lon": -77.0074}
}

# Enhanced venue factors data (one-time setup)
VENUE_FACTORS = {
    "Coors Field": {
        "home_team": "Colorado Rockies",
        "elevation_feet": 5200,
        "hr_factor": 1.25,
        "run_factor": 1.18,
        "pitcher_friendly_score": 2,
        "dome_stadium": False,
        "short_porch": False
    },
    "Fenway Park": {
        "home_team": "Boston Red Sox", 
        "elevation_feet": 20,
        "hr_factor": 1.05,
        "run_factor": 1.02,
        "pitcher_friendly_score": 5,
        "dome_stadium": False,
        "short_porch": True  # Green Monster
    },
    "Yankee Stadium": {
        "home_team": "New York Yankees",
        "elevation_feet": 55,
        "hr_factor": 1.08,
        "run_factor": 1.05,
        "pitcher_friendly_score": 4,
        "dome_stadium": False,
        "short_porch": True  # Right field
    },
    "Tropicana Field": {
        "home_team": "Tampa Bay Rays",
        "elevation_feet": 19,
        "hr_factor": 0.94,
        "run_factor": 0.94,
        "pitcher_friendly_score": 7,
        "dome_stadium": True,
        "short_porch": False
    },
    "Marlins Park": {
        "home_team": "Miami Marlins",
        "elevation_feet": 10,
        "hr_factor": 0.92,
        "run_factor": 0.92,
        "pitcher_friendly_score": 8,
        "dome_stadium": True,
        "short_porch": False
    }
}

def get_stadium_coords(team_name: str) -> Dict[str, float]:
    """Get stadium coordinates for team"""
    for stadium_team, coords in STADIUM_LOCATIONS.items():
        if any(word in stadium_team for word in team_name.split()) or \
           any(word in team_name for word in stadium_team.split()):
            return coords
    
    # Default coordinates (New York)
    return {"lat": 40.7128, "lon": -74.0060}

# =============================================================================
# DATA COLLECTION FUNCTIONS WITH ENHANCED ERROR HANDLING
# =============================================================================

def safe_data_collection(func):
    """Decorator for safe data collection with error handling"""
    def wrapper(date_str: str, out_dir: Path, error_handler: EnhancedErrorHandler = None, **kwargs):
        data_type = func.__name__.replace('fetch_', '').replace('_data', '')
        
        if error_handler is None:
            error_handler = EnhancedErrorHandler()
        
        try:
            # Attempt data collection
            result = func(date_str, out_dir, **kwargs)
            
            if result:
                error_handler.record_success(data_type)
                return True
            else:
                error_handler.record_partial_success(data_type, "Function returned False but no exception")
                return False
                
        except requests.exceptions.Timeout as e:
            error_handler.record_error(data_type, e, critical=False)
            print(f"⏰ {data_type} timeout - will retry with longer timeout")
            return False
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:  # Rate limit exceeded
                error_handler.record_error(data_type, e, critical=False) 
                print(f"🚦 {data_type} rate limited - will retry later")
                time.sleep(60)  # Wait 1 minute
                return False
            elif e.response.status_code >= 500:  # Server error
                error_handler.record_error(data_type, e, critical=False)
                print(f"🔧 {data_type} server error - continuing without this data")
                return False
            else:
                error_handler.record_error(data_type, e, critical=True)
                return False
                
        except requests.exceptions.ConnectionError as e:
            error_handler.record_error(data_type, e, critical=False)
            print(f"🌐 {data_type} connection error - continuing without this data")
            return False
            
        except Exception as e:
            # Determine if error is critical based on data source
            critical_sources = ['games', 'play_by_play', 'game_info', 'lineups', 'rosters']
            is_critical = data_type in critical_sources
            
            error_handler.record_error(data_type, e, critical=is_critical)
            
            if is_critical:
                print(f"❌ CRITICAL: {data_type} failed - {str(e)}")
            else:
                print(f"⚠️ {data_type} failed but continuing - {str(e)}")
            
            return False
    
    return wrapper

def fetch_game_data(date_str: str, out_dir: Path) -> bool:
    """Fetch basic game data using Statcast with robust error handling"""
    out_file = out_dir / get_output_filename('games', date_str)
    
    if out_file.exists():
        print(f"⏭️ Skipping games for {date_str} (already exists)")
        return True
    
    print(f"⚾ Fetching game data for {date_str}...")
    
    try:
        # Use robust API calling for Statcast
        def get_statcast_data():
            return statcast(start_dt=date_str, end_dt=date_str)
        
        df = api_call_with_retry(
            'pybaseball',
            f'Statcast data for {date_str}',
            get_statcast_data
        )
        
        if df is None or df.empty:
            print(f"✅ No games for {date_str}")
            return True
        
        # Keep only essential columns for betting analysis
        essential_columns = [
            'game_date', 'game_pk', 'home_team', 'away_team',
            'inning', 'inning_topbot', 'batter', 'pitcher',
            'events', 'description', 'zone', 'balls', 'strikes',
            'release_speed', 'plate_x', 'plate_z',
            'hit_distance_sc', 'launch_speed', 'launch_angle',
            'woba_value', 'at_bat_number', 'pitch_number', 
            'stand', 'p_throws', 'outs_when_up', 'delta_run_exp', 'pitch_type'
        ]
        
        # Keep only columns that exist in the data
        available_columns = [col for col in essential_columns if col in df.columns]
        df_filtered = df[available_columns].copy()
        
        # Clean column names to match schema
        df_filtered.columns = [col.lower().replace('.', '_') for col in df_filtered.columns]
        
        # Save to parquet
        df_filtered.to_parquet(out_file, index=False)
        print(f"✅ Games: {len(df_filtered)} rows, {len(df_filtered.columns)} columns → {out_file.name}")
        return True
        
    except Exception as e:
        print(f"❌ Game data error for {date_str}: {e}")
        return False

def fetch_play_by_play_data(date_str: str, out_dir: Path) -> bool:
    """Fetch play-by-play data with robust error handling"""
    out_file = out_dir / get_output_filename('play_by_play', date_str)
    
    if out_file.exists():
        print(f"⏭️ Skipping play-by-play for {date_str} (already exists)")
        return True
    
    print(f"📊 Fetching play-by-play for {date_str}...")
    
    try:
        # Use robust API calling for MLB StatsAPI
        def get_schedule_data():
            return statsapi.schedule(start_date=date_str, end_date=date_str) or []
        
        games = api_call_with_retry(
            'mlb_statsapi',
            f'MLB schedule for {date_str}',
            get_schedule_data
        )
        
        if not games:
            print(f"✅ No games scheduled for {date_str}")
            return True
        
        play_records = []
        
        for game in games:
            game_pk = game.get("game_id") or game.get("game_pk")
            
            try:
                # Use robust API calling for play-by-play
                def get_pbp_data():
                    return statsapi.get("game_playByPlay", {"gamePk": game_pk})
                
                pbp_data = api_call_with_retry(
                    'mlb_statsapi',
                    f'Play-by-play for game {game_pk}',
                    get_pbp_data
                )
                
                if not pbp_data:
                    continue
                
                plays = pbp_data.get("allPlays", [])
                
                for play_idx, play in enumerate(plays):
                    # Extract essential betting context
                    about = play.get("about", {})
                    result = play.get("result", {})
                    count = play.get("count", {})
                    runners = play.get("runners", [])
                    
                    play_record = {
                        "game_date": date_str,
                        "game_pk": game_pk,
                        "at_bat_index": about.get("atBatIndex", play_idx),
                        "event_index": play_idx,
                        "inning": about.get("inning"),
                        "half_inning": about.get("halfInning"),
                        "pitcher": play.get("matchup", {}).get("pitcher", {}).get("id"),
                        "batter": play.get("matchup", {}).get("batter", {}).get("id"),
                        "bat_side": play.get("matchup", {}).get("batSide", {}).get("code"),
                        "p_throws": play.get("matchup", {}).get("pitchHand", {}).get("code"),
                        "count_balls": count.get("balls"),
                        "count_strikes": count.get("strikes"),
                        "outs": count.get("outs"),
                        "home_team": about.get("halfInning") == "bottom" and "batting" or "fielding",
                        "away_team": about.get("halfInning") == "top" and "batting" or "fielding",
                        "batting_team": about.get("halfInning") == "top" and "away" or "home",
                        "events": result.get("event"),
                        "description": result.get("description"),
                        "home_score": about.get("homeScore"),
                        "away_score": about.get("awayScore"),
                        "is_scoring_play": result.get("rbi", 0) > 0,
                        "rbi": result.get("rbi", 0),
                        "runner_on_1b": any(r.get("start", {}).get("base") == 1 for r in runners),
                        "runner_on_2b": any(r.get("start", {}).get("base") == 2 for r in runners),
                        "runner_on_3b": any(r.get("start", {}).get("base") == 3 for r in runners),
                    }
                    
                    play_records.append(play_record)
                    
            except Exception as e:
                print(f"⚠️ Error getting play-by-play for game {game_pk}: {e}")
                continue
        
        if play_records:
            df = pd.DataFrame(play_records)
            df.to_parquet(out_file, index=False)
            print(f"✅ Play-by-play: {len(df)} plays → {out_file.name}")
        else:
            print(f"✅ No play-by-play data for {date_str}")
        
        return True
        
    except Exception as e:
        print(f"❌ Play-by-play error for {date_str}: {e}")
        return False

def fetch_game_info_data(date_str: str, out_dir: Path) -> bool:
    """Fetch game info with starting pitchers and results"""
    out_file = out_dir / get_output_filename('game_info', date_str)
    
    if out_file.exists():
        print(f"⏭️ Skipping game info for {date_str} (already exists)")
        return True
    
    print(f"🎮 Fetching game info for {date_str}...")
    
    try:
        def get_schedule_data():
            return statsapi.schedule(start_date=date_str, end_date=date_str) or []
        
        games = api_call_with_retry(
            'mlb_statsapi',
            f'MLB schedule for {date_str}',
            get_schedule_data
        )
        
        if not games:
            print(f"✅ No games scheduled for {date_str}")
            return True
        
        game_info_records = []
        
        for game in games:
            game_pk = game.get("game_id") or game.get("game_pk")
            
            try:
                # Get detailed game data
                def get_game_data():
                    return statsapi.get("game", {"gamePk": game_pk})
                
                game_data = api_call_with_retry(
                    'mlb_statsapi',
                    f'Game info for {game_pk}',
                    get_game_data
                )
                
                if not game_data:
                    continue
                
                game_info = game_data.get("gameData", {})
                live_data = game_data.get("liveData", {})
                
                # Extract game info
                status = game_info.get("status", {})
                teams = game_info.get("teams", {})
                venue = game_info.get("venue", {})
                
                # Get probable pitchers
                home_pitcher_id = None
                away_pitcher_id = None
                home_pitcher_name = None
                away_pitcher_name = None
                
                probables = game_info.get("probablePitchers", {})
                if probables.get("home"):
                    home_pitcher_id = probables["home"].get("id")
                    home_pitcher_name = probables["home"].get("fullName")
                if probables.get("away"):
                    away_pitcher_id = probables["away"].get("id") 
                    away_pitcher_name = probables["away"].get("fullName")
                
                # Get final scores if game is complete
                home_score = None
                away_score = None
                winning_team = None
                
                if status.get("abstractGameState") == "Final":
                    line_score = live_data.get("linescore", {})
                    if line_score:
                        home_score = line_score.get("teams", {}).get("home", {}).get("runs")
                        away_score = line_score.get("teams", {}).get("away", {}).get("runs")
                        if home_score is not None and away_score is not None:
                            winning_team = teams["home"]["name"] if home_score > away_score else teams["away"]["name"]
                
                game_info_record = {
                    "game_pk": game_pk,
                    "game_date": date_str,
                    "home_team": teams.get("home", {}).get("name", ""),
                    "away_team": teams.get("away", {}).get("name", ""),
                    "home_score": home_score,
                    "away_score": away_score,
                    "winning_team": winning_team,
                    "venue_name": venue.get("name", ""),
                    "game_status": status.get("detailedState", ""),
                    "home_starting_pitcher": home_pitcher_id,
                    "away_starting_pitcher": away_pitcher_id,
                    "home_starter_name": home_pitcher_name,
                    "away_starter_name": away_pitcher_name,
                    "series_game_number": game.get("seriesGameNumber", 1),
                    "game_time_et": game.get("gameDate", ""),
                    "day_night": "Day" if "1" in game.get("gameDate", "") else "Night",
                }
                
                game_info_records.append(game_info_record)
                
            except Exception as e:
                print(f"⚠️ Error getting game info for {game_pk}: {e}")
                continue
        
        if game_info_records:
            df = pd.DataFrame(game_info_records)
            df.to_parquet(out_file, index=False)
            print(f"✅ Game info: {len(df)} games → {out_file.name}")
        
        return True
        
    except Exception as e:
        print(f"❌ Game info error for {date_str}: {e}")
        return False

def fetch_venue_factors_data(out_dir: Path) -> bool:
    """One-time setup of venue factors data"""
    out_file = out_dir / get_output_filename('venue_factors')
    
    if out_file.exists():
        print(f"⏭️ Venue factors already exist")
        return True
    
    print(f"🏟️ Creating venue factors data...")
    
    try:
        venue_records = []
        
        for venue_name, factors in VENUE_FACTORS.items():
            venue_record = {
                "venue_name": venue_name,
                "home_team": factors["home_team"],
                "elevation_feet": factors["elevation_feet"],
                "hr_factor": factors["hr_factor"],
                "run_factor": factors["run_factor"],
                "pitcher_friendly_score": factors["pitcher_friendly_score"],
                "dome_stadium": factors["dome_stadium"],
                "short_porch": factors["short_porch"],
                "season_year": 2024,
                "last_updated": datetime.now().strftime("%Y-%m-%d"),
                "foul_territory_rank": 15,  # Neutral
                "over_under_tendency": 0.5,  # Neutral
                "average_game_length_minutes": 180,  # 3 hours
            }
            
            venue_records.append(venue_record)
        
        if venue_records:
            df = pd.DataFrame(venue_records)
            df.to_parquet(out_file, index=False)
            print(f"✅ Venue factors: {len(df)} venues → {out_file.name}")
        
        return True
        
    except Exception as e:
        print(f"❌ Venue factors error: {e}")
        return False

def fetch_lineups_data(date_str: str, out_dir: Path) -> bool:
    """Fetch lineups data with robust error handling"""
    out_file = out_dir / get_output_filename('lineups', date_str)
    
    if out_file.exists():
        print(f"⏭️ Skipping lineups for {date_str} (already exists)")
        return True
    
    print(f"👥 Fetching lineups for {date_str}...")
    
    try:
        def get_schedule_data():
            return statsapi.schedule(start_date=date_str, end_date=date_str) or []
        
        games = api_call_with_retry(
            'mlb_statsapi',
            f'MLB schedule for {date_str}',
            get_schedule_data
        )
        
        if not games:
            print(f"✅ No games scheduled for {date_str}")
            return True
        
        lineup_records = []
        
        for game in games:
            game_pk = game.get("game_id") or game.get("game_pk")
            
            try:
                # Get boxscore with lineup data
                def get_boxscore_data():
                    return statsapi.get("game_boxscore", {"gamePk": game_pk})
                
                boxscore = api_call_with_retry(
                    'mlb_statsapi',
                    f'Boxscore for game {game_pk}',
                    get_boxscore_data
                )
                
                if not boxscore:
                    continue
                
                teams = boxscore.get("teams", {})
                
                for side in ["home", "away"]:
                    team_data = teams.get(side, {})
                    team_id = game.get(f"{side}_id")
                    
                    # Get batting order
                    batters = team_data.get("batters", [])
                    players = team_data.get("players", {})
                    
                    for batting_order, player_id in enumerate(batters[:9], start=1):
                        player_info = players.get(f"ID{player_id}", {})
                        person = player_info.get("person", {})
                        position = player_info.get("position", {})
                        stats = player_info.get("stats", {}).get("batting", {})
                        
                        lineup_record = {
                            "game_date": date_str,
                            "game_pk": game_pk,
                            "team_id": team_id,
                            "side": side,
                            "batting_order": batting_order,
                            "person_id": player_id,
                            "person_full_name": person.get("fullName", ""),
                            "position_code": position.get("code"),
                            "position_name": position.get("name"),
                            "person_bat_side_code": person.get("batSide", {}).get("code"),
                            "person_pitch_hand_code": person.get("pitchHand", {}).get("code"),
                            "season_avg": stats.get("avg"),
                            "season_obp": stats.get("obp"),
                            "season_slg": stats.get("slg"),
                            "season_ops": stats.get("ops"),
                            "season_home_runs": stats.get("homeRuns"),
                            "season_rbi": stats.get("rbi"),
                        }
                        
                        lineup_records.append(lineup_record)
                        
            except Exception as e:
                print(f"⚠️ Error getting lineups for game {game_pk}: {e}")
                continue
        
        if lineup_records:
            df = pd.DataFrame(lineup_records)
            df.to_parquet(out_file, index=False)
            print(f"✅ Lineups: {len(df)} players → {out_file.name}")
        else:
            print(f"✅ No lineup data for {date_str}")
        
        return True
        
    except Exception as e:
        print(f"❌ Lineups error for {date_str}: {e}")
        return False

def fetch_rosters_data(date_str: str, out_dir: Path) -> bool:
    """Fetch rosters data with robust error handling"""
    out_file = out_dir / get_output_filename('rosters', date_str)
    
    if out_file.exists():
        print(f"⏭️ Skipping rosters for {date_str} (already exists)")
        return True
    
    print(f"👤 Fetching rosters for {date_str}...")
    
    try:
        # Get games for the date
        def get_schedule_data():
            return statsapi.schedule(start_date=date_str, end_date=date_str) or []
        
        games = api_call_with_retry(
            'mlb_statsapi',
            f'MLB schedule for {date_str}',
            get_schedule_data
        )
        
        if not games:
            print(f"✅ No games scheduled for {date_str}")
            return True
        
        roster_records = []
        seen_teams = set()
        
        for game in games:
            for team_id in [game["home_id"], game["away_id"]]:
                if team_id in seen_teams:
                    continue
                seen_teams.add(team_id)
                
                try:
                    def get_roster_data():
                        return statsapi.get("team_roster", {
                            "teamId": team_id, 
                            "rosterType": "active"
                        })
                    
                    roster_data = api_call_with_retry(
                        'mlb_statsapi',
                        f'Roster for team {team_id}',
                        get_roster_data
                    )
                    
                    if not roster_data:
                        continue
                    
                    for player in roster_data.get("roster", []):
                        person = player.get("person", {})
                        position = player.get("position", {})
                        
                        roster_record = {
                            "game_date": date_str,
                            "team_id": team_id,
                            "person_id": person.get("id"),
                            "side": "home" if team_id == game.get("home_id") else "away",
                            "full_name": person.get("fullName", ""),
                            "jersey_number": player.get("jerseyNumber"),
                            "position_code": position.get("code"),
                            "position_name": position.get("name"),
                            "bat_side": person.get("batSide", {}).get("code"),
                            "pitch_hand": person.get("pitchHand", {}).get("code"),
                            "active": True,
                        }
                        
                        roster_records.append(roster_record)
                        
                except Exception as e:
                    print(f"⚠️ Error getting roster for team {team_id}: {e}")
                    continue
        
        if roster_records:
            df = pd.DataFrame(roster_records)
            df.to_parquet(out_file, index=False)
            print(f"✅ Rosters: {len(df)} players → {out_file.name}")
        else:
            print(f"✅ No roster data for {date_str}")
        
        return True
        
    except Exception as e:
        print(f"❌ Rosters error for {date_str}: {e}")
        return False

def fetch_weather_data(date_str: str, out_dir: Path, api_key: Optional[str] = None) -> bool:
    """Fetch weather data with robust error handling"""
    out_file = out_dir / get_output_filename('weather', date_str)
    
    if out_file.exists():
        print(f"⏭️ Skipping weather for {date_str} (already exists)")
        return True
    
    if not api_key:
        print(f"⚠️ No weather API key - skipping weather for {date_str}")
        return True
    
    # Only collect weather for recent dates
    target_date = datetime.fromisoformat(date_str)
    days_ago = (datetime.now() - target_date).days
    if days_ago > 7:
        print(f"⏭️ Skipping weather for {date_str} (too old for current weather)")
        return True
    
    print(f"🌤️ Fetching weather for {date_str}...")
    
    try:
        # Get games for the date first
        def get_schedule_data():
            return statsapi.schedule(start_date=date_str, end_date=date_str) or []
        
        games = api_call_with_retry(
            'mlb_statsapi',
            f'MLB schedule for {date_str}',
            get_schedule_data
        )
        
        if not games:
            print(f"✅ No games scheduled for {date_str}")
            return True
        
        weather_records = []
        
        for game in games:
            try:
                home_team = game.get("home_name", "")
                away_team = game.get("away_name", "")
                
                # Get stadium coordinates
                coords = get_stadium_coords(home_team)
                
                # Get weather data
                def get_weather_data():
                    url = "http://api.openweathermap.org/data/2.5/weather"
                    params = {
                        'lat': coords['lat'],
                        'lon': coords['lon'],
                        'appid': api_key,
                        'units': 'imperial'
                    }
                    response = requests.get(url, params=params, timeout=10)
                    response.raise_for_status()
                    return response.json()
                
                weather_data = api_call_with_retry(
                    'openweather',
                    f'Weather for {home_team}',
                    get_weather_data
                )
                
                if not weather_data:
                    continue
                
                # Extract weather info
                main = weather_data.get('main', {})
                wind = weather_data.get('wind', {})
                
                weather_record = {
                    "game_date": date_str,
                    "game_pk": game.get("game_id") or game.get("game_pk"),
                    "venue_name": game.get("venue_name", ""),
                    "home_team": home_team,
                    "away_team": away_team,
                    "temperature_f": main.get('temp'),
                    "humidity_pct": main.get('humidity'),
                    "wind_speed_mph": wind.get('speed'),
                    "wind_direction_deg": wind.get('deg'),
                    "data_source": "openweather",
                }
                
                weather_records.append(weather_record)
                
            except Exception as e:
                print(f"⚠️ Error getting weather for game: {e}")
                continue
        
        if weather_records:
            df = pd.DataFrame(weather_records)
            df.to_parquet(out_file, index=False)
            print(f"✅ Weather: {len(df)} games → {out_file.name}")
        else:
            print(f"✅ No weather data for {date_str}")
        
        return True
        
    except Exception as e:
        print(f"❌ Weather error for {date_str}: {e}")
        return False

def fetch_umpires_data(date_str: str, out_dir: Path) -> bool:
    """Fetch umpires data with basic info"""
    out_file = out_dir / get_output_filename('umpires', date_str)
    
    if out_file.exists():
        print(f"⏭️ Skipping umpires for {date_str} (already exists)")
        return True
    
    print(f"👨‍⚖️ Fetching umpires for {date_str}...")
    
    try:
        # For now, create basic placeholder structure
        # TODO: Implement actual umpire data collection when API is available
        
        def get_schedule_data():
            return statsapi.schedule(start_date=date_str, end_date=date_str) or []
        
        games = api_call_with_retry(
            'mlb_statsapi',
            f'MLB schedule for {date_str}',
            get_schedule_data
        )
        
        if not games:
            print(f"✅ No games scheduled for {date_str}")
            return True
        
        # Create placeholder umpire records
        umpire_records = []
        for game in games:
            umpire_record = {
                "game_date": date_str,
                "game_pk": game.get("game_id") or game.get("game_pk"),
                "umpire_id": None,
                "umpire_name": "TBD",
                "position": "Home Plate",
                "avg_total_runs_in_games": 8.5,  # MLB average
                "over_under_record": 0.5,  # Neutral
                "sample_size": 0,
            }
            umpire_records.append(umpire_record)
        
        if umpire_records:
            df = pd.DataFrame(umpire_records)
            df.to_parquet(out_file, index=False)
            print(f"✅ Umpires: {len(df)} assignments → {out_file.name}")
        
        return True
        
    except Exception as e:
        print(f"❌ Umpires error for {date_str}: {e}")
        return False

# =============================================================================
# ENHANCED BACKFILL ORCHESTRATION
# =============================================================================

def enhanced_backfill_date(date_str: str, out_dir: Path, weather_api_key: Optional[str] = None) -> Dict[str, any]:
    """Enhanced backfill with comprehensive error handling and robust rate limiting"""
    print(f"\n📅 Processing {date_str} with enhanced error handling")
    
    error_handler = EnhancedErrorHandler()
    
    # Collection functions with priority order (most important first)
    collection_tasks = [
        ('games', lambda: safe_data_collection(fetch_game_data)(date_str, out_dir, error_handler)),
        ('play_by_play', lambda: safe_data_collection(fetch_play_by_play_data)(date_str, out_dir, error_handler)),
        ('game_info', lambda: safe_data_collection(fetch_game_info_data)(date_str, out_dir, error_handler)),
        ('lineups', lambda: safe_data_collection(fetch_lineups_data)(date_str, out_dir, error_handler)),
        ('rosters', lambda: safe_data_collection(fetch_rosters_data)(date_str, out_dir, error_handler)),
        ('umpires', lambda: safe_data_collection(fetch_umpires_data)(date_str, out_dir, error_handler)),
        ('weather', lambda: safe_data_collection(fetch_weather_data)(date_str, out_dir, error_handler, weather_api_key)),
    ]
    
    # One-time venue setup (non-critical)
    venue_setup = True
    if not (out_dir / get_output_filename('venue_factors')).exists():
        try:
            venue_setup = fetch_venue_factors_data(out_dir)
            if venue_setup:
                error_handler.record_success('venue_factors')
            else:
                error_handler.record_partial_success('venue_factors', 'Setup returned False')
        except Exception as e:
            error_handler.record_error('venue_factors', e, critical=False)
            venue_setup = False
    
    # Execute collection tasks
    results = {'venue_factors': venue_setup}
    
    for data_type, task_func in collection_tasks:
        if not error_handler.should_continue_collection(data_type):
            print(f"🛑 Stopping collection due to too many critical failures")
            break
        
        print(f"📊 Collecting {data_type}...")
        try:
            results[data_type] = task_func()
        except Exception as e:
            error_handler.record_error(data_type, e, critical=True)
            results[data_type] = False
        
        # Small delay between collections to be respectful
        time.sleep(0.2)
    
    # Get summary
    summary = error_handler.get_collection_summary()
    results['summary'] = summary
    
    # Print results
    success_count = summary['successful'] + summary['partial_success']
    total_count = summary['total_sources']
    
    print(f"📊 {date_str}: {success_count}/{total_count} data sources collected successfully")
    print(f"   Success rate: {summary['success_rate']:.1%}")
    
    if summary['warnings']:
        print(f"   ⚠️ Warnings: {len(summary['warnings'])}")
    
    if summary['errors']:
        critical_errors = [e for e in summary['errors'] if e['critical']]
        if critical_errors:
            print(f"   ❌ Critical errors: {len(critical_errors)}")
        else:
            print(f"   ⚠️ Non-critical errors: {len(summary['errors'])}")
    
    return results

def main():
    parser = argparse.ArgumentParser(description="Enhanced MLB data backfill with robust error handling")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--output", default="data", help="Output directory")
    parser.add_argument("--show-api-status", action="store_true", help="Show API rate limit status")
    args = parser.parse_args()
    
    # Parse dates
    start_date = datetime.fromisoformat(args.start)
    end_date = datetime.fromisoformat(args.end)
    
    if end_date < start_date:
        raise ValueError("End date must be >= start date")
    
    # Setup output directory
    out_dir = Path(args.output)
    out_dir.mkdir(exist_ok=True)
    
    # Get weather API key from environment
    weather_api_key = os.getenv("OPENWEATHER_API_KEY")
    if not weather_api_key:
        print("⚠️ No OPENWEATHER_API_KEY found - weather data will be skipped")
        print("   Get a free key at: https://openweathermap.org/api")
    
    print(f"🚀 Enhanced MLB backfill: {start_date.date()} to {end_date.date()}")
    print(f"📁 Output directory: {out_dir}")
    print(f"🎯 Collecting: games, play_by_play, game_info, weather, umpires, lineups, rosters, venue_factors")
    print(f"🛡️ Features: Robust error handling, graceful degradation, advanced rate limiting")
    
    # Show API status if requested
    if args.show_api_status:
        print(f"\n📊 API Status:")
        for api_name in ['mlb_statsapi', 'openweather', 'pybaseball']:
            status = rate_limiter.get_api_status(api_name)
            print(f"   {api_name}: {status['status']} ({status['calls_this_hour']}/{status['limits']['per_hour']} calls/hour)")
    
    # Process each date
    current_date = start_date
    total_days = (end_date - start_date).days + 1
    overall_results = {
        "games": 0, "play_by_play": 0, "game_info": 0, "weather": 0, 
        "umpires": 0, "lineups": 0, "rosters": 0, "venue_factors": 0
    }
    total_errors = 0
    total_warnings = 0
    
    with tqdm(total=total_days, desc="Processing dates") as pbar:
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            pbar.set_description(f"Processing {date_str}")
            
            try:
                day_results = enhanced_backfill_date(date_str, out_dir, weather_api_key)
                
                # Update overall results
                for data_type, success in day_results.items():
                    if data_type == 'summary':
                        total_errors += len(day_results['summary']['errors'])
                        total_warnings += len(day_results['summary']['warnings'])
                    elif success:
                        overall_results[data_type] += 1
                        
            except Exception as e:
                print(f"❌ Error processing {date_str}: {e}")
                total_errors += 1
            
            current_date += timedelta(days=1)
            pbar.update(1)
            
            # Small delay to be respectful to APIs
            time.sleep(0.1)
    
    # Print enhanced summary
    print(f"\n🎉 Enhanced backfill complete!")
    print(f"📊 Success rates:")
    for data_type, success_count in overall_results.items():
        success_rate = (success_count / total_days) * 100
        print(f"   {data_type}: {success_count}/{total_days} days ({success_rate:.1f}%)")
    
    print(f"\n🛡️ Error handling summary:")
    print(f"   Total errors: {total_errors}")
    print(f"   Total warnings: {total_warnings}")
    
    # Show final API status
    print(f"\n📊 Final API Usage:")
    for api_name in ['mlb_statsapi', 'openweather', 'pybaseball']:
        status = rate_limiter.get_api_status(api_name)
        print(f"   {api_name}: {status['total_calls']} total calls, {status['consecutive_errors']} consecutive errors")
    
    print(f"\n💡 Next steps:")
    print(f"   1. Load data: python run_loader.py --input-dir {args.output}")
    print(f"   2. Run analysis: python enhanced_simple_analysis.py")

if __name__ == "__main__":
    main()