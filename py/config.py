"""
Configuration management for MLB ETL pipeline
"""
import os
from typing import Optional

class Config:
    """Centralized configuration management"""
    
    # Database
    PG_DSN: str = os.getenv("PG_DSN", "")
    
    # API Keys
    OPENWEATHER_API_KEY: str = os.getenv("OPENWEATHER_API_KEY", "")
    
    # Directories
    OUTPUT_DIR: str = os.getenv("OUTPUT_DIR", "stage")
    MIGRATIONS_DIR: str = "migrations"
    
    # Rate Limiting
    MLB_API_DELAY: float = 0.1  # seconds between API calls
    WEATHER_API_DELAY: float = 0.2
    
    # Data Quality
    MIN_GAMES_FOR_ANALYSIS: int = 10
    MIN_SAMPLE_SIZE_UMPIRE: int = 20
    
    # Betting Thresholds
    STRONG_EDGE_THRESHOLD: float = 0.15
    MODERATE_EDGE_THRESHOLD: float = 0.08
    
    @classmethod
    def validate(cls) -> list[str]:
        """Validate configuration and return list of issues"""
        issues = []
        
        if not cls.PG_DSN:
            issues.append("PG_DSN environment variable not set")
        
        if not cls.OPENWEATHER_API_KEY:
            issues.append("OPENWEATHER_API_KEY not set (weather analysis will be limited)")
            
        return issues

# ==============================================================================
# FILE: py/rate_limiter.py (Add this for API rate limiting)
# ==============================================================================
#!/usr/bin/env python3
"""
Rate limiter for API calls
"""
import time
from typing import Dict
from datetime import datetime, timedelta

class RateLimiter:
    """Simple rate limiter for API calls"""
    
    def __init__(self):
        self.last_calls: Dict[str, datetime] = {}
        
    def wait_if_needed(self, api_name: str, min_delay: float = 0.1):
        """Wait if needed to respect rate limits"""
        now = datetime.now()
        
        if api_name in self.last_calls:
            time_since_last = (now - self.last_calls[api_name]).total_seconds()
            if time_since_last < min_delay:
                sleep_time = min_delay - time_since_last
                time.sleep(sleep_time)
        
        self.last_calls[api_name] = datetime.now()