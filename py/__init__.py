"""
MLB Betting Analysis Package

A comprehensive data collection and analysis system for MLB sports betting.
"""

__version__ = "1.0.0"
__author__ = "MLB Analytics Team"

# Optional: Import commonly used functions/classes
try:
    from .backfill import main as run_backfill
    from .daily_betting_analysis import get_complete_betting_analysis
except ImportError:
    # Handle cases where dependencies aren't available
    pass