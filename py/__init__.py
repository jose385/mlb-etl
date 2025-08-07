"""
MLB Betting Analysis Package

A comprehensive data collection and analysis system for MLB sports betting.
"""

__version__ = "1.0.0"
__author__ = "MLB Analytics Team"

# Optional: Import commonly used functions/classes
try:
    from .enhanced_simple_backfill import main as run_backfill
    from .config import get_config
except ImportError:
    # Handle cases where dependencies aren't available
    pass
