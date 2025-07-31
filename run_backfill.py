#!/usr/bin/env python3
"""
Wrapper script to run ENHANCED backfill with proper environment validation
"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def main():
    # Validate environment before importing main modules
    try:
        from py.config import require_config
        
        # Basic validation (weather is optional for backfill)
        config = require_config(require_database=False, require_weather=False)
        
        # Show configuration status if in verbose mode
        if config.VERBOSE:
            config.print_status()
        
    except SystemExit:
        print("\n💡 Tip: Run 'python setup_env.py' to configure your environment")
        sys.exit(1)
    
    # CHANGED: Import and run the ENHANCED backfill instead of old backfill
    from py.enhanced_simple_backfill import main as enhanced_backfill_main
    enhanced_backfill_main()

if __name__ == "__main__":
    main()