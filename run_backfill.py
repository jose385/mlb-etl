#!/usr/bin/env python3
"""
Wrapper script to run backfill with proper environment validation
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
    
    # Now import and run the main function
    from py.backfill import main as backfill_main
    backfill_main()

if __name__ == "__main__":
    main()