#!/usr/bin/env python3
"""
Wrapper script to run data loader with proper imports
"""
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Now import and run
from loader.load_parquet_into_pg import main

if __name__ == "__main__":
    main()