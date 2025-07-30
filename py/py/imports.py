"""
Central import handler to resolve import issues
"""
import sys
import os
from pathlib import Path

def setup_imports():
    """Add project root to Python path for absolute imports"""
    # Get project root (directory containing this file's parent)
    current_file = Path(__file__).resolve()
    project_root = current_file.parent.parent
    
    # Add to Python path if not already there
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

# Call setup when this module is imported
setup_imports()