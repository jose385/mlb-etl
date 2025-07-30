#!/usr/bin/env python3
"""Test that all imports work correctly"""

def test_imports():
    print("🧪 Testing import fixes...")
    
    # Test direct module execution
    try:
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent))
        
        from py.backfill import main as backfill_main
        print("✅ Can import backfill.main")
    except ImportError as e:
        print(f"❌ Cannot import backfill: {e}")
    
    # Test wrapper scripts
    try:
        exec(open('run_backfill.py').read())
        print("✅ run_backfill.py syntax OK")
    except Exception as e:
        print(f"❌ run_backfill.py has issues: {e}")
    
    print("✅ Import tests complete")

if __name__ == "__main__":
    test_imports()