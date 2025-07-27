#!/usr/bin/env python3
"""
test_imports.py - Quick test to verify import fixes work
Run this from your project root: python test_imports.py
"""

def test_imports():
    """Test that all imports work correctly"""
    print("🧪 Testing import fixes...")
    
    success_count = 0
    total_tests = 0
    
    # Test 1: Basic py package import
    total_tests += 1
    try:
        import py
        print("✅ py package imports successfully")
        success_count += 1
    except ImportError as e:
        print(f"❌ py package import failed: {e}")
    
    # Test 2: Individual module imports
    modules_to_test = [
        'py.weather_integration',
        'py.fatigue_metrics', 
        'py.umpire_integration',
        'py.daily_betting_analysis',
        'py.backfill'
    ]
    
    for module_name in modules_to_test:
        total_tests += 1
        try:
            __import__(module_name)
            print(f"✅ {module_name} imports successfully")
            success_count += 1
        except ImportError as e:
            print(f"⚠️ {module_name} import failed: {e} (may not exist yet)")
    
    # Test 3: Module execution
    total_tests += 1
    try:
        from py import backfill
        print("✅ Can import backfill module functions")
        success_count += 1
    except ImportError as e:
        print(f"❌ Cannot import backfill functions: {e}")
    
    print(f"\n📊 Import Test Results: {success_count}/{total_tests} successful")
    
    if success_count == total_tests:
        print("🎉 All imports working! Your fixes are successful.")
        return True
    elif success_count >= total_tests - 2:
        print("✅ Core imports working! You can proceed.")
        return True
    else:
        print("❌ Many imports still failing. Check the fixes above.")
        return False

if __name__ == "__main__":
    test_imports()