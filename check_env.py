#!/usr/bin/env python3
"""
Environment validation and diagnostic script
"""
import sys
from pathlib import Path

try:
    from py.config import get_config
except ImportError:
    print("❌ Cannot import configuration module")
    print("   Make sure you're running from the project root")
    sys.exit(1)

def check_environment():
    """Comprehensive environment check"""
    print("🔍 MLB Betting Analysis - Environment Check")
    print("=" * 50)
    
    config = get_config()
    
    # Print current status
    config.print_status()
    
    # Detailed validation
    print("\n🔬 Detailed Validation:")
    
    # Database check
    if config.PG_DSN:
        success, message = config.test_database_connection()
        print(f"   Database Connection: {'✅' if success else '❌'} {message}")
        
        if success:
            print("      You can run database operations")
        else:
            print("      ⚠️ Fix database connection before proceeding")
    else:
        print("   Database Connection: ❌ PG_DSN not set")
    
    # Weather API check
    if config.OPENWEATHER_API_KEY:
        success, message = config.test_weather_api()
        print(f"   Weather API: {'✅' if success else '❌'} {message}")
        
        if not success:
            print("      ⚠️ Weather analysis will be limited")
    else:
        print("   Weather API: ❌ Not configured (weather analysis disabled)")
    
    # Directory checks
    print(f"   Output Directory: {'✅' if Path(config.OUTPUT_DIR).exists() else '❌'} {config.OUTPUT_DIR}")
    print(f"   Migrations Directory: {'✅' if Path(config.MIGRATIONS_DIR).exists() else '❌'} {config.MIGRATIONS_DIR}")
    
    # Feature status
    print("\n🎛️ Feature Status:")
    print(f"   Weather Analysis: {'✅ Enabled' if config.ENABLE_WEATHER else '❌ Disabled'}")
    print(f"   Umpire Analysis: {'✅ Enabled' if config.ENABLE_UMPIRE_ANALYSIS else '❌ Disabled'}")
    print(f"   Fatigue Metrics: {'✅ Enabled' if config.ENABLE_FATIGUE_METRICS else '❌ Disabled'}")
    
    # Overall assessment
    issues = config.validate(require_weather=False, require_database=False)
    
    print(f"\n📋 Overall Assessment:")
    if not issues:
        print("✅ Environment is properly configured!")
        print("   You can run all MLB analysis scripts")
        return True
    else:
        print(f"⚠️ Found {len(issues)} configuration issues:")
        for issue in issues:
            print(f"   • {issue}")
        
        print("\n🔧 To fix these issues, run:")
        print("   python setup_env.py")
        return False

if __name__ == "__main__":
    success = check_environment()
    sys.exit(0 if success else 1)