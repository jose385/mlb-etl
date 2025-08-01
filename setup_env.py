#!/usr/bin/env python3
"""
Interactive environment setup for MLB betting analysis
"""
import os
import sys
from pathlib import Path
from datetime import datetime  # ADD THIS LINE
import getpass

def setup_environment():
    """Interactive environment variable setup"""
    print("🚀 MLB Betting Analysis - Environment Setup")
    print("=" * 50)
    
    env_vars = {}
    
    # Database setup
    print("\n📊 Database Configuration:")
    print("Example: postgresql://user:password@localhost:5432/mlb_db")
    
    while True:
        pg_dsn = input("Enter PostgreSQL DSN: ").strip()
        if pg_dsn:
            if pg_dsn.startswith(('postgresql://', 'postgres://')):
                env_vars['PG_DSN'] = pg_dsn
                break
            else:
                print("❌ DSN should start with 'postgresql://' or 'postgres://'")
        else:
            print("❌ Database DSN is required")
    
    # Weather API setup
    print("\n🌤️ Weather API Configuration (optional):")
    print("Get free key at: https://openweathermap.org/api")
    
    weather_key = input("Enter OpenWeather API key (or press Enter to skip): ").strip()
    if weather_key:
        env_vars['OPENWEATHER_API_KEY'] = weather_key
        env_vars['ENABLE_WEATHER'] = 'true'
    else:
        env_vars['ENABLE_WEATHER'] = 'false'
        print("⚠️ Weather analysis will be disabled")
    
    # Optional configurations
    print("\n⚙️ Optional Settings:")
    
    output_dir = input(f"Output directory (default: stage): ").strip() or "stage"
    env_vars['OUTPUT_DIR'] = output_dir
    
    debug = input("Enable debug mode? (y/N): ").strip().lower()
    env_vars['DEBUG'] = 'true' if debug in ['y', 'yes'] else 'false'
    
    # Create directories if they don't exist
    Path(output_dir).mkdir(exist_ok=True)
    Path('migrations').mkdir(exist_ok=True)
    Path('logs').mkdir(exist_ok=True)
    
    # Create .env file
    env_file = Path('.env')
    print(f"\n📝 Creating {env_file}...")
    
    with open(env_file, 'w') as f:
        f.write("# MLB Betting Analysis Environment Configuration\n")
        f.write(f"# Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        for key, value in env_vars.items():
            f.write(f"{key}={value}\n")
    
    print(f"✅ Environment configuration saved to {env_file}")
    
    # Test configuration
    print("\n🧪 Testing configuration...")
    
    # Load environment variables
    for key, value in env_vars.items():
        os.environ[key] = value
    
    try:
        from py.config import require_config
        config = require_config(require_weather=bool(weather_key), require_database=False)  # Don't require DB connection for setup
        
        # Test database
        if config.PG_DSN:
            success, message = config.test_database_connection()
            print(f"   Database: {'✅' if success else '❌'} {message}")
        
        # Test weather API
        if config.OPENWEATHER_API_KEY:
            success, message = config.test_weather_api()
            print(f"   Weather API: {'✅' if success else '❌'} {message}")
        
        print("\n🎉 Environment setup complete!")
        print("\nNext steps:")
        print("1. Source the environment: source .env")
        print("2. Or restart your terminal")
        print("3. Run: python run_backfill.py --help")
        
    except Exception as e:
        print(f"❌ Configuration test failed: {e}")
        print("⚠️ You may need to install dependencies: pip install -r py/requirements.txt")
        return False
    
    return True

if __name__ == "__main__":
    setup_environment()