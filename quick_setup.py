#!/usr/bin/env python3
"""
quick_setup.py - Minimal setup to get system working
Creates basic .env file and directories
"""
import os
from pathlib import Path

def quick_setup():
    """Quick setup to prevent configuration errors"""
    print("🚀 Quick MLB Betting Analysis Setup")
    print("=" * 40)
    
    # Create required directories
    directories = ['stage', 'migrations', 'logs']
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
        print(f"✅ Created directory: {directory}")
    
    # Check if .env already exists
    env_file = Path('.env')
    if env_file.exists():
        print(f"⚠️ .env file already exists, backing up to .env.backup")
        env_file.rename('.env.backup')
    
    # Create basic .env file
    env_content = """# MLB Betting Analysis Configuration
# REQUIRED: Set your database connection
PG_DSN=postgresql://username:password@localhost:5432/mlb_betting

# OPTIONAL: Weather API (get free key at https://openweathermap.org/api)
OPENWEATHER_API_KEY=
ENABLE_WEATHER=false

# Basic settings
OUTPUT_DIR=stage
DEBUG=false
LOG_LEVEL=INFO
VERBOSE=false

# Feature flags (can be enabled later)
ENABLE_UMPIRE_ANALYSIS=true
ENABLE_VENUE_FACTORS=true
ENABLE_RECENT_STATS=true
ENABLE_GAME_INFO=true

# S3 settings (optional)
ENABLE_S3_STORAGE=false
AWS_S3_BUCKET=
AWS_S3_PREFIX=mlb-data
"""
    
    with open('.env', 'w') as f:
        f.write(env_content)
    
    print(f"✅ Created .env configuration file")
    
    print(f"\n📝 Next Steps:")
    print(f"1. Edit .env file with your database details:")
    print(f"   nano .env")
    print(f"   # Change the PG_DSN line to your actual database")
    print(f"")
    print(f"2. Test configuration:")
    print(f"   python -c \"from py.config import get_config; get_config().print_status()\"")
    print(f"")
    print(f"3. Initialize database:")
    print(f"   python initialize_database.py --reset --force")
    print(f"")
    print(f"4. Test data collection:")
    print(f"   python py/enhanced_simple_backfill.py --start 2024-07-15 --end 2024-07-15")
    
    print(f"\n💡 Database Connection Examples:")
    print(f"   Local PostgreSQL:")
    print(f"   PG_DSN=postgresql://myuser:mypass@localhost:5432/mlb_betting")
    print(f"")
    print(f"   AWS RDS:")
    print(f"   PG_DSN=postgresql://user:pass@mydb.xyz.us-east-1.rds.amazonaws.com:5432/mlb")
    
    print(f"\n🎯 System is ready for setup!")

if __name__ == "__main__":
    quick_setup()