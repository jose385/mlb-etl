#!/usr/bin/env python3
"""
Interactive environment setup for MLB betting analysis
"""
import os
import sys
from pathlib import Path
from datetime import datetime
import getpass

def ensure_project_directories():
    """Ensure all project directories exist with proper structure"""
    
    # Core directories that must exist
    required_directories = {
        'stage': 'Data staging area for parquet files',
        'logs': 'Application and error logs',
        'migrations': 'Database schema migration files',
        'loader': 'Data loading scripts',
        'py': 'Main Python package'
    }
    
    # Optional directories for advanced features
    optional_directories = {
        '.github/workflows': 'GitHub Actions CI/CD workflows',
        'tests': 'Unit and integration tests',
        'docs': 'Documentation files',
        'backup': 'Data backup location'
    }
    
    all_directories = {**required_directories, **optional_directories}
    
    print("📁 Setting up project directory structure...")
    success_count = 0
    
    for directory, description in all_directories.items():
        dir_path = Path(directory)
        
        try:
            if dir_path.exists():
                if dir_path.is_dir():
                    print(f"   ✅ {directory:<20} - {description}")
                else:
                    print(f"   ⚠️  {directory:<20} - EXISTS BUT IS NOT A DIRECTORY!")
                    return False
            else:
                dir_path.mkdir(parents=True, exist_ok=True)
                print(f"   📂 {directory:<20} - Created: {description}")
                success_count += 1
                
        except PermissionError:
            print(f"   ❌ {directory:<20} - Permission denied")
            if directory in required_directories:
                print(f"      CRITICAL: {directory} is required for the system to work")
                return False
        except Exception as e:
            print(f"   ❌ {directory:<20} - Error: {e}")
            if directory in required_directories:
                return False
    
    if success_count > 0:
        print(f"   🎉 Successfully created {success_count} directories")
    
    return True

def setup_environment():
    """Interactive environment variable setup"""
    print("🚀 MLB Betting Analysis - Environment Setup")
    print("=" * 50)
    
    # Ensure all project directories exist first
    print("\n📁 Step 1: Setting up directory structure...")
    if not ensure_project_directories():
        print("❌ Failed to create required directories")
        print("   Please check permissions and try again")
        return False
    
    env_vars = {}
    
    # Database setup
    print("\n📊 Step 2: Database Configuration")
    print("Example: postgresql://user:password@localhost:5432/mlb_db")
    print("Or for RDS: postgresql://user:password@your-rds-endpoint.region.rds.amazonaws.com:5432/mlb_db")
    
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
    print("\n🌤️ Step 3: Weather API Configuration (optional)")
    print("Get free key at: https://openweathermap.org/api")
    
    weather_key = input("Enter OpenWeather API key (or press Enter to skip): ").strip()
    if weather_key:
        env_vars['OPENWEATHER_API_KEY'] = weather_key
        env_vars['ENABLE_WEATHER'] = 'true'
    else:
        env_vars['ENABLE_WEATHER'] = 'false'
        print("⚠️ Weather analysis will be disabled")
    
    # AWS S3 setup (optional)
    print("\n☁️ Step 4: AWS S3 Configuration (optional)")
    s3_bucket = input("Enter S3 bucket name (or press Enter to skip): ").strip()
    if s3_bucket:
        env_vars['AWS_S3_BUCKET'] = s3_bucket
        env_vars['ENABLE_S3_STORAGE'] = 'true'
        env_vars['AUTO_UPLOAD_TO_S3'] = 'true'
        
        region = input("Enter AWS region (default: us-east-1): ").strip() or "us-east-1"
        env_vars['AWS_DEFAULT_REGION'] = region
        print("✅ S3 storage enabled")
    else:
        env_vars['ENABLE_S3_STORAGE'] = 'false'
        env_vars['AUTO_UPLOAD_TO_S3'] = 'false'
        print("⚠️ S3 storage will be disabled")
    
    # Optional configurations
    print("\n⚙️ Step 5: Optional Settings")
    
    output_dir = input(f"Output directory (default: stage): ").strip() or "stage"
    env_vars['OUTPUT_DIR'] = output_dir
    
    # Ensure the custom output directory exists
    if output_dir != "stage":
        try:
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            print(f"   📂 Created custom output directory: {output_dir}")
        except Exception as e:
            print(f"   ❌ Failed to create {output_dir}: {e}")
            return False
    
    debug = input("Enable debug mode? (y/N): ").strip().lower()
    env_vars['DEBUG'] = 'true' if debug in ['y', 'yes'] else 'false'
    
    verbose = input("Enable verbose logging? (y/N): ").strip().lower()
    env_vars['VERBOSE'] = 'true' if verbose in ['y', 'yes'] else 'false'
    
    # Set other important defaults
    env_vars.update({
        'MIGRATIONS_DIR': 'migrations',
        'LOG_DIR': 'logs',
        'MLB_API_DELAY': '0.2',
        'WEATHER_API_DELAY': '0.5',
        'STATS_API_DELAY': '0.3',
        'MIN_GAMES_FOR_ANALYSIS': '5',
        'MIN_SAMPLE_SIZE_UMPIRE': '15',
        'MIN_PITCHER_STARTS': '3',
        'MIN_TEAM_GAMES': '7',
        'STRONG_EDGE_THRESHOLD': '0.12',
        'MODERATE_EDGE_THRESHOLD': '0.06',
        'WEATHER_IMPACT_THRESHOLD': '0.08',
        'ENABLE_UMPIRE_ANALYSIS': 'true',
        'ENABLE_VENUE_FACTORS': 'true',
        'ENABLE_RECENT_STATS': 'true',
        'ENABLE_GAME_INFO': 'true',
        'ENABLE_PITCHER_WORKLOAD': 'true',
        'ENABLE_TEAM_FORM_ANALYSIS': 'true',
        'ENABLE_BALLPARK_ADJUSTMENTS': 'true',
        'LOG_LEVEL': 'INFO',
        'LOG_TO_FILE': 'true',
        'PERFORMANCE_MONITORING': 'false'
    })
    
    # Create .env file
    env_file = Path('.env')
    print(f"\n📝 Step 6: Creating {env_file}...")
    
    with open(env_file, 'w') as f:
        f.write("# MLB Betting Analysis Environment Configuration\n")
        f.write(f"# Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # Database Configuration
        f.write("# Database Configuration (REQUIRED)\n")
        f.write(f"PG_DSN={env_vars['PG_DSN']}\n\n")
        
        # API Keys
        f.write("# API Keys (CONDITIONAL - required for specific features)\n")
        if 'OPENWEATHER_API_KEY' in env_vars:
            f.write(f"OPENWEATHER_API_KEY={env_vars['OPENWEATHER_API_KEY']}\n")
        f.write("\n")
        
        # AWS Configuration
        f.write("# AWS Configuration\n")
        for key in ['AWS_S3_BUCKET', 'AWS_DEFAULT_REGION', 'ENABLE_S3_STORAGE', 'AUTO_UPLOAD_TO_S3']:
            if key in env_vars:
                f.write(f"{key}={env_vars[key]}\n")
        f.write("AUTO_CLEANUP_LOCAL=false\n\n")
        
        # Directory Paths
        f.write("# Data Collection Settings\n")
        f.write(f"OUTPUT_DIR={env_vars['OUTPUT_DIR']}\n")
        f.write(f"MIGRATIONS_DIR={env_vars['MIGRATIONS_DIR']}\n")
        f.write(f"LOG_DIR={env_vars['LOG_DIR']}\n\n")
        
        # Rate Limiting
        f.write("# Enhanced Rate Limiting (seconds between API calls)\n")
        for key in ['MLB_API_DELAY', 'WEATHER_API_DELAY', 'STATS_API_DELAY']:
            f.write(f"{key}={env_vars[key]}\n")
        f.write("\n")
        
        # Data Quality Thresholds
        f.write("# Enhanced Data Quality Thresholds\n")
        for key in ['MIN_GAMES_FOR_ANALYSIS', 'MIN_SAMPLE_SIZE_UMPIRE', 'MIN_PITCHER_STARTS', 'MIN_TEAM_GAMES']:
            f.write(f"{key}={env_vars[key]}\n")
        f.write("\n")
        
        # Betting Analysis Thresholds
        f.write("# Enhanced Betting Analysis Thresholds\n")
        for key in ['STRONG_EDGE_THRESHOLD', 'MODERATE_EDGE_THRESHOLD', 'WEATHER_IMPACT_THRESHOLD']:
            f.write(f"{key}={env_vars[key]}\n")
        f.write("\n")
        
        # Feature Flags
        f.write("# Enhanced Feature Flags (true/false)\n")
        feature_flags = [
            'ENABLE_WEATHER', 'ENABLE_UMPIRE_ANALYSIS', 'ENABLE_VENUE_FACTORS',
            'ENABLE_RECENT_STATS', 'ENABLE_GAME_INFO', 'ENABLE_PITCHER_WORKLOAD',
            'ENABLE_TEAM_FORM_ANALYSIS', 'ENABLE_BALLPARK_ADJUSTMENTS'
        ]
        for key in feature_flags:
            f.write(f"{key}={env_vars[key]}\n")
        f.write("\n")
        
        # Debug/Development Settings
        f.write("# Debug/Development Settings\n")
        f.write(f"DEBUG={env_vars['DEBUG']}\n")
        f.write(f"VERBOSE={env_vars['VERBOSE']}\n")
        f.write("DRY_RUN=false\n\n")
        
        # Enhanced Logging
        f.write("# Enhanced Logging\n")
        for key in ['LOG_LEVEL', 'LOG_TO_FILE', 'PERFORMANCE_MONITORING']:
            f.write(f"{key}={env_vars[key]}\n")
    
    print(f"✅ Environment configuration saved to {env_file}")
    
    # Test configuration
    print("\n🧪 Step 7: Testing configuration...")
    
    # Load environment variables
    for key, value in env_vars.items():
        os.environ[key] = value
    
    try:
        from py.config import get_config
        config = get_config()
        
        # Test database
        if config.PG_DSN:
            success, message = config.test_database_connection()
            print(f"   Database: {'✅' if success else '❌'} {message}")
            if not success:
                print("   💡 Make sure your database server is running and accessible")
        
        # Test weather API
        if config.OPENWEATHER_API_KEY:
            success, message = config.test_weather_api()
            print(f"   Weather API: {'✅' if success else '❌'} {message}")
        
        # Test S3 access
        if config.ENABLE_S3_STORAGE:
            success, message = config.test_s3_access()
            print(f"   S3 Access: {'✅' if success else '❌'} {message}")
            if not success:
                print("   💡 Make sure AWS credentials are configured (aws configure)")
        
        print("\n🎉 Environment setup complete!")
        print("\n📋 Next steps:")
        print("1. Initialize database schema:")
        print("   python initialize_database.py")
        print("2. Run a test backfill:")
        print("   python py/enhanced_simple_backfill.py --start 2024-07-01 --end 2024-07-01")
        print("3. Load data into database:")
        print("   python loader/enhanced_load_parquet_into_pg.py")
        print("4. Run analysis:")
        print("   python py/simple_analysis.py")
        
    except ImportError:
        print("❌ Configuration test failed: Dependencies not installed")
        print("💡 Install dependencies first: pip install -r py/requirements.txt")
        return False
    except Exception as e:
        print(f"❌ Configuration test failed: {e}")
        print("💡 Check your configuration and try again")
        return False
    
    return True

if __name__ == "__main__":
    success = setup_environment()
    sys.exit(0 if success else 1)