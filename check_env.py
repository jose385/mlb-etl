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
    print("   Make sure dependencies are installed: pip install -r py/requirements.txt")
    sys.exit(1)

def validate_directory_structure():
    """Validate project directory structure"""
    print("\n📁 Directory Structure Validation:")
    
    # Required directories
    required_directories = {
        'stage': 'Data staging area for parquet files',
        'logs': 'Application and error logs',
        'migrations': 'Database schema migration files',
        'loader': 'Data loading scripts',
        'py': 'Main Python package'
    }
    
    # Optional but recommended directories
    optional_directories = {
        '.github/workflows': 'GitHub Actions CI/CD workflows',
        'tests': 'Unit and integration tests',
        'docs': 'Documentation files',
        'backup': 'Data backup location'
    }
    
    missing_required = []
    missing_optional = []
    
    print("   📂 Required directories:")
    for directory, description in required_directories.items():
        dir_path = Path(directory)
        if dir_path.exists():
            if dir_path.is_dir():
                print(f"      ✅ {directory:<15} - {description}")
            else:
                print(f"      ❌ {directory:<15} - EXISTS BUT IS NOT A DIRECTORY!")
                missing_required.append(directory)
        else:
            print(f"      ❌ {directory:<15} - MISSING: {description}")
            missing_required.append(directory)
    
    print("   📂 Optional directories:")
    for directory, description in optional_directories.items():
        dir_path = Path(directory)
        if dir_path.exists():
            if dir_path.is_dir():
                print(f"      ✅ {directory:<15} - {description}")
            else:
                print(f"      ⚠️  {directory:<15} - EXISTS BUT IS NOT A DIRECTORY!")
        else:
            print(f"      ⚪ {directory:<15} - Missing: {description}")
            missing_optional.append(directory)
    
    # Summary and recommendations
    if missing_required:
        print(f"\n   ❌ Missing {len(missing_required)} required directories")
        print(f"   🔧 To create missing directories, run:")
        print(f"      python setup_env.py")
        print(f"      # OR manually:")
        for directory in missing_required:
            print(f"      mkdir -p {directory}")
        return False
    else:
        print(f"   ✅ All required directories exist")
        
    if missing_optional:
        print(f"   ℹ️  Missing {len(missing_optional)} optional directories (recommended)")
    
    return True

def validate_file_structure():
    """Validate critical files exist"""
    print("\n📄 Critical Files Validation:")
    
    critical_files = {
        'py/__init__.py': 'Python package marker',
        'py/config.py': 'Configuration management',
        'py/requirements.txt': 'Python dependencies',
        'migrations/001_enhanced_simple_schema.sql': 'Database schema',
        'loader/enhanced_load_parquet_into_pg.py': 'Data loader'
    }
    
    missing_files = []
    
    for file_path, description in critical_files.items():
        if Path(file_path).exists():
            print(f"   ✅ {file_path:<40} - {description}")
        else:
            print(f"   ❌ {file_path:<40} - MISSING: {description}")
            missing_files.append(file_path)
    
    if missing_files:
        print(f"\n   ❌ Missing {len(missing_files)} critical files")
        print(f"   💡 These files are required for the system to work")
        return False
    else:
        print(f"   ✅ All critical files exist")
        return True

def check_environment():
    """Comprehensive environment check"""
    print("🔍 MLB Betting Analysis - Environment Check")
    print("=" * 50)
    
    # Validate directory structure first
    dirs_valid = validate_directory_structure()
    files_valid = validate_file_structure()
    
    if not (dirs_valid and files_valid):
        print(f"\n❌ Project structure validation failed")
        print(f"   Run 'python setup_env.py' to fix directory issues")
        return False
    
    try:
        config = get_config()
    except Exception as e:
        print(f"\n❌ Failed to load configuration: {e}")
        print(f"   Run 'python setup_env.py' to create proper configuration")
        return False
    
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
            print("      💡 Make sure your database server is running")
            print("      💡 Check your PG_DSN connection string")
    else:
        print("   Database Connection: ❌ PG_DSN not set")
    
    # Weather API check
    if config.OPENWEATHER_API_KEY:
        success, message = config.test_weather_api()
        print(f"   Weather API: {'✅' if success else '❌'} {message}")
        
        if not success:
            print("      ⚠️ Weather analysis will be limited")
            print("      💡 Check your API key at https://openweathermap.org/api")
    else:
        print("   Weather API: ❌ Not configured (weather analysis disabled)")
    
    # S3 check (if enabled)
    if config.ENABLE_S3_STORAGE:
        success, message = config.test_s3_access()
        print(f"   S3 Storage: {'✅' if success else '❌'} {message}")
        
        if not success:
            print("      ⚠️ S3 storage will not work")
            print("      💡 Run 'aws configure' to set up AWS credentials")
    else:
        print("   S3 Storage: ⚪ Disabled")
    
    # Directory checks with custom paths
    print(f"\n📁 Configuration Directory Paths:")
    config_dirs = {
        config.OUTPUT_DIR: "Output/staging directory",
        config.MIGRATIONS_DIR: "Database migrations", 
        config.LOG_DIR: "Application logs"
    }
    
    for directory, description in config_dirs.items():
        exists = Path(directory).exists()
        is_dir = Path(directory).is_dir() if exists else False
        
        if exists and is_dir:
            status = '✅'
            note = description
        elif exists and not is_dir:
            status = '❌'
            note = f"EXISTS BUT NOT A DIRECTORY - {description}"
        else:
            status = '❌'
            note = f"MISSING - {description}"
            
        print(f"   {status} {directory:<15} - {note}")
    
    # Feature status
    print(f"\n🎛️ Feature Status:")
    features = {
        'Weather Analysis': config.ENABLE_WEATHER,
        'Umpire Analysis': config.ENABLE_UMPIRE_ANALYSIS,
        'Venue Factors': config.ENABLE_VENUE_FACTORS,
        'Recent Stats': config.ENABLE_RECENT_STATS,
        'Game Info': config.ENABLE_GAME_INFO,
        'Pitcher Workload': config.ENABLE_PITCHER_WORKLOAD,
        'Team Form Analysis': config.ENABLE_TEAM_FORM_ANALYSIS,
        'Ballpark Adjustments': config.ENABLE_BALLPARK_ADJUSTMENTS,
        'S3 Storage': config.ENABLE_S3_STORAGE
    }
    
    for feature, enabled in features.items():
        status = '✅ Enabled' if enabled else '⚪ Disabled'
        print(f"   {feature}: {status}")
    
    # Overall assessment
    issues = config.validate(require_weather=False, require_database=False)
    
    print(f"\n📋 Overall Assessment:")
    if not issues:
        print("✅ Environment is properly configured!")
        print("   You can run all MLB analysis scripts")
        
        print(f"\n🚀 Ready to go! Try these commands:")
        print(f"   1. Initialize database: python initialize_database.py")
        print(f"   2. Run backfill: python py/enhanced_simple_backfill.py --start 2024-07-01 --end 2024-07-01")
        print(f"   3. Load data: python loader/enhanced_load_parquet_into_pg.py")
        print(f"   4. Run analysis: python py/simple_analysis.py")
        return True
    else:
        print(f"⚠️ Found {len(issues)} configuration issues:")
        for issue in issues:
            print(f"   • {issue}")
        
        print(f"\n🔧 To fix these issues:")
        print(f"   1. Run: python setup_env.py")
        print(f"   2. Or check the specific issues above")
        
        # Categorize issues
        db_issues = [i for i in issues if 'PG_DSN' in i or 'database' in i.lower()]
        api_issues = [i for i in issues if 'API' in i or 'key' in i.lower()]
        dir_issues = [i for i in issues if 'directory' in i.lower()]
        
        if db_issues:
            print(f"\n   📊 Database issues: Check your PostgreSQL connection")
        if api_issues:
            print(f"   🔑 API issues: Get valid API keys")
        if dir_issues:
            print(f"   📁 Directory issues: Run setup script to create missing directories")
        
        return False

if __name__ == "__main__":
    success = check_environment()
    sys.exit(0 if success else 1)