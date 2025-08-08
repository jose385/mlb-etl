#!/usr/bin/env python3
"""
test_pipeline.py - Quick test to verify the entire MLB pipeline works
Tests backfill → load → analysis in sequence with proper error handling
"""

import os
import sys
import time
import subprocess
from pathlib import Path
from datetime import datetime

def run_command(cmd, description, timeout=300):
    """Run a command with proper error handling and output"""
    print(f"\n🔄 {description}")
    print(f"   Command: {' '.join(cmd)}")
    
    start_time = time.time()
    
    try:
        result = subprocess.run(
            cmd, 
            capture_output=True, 
            text=True, 
            timeout=timeout,
            cwd=Path.cwd()
        )
        
        elapsed = time.time() - start_time
        
        if result.returncode == 0:
            print(f"   ✅ Success in {elapsed:.1f}s")
            if result.stdout.strip():
                # Show last few lines of output
                lines = result.stdout.strip().split('\n')
                for line in lines[-3:]:
                    if line.strip():
                        print(f"   📝 {line.strip()}")
            return True
        else:
            print(f"   ❌ Failed in {elapsed:.1f}s")
            print(f"   Error: {result.stderr}")
            if result.stdout:
                print(f"   Output: {result.stdout}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"   ⏰ Timeout after {timeout}s")
        return False
    except Exception as e:
        print(f"   💥 Exception: {e}")
        return False

def check_files_created(directory, expected_files):
    """Check if expected files were created"""
    print(f"\n📁 Checking files in {directory}")
    
    if not Path(directory).exists():
        print(f"   ❌ Directory {directory} doesn't exist")
        return False
    
    files = list(Path(directory).glob("*.parquet"))
    print(f"   📊 Found {len(files)} parquet files")
    
    for file in files:
        size_mb = file.stat().st_size / (1024 * 1024)
        print(f"   📄 {file.name} ({size_mb:.1f} MB)")
    
    if len(files) >= expected_files:
        print(f"   ✅ Expected at least {expected_files} files, found {len(files)}")
        return True
    else:
        print(f"   ❌ Expected at least {expected_files} files, only found {len(files)}")
        return False

def check_database_connection():
    """Test database connection"""
    print(f"\n🔍 Testing database connection")
    
    try:
        from py.config import get_config
        config = get_config()
        
        if not config.PG_DSN:
            print(f"   ❌ PG_DSN not configured")
            return False
        
        success, message = config.test_database_connection()
        if success:
            print(f"   ✅ Database connection: {message}")
            return True
        else:
            print(f"   ❌ Database connection failed: {message}")
            return False
            
    except Exception as e:
        print(f"   ❌ Database test error: {e}")
        return False

def check_configuration():
    """Check configuration and show current mode"""
    print(f"\n⚙️ Checking configuration")
    
    try:
        from py.config import get_config
        config = get_config()
        
        placeholder_mode = getattr(config, 'USE_PLACEHOLDER_DATA', True)
        weather_key = bool(config.OPENWEATHER_API_KEY)
        
        print(f"   🔧 Placeholder mode: {'✅ Enabled' if placeholder_mode else '❌ Disabled'}")
        print(f"   🌤️ Weather API key: {'✅ Set' if weather_key else '❌ Missing'}")
        print(f"   📁 Output directory: {config.OUTPUT_DIR}")
        print(f"   🗄️ Database: {'✅ Configured' if config.PG_DSN else '❌ Missing'}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Configuration error: {e}")
        return False

def test_analysis_output():
    """Test that analysis produces reasonable output"""
    print(f"\n🧪 Testing analysis output")
    
    try:
        import psycopg2
        from py.config import get_config
        
        config = get_config()
        conn = psycopg2.connect(config.PG_DSN)
        
        # Check table counts
        tables_to_check = [
            'game_info', 'games', 'play_by_play', 'lineups', 
            'rosters', 'weather', 'umpires', 'venue_factors', 'recent_stats'
        ]
        
        table_counts = {}
        with conn.cursor() as cur:
            for table in tables_to_check:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cur.fetchone()[0]
                    table_counts[table] = count
                    print(f"   📊 {table}: {count:,} records")
                except Exception as e:
                    print(f"   ❌ {table}: Error - {e}")
                    table_counts[table] = 0
        
        conn.close()
        
        # Check if we have reasonable data
        total_records = sum(table_counts.values())
        if total_records > 0:
            print(f"   ✅ Total records: {total_records:,}")
            return True
        else:
            print(f"   ❌ No data found in any tables")
            return False
            
    except Exception as e:
        print(f"   ❌ Analysis test error: {e}")
        return False

def main():
    """Run complete pipeline test"""
    print("🚀 MLB Betting Analysis Pipeline Test")
    print("=" * 50)
    print(f"📅 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Test parameters
    test_date = "2025-01-15"  # Good date for placeholder data
    output_dir = "stage"
    
    # Track results
    results = {}
    
    # 1. Check configuration
    results['config'] = check_configuration()
    
    # 2. Check database connection
    results['database'] = check_database_connection()
    
    if not results['database']:
        print(f"\n❌ Cannot proceed without database connection")
        print(f"💡 Fix database configuration and try again")
        return False
    
    # 3. Initialize database (if needed)
    print(f"\n🔄 Initializing database schema")
    init_cmd = [sys.executable, "initialize_database.py"]
    results['init_db'] = run_command(init_cmd, "Initialize database schema", timeout=60)
    
    # 4. Run backfill
    backfill_cmd = [
        sys.executable, "py/enhanced_simple_backfill.py",
        "--start", test_date,
        "--end", test_date,
        "--output", output_dir
    ]
    results['backfill'] = run_command(backfill_cmd, f"Backfill data for {test_date}", timeout=120)
    
    # 5. Check files were created
    if results['backfill']:
        results['files'] = check_files_created(output_dir, 5)  # Expect at least 5 files
    else:
        results['files'] = False
    
    # 6. Load data into database
    if results['files']:
        load_cmd = [
            sys.executable, "loader/enhanced_load_parquet_into_pg.py",
            "--input-dir", output_dir,
            "--validate-schema"
        ]
        results['load'] = run_command(load_cmd, "Load data into database", timeout=120)
    else:
        results['load'] = False
    
    # 7. Test analysis
    if results['load']:
        results['analysis_data'] = test_analysis_output()
        
        analysis_cmd = [sys.executable, "py/simple_analysis.py"]
        results['analysis'] = run_command(analysis_cmd, "Run betting analysis", timeout=60)
    else:
        results['analysis_data'] = False
        results['analysis'] = False
    
    # 8. Summary
    print(f"\n📋 Pipeline Test Results")
    print("=" * 30)
    
    test_steps = [
        ('Configuration', results['config']),
        ('Database Connection', results['database']),
        ('Database Schema', results['init_db']),
        ('Data Backfill', results['backfill']),
        ('File Creation', results['files']),
        ('Data Loading', results['load']),
        ('Data Validation', results['analysis_data']),
        ('Betting Analysis', results['analysis']),
    ]
    
    passed = 0
    for step_name, success in test_steps:
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"   {step_name:<20} {status}")
        if success:
            passed += 1
    
    success_rate = (passed / len(test_steps)) * 100
    print(f"\n📊 Overall: {passed}/{len(test_steps)} steps passed ({success_rate:.0f}%)")
    
    if passed == len(test_steps):
        print(f"\n🎉 SUCCESS! Your MLB pipeline is working perfectly!")
        print(f"🚀 Ready for production use!")
        
        print(f"\n💡 Next steps:")
        print(f"   1. Run with different dates: python py/enhanced_simple_backfill.py --start 2025-01-16 --end 2025-01-16")
        print(f"   2. Switch to real data: Set USE_PLACEHOLDER_DATA=false in .env")
        print(f"   3. Build your betting application!")
        
        return True
    else:
        print(f"\n⚠️ Some steps failed. Check the errors above.")
        
        if not results['config']:
            print(f"💡 Fix: Run python setup_env.py")
        
        if not results['database']:
            print(f"💡 Fix: Check your PG_DSN in .env file")
        
        if not results['backfill']:
            print(f"💡 Fix: Check python dependencies: pip install -r py/requirements.txt")
        
        if not results['load']:
            print(f"💡 Fix: Check database schema and permissions")
        
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)