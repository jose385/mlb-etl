#!/usr/bin/env python3
"""
Test runner for real MLB data collection
Identifies runtime issues when switching from placeholder to real data
"""
import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime, timedelta
import tempfile
import shutil

def backup_env_file():
    """Backup current .env file"""
    env_file = Path('.env')
    if env_file.exists():
        backup_file = Path('.env.backup_for_test')
        shutil.copy2(env_file, backup_file)
        print(f"✅ Backed up .env to {backup_file}")
        return True
    else:
        print("⚠️ No .env file found - will create temporary one")
        return False

def create_real_data_env():
    """Modify .env for real data testing"""
    env_file = Path('.env')
    
    if env_file.exists():
        # Read current .env
        with open(env_file, 'r') as f:
            content = f.read()
        
        # Replace placeholder setting
        content = content.replace('USE_PLACEHOLDER_DATA=true', 'USE_PLACEHOLDER_DATA=false')
        
        # Write back
        with open(env_file, 'w') as f:
            f.write(content)
        
        print("✅ Modified .env for real data testing")
    else:
        # Create minimal .env for testing
        with open(env_file, 'w') as f:
            f.write("""# Minimal .env for real data testing
USE_PLACEHOLDER_DATA=false
PG_DSN=postgresql://mlbadmin:Orangechips_17@mlb-pg-prod.cfcwyqumqdy8.us-east-2.rds.amazonaws.com:5432/mlb-pg-prod
OUTPUT_DIR=stage_real_test
DEBUG=true
VERBOSE=true
""")
        print("✅ Created minimal .env for real data testing")

def restore_env_file():
    """Restore original .env file"""
    backup_file = Path('.env.backup_for_test')
    env_file = Path('.env')
    
    if backup_file.exists():
        shutil.copy2(backup_file, env_file)
        backup_file.unlink()
        print("✅ Restored original .env file")
    else:
        print("⚠️ No backup found - .env file was created for testing")

def test_real_data_dependencies():
    """Test if real data dependencies are available"""
    print("🔍 Testing real data dependencies...")
    
    tests = [
        ("pybaseball", "import pybaseball; print(f'Version: {pybaseball.__version__}')"),
        ("requests", "import requests; print(f'Version: {requests.__version__}')"),
        ("pandas", "import pandas as pd; print(f'Version: {pd.__version__}')"),
        ("psycopg2", "import psycopg2; print(f'Version: {psycopg2.__version__}')"),
    ]
    
    all_passed = True
    
    for name, test_code in tests:
        try:
            result = subprocess.run([sys.executable, '-c', test_code], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                print(f"   ✅ {name}: {result.stdout.strip()}")
            else:
                print(f"   ❌ {name}: {result.stderr.strip()}")
                all_passed = False
        except subprocess.TimeoutExpired:
            print(f"   ❌ {name}: Import timeout")
            all_passed = False
        except Exception as e:
            print(f"   ❌ {name}: {e}")
            all_passed = False
    
    return all_passed

def test_pybaseball_connectivity():
    """Test basic pybaseball functionality"""
    print("\n📡 Testing pybaseball connectivity...")
    
    test_script = '''
import pybaseball as pyb
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Test with a known date that should have data
test_date = "2024-07-04"  # July 4th usually has games
print(f"Testing Statcast data for {test_date}")

try:
    # Try to get just a small sample
    data = pyb.statcast(start_dt=test_date, end_dt=test_date)
    
    if data is not None and len(data) > 0:
        print(f"✅ Retrieved {len(data)} records")
        print(f"📊 Columns: {len(data.columns)}")
        print(f"🔍 Sample columns: {list(data.columns)[:10]}")
        
        # Check for key columns our system expects
        key_columns = [
            'game_pk', 'game_date', 'pitcher', 'batter',
            'release_speed', 'launch_speed', 'launch_angle',
            'estimated_ba_using_speedangle', 'launch_speed_angle'
        ]
        
        missing_cols = [col for col in key_columns if col not in data.columns]
        if missing_cols:
            print(f"⚠️ Missing expected columns: {missing_cols}")
        else:
            print("✅ All expected columns present")
            
        # Check data quality
        null_counts = data[key_columns].isnull().sum()
        print(f"📋 Data quality check:")
        for col in key_columns:
            if col in data.columns:
                null_pct = (null_counts[col] / len(data)) * 100
                print(f"   {col}: {null_pct:.1f}% null")
        
        print("✅ pybaseball connectivity test PASSED")
    else:
        print(f"⚠️ No data returned for {test_date}")
        print("   This might be normal if no games occurred")
        
except Exception as e:
    print(f"❌ pybaseball test failed: {e}")
    raise
'''
    
    try:
        result = subprocess.run([sys.executable, '-c', test_script], 
                              capture_output=True, text=True, timeout=60)
        
        print(result.stdout)
        if result.stderr:
            print("Warnings/Errors:")
            print(result.stderr)
        
        return result.returncode == 0
        
    except subprocess.TimeoutExpired:
        print("❌ pybaseball test timed out (>60s)")
        return False
    except Exception as e:
        print(f"❌ pybaseball test error: {e}")
        return False

def test_real_data_collection():
    """Test actual data collection with real APIs"""
    print("\n⚾ Testing real data collection...")
    
    # Use a recent date that likely has games
    test_date = "2024-07-15"  # Mid-season date
    
    # Create test output directory
    test_dir = Path("stage_real_test")
    test_dir.mkdir(exist_ok=True)
    
    print(f"🎯 Testing real data collection for {test_date}")
    
    try:
        # Test the backfill script with real data
        cmd = [
            sys.executable, 
            "py/enhanced_simple_backfill.py",
            "--start", test_date,
            "--end", test_date,
            "--out-dir", str(test_dir),
            "--real-data"
        ]
        
        print(f"Running: {' '.join(cmd)}")
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        
        print("📤 Backfill Output:")
        print(result.stdout)
        
        if result.stderr:
            print("⚠️ Backfill Errors/Warnings:")
            print(result.stderr)
        
        # Check if files were created
        parquet_files = list(test_dir.glob("*.parquet"))
        print(f"\n📁 Generated files: {len(parquet_files)}")
        
        for file in parquet_files:
            print(f"   📄 {file.name}: {file.stat().st_size} bytes")
        
        if len(parquet_files) > 0:
            print("✅ Real data collection test PASSED")
            return True, test_dir
        else:
            print("❌ No parquet files generated")
            return False, test_dir
            
    except subprocess.TimeoutExpired:
        print("❌ Data collection timed out (>5 minutes)")
        return False, test_dir
    except Exception as e:
        print(f"❌ Data collection error: {e}")
        return False, test_dir

def test_real_data_loading(test_dir):
    """Test loading real data into database"""
    print(f"\n💾 Testing real data loading from {test_dir}...")
    
    try:
        cmd = [
            sys.executable,
            "loader/enhanced_load_parquet_into_pg.py",
            "--input-dir", str(test_dir),
            "--validate-schema",
            "--debug"
        ]
        
        print(f"Running: {' '.join(cmd)}")
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        
        print("📤 Loader Output:")
        print(result.stdout)
        
        if result.stderr:
            print("⚠️ Loader Errors/Warnings:")
            print(result.stderr)
        
        success = result.returncode == 0 and "Transaction committed successfully" in result.stdout
        
        if success:
            print("✅ Real data loading test PASSED")
        else:
            print("❌ Real data loading test FAILED")
        
        return success
        
    except subprocess.TimeoutExpired:
        print("❌ Data loading timed out (>2 minutes)")
        return False
    except Exception as e:
        print(f"❌ Data loading error: {e}")
        return False

def test_real_data_analysis():
    """Test analysis with real data"""
    print("\n📈 Testing analysis with real data...")
    
    try:
        cmd = [sys.executable, "py/simple_analysis.py"]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        print("📤 Analysis Output:")
        print(result.stdout)
        
        if result.stderr:
            print("⚠️ Analysis Errors/Warnings:")
            print(result.stderr)
        
        success = result.returncode == 0 and "DATA EXTRACTION SUMMARY" in result.stdout
        
        if success:
            print("✅ Real data analysis test PASSED")
        else:
            print("❌ Real data analysis test FAILED")
        
        return success
        
    except subprocess.TimeoutExpired:
        print("❌ Analysis timed out (>1 minute)")
        return False
    except Exception as e:
        print(f"❌ Analysis error: {e}")
        return False

def cleanup_test_files(test_dir):
    """Clean up test files"""
    try:
        if test_dir.exists():
            shutil.rmtree(test_dir)
            print(f"🧹 Cleaned up test directory: {test_dir}")
    except Exception as e:
        print(f"⚠️ Cleanup warning: {e}")

def main():
    """Run comprehensive real data test"""
    print("🚀 MLB Real Data Runtime Test")
    print("=" * 50)
    
    # Check current state
    print(f"📁 Current directory: {Path.cwd()}")
    print(f"📅 Test timestamp: {datetime.now()}")
    
    test_results = {}
    test_dir = None
    
    try:
        # Step 1: Backup and modify environment
        print(f"\n1️⃣ Setting up test environment...")
        had_env = backup_env_file()
        create_real_data_env()
        
        # Step 2: Test dependencies
        print(f"\n2️⃣ Testing dependencies...")
        deps_ok = test_real_data_dependencies()
        test_results["dependencies"] = deps_ok
        
        if not deps_ok:
            print("❌ Dependency test failed - cannot continue")
            return
        
        # Step 3: Test pybaseball connectivity
        print(f"\n3️⃣ Testing pybaseball connectivity...")
        pyb_ok = test_pybaseball_connectivity()
        test_results["pybaseball"] = pyb_ok
        
        if not pyb_ok:
            print("❌ pybaseball connectivity failed")
            print("💡 You might need to:")
            print("   • Check your internet connection")
            print("   • Update pybaseball: pip install --upgrade pybaseball")
            print("   • Try again later (MLB APIs can be temperamental)")
            return
        
        # Step 4: Test data collection
        print(f"\n4️⃣ Testing real data collection...")
        collection_ok, test_dir = test_real_data_collection()
        test_results["collection"] = collection_ok
        
        if not collection_ok:
            print("❌ Data collection failed")
            return
        
        # Step 5: Test data loading
        print(f"\n5️⃣ Testing real data loading...")
        loading_ok = test_real_data_loading(test_dir)
        test_results["loading"] = loading_ok
        
        # Step 6: Test analysis
        print(f"\n6️⃣ Testing real data analysis...")
        analysis_ok = test_real_data_analysis()
        test_results["analysis"] = analysis_ok
        
    except KeyboardInterrupt:
        print("\n⚠️ Test interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Always restore environment and cleanup
        print(f"\n🔄 Restoring environment...")
        restore_env_file()
        
        if test_dir:
            cleanup_test_files(test_dir)
    
    # Print final results
    print(f"\n🎯 REAL DATA TEST SUMMARY")
    print("=" * 40)
    
    passed = sum(test_results.values())
    total = len(test_results)
    
    for test_name, result in test_results.items():
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{test_name.capitalize():.<20} {status}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All real data tests PASSED!")
        print("✅ Your system is ready for live MLB data collection")
        print("\n💡 To switch to real data permanently:")
        print("   1. Set USE_PLACEHOLDER_DATA=false in .env")
        print("   2. Use past dates (not future dates)")
        print("   3. Be patient - real APIs are slower than placeholder")
    else:
        print(f"\n⚠️ {total - passed} tests failed")
        print("💡 Common issues with real data:")
        print("   • API rate limiting (be more conservative)")
        print("   • Network connectivity issues")
        print("   • MLB API changes (update pybaseball)")
        print("   • Date range issues (use dates with actual games)")
    
    return test_results

if __name__ == "__main__":
    main()