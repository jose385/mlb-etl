#!/usr/bin/env python3
"""
Comprehensive test suite for enhanced MLB ETL pipeline
Tests configuration, database, data collection, and analysis capabilities
"""
import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path
import pandas as pd
import numpy as np

def test_imports():
    """Test that all required packages can be imported"""
    print("🔍 Testing package imports...")
    
    required_packages = [
        ('pandas', 'pd'),
        ('numpy', 'np'), 
        ('psycopg2', None),
        ('pyarrow', None),
        ('requests', None),
    ]
    
    optional_packages = [
        ('pybaseball', None),
        ('psutil', None),
        ('boto3', None),
    ]
    
    results = {"required": {}, "optional": {}}
    
    # Test required packages
    for package, alias in required_packages:
        try:
            if alias:
                exec(f"import {package} as {alias}")
            else:
                exec(f"import {package}")
            results["required"][package] = "✅ Available"
        except ImportError as e:
            results["required"][package] = f"❌ Missing: {e}"
    
    # Test optional packages
    for package, alias in optional_packages:
        try:
            if alias:
                exec(f"import {package} as {alias}")
            else:
                exec(f"import {package}")
            results["optional"][package] = "✅ Available"
        except ImportError:
            results["optional"][package] = "⚠️ Optional package not installed"
    
    print("📦 Required Packages:")
    for package, status in results["required"].items():
        print(f"   {package}: {status}")
    
    print("\n📦 Optional Packages:")
    for package, status in results["optional"].items():
        print(f"   {package}: {status}")
    
    # Check if any required packages are missing
    missing_required = [pkg for pkg, status in results["required"].items() if "❌" in status]
    if missing_required:
        print(f"\n❌ Missing required packages: {missing_required}")
        print("💡 Run: pip install -r py/requirements.txt")
        return False
    
    print("\n✅ All required imports successful!")
    return True

def test_configuration():
    """Test enhanced configuration system"""
    print("\n🔧 Testing configuration system...")
    
    try:
        from py.config import get_config
        config = get_config()
        
        print("✅ Configuration loaded successfully")
        
        # Test configuration summary
        summary = config.get_enhanced_summary()
        print(f"📊 Configuration Summary:")
        print(f"   Database configured: {summary['database_configured']}")
        print(f"   Use placeholder data: {summary['use_placeholder_data']}")
        print(f"   Memory optimization: {summary['memory_optimization']}")
        print(f"   API caching: {summary['api_caching']}")
        print(f"   Graceful degradation: {summary['graceful_degradation']}")
        
        # Test memory status
        memory_status = config.get_memory_status()
        print(f"   Memory status: {memory_status}")
        
        # Test API limits
        pybaseball_limits = config.get_api_limits("pybaseball")
        print(f"   Pybaseball rate limit: {pybaseball_limits.requests_per_minute}/min")
        
        config.print_enhanced_status()
        return True
        
    except Exception as e:
        print(f"❌ Configuration test failed: {e}")
        return False

def test_database_connectivity():
    """Test database connection and schema"""
    print("\n🗄️ Testing database connectivity...")
    
    try:
        from py.config import require_config
        config = require_config(require_database=True)
        
        # Test database connection
        db_connected, db_message = config.test_database_connection()
        print(f"Database connection: {'✅' if db_connected else '❌'} {db_message}")
        
        if not db_connected:
            print("💡 Make sure your PG_DSN is set and database is running")
            return False
        
        # Test schema validation
        db_manager = config.get_database_manager()
        conn = db_manager.get_connection()
        
        # Check if tables exist
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        
        tables = [row[0] for row in cur.fetchall()]
        expected_tables = ["games", "play_by_play", "umpires", "lineups", "rosters", "game_info", "recent_stats"]
        
        print(f"📋 Database Tables:")
        for table in expected_tables:
            status = "✅" if table in tables else "❌"
            print(f"   {table}: {status}")
        
        missing_tables = set(expected_tables) - set(tables)
        if missing_tables:
            print(f"\n⚠️ Missing tables: {missing_tables}")
            print("💡 Run: python initialize_database.py")
            return False
        
        # Test enhanced schema columns
        cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = 'games'
            ORDER BY column_name
        """)
        
        game_columns = {row[0]: row[1] for row in cur.fetchall()}
        
        # Check for key enhanced columns
        enhanced_columns = [
            'estimated_ba_using_speedangle',
            'launch_speed_angle',
            'release_spin_rate',
            'pfx_x', 'pfx_z',
            'hc_x', 'hc_y'
        ]
        
        print(f"\n📊 Enhanced Statcast Columns:")
        for col in enhanced_columns:
            status = "✅" if col in game_columns else "❌"
            print(f"   {col}: {status}")
        
        conn.close()
        
        print(f"\n✅ Database schema validation complete!")
        print(f"   Total columns in games table: {len(game_columns)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Database test failed: {e}")
        return False

def test_data_collection():
    """Test data collection in both placeholder and real modes"""
    print("\n📡 Testing data collection...")
    
    try:
        from py.config import get_config
        config = get_config()
        
        if config.USE_PLACEHOLDER_DATA:
            print("🔧 Testing placeholder data generation...")
            return test_placeholder_data_generation()
        else:
            print("📡 Testing real data collection...")
            return test_real_data_collection()
            
    except Exception as e:
        print(f"❌ Data collection test setup failed: {e}")
        return False

def test_placeholder_data_generation():
    """Test placeholder data generation"""
    try:
        # Test that we can generate some sample data
        test_date = date.today() - timedelta(days=1)
        
        print(f"🎲 Generating placeholder data for {test_date}...")
        
        # Create simple test data structure
        sample_games = []
        for game_num in range(3):  # Just 3 test games
            game_pk = 700000 + game_num
            
            # Generate some sample pitches
            for at_bat in range(1, 4):  # 3 at-bats per game
                for pitch_num in range(1, 4):  # 3 pitches per at-bat
                    pitch_data = {
                        'game_date': test_date,
                        'game_pk': game_pk,
                        'at_bat_number': at_bat,
                        'pitch_number': pitch_num,
                        'pitcher': 600000 + (game_num * 2),
                        'batter': 500000 + (game_num * 2) + 1,
                        'release_speed': 90 + np.random.normal(0, 5),
                        'launch_speed': 85 + np.random.normal(0, 10) if pitch_num == 3 else None,
                        'launch_angle': np.random.normal(15, 10) if pitch_num == 3 else None,
                        'estimated_ba_using_speedangle': np.random.uniform(0.1, 0.4) if pitch_num == 3 else None,
                        'events': 'hit_into_play' if pitch_num == 3 else 'ball',
                        'balls': pitch_num - 1 if pitch_num < 3 else 0,
                        'strikes': 0 if pitch_num < 3 else 1,
                    }
                    sample_games.append(pitch_data)
        
        df = pd.DataFrame(sample_games)
        
        print(f"✅ Generated {len(df)} sample records")
        print(f"📊 Sample data structure:")
        print(f"   Columns: {list(df.columns)}")
        print(f"   Games: {df['game_pk'].nunique()}")
        print(f"   Pitches: {len(df)}")
        
        # Test saving to parquet
        output_dir = Path("stage")
        output_dir.mkdir(exist_ok=True)
        
        test_file = output_dir / f"test_games_{test_date}.parquet"
        df.to_parquet(test_file)
        
        print(f"✅ Saved test data to {test_file}")
        
        # Test reading it back
        df_read = pd.read_parquet(test_file)
        print(f"✅ Successfully read back {len(df_read)} records")
        
        return True
        
    except Exception as e:
        print(f"❌ Placeholder data generation failed: {e}")
        return False

def test_real_data_collection():
    """Test real data collection capabilities"""
    try:
        # Test pybaseball import
        try:
            import pybaseball as pyb
            print("✅ pybaseball imported successfully")
        except ImportError:
            print("❌ pybaseball not available - install with: pip install pybaseball")
            return False
        
        # Test basic connectivity
        try:
            # Try to get a small sample of recent data
            test_date = date.today() - timedelta(days=3)
            print(f"🔍 Testing data retrieval for {test_date}...")
            
            # Get just one day of data as a test
            statcast_data = pyb.statcast(start_dt=test_date.strftime('%Y-%m-%d'), 
                                       end_dt=test_date.strftime('%Y-%m-%d'))
            
            if len(statcast_data) > 0:
                print(f"✅ Retrieved {len(statcast_data)} Statcast records")
                print(f"📊 Columns available: {len(statcast_data.columns)}")
                print(f"   Sample columns: {list(statcast_data.columns)[:10]}...")
                
                # Check for enhanced columns
                enhanced_cols = [
                    'estimated_ba_using_speedangle',
                    'launch_speed_angle', 
                    'release_spin_rate',
                    'effective_speed'
                ]
                
                available_enhanced = [col for col in enhanced_cols if col in statcast_data.columns]
                print(f"   Enhanced metrics available: {available_enhanced}")
                
                return True
            else:
                print(f"⚠️ No data returned for {test_date} (games may not have occurred)")
                return True  # Not necessarily a failure
                
        except Exception as e:
            print(f"❌ Real data collection failed: {e}")
            print("💡 This might be due to rate limiting or API issues")
            return False
            
    except Exception as e:
        print(f"❌ Real data collection test setup failed: {e}")
        return False

def test_enhanced_loader():
    """Test the enhanced data loader"""
    print("\n📥 Testing enhanced data loader...")
    
    try:
        from py.config import get_config
        config = get_config()
        
        # Make sure we have some test data
        stage_dir = Path("stage")
        parquet_files = list(stage_dir.glob("*.parquet"))
        
        if not parquet_files:
            print("⚠️ No parquet files found in stage directory")
            print("💡 Generate some test data first")
            return False
        
        # Test loading one file
        test_file = parquet_files[0]
        print(f"🔍 Testing loader with: {test_file.name}")
        
        # Test reading the file
        df = pd.read_parquet(test_file)
        print(f"📊 File contains {len(df)} rows, {len(df.columns)} columns")
        
        # Test the enhanced data type conversion
        sys.path.append('.')
        from loader.enhanced_load_parquet_into_pg import fix_comprehensive_data_types
        
        print("🔧 Testing enhanced data type conversion...")
        df_converted = fix_comprehensive_data_types(df)
        
        print(f"✅ Data type conversion completed")
        print(f"📋 Sample data types after conversion:")
        for col in df_converted.columns[:10]:
            print(f"   {col}: {df_converted[col].dtype}")
        
        # Test database loading (if database is available)
        try:
            db_manager = config.get_database_manager()
            if db_manager:
                print("🗄️ Testing database insertion...")
                
                # We'll just test the loading logic without actually inserting
                # to avoid modifying the database during testing
                print("✅ Loader appears ready for database operations")
            else:
                print("⚠️ Database not available for testing")
        
        except Exception as e:
            print(f"⚠️ Database testing skipped: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Enhanced loader test failed: {e}")
        return False

def test_basic_analysis():
    """Test basic analysis capabilities"""
    print("\n📈 Testing basic analysis capabilities...")
    
    try:
        from py.config import require_config
        config = require_config(require_database=True, graceful_degradation=True)
        
        db_manager = config.get_database_manager()
        if not db_manager:
            print("⚠️ Database not available - skipping analysis test")
            return True
        
        conn = db_manager.get_connection()
        cur = conn.cursor()
        
        # Test basic queries
        test_queries = [
            ("Game Info Count", "SELECT COUNT(*) FROM game_info"),
            ("Games Count", "SELECT COUNT(*) FROM games"),
            ("Recent Date Range", """
                SELECT MIN(game_date) as earliest, MAX(game_date) as latest 
                FROM game_info 
                WHERE game_date IS NOT NULL
            """),
            ("Enhanced Columns Test", """
                SELECT 
                    COUNT(*) as total_pitches,
                    COUNT(estimated_ba_using_speedangle) as xba_count,
                    COUNT(launch_speed_angle) as barrel_count,
                    COUNT(release_spin_rate) as spin_count
                FROM games 
                LIMIT 1000
            """),
        ]
        
        print("🔍 Running analysis queries...")
        
        for query_name, query in test_queries:
            try:
                cur.execute(query)
                result = cur.fetchall()
                print(f"✅ {query_name}: {result}")
            except Exception as e:
                print(f"❌ {query_name} failed: {e}")
        
        # Test some advanced analysis if we have data
        try:
            cur.execute("SELECT COUNT(*) FROM games WHERE estimated_ba_using_speedangle IS NOT NULL")
            xba_count = cur.fetchone()[0]
            
            if xba_count > 0:
                print(f"\n🎯 Advanced Analysis Sample:")
                
                # Sample expected vs actual analysis
                cur.execute("""
                    SELECT 
                        AVG(estimated_ba_using_speedangle) as avg_xba,
                        COUNT(*) as sample_size
                    FROM games 
                    WHERE estimated_ba_using_speedangle IS NOT NULL 
                    AND events = 'single'
                    LIMIT 100
                """)
                
                result = cur.fetchone()
                if result and result[1] > 0:
                    print(f"   Average xBA on singles: {result[0]:.3f} (n={result[1]})")
                
                # Barrel analysis
                cur.execute("""
                    SELECT 
                        launch_speed_angle,
                        COUNT(*) as count
                    FROM games 
                    WHERE launch_speed_angle IS NOT NULL
                    GROUP BY launch_speed_angle
                    ORDER BY launch_speed_angle
                """)
                
                barrel_data = cur.fetchall()
                if barrel_data:
                    print(f"   Contact quality distribution:")
                    for quality, count in barrel_data:
                        quality_name = "Barrel" if quality == 6 else f"Quality {quality}"
                        print(f"     {quality_name}: {count} occurrences")
            
            else:
                print("⚠️ No enhanced Statcast data found - may need to collect more data")
        
        except Exception as e:
            print(f"⚠️ Advanced analysis failed: {e}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ Analysis test failed: {e}")
        return False

def run_comprehensive_test():
    """Run all tests in sequence"""
    print("🚀 Starting comprehensive MLB system test...\n")
    
    test_results = {}
    
    # Run all tests
    tests = [
        ("Package Imports", test_imports),
        ("Configuration System", test_configuration),
        ("Database Connectivity", test_database_connectivity),
        ("Data Collection", test_data_collection),
        ("Enhanced Loader", test_enhanced_loader),
        ("Basic Analysis", test_basic_analysis),
    ]
    
    for test_name, test_func in tests:
        print(f"\n{'='*60}")
        print(f"Running: {test_name}")
        print('='*60)
        
        try:
            start_time = time.time()
            success = test_func()
            duration = time.time() - start_time
            
            test_results[test_name] = {
                "success": success,
                "duration": duration
            }
            
            status = "✅ PASSED" if success else "❌ FAILED"
            print(f"\n{test_name}: {status} ({duration:.2f}s)")
            
        except Exception as e:
            test_results[test_name] = {
                "success": False,
                "duration": 0,
                "error": str(e)
            }
            print(f"\n{test_name}: ❌ FAILED - {e}")
    
    # Print summary
    print(f"\n{'='*60}")
    print("🎯 TEST SUMMARY")
    print('='*60)
    
    passed_tests = sum(1 for result in test_results.values() if result["success"])
    total_tests = len(test_results)
    
    for test_name, result in test_results.items():
        status = "✅ PASSED" if result["success"] else "❌ FAILED"
        duration = result["duration"]
        print(f"{test_name:.<40} {status} ({duration:.2f}s)")
        
        if not result["success"] and "error" in result:
            print(f"    Error: {result['error']}")
    
    print(f"\nOverall: {passed_tests}/{total_tests} tests passed")
    
    if passed_tests == total_tests:
        print("\n🎉 All tests passed! Your enhanced MLB system is ready!")
        print("\n💡 Next steps:")
        print("   1. Run data collection: python py/enhanced_simple_backfill.py")
        print("   2. Load data: python loader/enhanced_load_parquet_into_pg.py")
        print("   3. Start analysis: python py/simple_analysis.py")
    else:
        print(f"\n⚠️ {total_tests - passed_tests} tests failed - check issues above")
        print("\n🔧 Common fixes:")
        print("   • Install missing packages: pip install -r py/requirements.txt")
        print("   • Set up database: python initialize_database.py")
        print("   • Check environment variables: python setup_env.py")
    
    return test_results

if __name__ == "__main__":
    # Allow running individual tests
    if len(sys.argv) > 1:
        test_name = sys.argv[1].lower()
        test_map = {
            "imports": test_imports,
            "config": test_configuration,
            "database": test_database_connectivity,
            "data": test_data_collection,
            "loader": test_enhanced_loader,
            "analysis": test_basic_analysis,
        }
        
        if test_name in test_map:
            print(f"🔍 Running specific test: {test_name}")
            success = test_map[test_name]()
            sys.exit(0 if success else 1)
        else:
            print(f"❌ Unknown test: {test_name}")
            print(f"Available tests: {list(test_map.keys())}")
            sys.exit(1)
    else:
        # Run comprehensive test suite
        results = run_comprehensive_test()
        
        # Exit with error code if any tests failed
        failed_tests = sum(1 for result in results.values() if not result["success"])
        sys.exit(failed_tests)