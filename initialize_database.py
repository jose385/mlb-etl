#!/usr/bin/env python3
"""
initialize_database.py - Initialize database schema safely
"""
import sys
import argparse
from pathlib import Path
from py.config import require_config

def ensure_required_directories():
    """Ensure required directories exist before database initialization"""
    print("📁 Checking project directory structure...")
    
    required_dirs = {
        'migrations': 'Database migration files',
        'logs': 'Application logs',
        'stage': 'Data staging area'
    }
    
    missing_dirs = []
    created_dirs = []
    
    for directory, description in required_dirs.items():
        dir_path = Path(directory)
        if dir_path.exists():
            if dir_path.is_dir():
                print(f"   ✅ {directory:<12} - {description}")
            else:
                print(f"   ❌ {directory:<12} - EXISTS BUT IS NOT A DIRECTORY!")
                print(f"      Please remove the file '{directory}' and try again")
                return False
        else:
            missing_dirs.append(directory)
    
    if missing_dirs:
        print(f"📂 Creating {len(missing_dirs)} missing directories...")
        for directory in missing_dirs:
            try:
                Path(directory).mkdir(parents=True, exist_ok=True)
                created_dirs.append(directory)
                print(f"   📁 Created: {directory}")
            except PermissionError:
                print(f"   ❌ Permission denied creating: {directory}")
                print(f"      Run with appropriate permissions or create manually:")
                print(f"      mkdir -p {directory}")
                return False
            except Exception as e:
                print(f"   ❌ Failed to create {directory}: {e}")
                return False
    
    if created_dirs:
        print(f"   🎉 Successfully created {len(created_dirs)} directories")
    
    return True

def validate_migration_files():
    """Validate that migration files exist"""
    print("📄 Checking migration files...")
    
    migrations_dir = Path('migrations')
    if not migrations_dir.exists():
        print("   ❌ Migrations directory not found")
        return False
    
    required_migrations = [
        '001_enhanced_simple_schema.sql',
        '002_foreign_keys.sql'
    ]
    
    missing_migrations = []
    
    for migration_file in required_migrations:
        migration_path = migrations_dir / migration_file
        if migration_path.exists():
            print(f"   ✅ {migration_file}")
        else:
            print(f"   ❌ {migration_file} - MISSING")
            missing_migrations.append(migration_file)
    
    if missing_migrations:
        print(f"   💡 Missing migration files - database schema may be incomplete")
        print(f"      Make sure all migration files are present in the migrations/ directory")
        return False
    
    return True

def main():
    parser = argparse.ArgumentParser(description="Initialize MLB database schema")
    parser.add_argument("--reset", action="store_true", 
                       help="Reset entire schema (DELETES ALL DATA)")
    parser.add_argument("--force", action="store_true",
                       help="Force re-run all migrations")
    parser.add_argument("--skip-validation", action="store_true",
                       help="Skip directory and file validation")
    args = parser.parse_args()
    
    print("🔧 MLB Betting Analysis - Database Initialization")
    print("=" * 55)
    
    # Step 1: Ensure required directories exist
    if not args.skip_validation:
        if not ensure_required_directories():
            print("\n❌ Directory setup failed")
            print("💡 Try running: python setup_env.py")
            sys.exit(1)
        
        # Step 2: Validate migration files exist
        if not validate_migration_files():
            print("\n❌ Migration file validation failed")
            print("💡 Make sure all migration files are present")
            if input("Continue anyway? (y/N): ").strip().lower() != 'y':
                sys.exit(1)
    
    # Step 3: Load and validate configuration
    try:
        print("\n⚙️ Loading configuration...")
        config = require_config(require_database=True)
        print("   ✅ Configuration loaded successfully")
        
    except SystemExit as e:
        print("\n❌ Configuration validation failed")
        print("💡 Run 'python setup_env.py' to configure the environment")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Configuration error: {e}")
        sys.exit(1)
    
    # Step 4: Test database connection
    print("\n🔍 Testing database connection...")
    try:
        success, message = config.test_database_connection()
        
        if not success:
            print(f"❌ Database connection failed: {message}")
            print("\n💡 Common solutions:")
            print("   • Check if PostgreSQL server is running")
            print("   • Verify your PG_DSN connection string")
            print("   • Check network connectivity to database server")
            print("   • Verify database credentials")
            
            if "rds.amazonaws.com" in config.PG_DSN:
                print("   • For AWS RDS: Check security groups and VPC settings")
            
            sys.exit(1)
        
        print(f"✅ Database connection successful: {message}")
        
    except Exception as e:
        print(f"❌ Database connection test error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    # Step 5: Handle schema reset if requested
    if args.reset:
        print("\n🚨 WARNING: Schema reset requested!")
        print("   This will DELETE ALL DATA in the database!")
        print("   All tables, data, and indexes will be permanently lost!")
        
        confirm = input("\nType 'DELETE ALL DATA' to confirm: ").strip()
        if confirm == "DELETE ALL DATA":
            print("\n💥 Resetting database schema...")
            try:
                success = config.initialize_database(reset=True)
                if not success:
                    print("❌ Schema reset failed")
                    sys.exit(1)
                print("✅ Schema reset completed")
            except Exception as e:
                print(f"❌ Schema reset error: {e}")
                sys.exit(1)
        else:
            print("❌ Schema reset cancelled")
            print("   Must type exactly 'DELETE ALL DATA' to confirm")
            sys.exit(1)
    
    # Step 6: Initialize/update database schema
    print("\n🏗️ Initializing database schema...")
    try:
        success = config.initialize_database(reset=False)
        
        if success:
            print("🎉 Database initialization complete!")
            
            # Step 7: Verify schema
            print("\n📊 Verifying database schema...")
            db_manager = config.get_database_manager()
            
            with db_manager.get_cursor() as cur:
                # Check if tables exist
                cur.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """)
                
                tables = [row[0] for row in cur.fetchall()]
                
                print(f"   📋 Found {len(tables)} tables:")
                expected_tables = [
                    'games', 'play_by_play', 'game_info', 'weather', 
                    'umpires', 'lineups', 'rosters', 'recent_stats', 'venue_factors'
                ]
                
                for table in expected_tables:
                    if table in tables:
                        print(f"      ✅ {table}")
                    else:
                        print(f"      ❌ {table} - MISSING")
                
                # Check for any unexpected tables
                unexpected_tables = set(tables) - set(expected_tables) - {'schema_migrations'}
                if unexpected_tables:
                    print(f"   ℹ️  Additional tables: {', '.join(unexpected_tables)}")
            
            print("\n💡 Next steps:")
            print("   1. Run data collection:")
            print("      python py/enhanced_simple_backfill.py --start 2024-07-01 --end 2024-07-01")
            print("   2. Load data into database:")
            print("      python loader/enhanced_load_parquet_into_pg.py --input-dir stage")
            print("   3. Run betting analysis:")
            print("      python py/simple_analysis.py")
            print("   4. Check data quality:")
            print("      python check_env.py")
            
            sys.exit(0)
        else:
            print("❌ Database initialization failed")
            print("💡 Check the error messages above for details")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Database initialization error: {e}")
        import traceback
        traceback.print_exc()
        
        print(f"\n💡 Troubleshooting tips:")
        print(f"   • Check database permissions")
        print(f"   • Verify migration files are valid SQL")
        print(f"   • Check PostgreSQL logs for detailed errors")
        print(f"   • Try with --reset flag to start fresh (WARNING: deletes all data)")
        
        sys.exit(1)

if __name__ == "__main__":
    main()