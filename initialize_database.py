#!/usr/bin/env python3
"""
initialize_database.py - Initialize database schema safely with flexible configuration
UPDATED: Works with new placeholder mode and graceful degradation
"""
import sys
import argparse
from pathlib import Path

def main():
    parser = argparse.ArgumentParser(description="Initialize MLB database schema")
    parser.add_argument("--reset", action="store_true", 
                       help="Reset entire schema (DELETES ALL DATA)")
    parser.add_argument("--force", action="store_true",
                       help="Force re-run all migrations")
    parser.add_argument("--skip-db-test", action="store_true",
                       help="Skip database connection test")
    parser.add_argument("--create-dirs", action="store_true",
                       help="Create missing directories")
    args = parser.parse_args()
    
    try:
        # FIXED: Use new flexible configuration that doesn't exit on missing optional features
        print("🔍 Loading configuration...")
        from py.config import get_config
        config = get_config()  # This won't exit anymore for missing optional features
        
        # Show current mode
        placeholder_mode = getattr(config, 'USE_PLACEHOLDER_DATA', True)
        mode = "PLACEHOLDER" if placeholder_mode else "REAL DATA"
        print(f"🔧 Mode: {mode}")
        
        if placeholder_mode:
            print("💡 Placeholder mode - database setup optimized for test data")
        
        # Check if database is configured
        if not config.PG_DSN:
            print("❌ Database not configured")
            print("\n💡 Quick Setup Options:")
            print("   1. Interactive setup:")
            print("      python setup_env.py")
            print()
            print("   2. Manual setup:")
            print("      export PG_DSN='postgresql://user:password@host:5432/database'")
            print()
            print("   3. Create .env file:")
            print("      echo 'PG_DSN=postgresql://user:password@host:5432/database' > .env")
            print()
            print("      Example for local PostgreSQL:")
            print("      PG_DSN=postgresql://mlbuser:mypassword@localhost:5432/mlb_betting")
            print()
            print("      Example for AWS RDS (your current setup):")
            print("      PG_DSN=postgresql://mlbadmin:password@mlb-pg-prod.cfcwyqumqdy8.us-east-2.rds.amazonaws.com:5432/mlb-pg-prod")
            sys.exit(1)
        
        # Create directories if requested
        if args.create_dirs:
            print("📁 Creating missing directories...")
            directories = [
                config.OUTPUT_DIR,
                config.MIGRATIONS_DIR, 
                config.LOG_DIR,
                "backup"
            ]
            
            for directory in directories:
                dir_path = Path(directory)
                if not dir_path.exists():
                    dir_path.mkdir(parents=True, exist_ok=True)
                    print(f"   📂 Created: {directory}")
                else:
                    print(f"   ✅ Exists: {directory}")
        
        # Test database connection (unless skipped)
        if not args.skip_db_test:
            print("🔍 Testing database connection...")
            success, message = config.test_database_connection()
            
            if not success:
                print(f"❌ Database connection failed: {message}")
                print(f"💡 Check your PG_DSN environment variable")
                
                # Show masked DSN for debugging (security)
                masked_dsn = config.PG_DSN[:20] + "..." + config.PG_DSN[-15:] if len(config.PG_DSN) > 35 else "[DSN too short to mask safely]"
                print(f"   Current PG_DSN: {masked_dsn}")
                
                print("\n🔧 Common fixes:")
                print("   • Check database is running")
                print("   • Verify host, port, username, password")
                print("   • Ensure database exists")
                print("   • Check firewall/network connectivity")
                print("   • For AWS RDS: check security groups")
                
                # Offer to continue anyway
                continue_anyway = input("\nContinue with database initialization anyway? (y/N): ").lower().strip()
                if continue_anyway not in ['y', 'yes']:
                    print("❌ Database initialization cancelled")
                    sys.exit(1)
                else:
                    print("⚠️ Proceeding without connection test...")
            else:
                print(f"✅ Database connection successful: {message}")
        else:
            print("⏭️ Skipping database connection test")
        
        # Show current configuration status
        print("\n📊 Configuration Status:")
        config.print_status()
        
        # Check migration files exist
        migrations_dir = Path(config.MIGRATIONS_DIR)
        if not migrations_dir.exists():
            print(f"❌ Migrations directory not found: {migrations_dir}")
            print(f"💡 Create directory and add migration files")
            sys.exit(1)
        
        migration_files = list(migrations_dir.glob("*.sql"))
        if not migration_files:
            print(f"❌ No migration files found in {migrations_dir}")
            print(f"💡 Add SQL migration files to the migrations directory")
            sys.exit(1)
        
        print(f"📄 Found {len(migration_files)} migration files:")
        for file in sorted(migration_files):
            print(f"   • {file.name}")
        
        # Initialize schema
        print(f"\n🔄 Initializing database schema...")
        
        try:
            success = config.initialize_database(reset=args.reset)
        except Exception as e:
            print(f"❌ Database initialization error: {e}")
            
            print(f"\n🔍 Troubleshooting:")
            print(f"   • Check migration files are valid SQL")
            print(f"   • Verify database permissions")
            print(f"   • Check PostgreSQL version compatibility")
            
            if placeholder_mode:
                print(f"   • For placeholder mode, ensure schema supports test data")
            
            sys.exit(1)
        
        if success:
            print(f"\n🎉 Database initialization complete!")
            
            # Show enabled features
            enabled_features = config.get_enabled_features()
            if enabled_features:
                print(f"✅ Available features: {', '.join(enabled_features)}")
            
            print(f"\n💡 Next steps:")
            if placeholder_mode:
                print(f"   1. Test with placeholder data:")
                print(f"      python py/enhanced_simple_backfill.py --start 2025-01-15 --end 2025-01-15")
                print(f"   2. Or run complete pipeline:")
                print(f"      python run_pipeline.py --start 2025-01-15 --quick-test")
            else:
                print(f"   1. Collect real data:")
                print(f"      python py/enhanced_simple_backfill.py --start 2024-07-15 --end 2024-07-15")
            
            print(f"   3. Load the data:")
            print(f"      python loader/enhanced_load_parquet_into_pg.py --input-dir stage --validate-schema")
            print(f"   4. Run analysis:")
            print(f"      python py/simple_analysis.py")
            print(f"   5. Or test everything:")
            print(f"      python test_pipeline.py")
            
            # Show mode-specific tips
            if placeholder_mode:
                print(f"\n🔧 Placeholder Mode Tips:")
                print(f"   • Fast and reliable for development")
                print(f"   • Works with any date (including future)")
                print(f"   • Perfect for testing pipeline logic")
                print(f"   • To switch to real data: set USE_PLACEHOLDER_DATA=false in .env")
            
            # Show warnings if features are disabled
            if not config.ENABLE_WEATHER:
                print(f"⚠️ Weather analysis is disabled")
                if not config.OPENWEATHER_API_KEY:
                    print(f"   Get free key: https://openweathermap.org/api")
                    print(f"   Or keep placeholder mode for testing")
            
            if not config.ENABLE_S3_STORAGE:
                print(f"⚠️ S3 storage is disabled (optional)")
            
            print(f"\n🎯 Database is ready for MLB betting analysis!")
            sys.exit(0)
        else:
            print(f"❌ Database initialization failed")
            print(f"\n🔍 Troubleshooting:")
            print(f"   • Check migration files exist in {config.MIGRATIONS_DIR}/ directory")
            print(f"   • Verify database permissions (CREATE, ALTER, INSERT)")
            print(f"   • Run with --skip-db-test if connection issues persist")
            print(f"   • Check PostgreSQL logs for detailed errors")
            
            if args.force:
                print(f"   • Already used --force flag")
            else:
                print(f"   • Try with --force to re-run all migrations")
            
            sys.exit(1)
            
    except KeyboardInterrupt:
        print(f"\n⚠️ Initialization cancelled by user")
        sys.exit(1)
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print(f"💡 Install dependencies: pip install -r py/requirements.txt")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        
        # Show debug info if needed
        if args.force or '--debug' in sys.argv:
            print(f"\n🐛 Debug information:")
            import traceback
            traceback.print_exc()
        else:
            print(f"💡 Run with --force for more debug information")
        
        print(f"\n🔧 Common solutions:")
        print(f"   • Check .env file exists and has valid PG_DSN")
        print(f"   • Install dependencies: pip install -r py/requirements.txt")
        print(f"   • Ensure database server is running")
        print(f"   • Check migrations directory exists with SQL files")
        
        sys.exit(1)

if __name__ == "__main__":
    main()