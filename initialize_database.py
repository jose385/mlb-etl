#!/usr/bin/env python3
"""
initialize_database.py - Initialize database schema safely with flexible configuration
UPDATED: Flexible configuration that doesn't exit on missing optional features
"""
import sys
import argparse
from py.config import get_config

def main():
    parser = argparse.ArgumentParser(description="Initialize MLB database schema")
    parser.add_argument("--reset", action="store_true", 
                       help="Reset entire schema (DELETES ALL DATA)")
    parser.add_argument("--force", action="store_true",
                       help="Force re-run all migrations")
    parser.add_argument("--skip-db-test", action="store_true",
                       help="Skip database connection test")
    args = parser.parse_args()
    
    try:
        # FIXED: Use flexible configuration that doesn't exit on missing optional features
        print("🔍 Loading configuration...")
        config = get_config()  # This won't exit anymore for missing optional features
        
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
            sys.exit(1)
        
        # Test database connection (unless skipped)
        if not args.skip_db_test:
            print("🔍 Testing database connection...")
            success, message = config.test_database_connection()
            
            if not success:
                print(f"❌ Database connection failed: {message}")
                print(f"💡 Check your PG_DSN environment variable")
                
                # Show masked DSN for debugging
                masked_dsn = config.PG_DSN[:20] + "..." + config.PG_DSN[-10:] if len(config.PG_DSN) > 30 else "too short"
                print(f"   Current PG_DSN: {masked_dsn}")
                
                print("\n🔧 Common fixes:")
                print("   • Check database is running")
                print("   • Verify host, port, username, password")
                print("   • Ensure database exists")
                print("   • Check firewall/network connectivity")
                
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
        
        # Initialize schema
        print("\n🔄 Initializing database schema...")
        
        # Create required directories first
        from pathlib import Path
        Path(config.OUTPUT_DIR).mkdir(exist_ok=True)
        Path(config.MIGRATIONS_DIR).mkdir(exist_ok=True)
        Path(config.LOG_DIR).mkdir(exist_ok=True)
        
        success = config.initialize_database(reset=args.reset)
        
        if success:
            print("\n🎉 Database initialization complete!")
            
            # Show enabled features
            enabled_features = config.get_enabled_features()
            if enabled_features:
                print(f"✅ Available features: {', '.join(enabled_features)}")
            
            print("\n💡 Next steps:")
            print("   1. Test with small dataset:")
            print("      python py/enhanced_simple_backfill.py --start 2024-07-15 --end 2024-07-15")
            print()
            print("   2. Load the data:")
            print("      python loader/enhanced_load_parquet_into_pg.py --input-dir stage --validate-schema")
            print()
            print("   3. Run analysis:")
            print("      python py/simple_analysis.py")
            print()
            
            # Show warnings if features are disabled
            if not config.ENABLE_WEATHER:
                print("⚠️ Weather analysis is disabled (no OPENWEATHER_API_KEY)")
                print("   Get free key: https://openweathermap.org/api")
            
            if not config.ENABLE_S3_STORAGE:
                print("⚠️ S3 storage is disabled (no AWS_S3_BUCKET)")
            
            print("\n🎯 System is ready for MLB betting analysis!")
            sys.exit(0)
        else:
            print("❌ Database initialization failed")
            print("\n🔍 Troubleshooting:")
            print("   • Check migration files exist in migrations/ directory")
            print("   • Verify database permissions")
            print("   • Run with --skip-db-test if connection issues persist")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n⚠️ Initialization cancelled by user")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        
        # Show debug info if needed
        if args.force or '--debug' in sys.argv:
            print("\n🐛 Debug information:")
            import traceback
            traceback.print_exc()
        else:
            print("💡 Run with --force for more debug information")
        
        sys.exit(1)

if __name__ == "__main__":
    main()