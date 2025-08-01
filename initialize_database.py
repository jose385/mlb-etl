#!/usr/bin/env python3
"""
initialize_database.py - Initialize database schema safely
"""
import sys
import argparse
from py.config import require_config

def main():
    parser = argparse.ArgumentParser(description="Initialize MLB database schema")
    parser.add_argument("--reset", action="store_true", 
                       help="Reset entire schema (DELETES ALL DATA)")
    parser.add_argument("--force", action="store_true",
                       help="Force re-run all migrations")
    args = parser.parse_args()
    
    try:
        config = require_config(require_database=True)
        
        print("🔍 Testing database connection...")
        success, message = config.test_database_connection()
        
        if not success:
            print(f"❌ Database connection failed: {message}")
            print("💡 Check your PG_DSN environment variable")
            sys.exit(1)
        
        print(f"✅ Database connection successful")
        
        # Initialize schema
        success = config.initialize_database(reset=args.reset)
        
        if success:
            print("🎉 Database initialization complete!")
            print("\n💡 Next steps:")
            print("   1. Run backfill: python py/enhanced_simple_backfill.py --start 2024-07-01 --end 2024-07-01")
            print("   2. Load data: python loader/enhanced_load_parquet_into_pg.py")
            sys.exit(0)
        else:
            print("❌ Database initialization failed")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()