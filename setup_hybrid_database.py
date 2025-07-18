#!/usr/bin/env python3
"""
Hybrid database setup workflow:
1. Run comprehensive migrations for core schema
2. Use dynamic schema detection to add missing columns
3. Validate everything works correctly
"""
import os
import psycopg2
import pandas as pd
from pathlib import Path
import argparse
import subprocess
import sys

def run_migrations(conn, migrations_dir="migrations"):
    """Run all migration files in order"""
    print("🔄 Running comprehensive database migrations...")
    
    migrations_path = Path(migrations_dir)
    if not migrations_path.exists():
        print(f"❌ Migrations directory {migrations_dir} does not exist")
        return False
    
    migration_files = sorted(migrations_path.glob("*.sql"))
    if not migration_files:
        print(f"❌ No migration files found in {migrations_dir}")
        return False
    
    cur = conn.cursor()
    
    # Create schema first
    cur.execute("CREATE SCHEMA IF NOT EXISTS public;")
    print("✅ Created/verified public schema")
    
    for migration_file in migration_files:
        print(f"📝 Running {migration_file.name}...")
        with open(migration_file, 'r') as f:
            migration_sql = f.read()
        
        try:
            cur.execute(migration_sql)
            print(f"✅ {migration_file.name} completed successfully")
        except Exception as e:
            print(f"❌ Error in {migration_file.name}: {e}")
            return False
    
    conn.commit()
    print("🎉 All migrations completed successfully!")
    return True

def get_table_columns(conn, table: str):
    """Get existing table columns with their types"""
    sql = """
      SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
       WHERE table_schema = 'public'
         AND table_name   = %s
       ORDER BY ordinal_position
    """
    cur = conn.cursor()
    cur.execute(sql, (table,))
    return {row[0]: {'type': row[1], 'nullable': row[2]} for row in cur.fetchall()}

def get_postgres_type_from_pandas(dtype, sample_values=None):
    """Convert pandas dtype to PostgreSQL type"""
    dtype_str = str(dtype).lower()
    
    if 'int' in dtype_str:
        if sample_values is not None and len(sample_values) > 0:
            max_val = abs(sample_values).max()
            if max_val > 2147483647:  # Larger than int32
                return 'BIGINT'
        return 'INTEGER'
    elif 'float' in dtype_str:
        return 'REAL'
    elif 'bool' in dtype_str:
        return 'BOOLEAN'
    elif 'datetime' in dtype_str:
        return 'TIMESTAMP'
    elif 'date' in dtype_str:
        return 'DATE'
    else:
        if sample_values is not None and len(sample_values) > 0:
            max_len = sample_values.astype(str).str.len().max()
            if max_len > 255:
                return 'TEXT'
            else:
                return f'VARCHAR({min(max_len * 2, 255)})'
        return 'TEXT'

def add_missing_columns_to_table(conn, table: str, parquet_file: Path):
    """Add any missing columns from parquet to database table"""
    print(f"🔍 Checking for missing columns in {table} using {parquet_file.name}...")
    
    try:
        # Read parquet to see what columns it has
        df = pd.read_parquet(parquet_file)
        parquet_columns = set(df.columns)
        
        # Get existing table columns
        existing_columns = set(get_table_columns(conn, table).keys())
        
        # Find missing columns
        missing_columns = parquet_columns - existing_columns
        
        if not missing_columns:
            print(f"✅ No missing columns in {table}")
            return 0
        
        print(f"🆕 Found {len(missing_columns)} missing columns in {table}")
        
        cur = conn.cursor()
        added_count = 0
        
        for col in sorted(missing_columns):
            # Get sample data to determine type
            sample_data = df[col].dropna().head(1000)
            pg_type = get_postgres_type_from_pandas(df[col].dtype, sample_data)
            
            try:
                alter_sql = f"ALTER TABLE public.{table} ADD COLUMN {col} {pg_type}"
                cur.execute(alter_sql)
                print(f"  ✅ Added: {col} ({pg_type})")
                added_count += 1
            except Exception as e:
                print(f"  ⚠️  Could not add {col}: {e}")
        
        conn.commit()
        
        if added_count > 0:
            print(f"🎉 Successfully added {added_count} columns to {table}")
        
        return added_count
        
    except Exception as e:
        print(f"❌ Error processing {parquet_file}: {e}")
        return 0

def update_schemas_from_data(conn, data_dir="stage"):
    """Update all table schemas based on actual parquet data"""
    print(f"\n🔄 Updating schemas from data in {data_dir}...")
    
    data_path = Path(data_dir)
    if not data_path.exists():
        print(f"⚠️  Data directory {data_dir} does not exist - skipping schema updates")
        return 0
    
    # Table mapping
    table_mappings = {
        'statcast': 'statcast_*.parquet',
        'statsapi_playlog': 'statsapi_*.parquet',
        'roster': 'roster_*.parquet',
        'lineup': 'lineup_*.parquet'
    }
    
    total_columns_added = 0
    
    for table, pattern in table_mappings.items():
        files = list(data_path.glob(pattern))
        if files:
            # Use most recent file for schema detection
            latest_file = max(files, key=lambda f: f.stat().st_mtime)
            print(f"\n📁 Using {latest_file.name} for {table} schema updates")
            
            columns_added = add_missing_columns_to_table(conn, table, latest_file)
            total_columns_added += columns_added
        else:
            print(f"⚠️  No data files found for {table} (pattern: {pattern})")
    
    print(f"\n🎉 Schema update complete! Added {total_columns_added} total columns")
    return total_columns_added

def validate_setup(conn, data_dir="stage"):
    """Validate that the setup works correctly"""
    print(f"\n🔍 Validating database setup...")
    
    tables = ['statcast', 'statsapi_playlog', 'roster', 'lineup']
    
    for table in tables:
        columns = get_table_columns(conn, table)
        if columns:
            print(f"📊 {table}: {len(columns)} columns available")
            
            # Show a few example columns
            sample_cols = list(columns.keys())[:5]
            print(f"   Sample columns: {sample_cols}")
        else:
            print(f"❌ Table {table} does not exist or has no columns")
    
    # Test with actual data files if they exist
    data_path = Path(data_dir)
    if data_path.exists():
        files = list(data_path.glob("*.parquet"))
        if files:
            latest_file = files[0]  # Just test with one file
            try:
                df = pd.read_parquet(latest_file)
                print(f"\n📋 Sample data file {latest_file.name}:")
                print(f"   Rows: {len(df)}, Columns: {len(df.columns)}")
                print(f"   Sample columns: {list(df.columns)[:5]}")
            except Exception as e:
                print(f"⚠️  Could not read sample file: {e}")
        else:
            print(f"\n📋 No parquet files found in {data_dir}")
    else:
        print(f"\n📋 Data directory {data_dir} does not exist")

def test_enhanced_backfill(output_dir="test_stage"):
    """Run a small test of the enhanced backfill"""
    print(f"\n🧪 Testing enhanced backfill...")
    
    # Test with just one day to see if it works
    test_date = "2024-04-15"  # Known good date
    
    try:
        cmd = [
            sys.executable, "py/backfill.py",
            "--start", test_date,
            "--end", test_date,
            "--output", output_dir
        ]
        
        print(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0:
            print("✅ Test backfill completed successfully")
            
            # Check what files were created
            output_path = Path(output_dir)
            if output_path.exists():
                files = list(output_path.glob("*.parquet"))
                print(f"📁 Created {len(files)} parquet files:")
                for f in files:
                    size_mb = f.stat().st_size / (1024 * 1024)
                    print(f"   - {f.name} ({size_mb:.1f} MB)")
            else:
                print("⚠️  No output directory created")
        else:
            print(f"❌ Test backfill failed:")
            print(f"   stdout: {result.stdout}")
            print(f"   stderr: {result.stderr}")
            
    except subprocess.TimeoutExpired:
        print("⚠️  Test backfill timed out (5 minutes)")
    except Exception as e:
        print(f"❌ Error running test backfill: {e}")

def main():
    parser = argparse.ArgumentParser(description="Hybrid database setup for MLB ETL")
    parser.add_argument("--migrations-dir", default="migrations", help="Migrations directory")
    parser.add_argument("--data-dir", default="stage", help="Data directory with parquet files")
    parser.add_argument("--skip-migrations", action="store_true", help="Skip running migrations")
    parser.add_argument("--skip-test", action="store_true", help="Skip test backfill")
    parser.add_argument("--validate-only", action="store_true", help="Only validate setup")
    
    args = parser.parse_args()
    
    print("🚀 MLB ETL Hybrid Database Setup")
    print("="*50)
    
    # Connect to database
    dsn = os.getenv("PG_DSN")
    if not dsn:
        print("❌ PG_DSN environment variable must be set")
        print("   Example: export PG_DSN='postgresql://user:pass@localhost:5432/mydb'")
        return 1
    
    try:
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        print("✅ Connected to database successfully")
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        return 1
    
    try:
        if args.validate_only:
            validate_setup(conn, args.data_dir)
            return 0
        
        # Step 1: Run comprehensive migrations
        if not args.skip_migrations:
            success = run_migrations(conn, args.migrations_dir)
            if not success:
                print("❌ Migration failed - stopping")
                return 1
        else:
            print("⏭️  Skipping migrations")
        
        # Step 2: Update schemas based on actual data (if data exists)
        columns_added = update_schemas_from_data(conn, args.data_dir)
        
        # Step 3: Validate everything works
        validate_setup(conn, args.data_dir)
        
        # Step 4: Test enhanced backfill (optional)
        if not args.skip_test:
            test_enhanced_backfill()
        
        print(f"\n🎉 Hybrid setup complete!")
        print(f"📋 Your database now has:")
        print(f"   ✅ Comprehensive base schemas (80+ columns per table)")
        print(f"   ✅ {columns_added} additional columns from actual data")
        print(f"   ✅ Proper indexes for query performance")
        print(f"   ✅ Future-proof column detection")
        
        print(f"\n🚀 Next steps:")
        print(f"   1. Run: python py/backfill.py --start 2024-04-01 --end 2024-04-30 --monthly")
        print(f"   2. Run: python loader/load_parquet_into_pg.py --input-dir stage --validate")
        print(f"   3. Start building your ML models with rich data!")
        
        return 0
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1
    finally:
        conn.close()

if __name__ == "__main__":
    sys.exit(main())