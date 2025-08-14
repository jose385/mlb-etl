#!/usr/bin/env python3
"""
Check database schema to identify column mapping issues
"""

import psycopg2
import sys
import os

# Add project root to path
sys.path.append('/workspaces/mlb-etl')

from py.config import require_config

def check_table_schema(table_name, conn):
    """Check the column names for a specific table"""
    try:
        cur = conn.cursor()
        
        # Get column information
        cur.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = %s 
            ORDER BY ordinal_position
        """, (table_name,))
        
        columns = cur.fetchall()
        
        if columns:
            print(f"\n📊 Table: {table_name}")
            print("=" * 40)
            for col_name, data_type, is_nullable in columns:
                nullable = "NULL" if is_nullable == "YES" else "NOT NULL"
                print(f"   {col_name:<25} {data_type:<15} {nullable}")
                
            # Look for ID columns specifically
            id_columns = [col[0] for col in columns if 'id' in col[0].lower()]
            if id_columns:
                print(f"   🆔 ID columns: {id_columns}")
        else:
            print(f"\n❌ Table '{table_name}' not found")
            
    except Exception as e:
        print(f"❌ Error checking {table_name}: {e}")

def main():
    """Check all relevant table schemas"""
    print("🔍 CHECKING DATABASE SCHEMA FOR COLUMN MAPPING ISSUES")
    print("=" * 60)
    
    try:
        config = require_config(require_database=True, graceful_degradation=True)
        conn = psycopg2.connect(config.PG_DSN)
        
        # Tables that might have ID column issues
        tables_to_check = [
            'recent_stats',
            'lineups', 
            'rosters',
            'games',
            'game_info',
            'play_by_play'
        ]
        
        for table in tables_to_check:
            check_table_schema(table, conn)
        
        conn.close()
        
        print(f"\n🎯 WHAT TO LOOK FOR:")
        print(f"   • Does 'lineups' table use 'person_id' or 'player_id'?")
        print(f"   • Does 'rosters' table use 'person_id' or 'player_id'?") 
        print(f"   • Are all tables consistent with ID column naming?")
        
        print(f"\n💡 If you see mismatches:")
        print(f"   • Update the backfill script column names to match schema")
        print(f"   • Or update the database schema for consistency")
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print(f"💡 Make sure your database is running and .env is configured")

if __name__ == '__main__':
    main()