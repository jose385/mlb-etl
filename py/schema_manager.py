#!/usr/bin/env python3
"""
schema_manager.py - Handle database schema migrations safely
"""
import os
from pathlib import Path
from typing import List, Dict
import logging

class SchemaMigrationManager:
    def __init__(self, db_manager, migrations_dir: str = "migrations"):
        self.db_manager = db_manager
        self.migrations_dir = Path(migrations_dir)
        self.logger = logging.getLogger(__name__)
    
    def get_migration_files(self) -> List[Path]:
        """Get all migration files in order"""
        if not self.migrations_dir.exists():
            raise FileNotFoundError(f"Migrations directory not found: {self.migrations_dir}")
        
        migration_files = sorted(self.migrations_dir.glob("*.sql"))
        if not migration_files:
            raise FileNotFoundError(f"No migration files found in {self.migrations_dir}")
        
        return migration_files
    
    def create_migration_tracking_table(self):
        """Create table to track applied migrations"""
        sql = """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            id SERIAL PRIMARY KEY,
            filename VARCHAR(255) UNIQUE NOT NULL,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            checksum VARCHAR(64)
        );
        """
        
        with self.db_manager.get_cursor() as cur:
            cur.execute(sql)
    
    def is_migration_applied(self, filename: str) -> bool:
        """Check if migration has been applied"""
        with self.db_manager.get_cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM schema_migrations WHERE filename = %s",
                (filename,)
            )
            return cur.fetchone()[0] > 0
    
    def record_migration(self, filename: str):
        """Record that migration has been applied"""
        with self.db_manager.get_cursor() as cur:
            cur.execute(
                "INSERT INTO schema_migrations (filename) VALUES (%s) ON CONFLICT (filename) DO NOTHING",
                (filename,)
            )
    
    def run_migrations(self, force: bool = False) -> Dict[str, bool]:
        """Run all pending migrations"""
        results = {}
        
        # Create migration tracking table
        self.create_migration_tracking_table()
        
        migration_files = self.get_migration_files()
        
        for migration_file in migration_files:
            filename = migration_file.name
            
            if not force and self.is_migration_applied(filename):
                self.logger.info(f"⏭️ Skipping already applied migration: {filename}")
                results[filename] = True
                continue
            
            self.logger.info(f"🔄 Applying migration: {filename}")
            
            try:
                success = self.db_manager.execute_sql_file(str(migration_file))
                
                if success:
                    self.record_migration(filename)
                    self.logger.info(f"✅ Migration applied: {filename}")
                    results[filename] = True
                else:
                    self.logger.error(f"❌ Migration failed: {filename}")
                    results[filename] = False
                    
            except Exception as e:
                self.logger.error(f"❌ Migration error {filename}: {e}")
                results[filename] = False
        
        return results
    
    def reset_schema(self, confirm: bool = False):
        """Reset entire schema (DANGEROUS - development only)"""
        if not confirm:
            raise ValueError("Must explicitly confirm schema reset")
        
        reset_file = self.migrations_dir / "000_drop_and_recreate.sql"
        if reset_file.exists():
            self.logger.warning("🚨 RESETTING ENTIRE SCHEMA - ALL DATA WILL BE LOST")
            return self.db_manager.execute_sql_file(str(reset_file))
        else:
            raise FileNotFoundError("Reset migration file not found")