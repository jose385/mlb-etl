#!/usr/bin/env python3
"""
database.py - Robust database connection and management
"""
import psycopg2
import psycopg2.extras
import time
import logging
from typing import Optional, Dict, Any
from contextlib import contextmanager
from functools import wraps

class DatabaseManager:
    def __init__(self, dsn: str, max_retries: int = 5, retry_delay: float = 1.0):
        self.dsn = dsn
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.logger = logging.getLogger(__name__)
    
    def get_connection(self) -> psycopg2.extensions.connection:
        """Get database connection with retry logic"""
        last_exception = None
        
        for attempt in range(self.max_retries):
            try:
                conn = psycopg2.connect(self.dsn)
                conn.autocommit = True
                return conn
                
            except psycopg2.OperationalError as e:
                last_exception = e
                if attempt == self.max_retries - 1:
                    break
                
                wait_time = self.retry_delay * (2 ** attempt)  # Exponential backoff
                self.logger.warning(f"Database connection attempt {attempt + 1} failed, retrying in {wait_time}s: {e}")
                time.sleep(wait_time)
                
            except Exception as e:
                self.logger.error(f"Unexpected database error: {e}")
                raise
        
        raise last_exception
    
    @contextmanager
    def get_cursor(self, cursor_factory=None):
        """Context manager for database operations"""
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor(cursor_factory=cursor_factory)
            yield cursor
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                conn.close()
    
    def test_connection(self) -> tuple[bool, str]:
        """Test database connection"""
        try:
            with self.get_cursor() as cur:
                cur.execute("SELECT 1")
                return True, "Connection successful"
        except Exception as e:
            return False, f"Connection failed: {e}"
    
    def execute_sql_file(self, file_path: str) -> bool:
        """Execute SQL file with proper error handling"""
        try:
            with open(file_path, 'r') as f:
                sql_content = f.read()
            
            with self.get_cursor() as cur:
                cur.execute(sql_content)
            
            self.logger.info(f"Successfully executed SQL file: {file_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to execute SQL file {file_path}: {e}")
            return False