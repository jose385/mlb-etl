"""
Data Loading Package

Utilities for loading parquet files into PostgreSQL.
"""

__version__ = "1.0.0"

try:
    from .load_parquet_into_pg import main as load_data
except ImportError:
    pass