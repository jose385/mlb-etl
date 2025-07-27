#!/usr/bin/env python3
"""
Setup script for MLB betting analysis package
"""

from setuptools import setup, find_packages

setup(
    name="mlb-betting-analysis",
    version="1.0.0",
    description="MLB betting analysis system",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "pandas>=2.0.0",
        "numpy>=1.25.0",
        "psycopg2-binary>=2.9.0",
        "pybaseball>=2.2.1",
        "MLB-StatsAPI>=1.9.0",
        "requests>=2.31.0",
        "tqdm>=4.65.0",
        "pyarrow>=12.0.0",
    ],
    entry_points={
        "console_scripts": [
            "mlb-backfill=py.backfill:main",
            "mlb-daily-analysis=py.master_daily_analysis:main",
        ],
    },
)