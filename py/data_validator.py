# ==============================================================================
# FILE: py/data_validator.py (Add this for data quality checks)
# ==============================================================================
#!/usr/bin/env python3
"""
Data quality validation
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple

class DataValidator:
    """Validate data quality and completeness"""
    
    @staticmethod
    def validate_statcast_data(df: pd.DataFrame) -> Dict[str, any]:
        """Validate Statcast data quality"""
        issues = []
        
        # Check required columns
        required_cols = ['game_date', 'game_pk', 'pitcher', 'batter']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            issues.append(f"Missing required columns: {missing_cols}")
        
        # Check for reasonable data ranges
        if 'release_speed' in df.columns:
            speed_issues = df[(df['release_speed'] < 50) | (df['release_speed'] > 110)]
            if len(speed_issues) > 0:
                issues.append(f"Suspicious pitch speeds: {len(speed_issues)} pitches")
        
        # Check for excessive nulls
        for col in ['events', 'description', 'pitch_type']:
            if col in df.columns:
                null_pct = df[col].isnull().mean()
                if null_pct > 0.5:
                    issues.append(f"High null rate in {col}: {null_pct:.1%}")
        
        return {
            "total_records": len(df),
            "issues": issues,
            "quality_score": max(0, 100 - len(issues) * 10)
        }
    
    @staticmethod
    def validate_weather_data(df: pd.DataFrame) -> Dict[str, any]:
        """Validate weather data"""
        issues = []
        
        # Temperature range check
        if 'temperature_f' in df.columns:
            temp_issues = df[(df['temperature_f'] < -10) | (df['temperature_f'] > 120)]
            if len(temp_issues) > 0:
                issues.append(f"Extreme temperatures: {len(temp_issues)} records")
        
        # Wind speed check
        if 'wind_speed_mph' in df.columns:
            wind_issues = df[df['wind_speed_mph'] > 60]  # Hurricane-force winds
            if len(wind_issues) > 0:
                issues.append(f"Extreme wind speeds: {len(wind_issues)} records")
        
        return {
            "total_records": len(df),
            "issues": issues,
            "quality_score": max(0, 100 - len(issues) * 15)
        }