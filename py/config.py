"""
Centralized configuration management for MLB ETL pipeline
Handles all environment variables with validation and defaults
Enhanced for 9-table betting analysis system with AWS S3 and RDS support
"""
import os
import sys
from typing import Optional, Dict, List
from pathlib import Path

class ConfigError(Exception):
    """Custom exception for configuration errors"""
    pass

class Config:
    """Centralized configuration management with validation"""
    
    def __init__(self):
        self._validated = False
        self._load_config()
    
    def _load_config(self):
        """Load and set all configuration values"""
        
        # Database Configuration (REQUIRED)
        self.PG_DSN = os.getenv("PG_DSN", "")
        
        # API Keys (CONDITIONAL)
        self.OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY", "")
        self.MLB_API_KEY = os.getenv("MLB_API_KEY", "")  # Future use
        
        # AWS Configuration
        self.AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET", "")
        self.AWS_S3_PREFIX = os.getenv("AWS_S3_PREFIX", "mlb-data")
        self.AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        self.ENABLE_S3_STORAGE = self._str_to_bool(os.getenv("ENABLE_S3_STORAGE", "false"))
        self.AUTO_UPLOAD_TO_S3 = self._str_to_bool(os.getenv("AUTO_UPLOAD_TO_S3", "false"))
        self.AUTO_CLEANUP_LOCAL = self._str_to_bool(os.getenv("AUTO_CLEANUP_LOCAL", "false"))
        
        # Directory Paths
        self.OUTPUT_DIR = os.getenv("OUTPUT_DIR", "stage")
        self.MIGRATIONS_DIR = os.getenv("MIGRATIONS_DIR", "migrations")
        self.LOG_DIR = os.getenv("LOG_DIR", "logs")
        
        # Enhanced Rate Limiting Settings
        self.MLB_API_DELAY = float(os.getenv("MLB_API_DELAY", "0.2"))
        self.WEATHER_API_DELAY = float(os.getenv("WEATHER_API_DELAY", "0.5"))
        self.STATS_API_DELAY = float(os.getenv("STATS_API_DELAY", "0.3"))
        
        # Enhanced Data Quality Thresholds
        self.MIN_GAMES_FOR_ANALYSIS = int(os.getenv("MIN_GAMES_FOR_ANALYSIS", "5"))
        self.MIN_SAMPLE_SIZE_UMPIRE = int(os.getenv("MIN_SAMPLE_SIZE_UMPIRE", "15"))
        self.MIN_PITCHER_STARTS = int(os.getenv("MIN_PITCHER_STARTS", "3"))
        self.MIN_TEAM_GAMES = int(os.getenv("MIN_TEAM_GAMES", "7"))
        
        # Enhanced Betting Analysis Thresholds
        self.STRONG_EDGE_THRESHOLD = float(os.getenv("STRONG_EDGE_THRESHOLD", "0.12"))
        self.MODERATE_EDGE_THRESHOLD = float(os.getenv("MODERATE_EDGE_THRESHOLD", "0.06"))
        self.WEATHER_IMPACT_THRESHOLD = float(os.getenv("WEATHER_IMPACT_THRESHOLD", "0.08"))
        
        # Enhanced Feature Flags (true/false)
        self.ENABLE_WEATHER = self._str_to_bool(os.getenv("ENABLE_WEATHER", "true"))
        self.ENABLE_UMPIRE_ANALYSIS = self._str_to_bool(os.getenv("ENABLE_UMPIRE_ANALYSIS", "true"))
        self.ENABLE_VENUE_FACTORS = self._str_to_bool(os.getenv("ENABLE_VENUE_FACTORS", "true"))
        self.ENABLE_RECENT_STATS = self._str_to_bool(os.getenv("ENABLE_RECENT_STATS", "true"))
        self.ENABLE_GAME_INFO = self._str_to_bool(os.getenv("ENABLE_GAME_INFO", "true"))
        
        # New Enhanced Features
        self.ENABLE_PITCHER_WORKLOAD = self._str_to_bool(os.getenv("ENABLE_PITCHER_WORKLOAD", "true"))
        self.ENABLE_TEAM_FORM_ANALYSIS = self._str_to_bool(os.getenv("ENABLE_TEAM_FORM_ANALYSIS", "true"))
        self.ENABLE_BALLPARK_ADJUSTMENTS = self._str_to_bool(os.getenv("ENABLE_BALLPARK_ADJUSTMENTS", "true"))
        
        # Legacy Feature Flags (for backward compatibility)
        self.ENABLE_FATIGUE_METRICS = self._str_to_bool(os.getenv("ENABLE_FATIGUE_METRICS", "false"))
        
        # Enhanced Logging
        self.LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
        self.LOG_TO_FILE = self._str_to_bool(os.getenv("LOG_TO_FILE", "true"))
        self.PERFORMANCE_MONITORING = self._str_to_bool(os.getenv("PERFORMANCE_MONITORING", "false"))
        
        # Debug/Development
        self.DEBUG = self._str_to_bool(os.getenv("DEBUG", "false"))
        self.VERBOSE = self._str_to_bool(os.getenv("VERBOSE", "false"))
        self.DRY_RUN = self._str_to_bool(os.getenv("DRY_RUN", "false"))
    
    def _str_to_bool(self, value: str) -> bool:
        """Convert string environment variable to boolean"""
        return value.lower() in ('true', '1', 'yes', 'on', 'enabled')
    
    def validate(self, require_weather: bool = False, 
                 require_database: bool = True) -> List[str]:
        """
        Validate configuration and return list of issues
        
        Args:
            require_weather: Whether weather API key is required
            require_database: Whether database connection is required
        """
        issues = []
        
        # Database validation
        if require_database and not self.PG_DSN:
            issues.append("PG_DSN environment variable is required but not set")
        
        if self.PG_DSN and not self._validate_pg_dsn():
            issues.append("PG_DSN format appears invalid (should be postgresql://user:pass@host:port/db)")
        
        # Weather API validation
        if require_weather and not self.OPENWEATHER_API_KEY:
            issues.append("OPENWEATHER_API_KEY is required for weather analysis but not set")
        
        if self.ENABLE_WEATHER and not self.OPENWEATHER_API_KEY:
            issues.append("Weather is enabled but OPENWEATHER_API_KEY is not set")
        
        # S3 validation
        if self.ENABLE_S3_STORAGE and not self.AWS_S3_BUCKET:
            issues.append("S3 storage is enabled but AWS_S3_BUCKET is not set")
        
        # Directory validation
        directories_to_check = [
            (self.OUTPUT_DIR, "OUTPUT_DIR"),
            (self.MIGRATIONS_DIR, "MIGRATIONS_DIR"),
        ]
        
        for dir_path, var_name in directories_to_check:
            if not Path(dir_path).exists():
                issues.append(f"{var_name} directory '{dir_path}' does not exist")
        
        # Enhanced numeric validation
        numeric_validations = [
            (self.MLB_API_DELAY, "MLB_API_DELAY", 0),
            (self.WEATHER_API_DELAY, "WEATHER_API_DELAY", 0),
            (self.STATS_API_DELAY, "STATS_API_DELAY", 0),
            (self.MIN_GAMES_FOR_ANALYSIS, "MIN_GAMES_FOR_ANALYSIS", 1),
            (self.MIN_SAMPLE_SIZE_UMPIRE, "MIN_SAMPLE_SIZE_UMPIRE", 1),
            (self.MIN_PITCHER_STARTS, "MIN_PITCHER_STARTS", 1),
            (self.MIN_TEAM_GAMES, "MIN_TEAM_GAMES", 1),
            (self.STRONG_EDGE_THRESHOLD, "STRONG_EDGE_THRESHOLD", 0),
            (self.MODERATE_EDGE_THRESHOLD, "MODERATE_EDGE_THRESHOLD", 0),
            (self.WEATHER_IMPACT_THRESHOLD, "WEATHER_IMPACT_THRESHOLD", 0),
        ]
        
        for value, name, min_value in numeric_validations:
            if value < min_value:
                issues.append(f"{name} must be >= {min_value}")
        
        # Enhanced feature validation
        if self.ENABLE_VENUE_FACTORS and not self.ENABLE_WEATHER:
            issues.append("ENABLE_VENUE_FACTORS requires ENABLE_WEATHER to be true")
        
        # Log validation results
        if issues:
            if self.DEBUG:
                print(f"🔍 Configuration validation found {len(issues)} issues")
        else:
            if self.VERBOSE:
                print("✅ Configuration validation passed")
        
        self._validated = True
        return issues
    
    def _validate_pg_dsn(self) -> bool:
        """Validate PostgreSQL DSN format"""
        if not self.PG_DSN:
            return False
        
        # Basic format check
        if not self.PG_DSN.startswith(('postgresql://', 'postgres://')):
            return False
        
        # Check for required components
        required_parts = ['@', '/', ':']
        return all(part in self.PG_DSN for part in required_parts)
    
    def get_database_manager(self) -> 'DatabaseManager':
        """Get database manager instance"""
        if not self.PG_DSN:
            raise ConfigError("PG_DSN not configured")
        
        from .database import DatabaseManager
        return DatabaseManager(self.PG_DSN)
    
    def get_s3_manager(self):
        """Get S3 data manager instance"""
        if not self.ENABLE_S3_STORAGE:
            raise ConfigError("S3 storage not enabled")
        
        if not self.AWS_S3_BUCKET:
            raise ConfigError("AWS_S3_BUCKET not configured")
        
        from .s3_storage import S3DataManager
        return S3DataManager(self.AWS_S3_BUCKET, self.AWS_S3_PREFIX)
    
    def get_schema_manager(self):
        """Get schema migration manager"""
        from .schema_manager import SchemaMigrationManager
        db_manager = self.get_database_manager()
        return SchemaMigrationManager(db_manager, self.MIGRATIONS_DIR)
    
    def test_database_connection(self) -> tuple[bool, str]:
        """Test database connection with retry logic"""
        try:
            db_manager = self.get_database_manager()
            return db_manager.test_connection()
        except Exception as e:
            return False, f"Database manager creation failed: {e}"
    
    def test_weather_api(self) -> tuple[bool, str]:
        """Test weather API key"""
        if not self.OPENWEATHER_API_KEY:
            return False, "OPENWEATHER_API_KEY not set"
        
        try:
            import requests
            
            # Test with a simple API call
            url = "http://api.openweathermap.org/data/2.5/weather"
            params = {
                'q': 'New York',
                'appid': self.OPENWEATHER_API_KEY,
                'units': 'imperial'
            }
            
            response = requests.get(url, params=params, timeout=10)
            
            if response.status_code == 200:
                return True, "Weather API key valid"
            elif response.status_code == 401:
                return False, "Weather API key invalid or expired"
            else:
                return False, f"Weather API returned status {response.status_code}"
                
        except ImportError:
            return False, "requests library not installed"
        except Exception as e:
            return False, f"Weather API test failed: {e}"
    
    def test_s3_access(self) -> tuple[bool, str]:
        """Test S3 access"""
        if not self.ENABLE_S3_STORAGE:
            return False, "S3 storage not enabled"
        
        if not self.AWS_S3_BUCKET:
            return False, "AWS_S3_BUCKET not set"
        
        try:
            s3_manager = self.get_s3_manager()
            files = s3_manager.list_parquet_files()
            return True, f"S3 access successful ({len(files)} files found)"
        except Exception as e:
            return False, f"S3 access failed: {e}"
    
    def initialize_database(self, reset: bool = False) -> bool:
        """Initialize database with proper schema"""
        try:
            schema_manager = self.get_schema_manager()
            
            if reset:
                print("🚨 WARNING: This will delete ALL data!")
                confirm = input("Type 'DELETE ALL DATA' to confirm: ")
                if confirm == "DELETE ALL DATA":
                    schema_manager.reset_schema(confirm=True)
                else:
                    print("❌ Schema reset cancelled")
                    return False
            
            results = schema_manager.run_migrations()
            
            success_count = sum(1 for success in results.values() if success)
            total_count = len(results)
            
            print(f"📊 Migration results: {success_count}/{total_count} successful")
            
            if success_count == total_count:
                print("✅ Database schema initialized successfully")
                return True
            else:
                print("❌ Some migrations failed")
                return False
                
        except Exception as e:
            print(f"❌ Database initialization failed: {e}")
            return False
    
    def get_summary(self) -> Dict:
        """Get enhanced configuration summary for debugging"""
        return {
            "database_configured": bool(self.PG_DSN),
            "weather_configured": bool(self.OPENWEATHER_API_KEY),
            "weather_enabled": self.ENABLE_WEATHER,
            "s3_configured": bool(self.AWS_S3_BUCKET),
            "s3_enabled": self.ENABLE_S3_STORAGE,
            "venue_factors_enabled": self.ENABLE_VENUE_FACTORS,
            "recent_stats_enabled": self.ENABLE_RECENT_STATS,
            "game_info_enabled": self.ENABLE_GAME_INFO,
            "pitcher_workload_enabled": self.ENABLE_PITCHER_WORKLOAD,
            "output_dir": self.OUTPUT_DIR,
            "debug_mode": self.DEBUG,
            "validated": self._validated,
        }
    
    def print_status(self):
        """Print enhanced configuration status"""
        print("⚙️ Enhanced Configuration Status:")
        print(f"   Database: {'✅' if self.PG_DSN else '❌'} {'(RDS)' if 'rds.amazonaws.com' in self.PG_DSN else '(local)' if self.PG_DSN else '(missing)'}")
        print(f"   Weather API: {'✅' if self.OPENWEATHER_API_KEY else '❌'} {'(set)' if self.OPENWEATHER_API_KEY else '(missing)'}")
        print(f"   S3 Storage: {'✅' if self.AWS_S3_BUCKET else '❌'} {'(enabled)' if self.ENABLE_S3_STORAGE else '(disabled)' if self.AWS_S3_BUCKET else '(missing)'}")
        print(f"   Output Directory: {'✅' if Path(self.OUTPUT_DIR).exists() else '❌'} {self.OUTPUT_DIR}")
        print(f"   Debug Mode: {'🐛' if self.DEBUG else '📊'} {'ON' if self.DEBUG else 'OFF'}")
        
        # Enhanced feature status
        print(f"\n🎛️ Enhanced Features:")
        print(f"   Weather Analysis: {'✅' if self.ENABLE_WEATHER else '❌'}")
        print(f"   Venue Factors: {'✅' if self.ENABLE_VENUE_FACTORS else '❌'}")
        print(f"   Recent Stats: {'✅' if self.ENABLE_RECENT_STATS else '❌'}")
        print(f"   Game Info: {'✅' if self.ENABLE_GAME_INFO else '❌'}")
        print(f"   Pitcher Workload: {'✅' if self.ENABLE_PITCHER_WORKLOAD else '❌'}")
        print(f"   Team Form Analysis: {'✅' if self.ENABLE_TEAM_FORM_ANALYSIS else '❌'}")
        print(f"   Ballpark Adjustments: {'✅' if self.ENABLE_BALLPARK_ADJUSTMENTS else '❌'}")
        
        if self.ENABLE_S3_STORAGE:
            print(f"\n☁️ S3 Configuration:")
            print(f"   Bucket: {self.AWS_S3_BUCKET}")
            print(f"   Prefix: {self.AWS_S3_PREFIX}")
            print(f"   Auto Upload: {'✅' if self.AUTO_UPLOAD_TO_S3 else '❌'}")
            print(f"   Auto Cleanup: {'✅' if self.AUTO_CLEANUP_LOCAL else '❌'}")
    
    def get_enabled_features(self) -> List[str]:
        """Get list of enabled enhanced features"""
        features = []
        
        feature_mapping = {
            'weather_analysis': self.ENABLE_WEATHER,
            'umpire_analysis': self.ENABLE_UMPIRE_ANALYSIS,
            'venue_factors': self.ENABLE_VENUE_FACTORS,
            'recent_stats': self.ENABLE_RECENT_STATS,
            'game_info': self.ENABLE_GAME_INFO,
            'pitcher_workload': self.ENABLE_PITCHER_WORKLOAD,
            'team_form_analysis': self.ENABLE_TEAM_FORM_ANALYSIS,
            'ballpark_adjustments': self.ENABLE_BALLPARK_ADJUSTMENTS,
            's3_storage': self.ENABLE_S3_STORAGE,
        }
        
        for feature_name, enabled in feature_mapping.items():
            if enabled:
                features.append(feature_name)
        
        return features

# Global configuration instance
config = Config()

def require_config(require_weather: bool = False, require_database: bool = True) -> Config:
    """
    Validate and return configuration, exiting on errors
    
    Args:
        require_weather: Whether to require weather API
        require_database: Whether to require database connection
    
    Returns:
        Validated Config instance
    
    Raises:
        SystemExit: If validation fails
    """
    issues = config.validate(require_weather=require_weather, 
                           require_database=require_database)
    
    if issues:
        print("❌ Enhanced configuration validation failed:")
        for issue in issues:
            print(f"   • {issue}")
        
        print("\n🔧 To fix these issues:")
        if any("PG_DSN" in issue for issue in issues):
            print("   1. Set database connection:")
            print("      export PG_DSN='postgresql://user:password@rds-endpoint.region.rds.amazonaws.com:5432/db'")
        
        if any("OPENWEATHER_API_KEY" in issue for issue in issues):
            print("   2. Set weather API key:")
            print("      export OPENWEATHER_API_KEY='your_openweather_api_key'")
            print("      Get free key at: https://openweathermap.org/api")
        
        if any("AWS_S3_BUCKET" in issue for issue in issues):
            print("   3. Set S3 bucket:")
            print("      export AWS_S3_BUCKET='your-mlb-data-bucket'")
        
        if any("directory" in issue.lower() for issue in issues):
            print("   4. Create missing directories:")
            print(f"      mkdir -p {config.OUTPUT_DIR} {config.MIGRATIONS_DIR}")
        
        print("\n   5. Copy enhanced environment template:")
        print("      cp .env.enhanced_example .env")
        print("      # Then edit .env with your actual values")
        
        sys.exit(1)
    
    return config

def get_config() -> Config:
    """Get configuration instance without validation"""
    return config