"""
Streamlined configuration management for MLB ETL pipeline
REMOVED: Weather and venue factor configuration (Claude handles these)
ENHANCED: Focused on core data collection and advanced Statcast metrics
"""
import os
import sys
from typing import Optional, Dict, List
from pathlib import Path

class ConfigError(Exception):
    """Custom exception for configuration errors"""
    pass

class Config:
    """Streamlined configuration management focused on core data collection"""
    
    def __init__(self):
        self._validated = False
        self._load_config()
    
    def _load_config(self):
        """Load and set all configuration values"""
        
        # Database Configuration (REQUIRED)
        self.PG_DSN = os.getenv("PG_DSN", "")
        
        # API Keys (FOR REAL DATA MODE)
        self.MLB_API_KEY = os.getenv("MLB_API_KEY", "")  # Future use
        
        # AWS Configuration
        self.AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET", "")
        self.AWS_S3_PREFIX = os.getenv("AWS_S3_PREFIX", "mlb-data")
        self.AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        self.ENABLE_S3_STORAGE = self._str_to_bool(os.getenv("ENABLE_S3_STORAGE", "false"))
        self.AUTO_UPLOAD_TO_S3 = self._str_to_bool(os.getenv("AUTO_UPLOAD_TO_S3", "false"))
        self.AUTO_CLEANUP_LOCAL = self._str_to_bool(os.getenv("AUTO_CLEANUP_LOCAL", "false"))
        
        # PLACEHOLDER MODE (Key for testing)
        self.USE_PLACEHOLDER_DATA = self._str_to_bool(os.getenv("USE_PLACEHOLDER_DATA", "true"))
        self.PLACEHOLDER_GAMES_PER_DAY = int(os.getenv("PLACEHOLDER_GAMES_PER_DAY", "12"))
        
        # Directory Paths
        self.OUTPUT_DIR = os.getenv("OUTPUT_DIR", "stage")
        self.MIGRATIONS_DIR = os.getenv("MIGRATIONS_DIR", "migrations")
        self.LOG_DIR = os.getenv("LOG_DIR", "logs")
        
        # Enhanced Rate Limiting Settings
        self.MLB_API_DELAY = float(os.getenv("MLB_API_DELAY", "0.5"))
        self.STATS_API_DELAY = float(os.getenv("STATS_API_DELAY", "0.5"))
        
        # Enhanced Data Quality Thresholds
        self.MIN_GAMES_FOR_ANALYSIS = int(os.getenv("MIN_GAMES_FOR_ANALYSIS", "5"))
        self.MIN_SAMPLE_SIZE_UMPIRE = int(os.getenv("MIN_SAMPLE_SIZE_UMPIRE", "15"))
        self.MIN_PITCHER_STARTS = int(os.getenv("MIN_PITCHER_STARTS", "3"))
        self.MIN_TEAM_GAMES = int(os.getenv("MIN_TEAM_GAMES", "7"))
        
        # Enhanced Betting Analysis Thresholds
        self.STRONG_EDGE_THRESHOLD = float(os.getenv("STRONG_EDGE_THRESHOLD", "0.12"))
        self.MODERATE_EDGE_THRESHOLD = float(os.getenv("MODERATE_EDGE_THRESHOLD", "0.06"))
        
        # STREAMLINED: Core feature flags (true/false)
        self.ENABLE_UMPIRE_ANALYSIS = self._str_to_bool(os.getenv("ENABLE_UMPIRE_ANALYSIS", "true"))
        self.ENABLE_RECENT_STATS = self._str_to_bool(os.getenv("ENABLE_RECENT_STATS", "true"))
        self.ENABLE_GAME_INFO = self._str_to_bool(os.getenv("ENABLE_GAME_INFO", "true"))
        self.ENABLE_PITCHER_WORKLOAD = self._str_to_bool(os.getenv("ENABLE_PITCHER_WORKLOAD", "true"))
        self.ENABLE_TEAM_FORM_ANALYSIS = self._str_to_bool(os.getenv("ENABLE_TEAM_FORM_ANALYSIS", "true"))
        
        # ENHANCED: Advanced Statcast features
        self.ENABLE_ADVANCED_STATCAST = self._str_to_bool(os.getenv("ENABLE_ADVANCED_STATCAST", "true"))
        self.ENABLE_EXPECTED_STATS = self._str_to_bool(os.getenv("ENABLE_EXPECTED_STATS", "true"))
        self.ENABLE_BARREL_METRICS = self._str_to_bool(os.getenv("ENABLE_BARREL_METRICS", "true"))
        self.ENABLE_PITCH_MOVEMENT = self._str_to_bool(os.getenv("ENABLE_PITCH_MOVEMENT", "true"))
        
        # Enhanced Logging
        self.LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
        self.LOG_TO_FILE = self._str_to_bool(os.getenv("LOG_TO_FILE", "true"))
        self.PERFORMANCE_MONITORING = self._str_to_bool(os.getenv("PERFORMANCE_MONITORING", "false"))
        
        # Debug/Development
        self.DEBUG = self._str_to_bool(os.getenv("DEBUG", "false"))
        self.VERBOSE = self._str_to_bool(os.getenv("VERBOSE", "false"))
        self.DRY_RUN = self._str_to_bool(os.getenv("DRY_RUN", "false"))
        
        # API Error Handling
        self.API_RETRY_COUNT = int(os.getenv("API_RETRY_COUNT", "3"))
        self.API_TIMEOUT_SECONDS = float(os.getenv("API_TIMEOUT_SECONDS", "30.0"))
        self.GRACEFUL_API_DEGRADATION = self._str_to_bool(os.getenv("GRACEFUL_API_DEGRADATION", "true"))
    
    def _str_to_bool(self, value: str) -> bool:
        """Convert string environment variable to boolean"""
        if value is None or value == "":
            return False
        return str(value).lower() in ('true', '1', 'yes', 'on', 'enabled')
    
    def validate(self, require_database: bool = True) -> List[str]:
        """
        Validate streamlined configuration and return list of issues
        
        Args:
            require_database: Whether database connection is required
        """
        issues = []
        
        # Database validation
        if require_database and not self.PG_DSN:
            issues.append("PG_DSN environment variable is required but not set")
        
        if self.PG_DSN and not self._validate_pg_dsn():
            issues.append("PG_DSN format appears invalid (should be postgresql://user:pass@host:port/db)")
        
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
            (self.STATS_API_DELAY, "STATS_API_DELAY", 0),
            (self.MIN_GAMES_FOR_ANALYSIS, "MIN_GAMES_FOR_ANALYSIS", 1),
            (self.MIN_SAMPLE_SIZE_UMPIRE, "MIN_SAMPLE_SIZE_UMPIRE", 1),
            (self.MIN_PITCHER_STARTS, "MIN_PITCHER_STARTS", 1),
            (self.MIN_TEAM_GAMES, "MIN_TEAM_GAMES", 1),
            (self.STRONG_EDGE_THRESHOLD, "STRONG_EDGE_THRESHOLD", 0),
            (self.MODERATE_EDGE_THRESHOLD, "MODERATE_EDGE_THRESHOLD", 0),
            (self.PLACEHOLDER_GAMES_PER_DAY, "PLACEHOLDER_GAMES_PER_DAY", 1),
        ]
        
        for value, name, min_value in numeric_validations:
            if value < min_value:
                issues.append(f"{name} must be >= {min_value}")
        
        # Placeholder mode validation
        if self.USE_PLACEHOLDER_DATA and self.DEBUG:
            print(f"🔧 PLACEHOLDER MODE: Using generated test data for all collections")
        
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
        """Get database manager instance with error handling"""
        if not self.PG_DSN:
            raise ConfigError("PG_DSN not configured")
        
        try:
            from .database import DatabaseManager
            return DatabaseManager(self.PG_DSN)
        except ImportError as e:
            if self.GRACEFUL_API_DEGRADATION:
                print(f"⚠️ Database manager not available: {e}")
                return None
            else:
                raise ConfigError(f"Cannot import database manager: {e}")
    
    def get_s3_manager(self):
        """Get S3 data manager instance with error handling"""
        if not self.ENABLE_S3_STORAGE:
            raise ConfigError("S3 storage not enabled")
        
        if not self.AWS_S3_BUCKET:
            raise ConfigError("AWS_S3_BUCKET not configured")
        
        try:
            from .s3_storage import S3DataManager
            return S3DataManager(self.AWS_S3_BUCKET, self.AWS_S3_PREFIX)
        except ImportError as e:
            if self.GRACEFUL_API_DEGRADATION:
                print(f"⚠️ S3 manager not available: {e}")
                return None
            else:
                raise ConfigError(f"Cannot import S3 manager: {e}")
    
    def get_schema_manager(self):
        """Get schema migration manager with error handling"""
        try:
            from .schema_manager import SchemaMigrationManager
            db_manager = self.get_database_manager()
            if db_manager is None:
                return None
            return SchemaMigrationManager(db_manager, self.MIGRATIONS_DIR)
        except Exception as e:
            if self.GRACEFUL_API_DEGRADATION:
                print(f"⚠️ Schema manager not available: {e}")
                return None
            else:
                raise ConfigError(f"Cannot create schema manager: {e}")
    
    def test_database_connection(self) -> tuple[bool, str]:
        """Test database connection with retry logic and graceful degradation"""
        try:
            db_manager = self.get_database_manager()
            if db_manager is None:
                return False, "Database manager not available"
            return db_manager.test_connection()
        except Exception as e:
            return False, f"Database manager creation failed: {e}"
    
    def test_s3_access(self) -> tuple[bool, str]:
        """Test S3 access with graceful degradation"""
        if not self.ENABLE_S3_STORAGE:
            return False, "S3 storage not enabled"
        
        if not self.AWS_S3_BUCKET:
            return False, "AWS_S3_BUCKET not set"
        
        try:
            s3_manager = self.get_s3_manager()
            if s3_manager is None:
                return False, "S3 manager not available"
            files = s3_manager.list_parquet_files()
            return True, f"S3 access successful ({len(files)} files found)"
        except Exception as e:
            return False, f"S3 access failed: {e}"
    
    def test_pybaseball_import(self) -> tuple[bool, str]:
        """Test if pybaseball is available for real data collection"""
        if self.USE_PLACEHOLDER_DATA:
            return True, "Using placeholder data (pybaseball not needed)"
        
        try:
            import pybaseball
            return True, "pybaseball available for real Statcast data"
        except ImportError:
            if self.GRACEFUL_API_DEGRADATION:
                return False, "pybaseball not available (will use placeholder data)"
            else:
                return False, "pybaseball not installed - required for real data collection"
    
    def initialize_database(self, reset: bool = False) -> bool:
        """Initialize database with proper schema and graceful error handling"""
        try:
            schema_manager = self.get_schema_manager()
            if schema_manager is None:
                print("❌ Cannot initialize database - schema manager not available")
                return False
            
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
            if self.GRACEFUL_API_DEGRADATION:
                print("💡 Continuing without database initialization...")
                return False
            else:
                raise
    
    def get_summary(self) -> Dict:
        """Get streamlined configuration summary for debugging"""
        return {
            "database_configured": bool(self.PG_DSN),
            "s3_configured": bool(self.AWS_S3_BUCKET),
            "s3_enabled": self.ENABLE_S3_STORAGE,
            "recent_stats_enabled": self.ENABLE_RECENT_STATS,
            "game_info_enabled": self.ENABLE_GAME_INFO,
            "pitcher_workload_enabled": self.ENABLE_PITCHER_WORKLOAD,
            "advanced_statcast_enabled": self.ENABLE_ADVANCED_STATCAST,
            "expected_stats_enabled": self.ENABLE_EXPECTED_STATS,
            "barrel_metrics_enabled": self.ENABLE_BARREL_METRICS,
            "use_placeholder_data": self.USE_PLACEHOLDER_DATA,
            "output_dir": self.OUTPUT_DIR,
            "debug_mode": self.DEBUG,
            "validated": self._validated,
            "graceful_degradation": self.GRACEFUL_API_DEGRADATION,
        }
    
    def print_status(self):
        """Print streamlined configuration status"""
        mode = "PLACEHOLDER" if self.USE_PLACEHOLDER_DATA else "REAL DATA"
        print(f"⚙️ Streamlined Configuration Status ({mode} MODE):")
        print(f"   Database: {'✅' if self.PG_DSN else '❌'} {'(RDS)' if 'rds.amazonaws.com' in self.PG_DSN else '(local)' if self.PG_DSN else '(missing)'}")
        
        if self.USE_PLACEHOLDER_DATA:
            print(f"   Data Mode: 🔧 Using placeholder data (no API keys needed)")
        else:
            print(f"   Data Mode: 📡 Using real API calls")
        
        print(f"   S3 Storage: {'✅' if self.AWS_S3_BUCKET else '❌'} {'(enabled)' if self.ENABLE_S3_STORAGE else '(disabled)' if self.AWS_S3_BUCKET else '(missing)'}")
        print(f"   Output Directory: {'✅' if Path(self.OUTPUT_DIR).exists() else '❌'} {self.OUTPUT_DIR}")
        print(f"   Debug Mode: {'🐛' if self.DEBUG else '📊'} {'ON' if self.DEBUG else 'OFF'}")
        
        # Streamlined features status
        print(f"\n🎛️ Core Features:")
        print(f"   Umpire Analysis: {'✅' if self.ENABLE_UMPIRE_ANALYSIS else '❌'}")
        print(f"   Recent Stats: {'✅' if self.ENABLE_RECENT_STATS else '❌'}")
        print(f"   Game Info: {'✅' if self.ENABLE_GAME_INFO else '❌'}")
        print(f"   Pitcher Workload: {'✅' if self.ENABLE_PITCHER_WORKLOAD else '❌'}")
        print(f"   Team Form Analysis: {'✅' if self.ENABLE_TEAM_FORM_ANALYSIS else '❌'}")
        
        # Enhanced Statcast features
        print(f"\n✨ Advanced Statcast Features:")
        print(f"   Advanced Statcast: {'✅' if self.ENABLE_ADVANCED_STATCAST else '❌'}")
        print(f"   Expected Stats (xBA, xwOBA): {'✅' if self.ENABLE_EXPECTED_STATS else '❌'}")
        print(f"   Barrel Metrics: {'✅' if self.ENABLE_BARREL_METRICS else '❌'}")
        print(f"   Pitch Movement Data: {'✅' if self.ENABLE_PITCH_MOVEMENT else '❌'}")
        
        # Placeholder mode details
        if self.USE_PLACEHOLDER_DATA:
            print(f"\n🔧 Placeholder Mode Settings:")
            print(f"   Games per day: {self.PLACEHOLDER_GAMES_PER_DAY}")
            print(f"   API degradation: {'✅' if self.GRACEFUL_API_DEGRADATION else '❌'}")
            print(f"   💡 To use real data: set USE_PLACEHOLDER_DATA=false")
        
        if self.ENABLE_S3_STORAGE:
            print(f"\n☁️ S3 Configuration:")
            print(f"   Bucket: {self.AWS_S3_BUCKET}")
            print(f"   Prefix: {self.AWS_S3_PREFIX}")
            print(f"   Auto Upload: {'✅' if self.AUTO_UPLOAD_TO_S3 else '❌'}")
            print(f"   Auto Cleanup: {'✅' if self.AUTO_CLEANUP_LOCAL else '❌'}")
        
        print(f"\n🗑️ REMOVED: Weather & venue factors (Claude handles these)")
        print(f"✨ ENHANCED: All advanced Statcast metrics included")
    
    def get_enabled_features(self) -> List[str]:
        """Get list of enabled streamlined features"""
        features = []
        
        feature_mapping = {
            'umpire_analysis': self.ENABLE_UMPIRE_ANALYSIS,
            'recent_stats': self.ENABLE_RECENT_STATS,
            'game_info': self.ENABLE_GAME_INFO,
            'pitcher_workload': self.ENABLE_PITCHER_WORKLOAD,
            'team_form_analysis': self.ENABLE_TEAM_FORM_ANALYSIS,
            'advanced_statcast': self.ENABLE_ADVANCED_STATCAST,
            'expected_stats': self.ENABLE_EXPECTED_STATS,
            'barrel_metrics': self.ENABLE_BARREL_METRICS,
            'pitch_movement': self.ENABLE_PITCH_MOVEMENT,
            's3_storage': self.ENABLE_S3_STORAGE,
            'placeholder_mode': self.USE_PLACEHOLDER_DATA,
        }
        
        for feature_name, enabled in feature_mapping.items():
            if enabled:
                features.append(feature_name)
        
        return features

# Global configuration instance
config = Config()

def require_config(require_database: bool = True, graceful_degradation: bool = True) -> Config:
    """
    STREAMLINED: Configuration validation focused on core requirements
    
    Args:
        require_database: Whether to require database connection
        graceful_degradation: If True, warn about missing features instead of exiting
    
    Returns:
        Config instance (may have some features disabled)
    
    Raises:
        SystemExit: Only if critical requirements are missing and graceful_degradation=False
    """
    issues = config.validate(require_database=require_database)
    
    if not issues:
        return config
    
    # Separate critical vs non-critical issues
    critical_issues = []
    warning_issues = []
    
    for issue in issues:
        if require_database and "PG_DSN" in issue and "not set" in issue:
            critical_issues.append(issue)
        else:
            warning_issues.append(issue)
    
    # Show warnings for non-critical issues
    if warning_issues and graceful_degradation:
        print("⚠️ Configuration warnings (features will be limited):")
        for issue in warning_issues:
            print(f"   • {issue}")
        
        if any("S3" in issue for issue in warning_issues):
            config.ENABLE_S3_STORAGE = False
            print("   → S3 storage disabled")
    
    # Only exit for critical issues
    if critical_issues:
        print("❌ Critical configuration errors:")
        for issue in critical_issues:
            print(f"   • {issue}")
        
        print("\n🔧 Quick fixes:")
        if any("PG_DSN" in issue for issue in critical_issues):
            print("   1. Set database: export PG_DSN='postgresql://user:pass@host:5432/db'")
        
        print("   2. Or run: python setup_env.py")
        
        if not graceful_degradation:
            sys.exit(1)
        else:
            print("\n⚠️ Continuing with limited functionality...")
            print("💡 Consider enabling placeholder mode: USE_PLACEHOLDER_DATA=true")
    
    return config

# Helper functions for different use cases
def get_config_for_analysis() -> Config:
    """Get config optimized for analysis (database required)"""
    return require_config(require_database=True, graceful_degradation=True)

def get_config_for_setup() -> Config:
    """Get config for setup/initialization (more lenient)"""
    return require_config(require_database=False, graceful_degradation=True)

def get_config() -> Config:
    """Get configuration instance without strict validation"""
    return config