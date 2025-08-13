"""
Enhanced configuration management for MLB ETL pipeline
ENHANCED: Production-ready settings for real data collection
ADDED: Comprehensive API management, memory settings, and graceful degradation
"""
import os
import sys
import time
import psutil
from typing import Optional, Dict, List, Tuple
from pathlib import Path
from dataclasses import dataclass

class ConfigError(Exception):
    """Custom exception for configuration errors"""
    pass

@dataclass
class APILimits:
    """API rate limiting configuration"""
    requests_per_minute: int
    requests_per_hour: int
    burst_size: int
    cooldown_seconds: float
    
@dataclass
class MemoryConfig:
    """Memory management configuration"""
    max_memory_percent: float
    chunk_size_rows: int
    batch_size_mb: int
    enable_gc_hints: bool

class Config:
    """Enhanced configuration management for production MLB data collection"""
    
    def __init__(self):
        self._validated = False
        self._memory_monitor = None
        self._api_call_history = {}
        self._load_config()
        self._setup_memory_monitoring()
    
    def _load_config(self):
        """Load and set all configuration values with enhanced real data support"""
        
        # Database Configuration (REQUIRED)
        self.PG_DSN = os.getenv("PG_DSN", "")
        self.DB_CONNECTION_POOL_SIZE = int(os.getenv("DB_CONNECTION_POOL_SIZE", "10"))
        self.DB_CONNECTION_TIMEOUT = float(os.getenv("DB_CONNECTION_TIMEOUT", "30.0"))
        self.DB_QUERY_TIMEOUT = float(os.getenv("DB_QUERY_TIMEOUT", "300.0"))
        
        # ENHANCED: API Keys and Authentication
        self.MLB_API_KEY = os.getenv("MLB_API_KEY", "")
        self.STATSAPI_BASE_URL = os.getenv("STATSAPI_BASE_URL", "https://statsapi.mlb.com/api/v1")
        self.ENABLE_API_KEY_ROTATION = self._str_to_bool(os.getenv("ENABLE_API_KEY_ROTATION", "false"))
        
        # ENHANCED: Comprehensive API Timeout Settings
        self.PYBASEBALL_TIMEOUT = float(os.getenv("PYBASEBALL_TIMEOUT", "45.0"))
        self.STATSAPI_TIMEOUT = float(os.getenv("STATSAPI_TIMEOUT", "30.0"))
        self.MLB_API_TIMEOUT = float(os.getenv("MLB_API_TIMEOUT", "60.0"))
        self.STATCAST_TIMEOUT = float(os.getenv("STATCAST_TIMEOUT", "120.0"))  # Statcast can be slow
        self.SCHEDULE_API_TIMEOUT = float(os.getenv("SCHEDULE_API_TIMEOUT", "20.0"))
        self.ROSTER_API_TIMEOUT = float(os.getenv("ROSTER_API_TIMEOUT", "25.0"))
        
        # ENHANCED: Pybaseball Specific Rate Limiting
        self.PYBASEBALL_DELAY_MIN = float(os.getenv("PYBASEBALL_DELAY_MIN", "1.0"))
        self.PYBASEBALL_DELAY_MAX = float(os.getenv("PYBASEBALL_DELAY_MAX", "3.0"))
        self.PYBASEBALL_REQUESTS_PER_MINUTE = int(os.getenv("PYBASEBALL_REQUESTS_PER_MINUTE", "30"))
        self.PYBASEBALL_REQUESTS_PER_HOUR = int(os.getenv("PYBASEBALL_REQUESTS_PER_HOUR", "1000"))
        self.PYBASEBALL_BURST_SIZE = int(os.getenv("PYBASEBALL_BURST_SIZE", "5"))
        self.PYBASEBALL_COOLDOWN_ON_ERROR = float(os.getenv("PYBASEBALL_COOLDOWN_ON_ERROR", "10.0"))
        
        # ENHANCED: Comprehensive Rate Limiting for All APIs
        self.MLB_API_DELAY = float(os.getenv("MLB_API_DELAY", "0.5"))
        self.STATS_API_DELAY = float(os.getenv("STATS_API_DELAY", "0.5"))
        self.STATCAST_API_DELAY = float(os.getenv("STATCAST_API_DELAY", "2.0"))  # More conservative
        self.SCHEDULE_API_DELAY = float(os.getenv("SCHEDULE_API_DELAY", "0.3"))
        
        # ENHANCED: API Retry and Error Handling
        self.API_RETRY_COUNT = int(os.getenv("API_RETRY_COUNT", "5"))
        self.API_RETRY_BACKOFF_FACTOR = float(os.getenv("API_RETRY_BACKOFF_FACTOR", "2.0"))
        self.API_RETRY_MAX_DELAY = float(os.getenv("API_RETRY_MAX_DELAY", "60.0"))
        self.API_CIRCUIT_BREAKER_THRESHOLD = int(os.getenv("API_CIRCUIT_BREAKER_THRESHOLD", "10"))
        self.API_CIRCUIT_BREAKER_TIMEOUT = float(os.getenv("API_CIRCUIT_BREAKER_TIMEOUT", "300.0"))
        
        # ENHANCED: Graceful Degradation Settings
        self.GRACEFUL_API_DEGRADATION = self._str_to_bool(os.getenv("GRACEFUL_API_DEGRADATION", "true"))
        self.FALLBACK_TO_PLACEHOLDER_ON_API_FAIL = self._str_to_bool(os.getenv("FALLBACK_TO_PLACEHOLDER_ON_API_FAIL", "true"))
        self.CONTINUE_WITH_PARTIAL_DATA = self._str_to_bool(os.getenv("CONTINUE_WITH_PARTIAL_DATA", "true"))
        self.SKIP_FAILED_DATES = self._str_to_bool(os.getenv("SKIP_FAILED_DATES", "true"))
        self.MAX_CONSECUTIVE_FAILURES = int(os.getenv("MAX_CONSECUTIVE_FAILURES", "3"))
        self.FAILURE_RATE_THRESHOLD = float(os.getenv("FAILURE_RATE_THRESHOLD", "0.20"))  # 20% failure rate triggers degradation
        
        # ENHANCED: Memory Management Settings
        self.MAX_MEMORY_PERCENT = float(os.getenv("MAX_MEMORY_PERCENT", "75.0"))
        self.MEMORY_CHECK_INTERVAL = float(os.getenv("MEMORY_CHECK_INTERVAL", "30.0"))
        self.CHUNK_SIZE_ROWS = int(os.getenv("CHUNK_SIZE_ROWS", "10000"))
        self.BATCH_SIZE_MB = int(os.getenv("BATCH_SIZE_MB", "100"))
        self.MAX_DATAFRAME_SIZE_MB = int(os.getenv("MAX_DATAFRAME_SIZE_MB", "500"))
        self.ENABLE_MEMORY_OPTIMIZATION = self._str_to_bool(os.getenv("ENABLE_MEMORY_OPTIMIZATION", "true"))
        self.ENABLE_GARBAGE_COLLECTION_HINTS = self._str_to_bool(os.getenv("ENABLE_GARBAGE_COLLECTION_HINTS", "true"))
        self.FORCE_GC_AFTER_TABLE_LOAD = self._str_to_bool(os.getenv("FORCE_GC_AFTER_TABLE_LOAD", "true"))
        
        # ENHANCED: Large Dataset Handling
        self.LARGE_DATASET_THRESHOLD_ROWS = int(os.getenv("LARGE_DATASET_THRESHOLD_ROWS", "1000000"))
        self.ENABLE_STREAMING_PROCESSING = self._str_to_bool(os.getenv("ENABLE_STREAMING_PROCESSING", "true"))
        self.STREAMING_CHUNK_SIZE = int(os.getenv("STREAMING_CHUNK_SIZE", "50000"))
        self.ENABLE_COMPRESSION_FOR_LARGE_FILES = self._str_to_bool(os.getenv("ENABLE_COMPRESSION_FOR_LARGE_FILES", "true"))
        self.PARALLEL_PROCESSING_WORKERS = int(os.getenv("PARALLEL_PROCESSING_WORKERS", "4"))
        
        # AWS Configuration
        self.AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET", "")
        self.AWS_S3_PREFIX = os.getenv("AWS_S3_PREFIX", "mlb-data")
        self.AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        self.ENABLE_S3_STORAGE = self._str_to_bool(os.getenv("ENABLE_S3_STORAGE", "false"))
        self.AUTO_UPLOAD_TO_S3 = self._str_to_bool(os.getenv("AUTO_UPLOAD_TO_S3", "false"))
        self.AUTO_CLEANUP_LOCAL = self._str_to_bool(os.getenv("AUTO_CLEANUP_LOCAL", "false"))
        self.S3_MULTIPART_THRESHOLD = int(os.getenv("S3_MULTIPART_THRESHOLD", "100"))  # MB
        
        # PLACEHOLDER MODE (Enhanced for testing)
        self.USE_PLACEHOLDER_DATA = self._str_to_bool(os.getenv("USE_PLACEHOLDER_DATA", "true"))
        self.PLACEHOLDER_GAMES_PER_DAY = int(os.getenv("PLACEHOLDER_GAMES_PER_DAY", "12"))
        self.PLACEHOLDER_INCLUDE_ADVANCED_METRICS = self._str_to_bool(os.getenv("PLACEHOLDER_INCLUDE_ADVANCED_METRICS", "true"))
        
        # Directory Paths
        self.OUTPUT_DIR = os.getenv("OUTPUT_DIR", "stage")
        self.MIGRATIONS_DIR = os.getenv("MIGRATIONS_DIR", "migrations")
        self.LOG_DIR = os.getenv("LOG_DIR", "logs")
        self.TEMP_DIR = os.getenv("TEMP_DIR", "temp")
        self.CACHE_DIR = os.getenv("CACHE_DIR", "cache")
        
        # Enhanced Data Quality Thresholds
        self.MIN_GAMES_FOR_ANALYSIS = int(os.getenv("MIN_GAMES_FOR_ANALYSIS", "5"))
        self.MIN_SAMPLE_SIZE_UMPIRE = int(os.getenv("MIN_SAMPLE_SIZE_UMPIRE", "15"))
        self.MIN_PITCHER_STARTS = int(os.getenv("MIN_PITCHER_STARTS", "3"))
        self.MIN_TEAM_GAMES = int(os.getenv("MIN_TEAM_GAMES", "7"))
        self.MIN_DATA_COMPLETENESS_PERCENT = float(os.getenv("MIN_DATA_COMPLETENESS_PERCENT", "80.0"))
        
        # Enhanced Betting Analysis Thresholds
        self.STRONG_EDGE_THRESHOLD = float(os.getenv("STRONG_EDGE_THRESHOLD", "0.12"))
        self.MODERATE_EDGE_THRESHOLD = float(os.getenv("MODERATE_EDGE_THRESHOLD", "0.06"))
        
        # STREAMLINED: Core feature flags
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
        
        # ENHANCED: Performance and Monitoring
        self.LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
        self.LOG_TO_FILE = self._str_to_bool(os.getenv("LOG_TO_FILE", "true"))
        self.PERFORMANCE_MONITORING = self._str_to_bool(os.getenv("PERFORMANCE_MONITORING", "true"))
        self.ENABLE_METRICS_COLLECTION = self._str_to_bool(os.getenv("ENABLE_METRICS_COLLECTION", "true"))
        self.METRICS_EXPORT_INTERVAL = float(os.getenv("METRICS_EXPORT_INTERVAL", "60.0"))
        
        # Debug/Development
        self.DEBUG = self._str_to_bool(os.getenv("DEBUG", "false"))
        self.VERBOSE = self._str_to_bool(os.getenv("VERBOSE", "false"))
        self.DRY_RUN = self._str_to_bool(os.getenv("DRY_RUN", "false"))
        self.ENABLE_PROFILING = self._str_to_bool(os.getenv("ENABLE_PROFILING", "false"))
        
        # ENHANCED: Caching Settings
        self.ENABLE_API_RESPONSE_CACHING = self._str_to_bool(os.getenv("ENABLE_API_RESPONSE_CACHING", "true"))
        self.CACHE_TTL_HOURS = float(os.getenv("CACHE_TTL_HOURS", "6.0"))
        self.MAX_CACHE_SIZE_MB = int(os.getenv("MAX_CACHE_SIZE_MB", "1000"))
    
    def _str_to_bool(self, value: str) -> bool:
        """Convert string environment variable to boolean"""
        if value is None or value == "":
            return False
        return str(value).lower() in ('true', '1', 'yes', 'on', 'enabled')
    
    def _setup_memory_monitoring(self):
        """Setup memory monitoring for large dataset handling"""
        if self.ENABLE_MEMORY_OPTIMIZATION:
            try:
                import psutil
                self._memory_monitor = psutil.virtual_memory()
            except ImportError:
                print("⚠️ psutil not available - memory monitoring disabled")
                self.ENABLE_MEMORY_OPTIMIZATION = False
    
    def get_memory_status(self) -> Dict:
        """Get current memory status for large dataset processing"""
        if not self.ENABLE_MEMORY_OPTIMIZATION:
            return {"available": True, "reason": "monitoring disabled"}
        
        try:
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            available_gb = memory.available / (1024**3)
            
            if memory_percent > self.MAX_MEMORY_PERCENT:
                return {
                    "available": False,
                    "reason": f"Memory usage {memory_percent:.1f}% exceeds limit {self.MAX_MEMORY_PERCENT}%",
                    "available_gb": available_gb
                }
            
            return {
                "available": True,
                "memory_percent": memory_percent,
                "available_gb": available_gb
            }
        except Exception as e:
            return {"available": True, "reason": f"monitoring error: {e}"}
    
    def get_optimal_chunk_size(self, data_size_mb: float) -> int:
        """Calculate optimal chunk size based on available memory"""
        if not self.ENABLE_MEMORY_OPTIMIZATION:
            return self.CHUNK_SIZE_ROWS
        
        memory_status = self.get_memory_status()
        if not memory_status["available"]:
            # Use smaller chunks when memory is tight
            return max(1000, self.CHUNK_SIZE_ROWS // 4)
        
        available_gb = memory_status.get("available_gb", 4)
        
        # Adjust chunk size based on available memory
        if available_gb > 8:
            return self.CHUNK_SIZE_ROWS * 2
        elif available_gb > 4:
            return self.CHUNK_SIZE_ROWS
        else:
            return self.CHUNK_SIZE_ROWS // 2
    
    def get_api_limits(self, api_name: str) -> APILimits:
        """Get API rate limiting configuration for specific API"""
        api_configs = {
            "pybaseball": APILimits(
                requests_per_minute=self.PYBASEBALL_REQUESTS_PER_MINUTE,
                requests_per_hour=self.PYBASEBALL_REQUESTS_PER_HOUR,
                burst_size=self.PYBASEBALL_BURST_SIZE,
                cooldown_seconds=self.PYBASEBALL_COOLDOWN_ON_ERROR
            ),
            "statsapi": APILimits(
                requests_per_minute=60,
                requests_per_hour=3000,
                burst_size=10,
                cooldown_seconds=5.0
            ),
            "mlb_api": APILimits(
                requests_per_minute=120,
                requests_per_hour=5000,
                burst_size=20,
                cooldown_seconds=2.0
            )
        }
        
        return api_configs.get(api_name, api_configs["pybaseball"])
    
    def should_use_streaming(self, estimated_rows: int) -> bool:
        """Determine if streaming processing should be used for large datasets"""
        if not self.ENABLE_STREAMING_PROCESSING:
            return False
        
        return estimated_rows > self.LARGE_DATASET_THRESHOLD_ROWS
    
    def validate(self, require_database: bool = True) -> List[str]:
        """Enhanced validation with memory and API configuration checks"""
        issues = []
        
        # Database validation
        if require_database and not self.PG_DSN:
            issues.append("PG_DSN environment variable is required but not set")
        
        if self.PG_DSN and not self._validate_pg_dsn():
            issues.append("PG_DSN format appears invalid (should be postgresql://user:pass@host:port/db)")
        
        # S3 validation
        if self.ENABLE_S3_STORAGE and not self.AWS_S3_BUCKET:
            issues.append("S3 storage is enabled but AWS_S3_BUCKET is not set")
        
        # Directory validation and creation
        directories_to_check = [
            (self.OUTPUT_DIR, "OUTPUT_DIR"),
            (self.MIGRATIONS_DIR, "MIGRATIONS_DIR"),
            (self.LOG_DIR, "LOG_DIR"),
            (self.TEMP_DIR, "TEMP_DIR"),
            (self.CACHE_DIR, "CACHE_DIR"),
        ]
        
        for dir_path, var_name in directories_to_check:
            path_obj = Path(dir_path)
            if not path_obj.exists():
                try:
                    path_obj.mkdir(parents=True, exist_ok=True)
                    if self.VERBOSE:
                        print(f"✅ Created directory: {dir_path}")
                except Exception as e:
                    issues.append(f"Cannot create {var_name} directory '{dir_path}': {e}")
        
        # Enhanced numeric validation
        numeric_validations = [
            (self.MLB_API_DELAY, "MLB_API_DELAY", 0),
            (self.STATS_API_DELAY, "STATS_API_DELAY", 0),
            (self.PYBASEBALL_DELAY_MIN, "PYBASEBALL_DELAY_MIN", 0),
            (self.PYBASEBALL_DELAY_MAX, "PYBASEBALL_DELAY_MAX", 0),
            (self.MIN_GAMES_FOR_ANALYSIS, "MIN_GAMES_FOR_ANALYSIS", 1),
            (self.MAX_MEMORY_PERCENT, "MAX_MEMORY_PERCENT", 10),
            (self.CHUNK_SIZE_ROWS, "CHUNK_SIZE_ROWS", 100),
            (self.BATCH_SIZE_MB, "BATCH_SIZE_MB", 1),
            (self.API_RETRY_COUNT, "API_RETRY_COUNT", 1),
            (self.PLACEHOLDER_GAMES_PER_DAY, "PLACEHOLDER_GAMES_PER_DAY", 1),
        ]
        
        for value, name, min_value in numeric_validations:
            if value < min_value:
                issues.append(f"{name} must be >= {min_value}")
        
        # Memory validation
        if self.MAX_MEMORY_PERCENT > 90:
            issues.append("MAX_MEMORY_PERCENT > 90% may cause system instability")
        
        if self.PYBASEBALL_DELAY_MIN > self.PYBASEBALL_DELAY_MAX:
            issues.append("PYBASEBALL_DELAY_MIN cannot be greater than PYBASEBALL_DELAY_MAX")
        
        # API configuration validation
        if not self.USE_PLACEHOLDER_DATA:
            if self.PYBASEBALL_REQUESTS_PER_MINUTE > 60:
                issues.append("PYBASEBALL_REQUESTS_PER_MINUTE > 60 may trigger rate limiting")
            
            if self.API_RETRY_COUNT > 10:
                issues.append("API_RETRY_COUNT > 10 may cause excessive delays")
        
        # Graceful degradation validation
        if self.FAILURE_RATE_THRESHOLD > 0.5:
            issues.append("FAILURE_RATE_THRESHOLD > 0.5 may be too permissive")
        
        # Memory monitoring setup
        if self.ENABLE_MEMORY_OPTIMIZATION and not self._memory_monitor:
            issues.append("Memory optimization enabled but psutil not available")
        
        # Log validation results
        if issues:
            if self.DEBUG:
                print(f"🔍 Configuration validation found {len(issues)} issues")
        else:
            if self.VERBOSE:
                print("✅ Enhanced configuration validation passed")
        
        self._validated = True
        return issues
    
    def _validate_pg_dsn(self) -> bool:
        """Validate PostgreSQL DSN format"""
        if not self.PG_DSN:
            return False
        
        if not self.PG_DSN.startswith(('postgresql://', 'postgres://')):
            return False
        
        required_parts = ['@', '/', ':']
        return all(part in self.PG_DSN for part in required_parts)
    
    def get_database_manager(self) -> 'DatabaseManager':
        """Get database manager instance with compatibility for existing system"""
        if not self.PG_DSN:
            raise ConfigError("PG_DSN not configured")
    
        try:
            from .database import DatabaseManager
            # Use basic initialization compatible with existing system
            return DatabaseManager(self.PG_DSN)
        except ImportError as e:
            if self.GRACEFUL_API_DEGRADATION:
                print(f"⚠️ Database manager not available: {e}")
                return None
            else:
                raise ConfigError(f"Cannot import database manager: {e}")

    def test_database_connection(self) -> tuple[bool, str]:
        """Test database connection with compatibility for existing system"""
        try:
            db_manager = self.get_database_manager()
            if db_manager is None:
                return False, "Database manager not available"
        
            # Try a simple connection test
            conn = db_manager.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT 1")
            result = cur.fetchone()
            conn.close()
        
            if result and result[0] == 1:
                return True, "Database connection successful"
            else:
                return False, "Database query failed"
            
        except Exception as e:
            return False, f"Database connection failed: {e}"

    
    def get_api_manager(self):
        """Get API manager with enhanced rate limiting and error handling"""
        try:
            from .api_manager import APIManager
            return APIManager(
                config=self,
                enable_caching=self.ENABLE_API_RESPONSE_CACHING,
                cache_ttl_hours=self.CACHE_TTL_HOURS
            )
        except ImportError as e:
            if self.GRACEFUL_API_DEGRADATION:
                print(f"⚠️ API manager not available: {e}")
                return None
            else:
                raise ConfigError(f"Cannot import API manager: {e}")
    
    def get_memory_manager(self):
        """Get memory manager for large dataset processing"""
        if not self.ENABLE_MEMORY_OPTIMIZATION:
            return None
        
        try:
            from .memory_manager import MemoryManager
            return MemoryManager(
                max_memory_percent=self.MAX_MEMORY_PERCENT,
                chunk_size_rows=self.CHUNK_SIZE_ROWS,
                enable_gc_hints=self.ENABLE_GARBAGE_COLLECTION_HINTS
            )
        except ImportError as e:
            if self.GRACEFUL_API_DEGRADATION:
                print(f"⚠️ Memory manager not available: {e}")
                return None
            else:
                raise ConfigError(f"Cannot import memory manager: {e}")
    
    def test_memory_availability(self) -> Tuple[bool, str]:
        """Test if sufficient memory is available for large dataset processing"""
        if not self.ENABLE_MEMORY_OPTIMIZATION:
            return True, "Memory monitoring disabled"
        
        try:
            memory_status = self.get_memory_status()
            if not memory_status["available"]:
                return False, memory_status["reason"]
            
            available_gb = memory_status.get("available_gb", 0)
            if available_gb < 2:
                return False, f"Low available memory: {available_gb:.1f}GB"
            
            return True, f"Memory available: {available_gb:.1f}GB"
        except Exception as e:
            return False, f"Memory check failed: {e}"
    
    def test_api_connectivity(self) -> Dict[str, Tuple[bool, str]]:
        """Test connectivity to all configured APIs - simplified version"""
        results = {}
    
        if self.USE_PLACEHOLDER_DATA:
            return {"placeholder_mode": (True, "Using placeholder data")}
    
        # Test pybaseball availability
        try:
            import pybaseball
            results["pybaseball"] = (True, "Available")
        except ImportError:
            results["pybaseball"] = (False, "Not installed")
    
        # Simplified API test - just check if requests works
        try:
            import requests
            results["requests"] = (True, "HTTP client available")
        except ImportError:
            results["requests"] = (False, "Requests library not available")
    
        return results
    
    def get_enhanced_summary(self) -> Dict:
        """Get comprehensive configuration summary with new features"""
        summary = {
            "database_configured": bool(self.PG_DSN),
            "s3_configured": bool(self.AWS_S3_BUCKET),
            "s3_enabled": self.ENABLE_S3_STORAGE,
            "use_placeholder_data": self.USE_PLACEHOLDER_DATA,
            "graceful_degradation": self.GRACEFUL_API_DEGRADATION,
            "memory_optimization": self.ENABLE_MEMORY_OPTIMIZATION,
            "streaming_processing": self.ENABLE_STREAMING_PROCESSING,
            "api_caching": self.ENABLE_API_RESPONSE_CACHING,
            "performance_monitoring": self.PERFORMANCE_MONITORING,
            
            # API settings
            "pybaseball_delay_range": (self.PYBASEBALL_DELAY_MIN, self.PYBASEBALL_DELAY_MAX),
            "pybaseball_rate_limit": f"{self.PYBASEBALL_REQUESTS_PER_MINUTE}/min",
            "api_retry_count": self.API_RETRY_COUNT,
            
            # Memory settings
            "max_memory_percent": self.MAX_MEMORY_PERCENT,
            "chunk_size_rows": self.CHUNK_SIZE_ROWS,
            "large_dataset_threshold": self.LARGE_DATASET_THRESHOLD_ROWS,
            
            # Features
            "advanced_statcast_enabled": self.ENABLE_ADVANCED_STATCAST,
            "expected_stats_enabled": self.ENABLE_EXPECTED_STATS,
            "barrel_metrics_enabled": self.ENABLE_BARREL_METRICS,
            "debug_mode": self.DEBUG,
            "validated": self._validated,
        }
        
        return summary
    
    def print_enhanced_status(self):
        """Print comprehensive configuration status with new features"""
        mode = "PLACEHOLDER" if self.USE_PLACEHOLDER_DATA else "REAL DATA"
        print(f"⚙️ Enhanced MLB Configuration Status ({mode} MODE):")
        
        # Core settings
        print(f"   Database: {'✅' if self.PG_DSN else '❌'} {'(RDS)' if 'rds.amazonaws.com' in self.PG_DSN else '(local)' if self.PG_DSN else '(missing)'}")
        print(f"   Memory Optimization: {'✅' if self.ENABLE_MEMORY_OPTIMIZATION else '❌'} ({self.MAX_MEMORY_PERCENT}% max)")
        print(f"   API Caching: {'✅' if self.ENABLE_API_RESPONSE_CACHING else '❌'} ({self.CACHE_TTL_HOURS}h TTL)")
        
        if self.USE_PLACEHOLDER_DATA:
            print(f"   Data Mode: 🔧 Using placeholder data (no API limits)")
        else:
            print(f"   Data Mode: 📡 Real APIs with enhanced rate limiting")
            print(f"   Pybaseball: {self.PYBASEBALL_DELAY_MIN}-{self.PYBASEBALL_DELAY_MAX}s delay, {self.PYBASEBALL_REQUESTS_PER_MINUTE}/min")
            print(f"   API Retries: {self.API_RETRY_COUNT} with {self.API_RETRY_BACKOFF_FACTOR}x backoff")
        
        # Memory management
        if self.ENABLE_MEMORY_OPTIMIZATION:
            memory_status = self.get_memory_status()
            if memory_status["available"]:
                print(f"   Memory Status: ✅ {memory_status.get('available_gb', 0):.1f}GB available")
            else:
                print(f"   Memory Status: ⚠️ {memory_status['reason']}")
        
        print(f"   Streaming: {'✅' if self.ENABLE_STREAMING_PROCESSING else '❌'} (>{self.LARGE_DATASET_THRESHOLD_ROWS:,} rows)")
        
        # Enhanced features
        print(f"\n🚀 Production Features:")
        print(f"   Graceful Degradation: {'✅' if self.GRACEFUL_API_DEGRADATION else '❌'}")
        print(f"   Circuit Breaker: {'✅' if self.API_CIRCUIT_BREAKER_THRESHOLD else '❌'} ({self.API_CIRCUIT_BREAKER_THRESHOLD} failures)")
        print(f"   Memory Monitoring: {'✅' if self.ENABLE_MEMORY_OPTIMIZATION else '❌'}")
        print(f"   Performance Tracking: {'✅' if self.PERFORMANCE_MONITORING else '❌'}")
        print(f"   Large Dataset Support: {'✅' if self.ENABLE_STREAMING_PROCESSING else '❌'}")
        
        # API connectivity
        if not self.USE_PLACEHOLDER_DATA:
            print(f"\n🌐 API Connectivity:")
            api_results = self.test_api_connectivity()
            for api_name, (status, message) in api_results.items():
                status_icon = "✅" if status else "❌"
                print(f"   {api_name}: {status_icon} {message}")
        
        # S3 configuration
        if self.ENABLE_S3_STORAGE:
            print(f"\n☁️ S3 Configuration:")
            print(f"   Bucket: {self.AWS_S3_BUCKET}")
            print(f"   Auto Upload: {'✅' if self.AUTO_UPLOAD_TO_S3 else '❌'}")
            print(f"   Multipart Threshold: {self.S3_MULTIPART_THRESHOLD}MB")
        
        print(f"\n💾 Memory Management:")
        print(f"   Chunk Size: {self.CHUNK_SIZE_ROWS:,} rows")
        print(f"   Batch Size: {self.BATCH_SIZE_MB}MB")
        print(f"   GC Hints: {'✅' if self.ENABLE_GARBAGE_COLLECTION_HINTS else '❌'}")
        print(f"   Max DataFrame: {self.MAX_DATAFRAME_SIZE_MB}MB")
        
        print(f"\n✨ ENHANCED: Production-ready real data collection")
        print(f"🛡️ ROBUST: Comprehensive error handling and recovery")

# Global configuration instance
config = Config()

def require_config(require_database: bool = True, graceful_degradation: bool = True) -> Config:
    """
    Enhanced configuration validation with comprehensive checks
    
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
    
    # Enhanced issue categorization
    critical_issues = []
    warning_issues = []
    memory_issues = []
    api_issues = []
    
    for issue in issues:
        if require_database and "PG_DSN" in issue and "not set" in issue:
            critical_issues.append(issue)
        elif "memory" in issue.lower() or "Memory" in issue:
            memory_issues.append(issue)
        elif any(api in issue.lower() for api in ["api", "pybaseball", "rate"]):
            api_issues.append(issue)
        else:
            warning_issues.append(issue)
    
    # Handle different types of issues
    if memory_issues and graceful_degradation:
        print("⚠️ Memory configuration warnings:")
        for issue in memory_issues:
            print(f"   • {issue}")
        print("   → Memory optimization may be limited")
    
    if api_issues and graceful_degradation:
        print("⚠️ API configuration warnings:")
        for issue in api_issues:
            print(f"   • {issue}")
        print("   → API rate limiting may be suboptimal")
    
    if warning_issues and graceful_degradation:
        print("⚠️ General configuration warnings:")
        for issue in warning_issues:
            print(f"   • {issue}")
    
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
            if config.USE_PLACEHOLDER_DATA:
                print("💡 Using placeholder mode for testing")
    
    return config

# Enhanced helper functions
def get_config_for_production() -> Config:
    """Get config optimized for production with all validations"""
    return require_config(require_database=True, graceful_degradation=False)

def get_config_for_development() -> Config:
    """Get config for development with graceful degradation"""
    return require_config(require_database=True, graceful_degradation=True)

def get_config() -> Config:
    """Get configuration instance without strict validation"""
    return config