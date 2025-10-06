"""
Configuration Management for Distributed Function Runtime

This module handles environment-based configuration using .env files
and provides centralized access to all configurable parameters.
"""

import os
from typing import Union, Any
from dataclasses import dataclass
from pathlib import Path

# Try to import python-dotenv for .env file support
try:
    from dotenv import load_dotenv
    HAS_DOTENV = True
except ImportError:
    HAS_DOTENV = False


@dataclass
class NetworkConfig:
    """Network-related configuration."""
    default_port: int = 8080
    default_host: str = "127.0.0.1"
    bind_address: str = "0.0.0.0"
    production_host: str = "0.0.0.0"
    production_port: int = 8080


@dataclass
class RPCConfig:
    """RPC communication configuration."""
    timeout_seconds: float = 30.0
    max_retries: int = 3
    retry_delay: float = 1.0
    max_request_size_mb: int = 10


@dataclass
class FunctionConfig:
    """Function execution configuration."""
    execution_timeout: float = 30.0
    max_memory_mb: int = 512
    max_concurrent_functions: int = 10


@dataclass
class HeartbeatConfig:
    """Heartbeat and monitoring configuration."""
    interval_seconds: float = 30.0
    timeout_seconds: float = 90.0
    cleanup_interval_seconds: float = 60.0


@dataclass
class ResourceConfig:
    """Resource threshold configuration."""
    cpu_usage_threshold: float = 80.0
    memory_usage_threshold: float = 90.0
    disk_usage_threshold: float = 85.0


@dataclass
class PathConfig:
    """File path configuration."""
    registry_file_path: str = "./data/registry.json"
    log_file_path: str = "./logs/distributed_runtime.log"
    function_cache_path: str = "./cache/functions"


@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    enable_file_logging: bool = True
    enable_console_logging: bool = True


@dataclass
class SecurityConfig:
    """Security-related configuration."""
    enable_cors: bool = True
    allowed_origins: str = "*"
    enable_request_validation: bool = True


@dataclass
class ClusterConfig:
    """Cluster and deployment configuration."""
    environment: str = "development"
    debug_mode: bool = False
    enable_metrics: bool = True
    cluster_name: str = "distro-cluster"
    node_discovery_mode: str = "heartbeat"
    registry_type: str = "file"


class Config:
    """
    Centralized configuration management.
    
    Loads configuration from environment variables and .env files.
    """
    
    def __init__(self, env_file: str = ".env"):
        """
        Initialize configuration.
        
        Args:
            env_file: Path to .env file (optional)
        """
        self.env_file = env_file
        self._load_env_file()
        self._initialize_configs()
        self._ensure_directories()
    
    def _load_env_file(self):
        """Load environment variables from .env file if available."""
        if HAS_DOTENV and os.path.exists(self.env_file):
            load_dotenv(self.env_file)
        elif os.path.exists(self.env_file) and not HAS_DOTENV:
            # Manual .env parsing if python-dotenv not available
            with open(self.env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        os.environ[key.strip()] = value.strip()
    
    def _get_env(self, key: str, default: Any, type_cast: type = str) -> Any:
        """Get environment variable with type casting and default."""
        value = os.environ.get(key, default)
        
        if type_cast == bool:
            if isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 'on')
            return bool(value)
        
        try:
            return type_cast(value)
        except (ValueError, TypeError):
            return default
    
    def _initialize_configs(self):
        """Initialize all configuration sections."""
        self.network = NetworkConfig(
            default_port=self._get_env("DEFAULT_PORT", 8080, int),
            default_host=self._get_env("DEFAULT_HOST", "127.0.0.1"),
            bind_address=self._get_env("BIND_ADDRESS", "0.0.0.0"),
            production_host=self._get_env("PRODUCTION_HOST", "0.0.0.0"),
            production_port=self._get_env("PRODUCTION_PORT", 8080, int)
        )
        
        self.rpc = RPCConfig(
            timeout_seconds=self._get_env("RPC_TIMEOUT_SECONDS", 30.0, float),
            max_retries=self._get_env("RPC_MAX_RETRIES", 3, int),
            retry_delay=self._get_env("RPC_RETRY_DELAY", 1.0, float),
            max_request_size_mb=self._get_env("MAX_REQUEST_SIZE_MB", 10, int)
        )
        
        self.functions = FunctionConfig(
            execution_timeout=self._get_env("FUNCTION_EXECUTION_TIMEOUT", 30.0, float),
            max_memory_mb=self._get_env("FUNCTION_MAX_MEMORY_MB", 512, int),
            max_concurrent_functions=self._get_env("MAX_CONCURRENT_FUNCTIONS", 10, int)
        )
        
        self.heartbeat = HeartbeatConfig(
            interval_seconds=self._get_env("HEARTBEAT_INTERVAL_SECONDS", 30.0, float),
            timeout_seconds=self._get_env("HEARTBEAT_TIMEOUT_SECONDS", 90.0, float),
            cleanup_interval_seconds=self._get_env("NODE_CLEANUP_INTERVAL_SECONDS", 60.0, float)
        )
        
        self.resources = ResourceConfig(
            cpu_usage_threshold=self._get_env("CPU_USAGE_THRESHOLD", 80.0, float),
            memory_usage_threshold=self._get_env("MEMORY_USAGE_THRESHOLD", 90.0, float),
            disk_usage_threshold=self._get_env("DISK_USAGE_THRESHOLD", 85.0, float)
        )
        
        self.paths = PathConfig(
            registry_file_path=self._get_env("REGISTRY_FILE_PATH", "./data/registry.json"),
            log_file_path=self._get_env("LOG_FILE_PATH", "./logs/distributed_runtime.log"),
            function_cache_path=self._get_env("FUNCTION_CACHE_PATH", "./cache/functions")
        )
        
        self.logging = LoggingConfig(
            level=self._get_env("LOG_LEVEL", "INFO"),
            format=self._get_env("LOG_FORMAT", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
            enable_file_logging=self._get_env("ENABLE_FILE_LOGGING", True, bool),
            enable_console_logging=self._get_env("ENABLE_CONSOLE_LOGGING", True, bool)
        )
        
        self.security = SecurityConfig(
            enable_cors=self._get_env("ENABLE_CORS", True, bool),
            allowed_origins=self._get_env("ALLOWED_ORIGINS", "*"),
            enable_request_validation=self._get_env("ENABLE_REQUEST_VALIDATION", True, bool)
        )
        
        self.cluster = ClusterConfig(
            environment=self._get_env("ENVIRONMENT", "development"),
            debug_mode=self._get_env("DEBUG_MODE", False, bool),
            enable_metrics=self._get_env("ENABLE_METRICS", True, bool),
            cluster_name=self._get_env("CLUSTER_NAME", "distro-cluster"),
            node_discovery_mode=self._get_env("NODE_DISCOVERY_MODE", "heartbeat"),
            registry_type=self._get_env("REGISTRY_TYPE", "file")
        )
    
    def _ensure_directories(self):
        """Ensure required directories exist."""
        directories = [
            os.path.dirname(self.paths.registry_file_path),
            os.path.dirname(self.paths.log_file_path),
            self.paths.function_cache_path
        ]
        
        for directory in directories:
            if directory and not os.path.exists(directory):
                Path(directory).mkdir(parents=True, exist_ok=True)
    
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.cluster.environment.lower() == "production"
    
    def is_development(self) -> bool:
        """Check if running in development environment."""
        return self.cluster.environment.lower() == "development"
    
    def get_host_port(self) -> tuple[str, int]:
        """Get appropriate host and port based on environment."""
        if self.is_production():
            return self.network.production_host, self.network.production_port
        return self.network.default_host, self.network.default_port
    
    def to_dict(self) -> dict:
        """Convert configuration to dictionary for debugging."""
        return {
            "network": self.network.__dict__,
            "rpc": self.rpc.__dict__,
            "functions": self.functions.__dict__,
            "heartbeat": self.heartbeat.__dict__,
            "resources": self.resources.__dict__,
            "paths": self.paths.__dict__,
            "logging": self.logging.__dict__,
            "security": self.security.__dict__,
            "cluster": self.cluster.__dict__
        }


# Global configuration instance
config = Config()


# Convenience functions for backward compatibility
def get_default_port() -> int:
    """Get default port from configuration."""
    return config.network.default_port


def get_default_host() -> str:
    """Get default host from configuration."""
    return config.network.default_host


def get_rpc_timeout() -> float:
    """Get RPC timeout from configuration."""
    return config.rpc.timeout_seconds


def is_production() -> bool:
    """Check if running in production."""
    return config.is_production()