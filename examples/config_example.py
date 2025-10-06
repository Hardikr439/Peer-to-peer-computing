"""
Example of integrating configuration management into existing code.

This shows how to migrate from hardcoded values to environment-based configuration.
"""

from src.config import config

# Before (hardcoded):
# DEFAULT_PORT = 8080
# DEFAULT_HOST = "127.0.0.1"
# RPC_TIMEOUT = 30.0

# After (configuration-based):
def create_rpc_server():
    """Example of using configuration in RPC server creation."""
    from src.communication import RPCServer
    from src.networking.node_info import NodeInfo, SystemCapacity
    
    # Use configuration instead of hardcoded values
    host, port = config.get_host_port()
    
    node_info = NodeInfo(
        node_id="example-node",
        hostname="example-host",
        ip_address=host,
        port=port,
        capacity=SystemCapacity()
    )
    
    # Use configuration-based timeouts
    server = RPCServer(
        node_info=node_info,
        executor=None  # Would be provided
    )
    
    return server


def create_rpc_client():
    """Example of using configuration in RPC client creation."""
    from src.communication import RPCClient
    
    # Use configuration for client settings
    client = RPCClient(
        default_timeout=config.rpc.timeout_seconds,
        max_retries=config.rpc.max_retries,
        retry_delay=config.rpc.retry_delay
    )
    
    return client


def setup_logging():
    """Example of configuration-based logging setup."""
    import logging
    import os
    
    # Create log directory if needed
    log_dir = os.path.dirname(config.paths.log_file_path)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)
    
    # Configure logging based on settings
    handlers = []
    
    if config.logging.enable_console_logging:
        handlers.append(logging.StreamHandler())
    
    if config.logging.enable_file_logging:
        handlers.append(logging.FileHandler(config.paths.log_file_path))
    
    logging.basicConfig(
        level=getattr(logging, config.logging.level.upper()),
        format=config.logging.format,
        handlers=handlers
    )


def check_resource_thresholds(cpu_percent: float, memory_percent: float) -> bool:
    """Example of using configuration for resource thresholds."""
    return (
        cpu_percent < config.resources.cpu_usage_threshold and
        memory_percent < config.resources.memory_usage_threshold
    )


# Environment-specific behavior
if config.is_production():
    print(f"Running in PRODUCTION mode on {config.cluster.cluster_name}")
    print(f"Host: {config.network.production_host}:{config.network.production_port}")
else:
    print(f"Running in DEVELOPMENT mode")
    print(f"Debug mode: {config.cluster.debug_mode}")


if __name__ == "__main__":
    # Example usage
    print("Configuration loaded:")
    print(f"  Environment: {config.cluster.environment}")
    print(f"  Default port: {config.network.default_port}")
    print(f"  RPC timeout: {config.rpc.timeout_seconds}s")
    print(f"  Log level: {config.logging.level}")
    print(f"  CPU threshold: {config.resources.cpu_usage_threshold}%")