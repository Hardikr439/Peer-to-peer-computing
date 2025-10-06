#!/usr/bin/env python3
"""
Distributed Function Runtime - Main Driver

This is the main entry point for the distributed serverless function runtime.
It provides different operational modes for showcasing the system across multiple devices.

Usage Examples:
    # Start as a worker node
    python main.py --mode worker --port 8080

    # Start as a coordinator node  
    python main.py --mode coordinator --port 8080

    # Start a client to execute functions
    python main.py --mode client --target 192.168.1.100:8080

    # Run interactive demo
    python main.py --mode demo

    # Auto-discovery showcase
    python main.py --mode showcase --devices 2
"""

import argparse
import asyncio
import json
import logging
import sys
import time
import threading
from typing import List, Dict, Any, Optional
import signal
import os

# Add src to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.core.local_executor import LocalExecutor
from src.networking.node_info import NodeInfo, NodeRole, NodeStatus, SystemCapacity
from src.networking.registry import InMemoryRegistry, FileBasedRegistry
from src.networking.heartbeat import HeartbeatService
from src.communication import RPCServer, RPCClient, NodeEndpoint
from examples.sample_functions import (
    HelloWorldFunction, MathOperationsFunction, DataProcessingFunction,
    SlowFunction, ErrorFunction, JSONValidationFunction
)


class DistributedRuntimeDriver:
    """Main driver for the distributed function runtime."""
    
    def __init__(self):
        """Initialize the driver."""
        self.logger = self._setup_logging()
        self.shutdown_event = threading.Event()
        self.components = []  # Track all started components for cleanup
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler('distributed_runtime.log')
            ]
        )
        return logging.getLogger(__name__)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.shutdown_event.set()
    
    def _create_node_info(self, 
                         node_id: str, 
                         role: NodeRole, 
                         port: int,
                         ip_address: str = "0.0.0.0") -> NodeInfo:
        """Create a NodeInfo instance."""
        capacity = SystemCapacity()
        
        return NodeInfo(
            node_id=node_id,
            hostname=f"runtime-{node_id}",
            ip_address=ip_address,
            port=port,
            role=role,
            capacity=capacity,
            status=NodeStatus.ONLINE
        )
    
    def _setup_executor_with_functions(self, node_type: str = "all") -> LocalExecutor:
        """Setup executor with appropriate functions based on node type."""
        executor = LocalExecutor()
        
        # Register functions based on node specialization
        if node_type in ["all", "basic"]:
            executor.register_function("hello_world", HelloWorldFunction())
            executor.register_function("math_operations", MathOperationsFunction())
            self.logger.info("Registered basic functions: hello_world, math_operations")
        
        if node_type in ["all", "processing"]:
            executor.register_function("data_processing", DataProcessingFunction())
            executor.register_function("json_validation", JSONValidationFunction())
            self.logger.info("Registered processing functions: data_processing, json_validation")
        
        if node_type in ["all", "utility"]:
            executor.register_function("slow_function", SlowFunction())
            executor.register_function("error_function", ErrorFunction())
            self.logger.info("Registered utility functions: slow_function, error_function")
        
        return executor
    
    def mode_worker(self, args) -> None:
        """Run as a worker node."""
        print(f"🔧 Starting WORKER NODE on port {args.port}")
        print("="*60)
        
        # Create node info
        node_info = self._create_node_info(
            node_id=f"worker-{args.port}",
            role=NodeRole.WORKER,
            port=args.port,
            ip_address=args.bind_address
        )
        
        # Setup executor
        executor = self._setup_executor_with_functions(args.node_type)
        
        # Create registry (file-based for persistence across devices)
        registry = FileBasedRegistry(
            registry_file=args.registry_file,
            stale_timeout=args.heartbeat_timeout * 3
        )
        
        # Start RPC server
        rpc_server = RPCServer(node_info, executor, host=args.bind_address, port=args.port)
        if not rpc_server.start():
            self.logger.error("Failed to start RPC server")
            return
        
        self.components.append(rpc_server)
        
        # Start heartbeat service
        heartbeat = HeartbeatService(
            node_info=node_info,
            registry=registry,
            interval=args.heartbeat_interval
        )
        heartbeat.start()
        self.components.append(heartbeat)
        
        # Register this node
        registry.register_node(node_info)
        
        print(f"✅ Worker node '{node_info.node_id}' started successfully!")
        print(f"📡 RPC Server: {args.bind_address}:{args.port}")
        print(f"📁 Registry: {args.registry_file}")
        print(f"🔧 Functions: {', '.join(executor.list_functions())}")
        print(f"💓 Heartbeat: {args.heartbeat_interval}s interval")
        print("\n🎯 Ready to receive function execution requests!")
        print("Press Ctrl+C to stop gracefully...")
        
        # Keep running until shutdown
        try:
            while not self.shutdown_event.is_set():
                time.sleep(1)
                
                # Show periodic status
                if int(time.time()) % 30 == 0:  # Every 30 seconds
                    nodes = registry.get_active_nodes()
                    print(f"\n📊 Status Update - Active nodes: {len(nodes)}")
                    for node in nodes:
                        print(f"   • {node.node_id} ({node.role.value}) - {node.ip_address}:{node.port}")
        
        except KeyboardInterrupt:
            pass
        
        self._cleanup()
    
    def mode_coordinator(self, args) -> None:
        """Run as a coordinator node."""
        print(f"👑 Starting COORDINATOR NODE on port {args.port}")
        print("="*60)
        
        # Create node info
        node_info = self._create_node_info(
            node_id=f"coordinator-{args.port}",
            role=NodeRole.COORDINATOR,
            port=args.port,
            ip_address=args.bind_address
        )
        
        # Setup executor with all functions
        executor = self._setup_executor_with_functions("all")
        
        # Create registry
        registry = FileBasedRegistry(
            registry_file=args.registry_file,
            stale_timeout=args.heartbeat_timeout * 3
        )
        
        # Start RPC server
        rpc_server = RPCServer(node_info, executor, host=args.bind_address, port=args.port)
        if not rpc_server.start():
            self.logger.error("Failed to start RPC server")
            return
        
        self.components.append(rpc_server)
        
        # Start heartbeat service
        heartbeat = HeartbeatService(
            node_info=node_info,
            registry=registry,
            interval=args.heartbeat_interval
        )
        heartbeat.start()
        self.components.append(heartbeat)
        
        # Register this node
        registry.register_node(node_info)
        
        print(f"✅ Coordinator node '{node_info.node_id}' started successfully!")
        print(f"📡 RPC Server: {args.bind_address}:{args.port}")
        print(f"📁 Registry: {args.registry_file}")
        print(f"🔧 Functions: {', '.join(executor.list_functions())}")
        print(f"💓 Heartbeat: {args.heartbeat_interval}s interval")
        print("\n🎯 Coordinating distributed function execution!")
        
        # Start monitoring loop
        try:
            while not self.shutdown_event.is_set():
                time.sleep(5)
                
                # Show network status
                nodes = registry.get_active_nodes()
                worker_nodes = [n for n in nodes if n.role == NodeRole.WORKER]
                
                print(f"\n📋 Network Status:")
                print(f"   Total Nodes: {len(nodes)}")
                print(f"   Worker Nodes: {len(worker_nodes)}")
                print(f"   Server Stats: {rpc_server.get_stats()}")
                
                if worker_nodes:
                    print("   Active Workers:")
                    for worker in worker_nodes:
                        print(f"     • {worker.node_id} - {worker.ip_address}:{worker.port}")
                        print(f"       CPU: {worker.capacity.cpu_percent:.1f}%, "
                              f"Memory: {worker.capacity.memory_percent:.1f}%")
        
        except KeyboardInterrupt:
            pass
        
        self._cleanup()
    
    def mode_client(self, args) -> None:
        """Run as a client to execute functions on remote nodes."""
        print(f"🖥️  Starting CLIENT MODE - Target: {args.target}")
        print("="*60)
        
        # Parse target
        if ':' in args.target:
            host, port = args.target.split(':')
            port = int(port)
        else:
            host = args.target
            port = 8080
        
        endpoint = NodeEndpoint(node_id="target", host=host, port=port)
        client = RPCClient(default_timeout=30.0, max_retries=3)
        
        print(f"🎯 Target Endpoint: {host}:{port}")
        
        # Test connectivity
        print("\n🔍 Testing connectivity...")
        is_healthy, health_data = client.health_check(endpoint)
        if not is_healthy:
            print(f"❌ Cannot connect to {host}:{port}")
            print("   Make sure the target node is running!")
            return
        
        print(f"✅ Connected to node: {health_data.get('node_id', 'unknown')}")
        
        # Get available functions
        functions_info = client.list_remote_functions(endpoint)
        if not functions_info:
            print("❌ Could not retrieve function list")
            return
        
        available_functions = functions_info['functions']
        print(f"📋 Available functions: {', '.join(available_functions)}")
        
        # Interactive function execution
        print("\n🚀 INTERACTIVE FUNCTION EXECUTION")
        print("Available commands:")
        print("  • 'list' - Show available functions")
        print("  • 'status' - Show node status")
        print("  • 'ping' - Test connectivity")
        print("  • 'exec <function_name>' - Execute a function")
        print("  • 'demo' - Run demo functions")
        print("  • 'quit' - Exit")
        
        while True:
            try:
                command = input("\n> ").strip().lower()
                
                if command == 'quit' or command == 'exit':
                    break
                
                elif command == 'list':
                    functions_info = client.list_remote_functions(endpoint)
                    if functions_info:
                        print(f"Available functions: {', '.join(functions_info['functions'])}")
                        print(f"Total count: {functions_info['total_count']}")
                
                elif command == 'status':
                    status = client.get_node_status(endpoint)
                    if status:
                        print(f"Node ID: {status['node_id']}")
                        print(f"Role: {status['role']}")
                        print(f"CPU: {status['capacity']['cpu_percent']:.1f}%")
                        print(f"Memory: {status['capacity']['memory_percent']:.1f}%")
                        print(f"Functions: {', '.join(status['functions'])}")
                
                elif command == 'ping':
                    success, response_time, error = client.ping_node(endpoint)
                    if success:
                        print(f"🏓 Ping successful: {response_time*1000:.1f}ms")
                    else:
                        print(f"❌ Ping failed: {error}")
                
                elif command.startswith('exec '):
                    func_name = command[5:].strip()
                    if func_name in available_functions:
                        input_data = self._get_function_input(func_name)
                        print(f"🎯 Executing {func_name}...")
                        
                        result = client.execute_function(endpoint, func_name, input_data)
                        if result.success:
                            print(f"✅ Success: {result.output}")
                            print(f"⏱️  Time: {result.execution_time_ms/1000:.3f}s")
                        else:
                            print(f"❌ Error: {result.error_message}")
                    else:
                        print(f"❌ Function '{func_name}' not available")
                
                elif command == 'demo':
                    self._run_client_demo(client, endpoint, available_functions)
                
                else:
                    print("❌ Unknown command. Type 'quit' to exit.")
            
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"❌ Error: {e}")
        
        print("\n👋 Client session ended")
    
    def mode_demo(self, args) -> None:
        """Run interactive demo mode."""
        print("🎬 INTERACTIVE DEMO MODE")
        print("="*60)
        
        # Import and run the demo
        sys.path.insert(0, 'demos')
        from .demos.demo_increment3 import RPCDemo
        
        demo = RPCDemo()
        demo.run_complete_demo()
    
    def mode_showcase(self, args) -> None:
        """Run multi-device showcase mode."""
        print(f"🌟 MULTI-DEVICE SHOWCASE MODE ({args.devices} devices)")
        print("="*60)
        
        print("This mode helps coordinate demonstrations across multiple devices.")
        print("\nSuggested setup:")
        print("Device 1 (Coordinator):")
        print("  python main.py --mode coordinator --port 8080")
        print("\nDevice 2 (Worker):")
        print("  python main.py --mode worker --port 8081 --node-type processing")
        print("\nDevice 3 (Client):")
        print("  python main.py --mode client --target <device1-ip>:8080")
        
        print("\n📋 Registry file location:")
        print(f"  {os.path.abspath(args.registry_file)}")
        print("  💡 Tip: Use shared storage for the registry file across devices")
        
        # Create initial registry
        registry = FileBasedRegistry(args.registry_file)
        print(f"\n✅ Registry initialized at {args.registry_file}")
        
        print("\n🚀 Start your nodes and begin the showcase!")
    
    def _get_function_input(self, func_name: str) -> Dict[str, Any]:
        """Get input data for a specific function."""
        if func_name == "hello_world":
            name = input("Enter name (default: World): ").strip() or "World"
            return {"name": name}
        
        elif func_name == "math_operations":
            operation = input("Operation [add/subtract/multiply/divide] (default: add): ").strip() or "add"
            try:
                a = float(input("First number (default: 10): ").strip() or "10")
                b = float(input("Second number (default: 5): ").strip() or "5")
                return {"operation": operation, "a": a, "b": b}
            except ValueError:
                return {"operation": "add", "a": 10, "b": 5}
        
        elif func_name == "data_processing":
            print("Data processing - using default data: [1,2,3,4,5]")
            operation = input("Operation [sum/avg/max/min] (default: sum): ").strip() or "sum"
            return {"data": [1, 2, 3, 4, 5], "operation": operation}
        
        elif func_name == "slow_function":
            try:
                duration = float(input("Sleep duration in seconds (default: 2): ").strip() or "2")
                return {"duration": duration}
            except ValueError:
                return {"duration": 2.0}
        
        elif func_name == "json_validation":
            print("JSON validation - using default data")
            return {"data": {"name": "Alice", "age": 30, "city": "New York"}}
        
        elif func_name == "error_function":
            should_error = input("Should produce error? [y/N] (default: N): ").strip().lower()
            return {"should_error": should_error.startswith('y')}
        
        else:
            return {}
    
    def _run_client_demo(self, client: RPCClient, endpoint: NodeEndpoint, available_functions: List[str]):
        """Run a demo sequence of function calls."""
        demo_calls = [
            ("hello_world", {"name": "Demo User"}),
            ("math_operations", {"operation": "add", "a": 42, "b": 8}),
            ("data_processing", {"data": [10, 20, 30, 40, 50], "operation": "avg"}),
            ("json_validation", {"data": {"demo": True, "timestamp": time.time()}}),
        ]
        
        print("\n🎬 Running demo sequence...")
        
        for func_name, input_data in demo_calls:
            if func_name not in available_functions:
                print(f"⏭️  Skipping {func_name} (not available)")
                continue
            
            print(f"\n🎯 Executing {func_name}...")
            print(f"   Input: {input_data}")
            
            result = client.execute_function(endpoint, func_name, input_data)
            if result.success:
                print(f"   ✅ Result: {result.output}")
                print(f"   ⏱️  Time: {result.execution_time_ms/1000:.3f}s")
            else:
                print(f"   ❌ Error: {result.error_message}")
            
            time.sleep(1)  # Brief pause between calls
        
        print("\n🎬 Demo sequence completed!")
    
    def _cleanup(self):
        """Cleanup all components."""
        print("\n🧹 Cleaning up components...")
        
        for component in reversed(self.components):  # Cleanup in reverse order
            try:
                if hasattr(component, 'stop'):
                    component.stop()
                    print(f"   ✅ Stopped {component.__class__.__name__}")
            except Exception as e:
                print(f"   ⚠️  Error stopping {component.__class__.__name__}: {e}")
        
        self.components.clear()
        print("✅ Cleanup completed")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Distributed Serverless Function Runtime",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --mode worker --port 8080
  %(prog)s --mode coordinator --port 8080 --bind-address 0.0.0.0
  %(prog)s --mode client --target 192.168.1.100:8080
  %(prog)s --mode demo
  %(prog)s --mode showcase --devices 3
        """
    )
    
    parser.add_argument(
        '--mode', 
        required=True,
        choices=['worker', 'coordinator', 'client', 'demo', 'showcase'],
        help='Operational mode'
    )
    
    parser.add_argument(
        '--port', 
        type=int, 
        default=8080,
        help='Port to bind server (default: 8080)'
    )
    
    parser.add_argument(
        '--bind-address', 
        default='0.0.0.0',
        help='Address to bind server (default: 0.0.0.0)'
    )
    
    parser.add_argument(
        '--target',
        help='Target node for client mode (host:port)'
    )
    
    parser.add_argument(
        '--node-type',
        choices=['all', 'basic', 'processing', 'utility'],
        default='all',
        help='Type of functions to register (default: all)'
    )
    
    parser.add_argument(
        '--registry-file',
        default='node_registry.json',
        help='Registry file path (default: node_registry.json)'
    )
    
    parser.add_argument(
        '--heartbeat-interval',
        type=float,
        default=5.0,
        help='Heartbeat interval in seconds (default: 5.0)'
    )
    
    parser.add_argument(
        '--heartbeat-timeout',
        type=float,
        default=15.0,
        help='Heartbeat timeout in seconds (default: 15.0)'
    )
    
    parser.add_argument(
        '--devices',
        type=int,
        default=2,
        help='Number of devices for showcase mode (default: 2)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.mode == 'client' and not args.target:
        parser.error("Client mode requires --target argument")
    
    # Setup logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create and run driver
    driver = DistributedRuntimeDriver()
    
    try:
        if args.mode == 'worker':
            driver.mode_worker(args)
        elif args.mode == 'coordinator':
            driver.mode_coordinator(args)
        elif args.mode == 'client':
            driver.mode_client(args)
        elif args.mode == 'demo':
            driver.mode_demo(args)
        elif args.mode == 'showcase':
            driver.mode_showcase(args)
    
    except KeyboardInterrupt:
        print("\n👋 Shutdown requested by user")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        logging.exception("Fatal error occurred")
        sys.exit(1)


if __name__ == "__main__":
    main()