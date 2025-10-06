"""
Demo for Increment 3: Basic Remote Execution (RPC/API)

This demo showcases the RPC communication capabilities including:
- Starting RPC servers on multiple nodes
- Remote function execution via RPC client
- Node discovery and health monitoring
- Error handling and retry mechanisms
"""

import time
import threading
import logging
from typing import List

import sys
import os

# Add the parent directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.communication import RPCServer, RPCClient, NodeEndpoint
from src.core.local_executor import LocalExecutor
from src.networking.node_info import NodeInfo, NodeRole, NodeStatus, SystemCapacity
from examples.sample_functions import (
    HelloWorldFunction, MathOperationsFunction, DataProcessingFunction,
    SlowFunction, ErrorFunction, JSONValidationFunction
)


class RPCDemo:
    """Demo for RPC communication between nodes."""
    
    def __init__(self):
        """Initialize the demo."""
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Demo components
        self.servers: List[RPCServer] = []
        self.client = RPCClient(default_timeout=10.0, max_retries=3)
        self.node_endpoints: List[NodeEndpoint] = []
    
    def setup_demo_nodes(self, num_nodes: int = 3):
        """Set up multiple demo nodes with different functions."""
        self.logger.info(f"Setting up {num_nodes} demo nodes...")
        
        base_port = 8080
        
        for i in range(num_nodes):
            # Create node info
            capacity = SystemCapacity()  # Let it auto-populate from system
            node_info = NodeInfo(
                node_id=f"demo-node-{i+1}",
                hostname=f"demo-node-{i+1}",
                ip_address="127.0.0.1",
                port=base_port + i,
                capacity=capacity,
                role=NodeRole.WORKER
            )
            
            # Create executor and register functions based on node
            executor = LocalExecutor()
            
            if i == 0:  # Node 1: Basic functions
                executor.register_function("hello_world", HelloWorldFunction())
                executor.register_function("math_add", MathOperationsFunction())
                self.logger.info(f"Node 1: Registered hello_world, math_add")
                
            elif i == 1:  # Node 2: Data processing
                executor.register_function("data_processing", DataProcessingFunction())
                executor.register_function("slow_function", SlowFunction())
                self.logger.info(f"Node 2: Registered data_processing, slow_function")
                
            else:  # Node 3: JSON and Error functions
                executor.register_function("json_validation", JSONValidationFunction())
                executor.register_function("error_function", ErrorFunction())
                self.logger.info(f"Node 3: Registered json_validation, error_function")
            
            # Create and configure server
            server = RPCServer(
                node_info=node_info,
                executor=executor
            )
            
            self.servers.append(server)
            self.node_endpoints.append(NodeEndpoint(
                node_id=f"demo-node-{i+1}",
                host="127.0.0.1", 
                port=base_port + i
            ))
        
        self.logger.info(f"Created {len(self.servers)} demo nodes")
    
    def start_all_servers(self):
        """Start all demo servers."""
        self.logger.info("Starting all RPC servers...")
        
        for i, server in enumerate(self.servers):
            success = server.start()
            if success:
                # Wait a moment for server to start
                time.sleep(0.2)
                actual_port = server.server.server_address[1]
                self.node_endpoints[i] = NodeEndpoint(
                    node_id=f"demo-node-{i+1}",
                    host="127.0.0.1",
                    port=actual_port
                )
                self.logger.info(f"Started server {i+1} on port {actual_port}")
            else:
                self.logger.error(f"Failed to start server {i+1}")
        
        # Give servers time to fully initialize
        time.sleep(1.0)
    
    def stop_all_servers(self):
        """Stop all demo servers."""
        self.logger.info("Stopping all RPC servers...")
        
        for i, server in enumerate(self.servers):
            if server.is_running:
                success = server.stop()
                if success:
                    self.logger.info(f"Stopped server {i+1}")
                else:
                    self.logger.error(f"Failed to stop server {i+1}")
    
    def demo_node_discovery(self):
        """Demonstrate node discovery and health checking."""
        print("\\n" + "="*60)
        print("DEMO: Node Discovery and Health Monitoring")
        print("="*60)
        
        for i, endpoint in enumerate(self.node_endpoints):
            print(f"\\nDiscovering Node {i+1} at {endpoint}...")
            
            # Health check
            is_healthy, health_data = self.client.health_check(endpoint)
            if is_healthy:
                print(f"  ✓ Health: {health_data['status']}")
                print(f"  ✓ Node ID: {health_data['node_id']}")
            else:
                print(f"  ✗ Health check failed")
                continue
            
            # Get detailed status
            status = self.client.get_node_status(endpoint)
            if status:
                print(f"  ✓ Hostname: {status['hostname']}")
                print(f"  ✓ Role: {status['role']}")
                print(f"  ✓ CPU Load: {status['capacity']['cpu_percent']:.1f}%")
                print(f"  ✓ Memory Usage: {status['capacity']['memory_percent']:.1f}%")
                print(f"  ✓ Can Accept Work: {status['capacity']['can_accept_work']}")
            
            # List available functions
            functions = self.client.list_remote_functions(endpoint)
            if functions:
                print(f"  ✓ Available Functions: {', '.join(functions['functions'])}")
                print(f"  ✓ Total Functions: {functions['total_count']}")
            
            # Ping test
            success, response_time, error = self.client.ping_node(endpoint)
            if success:
                print(f"  ✓ Ping: {response_time*1000:.1f}ms")
            else:
                print(f"  ✗ Ping failed: {error}")
    
    def demo_remote_execution(self):
        """Demonstrate remote function execution."""
        print("\\n" + "="*60)
        print("DEMO: Remote Function Execution")
        print("="*60)
        
        # Test cases for each node
        test_cases = [
            {
                "node": 0,
                "function": "hello_world",
                "input": {"name": "Alice"},
                "description": "Simple greeting function"
            },
            {
                "node": 0,
                "function": "math_add",
                "input": {"operation": "add", "a": 15, "b": 27},
                "description": "Mathematical addition"
            },
            {
                "node": 1,
                "function": "data_processing",
                "input": {"numbers": [1, 2, 3, 4, 5]},
                "description": "Data processing with statistical analysis"
            },
            {
                "node": 1,
                "function": "slow_function",
                "input": {"duration": 1},
                "description": "Slow processing simulation"
            },
            {
                "node": 2,
                "function": "json_validation",
                "input": {"data": {"name": "Alice", "age": 30}},
                "description": "JSON validation function"
            },
            {
                "node": 2,
                "function": "error_function",
                "input": {"error_type": "none"},
                "description": "Error handling demonstration"
            }
        ]
        
        for test_case in test_cases:
            node_idx = test_case["node"]
            endpoint = self.node_endpoints[node_idx]
            
            print(f"\\n🚀 Executing '{test_case['function']}' on Node {node_idx + 1}")
            print(f"   Description: {test_case['description']}")
            print(f"   Endpoint: {endpoint}")
            print(f"   Input: {test_case['input']}")
            
            start_time = time.time()
            result = self.client.execute_function(
                endpoint=endpoint,
                function_name=test_case["function"],
                input_data=test_case["input"],
                timeout_seconds=15.0
            )
            total_time = time.time() - start_time
            
            if result.success:
                print(f"   ✓ Result: {result.output}")
                print(f"   ✓ Execution Time: {result.execution_time_ms/1000:.3f}s")
                print(f"   ✓ Total Time (including network): {total_time:.3f}s")
            else:
                print(f"   ✗ Error: {result.error_message}")
                print(f"   ✗ Execution Time: {result.execution_time_ms/1000:.3f}s")
    
    def demo_error_handling(self):
        """Demonstrate error handling and edge cases."""
        print("\\n" + "="*60)
        print("DEMO: Error Handling and Edge Cases")
        print("="*60)
        
        endpoint = self.node_endpoints[0]  # Use first node
        
        # Test function not found
        print("\\n🔍 Testing function not found...")
        result = self.client.execute_function(
            endpoint=endpoint,
            function_name="nonexistent_function",
            input_data={}
        )
        if not result.success:
            print(f"   ✓ Correctly handled: {result.error_message}")
        else:
            print(f"   ✗ Unexpected success")
        
        # Test invalid input
        print("\\n🔍 Testing invalid input...")
        result = self.client.execute_function(
            endpoint=endpoint,
            function_name="math_add",
            input_data={"a": "not_a_number", "b": 5}
        )
        if not result.success:
            print(f"   ✓ Correctly handled: {result.error_message}")
        else:
            print(f"   ✗ Unexpected success: {result.output}")
        
        # Test connection to non-existent node
        print("\\n🔍 Testing connection to non-existent node...")
        fake_endpoint = NodeEndpoint(
            node_id="fake-node",
            host="127.0.0.1",
            port=9999
        )
        success, response_time, error = self.client.ping_node(fake_endpoint, timeout_seconds=2.0)
        if not success:
            print(f"   ✓ Correctly handled connection failure: {error}")
        else:
            print(f"   ✗ Unexpected success")
    
    def demo_performance_metrics(self):
        """Demonstrate performance monitoring and metrics."""
        print("\\n" + "="*60)
        print("DEMO: Performance Metrics and Monitoring")
        print("="*60)
        
        # Execute multiple requests to gather statistics
        endpoint = self.node_endpoints[0]
        
        print("\\n📊 Running performance tests...")
        
        # Perform multiple function calls
        for i in range(5):
            self.client.execute_function(
                endpoint=endpoint,
                function_name="hello_world",
                input_data={"name": f"User{i+1}"}
            )
            self.client.ping_node(endpoint)
        
        # Show client statistics
        client_stats = self.client.get_stats()
        print("\\n📈 Client Statistics:")
        print(f"   Total Requests: {client_stats['requests_sent']}")
        print(f"   Successful Requests: {client_stats['successful_requests']}")
        print(f"   Failed Requests: {client_stats['failed_requests']}")
        print(f"   Success Rate: {client_stats['success_rate']:.1%}")
        print(f"   Average Response Time: {client_stats['average_response_time']:.3f}s")
        print(f"   Retry Attempts: {client_stats['retry_attempts']}")
        
        # Show server statistics
        for i, server in enumerate(self.servers):
            server_stats = server.get_stats()
            print(f"\\n📈 Server {i+1} Statistics:")
            print(f"   Uptime: {server_stats['uptime_seconds']:.1f}s")
            print(f"   Requests Handled: {server_stats['requests_handled']}")
            print(f"   Functions Executed: {server_stats['functions_executed']}")
            print(f"   Errors Encountered: {server_stats['errors_encountered']}")
            print(f"   Running: {server_stats['is_running']}")
    
    def run_complete_demo(self):
        """Run the complete RPC demo."""
        print("🌟 STARTING DISTRIBUTED FUNCTION RUNTIME - RPC DEMO")
        print("="*70)
        
        try:
            # Setup
            self.setup_demo_nodes(3)
            self.start_all_servers()
            
            # Run demo sections
            self.demo_node_discovery()
            self.demo_remote_execution()
            self.demo_error_handling()
            self.demo_performance_metrics()
            
            print("\\n" + "="*70)
            print("🎉 RPC DEMO COMPLETED SUCCESSFULLY!")
            print("="*70)
            
        except Exception as e:
            self.logger.error(f"Demo failed: {e}")
            print(f"\\n❌ Demo failed: {e}")
            
        finally:
            # Cleanup
            self.stop_all_servers()
            print("\\n🧹 Cleanup completed")


def main():
    """Main demo function."""
    demo = RPCDemo()
    demo.run_complete_demo()


if __name__ == "__main__":
    main()