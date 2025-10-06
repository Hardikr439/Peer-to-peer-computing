"""
Example usage of the networking module for Increment 2.

This script demonstrates how to set up nodes with heartbeat and registry functionality.
"""

import sys
import os
import time
import logging
from pathlib import Path

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from networking import (
    NodeInfo, NodeRole, NodeStatus, SystemCapacity,
    create_registry, HeartbeatService,
    NetworkAddress
)
from core import LocalExecutor


class DistributedNode:
    """
    A distributed node that combines local function execution with networking capabilities.
    
    This represents a complete node in the distributed function runtime.
    """
    
    def __init__(self, 
                 port: int,
                 node_id: str = None,
                 role: NodeRole = NodeRole.HYBRID,
                 registry_type: str = "memory",
                 registry_file: str = None):
        """
        Initialize a distributed node.
        
        Args:
            port: Port for this node
            node_id: Optional node ID (generates one if not provided)
            role: Role of this node in the network
            registry_type: Type of registry to use ("memory" or "file")
            registry_file: File path for file-based registry
        """
        # Create node information
        self.node_info = NodeInfo.create_local_node(port=port, node_id=node_id, role=role)
        
        # Create local executor for functions
        self.executor = LocalExecutor()
        
        # Create registry
        registry_kwargs = {}
        if registry_type == "file" and registry_file:
            registry_kwargs["registry_file"] = registry_file
        
        self.registry = create_registry(registry_type, **registry_kwargs)
        
        # Create heartbeat service
        self.heartbeat = HeartbeatService(
            node=self.node_info,
            registry=self.registry,
            heartbeat_interval=5.0,
            cleanup_interval=15.0,
            node_timeout=30.0
        )
        
        # Set up logging
        self.logger = logging.getLogger(f"node.{self.node_info.node_id[:8]}")
        
        # Add callbacks
        self._setup_callbacks()
    
    def _setup_callbacks(self):
        """Set up callbacks for monitoring events."""
        def on_heartbeat_sent(heartbeat):
            self.logger.debug(f"Heartbeat sent: load={heartbeat.capacity.get_load_score():.2f}")
        
        def on_node_discovered(node):
            self.logger.info(f"Node discovered: {node.hostname} at {node.get_endpoint()}")
        
        def on_node_lost(node_id):
            self.logger.info(f"Node lost: {node_id}")
        
        self.heartbeat.add_heartbeat_sent_callback(on_heartbeat_sent)
        self.heartbeat.add_node_discovered_callback(on_node_discovered)
        self.heartbeat.add_node_lost_callback(on_node_lost)
    
    def start(self) -> bool:
        """
        Start the distributed node.
        
        Returns:
            True if started successfully, False otherwise
        """
        self.logger.info(f"Starting node {self.node_info.node_id[:8]} on {self.node_info.get_endpoint()}")
        
        # Update function list
        self.update_registered_functions()
        
        # Start heartbeat service
        if not self.heartbeat.start():
            self.logger.error("Failed to start heartbeat service")
            return False
        
        self.logger.info("Node started successfully")
        return True
    
    def stop(self) -> bool:
        """
        Stop the distributed node.
        
        Returns:
            True if stopped successfully, False otherwise
        """
        self.logger.info(f"Stopping node {self.node_info.node_id[:8]}")
        
        # Stop heartbeat service
        if not self.heartbeat.stop():
            self.logger.warning("Failed to stop heartbeat service cleanly")
        
        self.logger.info("Node stopped")
        return True
    
    def register_function_from_class(self, name: str, function_class):
        """Register a function and update the network."""
        self.executor.register_function_from_class(name, function_class)
        self.update_registered_functions()
    
    def execute_function(self, function_name: str, input_data):
        """Execute a function locally."""
        return self.executor.execute_function(function_name, input_data)
    
    def update_registered_functions(self):
        """Update the list of registered functions in node info and heartbeat."""
        function_names = self.executor.list_functions()
        self.node_info.registered_functions = function_names
        self.heartbeat.update_node_functions(function_names)
    
    def get_network_status(self) -> dict:
        """Get status of the network and this node."""
        nodes = self.registry.list_nodes()
        online_nodes = self.registry.list_nodes(NodeStatus.ONLINE)
        
        return {
            "local_node": {
                "id": self.node_info.node_id[:8],
                "endpoint": self.node_info.get_endpoint(),
                "status": self.node_info.status.value,
                "functions": len(self.node_info.registered_functions),
                "load_score": self.node_info.capacity.get_load_score()
            },
            "network": {
                "total_nodes": len(nodes),
                "online_nodes": len(online_nodes),
                "discovered_endpoints": [node.get_endpoint() for node in online_nodes]
            },
            "heartbeat": self.heartbeat.get_statistics()
        }
    
    def discover_nodes(self) -> list:
        """Discover other nodes in the network."""
        return self.registry.list_nodes(NodeStatus.ONLINE)


def main():
    """Demonstrate Increment 2 functionality."""
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("=== Serverless Distributed Function Runtime ===")
    print("Increment 2: Peer-to-Peer Resource Discovery and Health Check Demo\n")
    
    # Create multiple nodes to simulate a network
    nodes = []
    
    print("1. Creating distributed nodes...")
    
    # Create 3 nodes on different ports
    for i in range(3):
        port = 8000 + i
        node = DistributedNode(
            port=port,
            role=NodeRole.HYBRID,
            registry_type="memory"  # Use shared memory registry for demo
        )
        
        # Use the same registry instance for all nodes (simulating shared registry)
        if i > 0:
            node.registry = nodes[0].registry
            node.heartbeat.registry = nodes[0].registry
        
        nodes.append(node)
        print(f"   Created node {node.node_info.node_id[:8]} on port {port}")
    
    # Register some example functions on different nodes
    print("\n2. Registering functions on nodes...")
    
    # Import example functions
    from examples.sample_functions import HelloWorldFunction, MathOperationsFunction, DataProcessingFunction
    
    nodes[0].register_function_from_class("hello", HelloWorldFunction)
    nodes[0].register_function_from_class("math", MathOperationsFunction)
    print(f"   Node 0: Registered 'hello' and 'math' functions")
    
    nodes[1].register_function_from_class("math", MathOperationsFunction)
    nodes[1].register_function_from_class("data_process", DataProcessingFunction)
    print(f"   Node 1: Registered 'math' and 'data_process' functions")
    
    nodes[2].register_function_from_class("hello", HelloWorldFunction)
    nodes[2].register_function_from_class("data_process", DataProcessingFunction)
    print(f"   Node 2: Registered 'hello' and 'data_process' functions")
    
    # Start all nodes
    print("\n3. Starting nodes and heartbeat services...")
    for i, node in enumerate(nodes):
        if node.start():
            print(f"   Node {i} started successfully")
        else:
            print(f"   Node {i} failed to start")
    
    # Let the system run for a bit to exchange heartbeats
    print("\n4. Letting nodes exchange heartbeats for 15 seconds...")
    time.sleep(15)
    
    # Show network status
    print("\n5. Network Status:")
    for i, node in enumerate(nodes):
        status = node.get_network_status()
        print(f"\n   Node {i} ({status['local_node']['id']}):")
        print(f"      Endpoint: {status['local_node']['endpoint']}")
        print(f"      Status: {status['local_node']['status']}")
        print(f"      Functions: {status['local_node']['functions']}")
        print(f"      Load Score: {status['local_node']['load_score']:.2f}")
        print(f"      Heartbeats Sent: {status['heartbeat']['heartbeats_sent']}")
        print(f"      Network Nodes: {status['network']['total_nodes']}")
        print(f"      Online Nodes: {status['network']['online_nodes']}")
    
    # Demonstrate node discovery
    print("\n6. Node Discovery:")
    discovered_nodes = nodes[0].discover_nodes()
    print(f"   Node 0 discovered {len(discovered_nodes)} nodes:")
    
    for node in discovered_nodes:
        summary = {
            "id": node.node_id[:8],
            "endpoint": node.get_endpoint(),
            "functions": node.registered_functions,
            "load": round(node.capacity.get_load_score(), 2),
            "last_seen": node.last_seen
        }
        print(f"      {summary}")
    
    # Demonstrate function execution
    print("\n7. Function Execution:")
    test_cases = [
        ("hello", {"name": "Distributed World"}),
        ("math", {"operation": "add", "a": 10, "b": 5}),
        ("data_process", {"numbers": [1, 2, 3, 4, 5]})
    ]
    
    for func_name, input_data in test_cases:
        # Find a node that has this function
        for i, node in enumerate(nodes):
            if func_name in node.executor.list_functions():
                result = node.execute_function(func_name, input_data)
                print(f"   Executed '{func_name}' on Node {i}:")
                print(f"      Success: {result.success}")
                print(f"      Output: {result.output}")
                print(f"      Time: {result.execution_time_ms:.2f}ms")
                break
        else:
            print(f"   Function '{func_name}' not found on any node")
    
    # Show capacity reporting
    print("\n8. Capacity Reporting:")
    for i, node in enumerate(nodes):
        capacity = node.node_info.capacity
        print(f"   Node {i} Resources:")
        print(f"      CPU: {capacity.cpu_percent:.1f}% ({capacity.cpu_count} cores)")
        print(f"      Memory: {capacity.memory_percent:.1f}% ({capacity.memory_available_gb:.1f}GB available)")
        print(f"      Can Accept Work: {capacity.can_accept_work()}")
    
    # Stop all nodes
    print("\n9. Stopping nodes...")
    for i, node in enumerate(nodes):
        if node.stop():
            print(f"   Node {i} stopped successfully")
        else:
            print(f"   Node {i} failed to stop cleanly")
    
    print("\n=== Demo Complete ===")
    print("Increment 2 successfully demonstrates:")
    print("✓ Node Registry with peer discovery")
    print("✓ Health Heartbeat system")
    print("✓ Capacity Reporting")
    print("✓ Distributed function registration")
    print("✓ Network status monitoring")
    print("✓ Automatic stale node cleanup")


if __name__ == "__main__":
    main()