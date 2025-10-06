"""
Test suite for Increment 2: Peer-to-Peer Resource Discovery and Health Check

Tests the networking components including node information, registry, and heartbeat systems.
"""

import unittest
import time
import threading
import tempfile
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from networking import (
    NodeInfo, NodeRole, NodeStatus, SystemCapacity, HeartbeatMessage,
    NetworkAddress, create_node_summary,
    NodeRegistry, InMemoryRegistry, FileBasedRegistry, create_registry,
    HeartbeatService, HeartbeatMonitor, HeartbeatStatus
)


class TestSystemCapacity(unittest.TestCase):
    """Test the SystemCapacity class."""
    
    def test_capacity_creation(self):
        """Test creating system capacity instance."""
        capacity = SystemCapacity()
        
        # Should auto-populate from system
        self.assertGreater(capacity.cpu_count, 0)
        self.assertGreaterEqual(capacity.cpu_percent, 0)
        self.assertGreaterEqual(capacity.memory_total_gb, 0)
        self.assertGreaterEqual(capacity.memory_available_gb, 0)
    
    def test_load_score_calculation(self):
        """Test load score calculation."""
        # Create capacity with specific values, avoiding auto-update
        capacity = SystemCapacity(
            cpu_count=8,  # Set to avoid auto-update
            cpu_percent=50.0,
            memory_percent=60.0,
            disk_percent=30.0
        )
        
        # Expected: 0.5 * 0.5 + 0.4 * 0.6 + 0.1 * 0.3 = 0.25 + 0.24 + 0.03 = 0.52
        expected_score = 0.52
        actual_score = capacity.get_load_score()
        self.assertAlmostEqual(actual_score, expected_score, places=2)
    
    def test_can_accept_work(self):
        """Test work acceptance logic."""
        # Low load - should accept work
        capacity1 = SystemCapacity(cpu_count=8, cpu_percent=50.0, memory_percent=60.0)
        self.assertTrue(capacity1.can_accept_work())
        
        # High CPU - should not accept work
        capacity2 = SystemCapacity(cpu_count=8, cpu_percent=90.0, memory_percent=60.0)
        self.assertFalse(capacity2.can_accept_work())
        
        # High memory - should not accept work
        capacity3 = SystemCapacity(cpu_count=8, cpu_percent=50.0, memory_percent=95.0)
        self.assertFalse(capacity3.can_accept_work())


class TestNodeInfo(unittest.TestCase):
    """Test the NodeInfo class."""
    
    def test_node_creation(self):
        """Test creating a node info instance."""
        node = NodeInfo(
            node_id="test-node-123",
            hostname="testhost",
            ip_address="192.168.1.100",
            port=8000
        )
        
        self.assertEqual(node.node_id, "test-node-123")
        self.assertEqual(node.hostname, "testhost")
        self.assertEqual(node.ip_address, "192.168.1.100")
        self.assertEqual(node.port, 8000)
        self.assertEqual(node.status, NodeStatus.ONLINE)
        self.assertEqual(node.role, NodeRole.HYBRID)
        self.assertIsNotNone(node.last_seen)
        self.assertIsInstance(node.capacity, SystemCapacity)
    
    def test_local_node_creation(self):
        """Test creating a local node."""
        node = NodeInfo.create_local_node(port=8000)
        
        self.assertIsNotNone(node.node_id)
        self.assertIsNotNone(node.hostname)
        self.assertIsNotNone(node.ip_address)
        self.assertEqual(node.port, 8000)
        self.assertEqual(node.status, NodeStatus.ONLINE)
    
    def test_node_serialization(self):
        """Test node serialization and deserialization."""
        original_node = NodeInfo(
            node_id="test-node",
            hostname="testhost",
            ip_address="192.168.1.100",
            port=8000,
            status=NodeStatus.ONLINE,
            role=NodeRole.WORKER
        )
        
        # Test to_dict and from_dict
        node_dict = original_node.to_dict()
        reconstructed_node = NodeInfo.from_dict(node_dict)
        
        self.assertEqual(reconstructed_node.node_id, original_node.node_id)
        self.assertEqual(reconstructed_node.hostname, original_node.hostname)
        self.assertEqual(reconstructed_node.status, original_node.status)
        self.assertEqual(reconstructed_node.role, original_node.role)
        
        # Test JSON serialization
        json_str = original_node.to_json()
        node_from_json = NodeInfo.from_json(json_str)
        self.assertEqual(node_from_json.node_id, original_node.node_id)
    
    def test_node_staleness(self):
        """Test node staleness detection."""
        node = NodeInfo(
            node_id="test-node",
            hostname="testhost", 
            ip_address="192.168.1.100",
            port=8000
        )
        
        # Fresh node should not be stale
        self.assertFalse(node.is_stale(max_age_seconds=60))
        
        # Manually set an old timestamp
        old_time = datetime.utcnow() - timedelta(seconds=120)
        node.last_seen = old_time.isoformat()
        
        # Should now be stale
        self.assertTrue(node.is_stale(max_age_seconds=60))


class TestHeartbeatMessage(unittest.TestCase):
    """Test the HeartbeatMessage class."""
    
    def test_heartbeat_creation(self):
        """Test creating heartbeat messages."""
        capacity = SystemCapacity(cpu_percent=50.0, memory_percent=60.0)
        
        heartbeat = HeartbeatMessage(
            node_id="test-node",
            timestamp="2023-01-01T12:00:00",
            capacity=capacity,
            status=NodeStatus.ONLINE,
            registered_functions=["func1", "func2"]
        )
        
        self.assertEqual(heartbeat.node_id, "test-node")
        self.assertEqual(heartbeat.status, NodeStatus.ONLINE)
        self.assertEqual(heartbeat.registered_functions, ["func1", "func2"])
        self.assertIsInstance(heartbeat.capacity, SystemCapacity)
    
    def test_heartbeat_serialization(self):
        """Test heartbeat serialization."""
        capacity = SystemCapacity(cpu_percent=50.0, memory_percent=60.0)
        original_heartbeat = HeartbeatMessage(
            node_id="test-node",
            timestamp="2023-01-01T12:00:00",
            capacity=capacity
        )
        
        # Test JSON serialization
        json_str = original_heartbeat.to_json()
        reconstructed = HeartbeatMessage.from_json(json_str)
        
        self.assertEqual(reconstructed.node_id, original_heartbeat.node_id)
        self.assertEqual(reconstructed.status, original_heartbeat.status)


class TestNetworkAddress(unittest.TestCase):
    """Test the NetworkAddress utility class."""
    
    def test_get_local_ip(self):
        """Test getting local IP address."""
        ip = NetworkAddress.get_local_ip()
        self.assertIsInstance(ip, str)
        # Should be either a real IP or localhost
        self.assertTrue(ip == "127.0.0.1" or len(ip.split('.')) == 4)
    
    def test_port_availability(self):
        """Test port availability checking."""
        # Test with a very high port that should be available
        self.assertTrue(NetworkAddress.is_port_available("localhost", 59999))
        
        # Test finding an available port
        port = NetworkAddress.find_available_port("localhost", 58000, 100)
        self.assertGreaterEqual(port, 58000)
        self.assertLess(port, 58100)


class TestInMemoryRegistry(unittest.TestCase):
    """Test the InMemoryRegistry class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.registry = InMemoryRegistry()
        self.test_node = NodeInfo(
            node_id="test-node-123",
            hostname="testhost",
            ip_address="192.168.1.100",
            port=8000
        )
    
    def test_node_registration(self):
        """Test node registration and retrieval."""
        # Register node
        result = self.registry.register_node(self.test_node)
        self.assertTrue(result)
        
        # Retrieve node
        retrieved_node = self.registry.get_node("test-node-123")
        self.assertIsNotNone(retrieved_node)
        self.assertEqual(retrieved_node.node_id, "test-node-123")
        
        # List nodes
        nodes = self.registry.list_nodes()
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].node_id, "test-node-123")
    
    def test_node_unregistration(self):
        """Test node unregistration."""
        # Register then unregister
        self.registry.register_node(self.test_node)
        result = self.registry.unregister_node("test-node-123")
        self.assertTrue(result)
        
        # Should not be retrievable
        retrieved_node = self.registry.get_node("test-node-123")
        self.assertIsNone(retrieved_node)
        
        # List should be empty
        nodes = self.registry.list_nodes()
        self.assertEqual(len(nodes), 0)
    
    def test_node_update(self):
        """Test node updates via heartbeat."""
        # Register node
        self.registry.register_node(self.test_node)
        
        # Create heartbeat with updated info
        new_capacity = SystemCapacity(cpu_count=8, cpu_percent=75.0, memory_percent=80.0)
        heartbeat = HeartbeatMessage(
            node_id="test-node-123",
            timestamp=datetime.utcnow().isoformat(),
            capacity=new_capacity,
            status=NodeStatus.BUSY,
            registered_functions=["new_func"]
        )
        
        # Update node
        result = self.registry.update_node("test-node-123", heartbeat)
        self.assertTrue(result)
        
        # Verify update
        updated_node = self.registry.get_node("test-node-123")
        self.assertEqual(updated_node.status, NodeStatus.BUSY)
        self.assertEqual(updated_node.registered_functions, ["new_func"])
        self.assertEqual(updated_node.capacity.cpu_percent, 75.0)
    
    def test_status_filtering(self):
        """Test filtering nodes by status."""
        # Create nodes with different statuses
        node1 = NodeInfo("node1", "host1", "1.1.1.1", 8001, status=NodeStatus.ONLINE)
        node2 = NodeInfo("node2", "host2", "1.1.1.2", 8002, status=NodeStatus.BUSY)
        node3 = NodeInfo("node3", "host3", "1.1.1.3", 8003, status=NodeStatus.OFFLINE)
        
        self.registry.register_node(node1)
        self.registry.register_node(node2)
        self.registry.register_node(node3)
        
        # Test filtering
        online_nodes = self.registry.list_nodes(NodeStatus.ONLINE)
        self.assertEqual(len(online_nodes), 1)
        self.assertEqual(online_nodes[0].node_id, "node1")
        
        busy_nodes = self.registry.list_nodes(NodeStatus.BUSY)
        self.assertEqual(len(busy_nodes), 1)
        self.assertEqual(busy_nodes[0].node_id, "node2")
    
    def test_stale_node_cleanup(self):
        """Test cleanup of stale nodes."""
        # Create a stale node
        stale_node = NodeInfo("stale-node", "stalehost", "1.1.1.1", 8001)
        old_time = datetime.utcnow() - timedelta(seconds=120)
        stale_node.last_seen = old_time.isoformat()
        
        # Register both fresh and stale nodes
        self.registry.register_node(self.test_node)  # Fresh
        self.registry.register_node(stale_node)      # Stale
        
        # Cleanup stale nodes
        removed_count = self.registry.cleanup_stale_nodes(max_age_seconds=60)
        self.assertEqual(removed_count, 1)
        
        # Verify only fresh node remains
        nodes = self.registry.list_nodes()
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].node_id, "test-node-123")


class TestFileBasedRegistry(unittest.TestCase):
    """Test the FileBasedRegistry class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.temp_dir = tempfile.mkdtemp()
        self.registry_file = Path(self.temp_dir) / "test_registry.json"
        self.registry = FileBasedRegistry(self.registry_file)
        
        self.test_node = NodeInfo(
            node_id="test-node-123",
            hostname="testhost",
            ip_address="192.168.1.100",
            port=8000
        )
    
    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_file_registry_persistence(self):
        """Test that file registry persists data."""
        # Register node
        result = self.registry.register_node(self.test_node)
        self.assertTrue(result)
        
        # Create new registry instance with same file
        new_registry = FileBasedRegistry(self.registry_file)
        
        # Should be able to retrieve the node
        retrieved_node = new_registry.get_node("test-node-123")
        self.assertIsNotNone(retrieved_node)
        self.assertEqual(retrieved_node.node_id, "test-node-123")
    
    def test_file_registry_operations(self):
        """Test basic file registry operations."""
        # Test same operations as in-memory registry
        self.registry.register_node(self.test_node)
        
        retrieved_node = self.registry.get_node("test-node-123")
        self.assertIsNotNone(retrieved_node)
        
        nodes = self.registry.list_nodes()
        self.assertEqual(len(nodes), 1)
        
        # Test unregistration
        result = self.registry.unregister_node("test-node-123")
        self.assertTrue(result)
        
        nodes = self.registry.list_nodes()
        self.assertEqual(len(nodes), 0)


class TestHeartbeatService(unittest.TestCase):
    """Test the HeartbeatService class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.registry = InMemoryRegistry()
        self.node = NodeInfo.create_local_node(port=8000)
        self.heartbeat_service = HeartbeatService(
            node=self.node,
            registry=self.registry,
            heartbeat_interval=0.5,  # Fast interval for testing
            cleanup_interval=1.0,
            node_timeout=2.0
        )
    
    def tearDown(self):
        """Clean up test fixtures."""
        if self.heartbeat_service.get_status() == HeartbeatStatus.RUNNING:
            self.heartbeat_service.stop()
    
    def test_heartbeat_service_lifecycle(self):
        """Test starting and stopping heartbeat service."""
        # Initially stopped
        self.assertEqual(self.heartbeat_service.get_status(), HeartbeatStatus.STOPPED)
        
        # Start service
        result = self.heartbeat_service.start()
        self.assertTrue(result)
        self.assertEqual(self.heartbeat_service.get_status(), HeartbeatStatus.RUNNING)
        
        # Stop service
        result = self.heartbeat_service.stop()
        self.assertTrue(result)
        self.assertEqual(self.heartbeat_service.get_status(), HeartbeatStatus.STOPPED)
    
    def test_heartbeat_sending(self):
        """Test that heartbeats are sent and recorded."""
        # Start service
        self.heartbeat_service.start()
        
        # Wait for a few heartbeats
        time.sleep(2)
        
        # Check statistics
        stats = self.heartbeat_service.get_statistics()
        self.assertGreater(stats["heartbeats_sent"], 0)
        self.assertEqual(stats["heartbeats_failed"], 0)
        
        # Check that node is registered in registry
        registered_node = self.registry.get_node(self.node.node_id)
        self.assertIsNotNone(registered_node)
        
        # Stop service
        self.heartbeat_service.stop()
    
    def test_function_updates(self):
        """Test updating function list."""
        # Start service
        self.heartbeat_service.start()
        
        # Update functions
        test_functions = ["func1", "func2", "func3"]
        self.heartbeat_service.update_node_functions(test_functions)
        
        # Wait for heartbeat to propagate
        time.sleep(1)
        
        # Check that functions are registered
        registered_node = self.registry.get_node(self.node.node_id)
        self.assertEqual(registered_node.registered_functions, test_functions)
        
        # Stop service
        self.heartbeat_service.stop()


class TestHeartbeatMonitor(unittest.TestCase):
    """Test the HeartbeatMonitor class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.registry = InMemoryRegistry()
        self.monitor = HeartbeatMonitor(self.registry)
    
    def test_heartbeat_monitoring(self):
        """Test heartbeat monitoring and health calculation."""
        node_id = "test-node"
        
        # Record some heartbeats
        now = datetime.utcnow()
        self.monitor.record_heartbeat(node_id, now - timedelta(seconds=30))
        self.monitor.record_heartbeat(node_id, now - timedelta(seconds=20))
        self.monitor.record_heartbeat(node_id, now - timedelta(seconds=10))
        self.monitor.record_heartbeat(node_id, now)
        
        # Check health
        health = self.monitor.get_node_health(node_id)
        self.assertEqual(health["status"], "healthy")
        self.assertEqual(health["heartbeat_count"], 4)
        self.assertLess(health["seconds_since_last"], 5)
    
    def test_overall_health_statistics(self):
        """Test overall health statistics."""
        # Record heartbeats for multiple nodes
        now = datetime.utcnow()
        
        # Healthy node
        self.monitor.record_heartbeat("healthy-node", now)
        
        # Warning node (last heartbeat 45 seconds ago)
        self.monitor.record_heartbeat("warning-node", now - timedelta(seconds=45))
        
        # Unhealthy node (last heartbeat 90 seconds ago)
        self.monitor.record_heartbeat("unhealthy-node", now - timedelta(seconds=90))
        
        # Get overall health
        overall = self.monitor.get_overall_health()
        self.assertEqual(overall["total_nodes"], 3)
        self.assertEqual(overall["healthy"], 1)
        self.assertEqual(overall["warning"], 1)
        self.assertEqual(overall["unhealthy"], 1)


if __name__ == "__main__":
    # Create a test suite
    suite = unittest.TestSuite()
    
    # Add test cases
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestSystemCapacity))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestNodeInfo))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestHeartbeatMessage))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestNetworkAddress))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestInMemoryRegistry))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestFileBasedRegistry))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestHeartbeatService))
    suite.addTest(unittest.TestLoader().loadTestsFromTestCase(TestHeartbeatMonitor))
    
    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Exit with error code if tests failed
    sys.exit(0 if result.wasSuccessful() else 1)