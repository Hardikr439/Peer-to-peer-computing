"""
Test Suite for Increment 3: Basic Remote Execution (RPC/API)

This module tests the RPC communication layer including protocol definitions,
HTTP server, and client functionality.
"""

import json
import time
import unittest
import threading
from unittest.mock import patch, MagicMock
import urllib.request
import urllib.error

from src.communication import (
    RPCRequest, RPCResponse, RequestType, ResponseStatus, NodeEndpoint,
    RPCServer, RPCClient, RPCError, RPCTimeoutError, ConnectionError,
    validate_rpc_request
)
from src.core.local_executor import LocalExecutor
from src.core.function_interface import ExecutionResult
from src.networking.node_info import NodeInfo, NodeRole, NodeStatus, SystemCapacity
from examples.sample_functions import HelloWorldFunction, MathOperationsFunction


class TestRPCProtocol(unittest.TestCase):
    """Test RPC protocol components."""
    
    def test_rpc_request_creation(self):
        """Test RPC request creation."""
        # Test execute request
        request = RPCRequest.create_execute_request(
            request_id="test-123",
            function_name="hello_world",
            input_data={"name": "Alice"},
            timeout_seconds=30.0,
            metadata={"priority": "high"}
        )
        
        self.assertEqual(request.request_id, "test-123")
        self.assertEqual(request.request_type, RequestType.EXECUTE_FUNCTION)
        self.assertEqual(request.function_name, "hello_world")
        self.assertEqual(request.input_data, {"name": "Alice"})
        self.assertEqual(request.timeout_seconds, 30.0)
        self.assertEqual(request.metadata, {"priority": "high"})
        
        # Test ping request
        ping_request = RPCRequest.create_ping_request(
            request_id="ping-456",
            metadata={"source": "test"}
        )
        
        self.assertEqual(ping_request.request_id, "ping-456")
        self.assertEqual(ping_request.request_type, RequestType.PING)
        self.assertIsNone(ping_request.function_name)
        self.assertIsNone(ping_request.input_data)
    
    def test_rpc_request_serialization(self):
        """Test RPC request serialization."""
        request = RPCRequest.create_execute_request(
            request_id="test-123",
            function_name="hello_world",
            input_data={"name": "Alice"}
        )
        
        # Test to_dict
        request_dict = request.to_dict()
        self.assertIsInstance(request_dict, dict)
        self.assertEqual(request_dict["request_id"], "test-123")
        self.assertEqual(request_dict["request_type"], "execute_function")
        
        # Test from_dict
        deserialized = RPCRequest.from_dict(request_dict)
        self.assertEqual(deserialized.request_id, request.request_id)
        self.assertEqual(deserialized.request_type, request.request_type)
        self.assertEqual(deserialized.function_name, request.function_name)
        self.assertEqual(deserialized.input_data, request.input_data)
    
    def test_rpc_response_creation(self):
        """Test RPC response creation."""
        # Test success response
        result = ExecutionResult(
            success=True,
            output="Hello Alice!",
            error_message=None,
            execution_time=0.1
        )
        
        response = RPCResponse.create_success_response(
            request_id="test-123",
            result="Hello Alice!",
            execution_result=result
        )
        
        self.assertEqual(response.request_id, "test-123")
        self.assertEqual(response.status, ResponseStatus.SUCCESS)
        self.assertEqual(response.result, "Hello Alice!")
        self.assertEqual(response.execution_result, result)
        self.assertIsNone(response.error_message)
        
        # Test error response
        error_response = RPCResponse.create_error_response(
            request_id="test-456",
            error_message="Function not found",
            status=ResponseStatus.NOT_FOUND
        )
        
        self.assertEqual(error_response.request_id, "test-456")
        self.assertEqual(error_response.status, ResponseStatus.NOT_FOUND)
        self.assertEqual(error_response.error_message, "Function not found")
        self.assertIsNone(error_response.result)
    
    def test_rpc_response_serialization(self):
        """Test RPC response serialization."""
        response = RPCResponse.create_success_response(
            request_id="test-123",
            result="Hello World!"
        )
        
        # Test to_dict
        response_dict = response.to_dict()
        self.assertIsInstance(response_dict, dict)
        self.assertEqual(response_dict["request_id"], "test-123")
        self.assertEqual(response_dict["status"], "success")
        
        # Test from_dict
        deserialized = RPCResponse.from_dict(response_dict)
        self.assertEqual(deserialized.request_id, response.request_id)
        self.assertEqual(deserialized.status, response.status)
        self.assertEqual(deserialized.result, response.result)
    
    def test_node_endpoint(self):
        """Test NodeEndpoint functionality."""
        endpoint = NodeEndpoint("192.168.1.100", 8080)
        
        self.assertEqual(endpoint.ip_address, "192.168.1.100")
        self.assertEqual(endpoint.port, 8080)
        self.assertEqual(str(endpoint), "192.168.1.100:8080")
        
        # Test from_string
        endpoint2 = NodeEndpoint.from_string("10.0.0.1:9000")
        self.assertEqual(endpoint2.ip_address, "10.0.0.1")
        self.assertEqual(endpoint2.port, 9000)
    
    def test_request_validation(self):
        """Test RPC request validation."""
        # Valid request
        valid_request = RPCRequest.create_execute_request(
            request_id="test-123",
            function_name="hello_world"
        )
        errors = validate_rpc_request(valid_request)
        self.assertEqual(len(errors), 0)
        
        # Invalid request - missing request_id
        invalid_request = RPCRequest(
            request_id="",
            request_type=RequestType.EXECUTE_FUNCTION,
            function_name="hello_world"
        )
        errors = validate_rpc_request(invalid_request)
        self.assertGreater(len(errors), 0)
        self.assertTrue(any("request_id" in error for error in errors))


class TestRPCServer(unittest.TestCase):
    """Test RPC server functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create node info
        capacity = SystemCapacity(cpu_count=4)
        self.node_info = NodeInfo(
            hostname="test-node",
            ip_address="127.0.0.1",
            port=8080,
            capacity=capacity,
            role=NodeRole.WORKER
        )
        
        # Create executor with sample functions
        self.executor = LocalExecutor()
        self.executor.register_function("hello_world", HelloWorldFunction())
        self.executor.register_function("math_add", MathOperationsFunction())
        
        # Create server
        self.server = RPCServer(
            node_info=self.node_info,
            executor=self.executor,
            port=0  # Use random port for testing
        )
    
    def tearDown(self):
        """Clean up after tests."""
        if self.server.is_running:
            self.server.stop()
    
    def test_server_start_stop(self):
        """Test server startup and shutdown."""
        # Test start
        self.assertFalse(self.server.is_running)
        success = self.server.start()
        self.assertTrue(success)
        self.assertTrue(self.server.is_running)
        
        # Give server time to start
        time.sleep(0.1)
        
        # Test stop
        success = self.server.stop()
        self.assertTrue(success)
        self.assertFalse(self.server.is_running)
    
    def test_server_endpoints(self):
        """Test server HTTP endpoints."""
        # Start server
        self.server.start()
        time.sleep(0.1)
        
        # Get actual port (since we used port=0)
        actual_port = self.server.server.server_address[1]
        base_url = f"http://127.0.0.1:{actual_port}"
        
        try:
            # Test health endpoint
            with urllib.request.urlopen(f"{base_url}/health", timeout=5) as response:
                data = json.loads(response.read().decode())
                self.assertEqual(data["status"], "healthy")
                self.assertEqual(data["node_id"], self.node_info.node_id)
            
            # Test status endpoint
            with urllib.request.urlopen(f"{base_url}/status", timeout=5) as response:
                data = json.loads(response.read().decode())
                self.assertEqual(data["node_id"], self.node_info.node_id)
                self.assertIn("capacity", data)
            
            # Test functions endpoint
            with urllib.request.urlopen(f"{base_url}/functions", timeout=5) as response:
                data = json.loads(response.read().decode())
                self.assertIn("functions", data)
                self.assertIn("hello_world", data["functions"])
                self.assertIn("math_add", data["functions"])
        
        finally:
            self.server.stop()
    
    def test_rpc_execute_endpoint(self):
        """Test RPC execute endpoint."""
        # Start server
        self.server.start()
        time.sleep(0.1)
        
        actual_port = self.server.server.server_address[1]
        url = f"http://127.0.0.1:{actual_port}/rpc/execute"
        
        try:
            # Test successful execution
            request_data = {
                "request_id": "test-123",
                "request_type": "execute_function",
                "function_name": "hello_world",
                "input_data": {"name": "Alice"},
                "timeout_seconds": 10.0
            }
            
            req = urllib.request.Request(
                url,
                data=json.dumps(request_data).encode(),
                headers={'Content-Type': 'application/json'}
            )
            
            with urllib.request.urlopen(req, timeout=5) as response:
                data = json.loads(response.read().decode())
                self.assertEqual(data["request_id"], "test-123")
                self.assertEqual(data["status"], "success")
                self.assertIn("Hello Alice", data["result"])
            
            # Test function not found
            request_data["function_name"] = "nonexistent_function"
            req = urllib.request.Request(
                url,
                data=json.dumps(request_data).encode(),
                headers={'Content-Type': 'application/json'}
            )
            
            try:
                with urllib.request.urlopen(req, timeout=5) as response:
                    pass
            except urllib.error.HTTPError as e:
                self.assertEqual(e.code, 404)
                error_data = json.loads(e.read().decode())
                self.assertEqual(error_data["status"], "not_found")
        
        finally:
            self.server.stop()
    
    def test_rpc_ping_endpoint(self):
        """Test RPC ping endpoint."""
        # Start server
        self.server.start()
        time.sleep(0.1)
        
        actual_port = self.server.server.server_address[1]
        url = f"http://127.0.0.1:{actual_port}/rpc/ping"
        
        try:
            # Test ping with request body
            request_data = {
                "request_id": "ping-123",
                "request_type": "ping"
            }
            
            req = urllib.request.Request(
                url,
                data=json.dumps(request_data).encode(),
                headers={'Content-Type': 'application/json'}
            )
            
            with urllib.request.urlopen(req, timeout=5) as response:
                data = json.loads(response.read().decode())
                self.assertEqual(data["request_id"], "ping-123")
                self.assertEqual(data["status"], "success")
                self.assertEqual(data["result"]["message"], "pong")
            
            # Test ping without request body
            req = urllib.request.Request(url, data=b'')
            with urllib.request.urlopen(req, timeout=5) as response:
                data = json.loads(response.read().decode())
                self.assertEqual(data["status"], "success")
                self.assertEqual(data["result"]["message"], "pong")
        
        finally:
            self.server.stop()
    
    def test_server_stats(self):
        """Test server statistics."""
        stats = self.server.get_stats()
        
        self.assertIn("requests_handled", stats)
        self.assertIn("functions_executed", stats)
        self.assertIn("errors_encountered", stats)
        self.assertIn("is_running", stats)
        self.assertIn("endpoint", stats)
    
    def test_get_endpoint_url(self):
        """Test endpoint URL generation."""
        url = self.server.get_endpoint_url()
        expected = f"http://{self.node_info.ip_address}:{self.node_info.port}"
        self.assertEqual(url, expected)
        
        url_with_path = self.server.get_endpoint_url("/health")
        expected_with_path = f"http://{self.node_info.ip_address}:{self.node_info.port}/health"
        self.assertEqual(url_with_path, expected_with_path)


class TestRPCClient(unittest.TestCase):
    """Test RPC client functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.client = RPCClient(default_timeout=5.0, max_retries=2)
        self.endpoint = NodeEndpoint("127.0.0.1", 8080)
    
    def test_client_stats(self):
        """Test client statistics."""
        stats = self.client.get_stats()
        
        self.assertIn("requests_sent", stats)
        self.assertIn("successful_requests", stats)
        self.assertIn("failed_requests", stats)
        self.assertIn("success_rate", stats)
        self.assertIn("average_response_time", stats)
        
        # Initial stats should be zero
        self.assertEqual(stats["requests_sent"], 0)
        self.assertEqual(stats["successful_requests"], 0)
        self.assertEqual(stats["success_rate"], 0.0)
    
    def test_stats_reset(self):
        """Test statistics reset."""
        # Modify stats
        self.client.stats["requests_sent"] = 10
        self.client.stats["successful_requests"] = 8
        
        # Reset
        self.client.reset_stats()
        
        stats = self.client.get_stats()
        self.assertEqual(stats["requests_sent"], 0)
        self.assertEqual(stats["successful_requests"], 0)
    
    @patch('urllib.request.urlopen')
    def test_execute_function_success(self, mock_urlopen):
        """Test successful function execution."""
        # Mock response
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "request_id": "test-123",
            "status": "success",
            "result": "Hello Alice!",
            "execution_result": {
                "success": True,
                "output": "Hello Alice!",
                "error_message": None,
                "execution_time": 0.1
            }
        }).encode()
        mock_response.__enter__.return_value = mock_response
        mock_urlopen.return_value = mock_response
        
        # Execute function
        result = self.client.execute_function(
            endpoint=self.endpoint,
            function_name="hello_world",
            input_data={"name": "Alice"}
        )
        
        self.assertTrue(result.success)
        self.assertEqual(result.output, "Hello Alice!")
        self.assertIsNone(result.error_message)
        
        # Check stats
        stats = self.client.get_stats()
        self.assertEqual(stats["requests_sent"], 1)
        self.assertEqual(stats["successful_requests"], 1)
    
    @patch('urllib.request.urlopen')
    def test_execute_function_error(self, mock_urlopen):
        """Test function execution error."""
        # Mock error response
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "request_id": "test-123",
            "status": "error",
            "error_message": "Function failed",
            "result": None
        }).encode()
        mock_response.__enter__.return_value = mock_response
        mock_urlopen.return_value = mock_response
        
        # Execute function
        result = self.client.execute_function(
            endpoint=self.endpoint,
            function_name="failing_function"
        )
        
        self.assertFalse(result.success)
        self.assertEqual(result.error_message, "Function failed")
        self.assertIsNone(result.output)
        
        # Check stats
        stats = self.client.get_stats()
        self.assertEqual(stats["requests_sent"], 1)
        self.assertEqual(stats["failed_requests"], 1)
    
    @patch('urllib.request.urlopen')
    def test_ping_node_success(self, mock_urlopen):
        """Test successful node ping."""
        # Mock response
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "request_id": "ping-123",
            "status": "success",
            "result": {
                "message": "pong",
                "node_id": "test-node-id",
                "timestamp": time.time()
            }
        }).encode()
        mock_response.__enter__.return_value = mock_response
        mock_urlopen.return_value = mock_response
        
        # Ping node
        success, response_time, error = self.client.ping_node(self.endpoint)
        
        self.assertTrue(success)
        self.assertGreater(response_time, 0)
        self.assertIsNone(error)
    
    @patch('urllib.request.urlopen')
    def test_ping_node_failure(self, mock_urlopen):
        """Test node ping failure."""
        # Mock connection error
        mock_urlopen.side_effect = urllib.error.URLError("Connection refused")
        
        # Ping node
        success, response_time, error = self.client.ping_node(self.endpoint)
        
        self.assertFalse(success)
        self.assertGreater(response_time, 0)
        self.assertIsNotNone(error)
    
    @patch('urllib.request.urlopen')
    def test_get_node_status(self, mock_urlopen):
        """Test getting node status."""
        # Mock response
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "node_id": "test-node",
            "status": "active",
            "capacity": {"cpu_percent": 25.0}
        }).encode()
        mock_response.__enter__.return_value = mock_response
        mock_urlopen.return_value = mock_response
        
        # Get status
        status = self.client.get_node_status(self.endpoint)
        
        self.assertIsNotNone(status)
        self.assertEqual(status["node_id"], "test-node")
        self.assertEqual(status["status"], "active")
    
    @patch('urllib.request.urlopen')
    def test_list_remote_functions(self, mock_urlopen):
        """Test listing remote functions."""
        # Mock response
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "functions": ["hello_world", "math_add"],
            "total_count": 2
        }).encode()
        mock_response.__enter__.return_value = mock_response
        mock_urlopen.return_value = mock_response
        
        # List functions
        functions = self.client.list_remote_functions(self.endpoint)
        
        self.assertIsNotNone(functions)
        self.assertEqual(functions["total_count"], 2)
        self.assertIn("hello_world", functions["functions"])
        self.assertIn("math_add", functions["functions"])
    
    @patch('urllib.request.urlopen')
    def test_health_check(self, mock_urlopen):
        """Test health check."""
        # Mock response
        mock_response = MagicMock()
        mock_response.read.return_value = json.dumps({
            "status": "healthy",
            "node_id": "test-node"
        }).encode()
        mock_response.__enter__.return_value = mock_response
        mock_urlopen.return_value = mock_response
        
        # Health check
        is_healthy, health_data = self.client.health_check(self.endpoint)
        
        self.assertTrue(is_healthy)
        self.assertIsNotNone(health_data)
        self.assertEqual(health_data["status"], "healthy")


class TestIntegratedRPCCommunication(unittest.TestCase):
    """Test integrated RPC communication between server and client."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create node info
        capacity = SystemCapacity(cpu_count=4)
        self.node_info = NodeInfo(
            hostname="test-node",
            ip_address="127.0.0.1",
            port=0,  # Use random port
            capacity=capacity,
            role=NodeRole.WORKER
        )
        
        # Create executor with sample functions
        self.executor = LocalExecutor()
        self.executor.register_function("hello_world", HelloWorldFunction())
        self.executor.register_function("math_add", MathOperationsFunction())
        
        # Create server
        self.server = RPCServer(
            node_info=self.node_info,
            executor=self.executor
        )
        
        # Create client
        self.client = RPCClient(default_timeout=5.0)
    
    def tearDown(self):
        """Clean up after tests."""
        if self.server.is_running:
            self.server.stop()
    
    def test_end_to_end_function_execution(self):
        """Test complete end-to-end function execution."""
        # Start server
        self.server.start()
        time.sleep(0.1)
        
        # Get actual server endpoint
        actual_port = self.server.server.server_address[1]
        endpoint = NodeEndpoint("127.0.0.1", actual_port)
        
        try:
            # Test successful execution
            result = self.client.execute_function(
                endpoint=endpoint,
                function_name="hello_world",
                input_data={"name": "Alice"}
            )
            
            self.assertTrue(result.success)
            self.assertIn("Hello Alice", result.output)
            self.assertIsNone(result.error_message)
            self.assertGreater(result.execution_time, 0)
            
            # Test function with parameters
            result = self.client.execute_function(
                endpoint=endpoint,
                function_name="math_add",
                input_data={"operation": "add", "a": 5, "b": 3}
            )
            
            self.assertTrue(result.success)
            self.assertEqual(result.output["result"], 8)
            
            # Test function not found
            result = self.client.execute_function(
                endpoint=endpoint,
                function_name="nonexistent_function"
            )
            
            self.assertFalse(result.success)
            self.assertIn("not found", result.error_message.lower())
        
        finally:
            self.server.stop()
    
    def test_end_to_end_ping(self):
        """Test complete end-to-end ping communication."""
        # Start server
        self.server.start()
        time.sleep(0.1)
        
        actual_port = self.server.server.server_address[1]
        endpoint = NodeEndpoint("127.0.0.1", actual_port)
        
        try:
            # Test ping
            success, response_time, error = self.client.ping_node(endpoint)
            
            self.assertTrue(success)
            self.assertGreater(response_time, 0)
            self.assertIsNone(error)
        
        finally:
            self.server.stop()
    
    def test_end_to_end_node_discovery(self):
        """Test complete end-to-end node discovery."""
        # Start server
        self.server.start()
        time.sleep(0.1)
        
        actual_port = self.server.server.server_address[1]
        endpoint = NodeEndpoint("127.0.0.1", actual_port)
        
        try:
            # Test health check
            is_healthy, health_data = self.client.health_check(endpoint)
            self.assertTrue(is_healthy)
            self.assertEqual(health_data["status"], "healthy")
            
            # Test status retrieval
            status = self.client.get_node_status(endpoint)
            self.assertIsNotNone(status)
            self.assertEqual(status["node_id"], self.node_info.node_id)
            
            # Test function listing
            functions = self.client.list_remote_functions(endpoint)
            self.assertIsNotNone(functions)
            self.assertIn("hello_world", functions["functions"])
            self.assertIn("math_add", functions["functions"])
        
        finally:
            self.server.stop()


if __name__ == "__main__":
    # Set up logging for tests
    import logging
    logging.basicConfig(level=logging.WARNING)
    
    # Run tests
    unittest.main(verbosity=2)
