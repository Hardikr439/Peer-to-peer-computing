"""
HTTP Server for RPC Endpoints

This module provides an HTTP server that exposes RPC endpoints for remote
function execution. It handles incoming requests and routes them to the
appropriate handlers.
"""

import json
import logging
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from typing import Dict, Any, Optional, Callable
import traceback

from .rpc_protocol import (
    RPCRequest, RPCResponse, RequestType, ResponseStatus,
    RPCError, FunctionNotFoundError, validate_rpc_request
)
from ..core.local_executor import LocalExecutor
from ..networking.node_info import NodeInfo


class RPCRequestHandler(BaseHTTPRequestHandler):
    """HTTP request handler for RPC endpoints."""
    
    def __init__(self, rpc_server, *args, **kwargs):
        """Initialize the request handler with reference to RPC server."""
        self.rpc_server = rpc_server
        super().__init__(*args, **kwargs)
    
    def log_message(self, format, *args):
        """Override to use our logger instead of stderr."""
        self.rpc_server.logger.debug(format % args)
    
    def do_GET(self):
        """Handle GET requests."""
        try:
            # Update stats for all GET requests
            self.rpc_server.stats["requests_handled"] += 1
            
            parsed_url = urlparse(self.path)
            path = parsed_url.path
            
            if path == '/health':
                self._handle_health_check()
            elif path == '/status':
                self._handle_status_request()
            elif path == '/functions':
                self._handle_list_functions()
            else:
                self._send_error_response(404, "Endpoint not found")
                
        except Exception as e:
            self.rpc_server.stats["errors_encountered"] += 1
            self.rpc_server.logger.error(f"Error handling GET request: {e}")
            self._send_error_response(500, f"Internal server error: {str(e)}")
    
    def do_POST(self):
        """Handle POST requests."""
        try:
            if self.path == '/rpc/execute':
                self._handle_execute_request()
            elif self.path == '/rpc/ping':
                self._handle_ping_request()
            else:
                self._send_error_response(404, "Endpoint not found")
                
        except Exception as e:
            self.rpc_server.logger.error(f"Error handling POST request: {e}")
            self._send_error_response(500, f"Internal server error: {str(e)}")
    
    def _handle_health_check(self):
        """Handle health check requests."""
        response = {
            "status": "healthy",
            "node_id": self.rpc_server.node_info.node_id,
            "timestamp": time.time()
        }
        self._send_json_response(200, response)
    
    def _handle_status_request(self):
        """Handle node status requests."""
        node = self.rpc_server.node_info
        capacity = node.capacity
        
        response = {
            "node_id": node.node_id,
            "hostname": node.hostname,
            "status": node.status.value,
            "role": node.role.value,
            "endpoint": node.get_endpoint(),
            "capacity": {
                "cpu_percent": capacity.cpu_percent,
                "memory_percent": capacity.memory_percent,
                "load_score": capacity.get_load_score(),
                "can_accept_work": capacity.can_accept_work()
            },
            "functions": node.registered_functions,
            "last_seen": node.last_seen
        }
        self._send_json_response(200, response)
    
    def _handle_list_functions(self):
        """Handle list functions requests."""
        functions = self.rpc_server.executor.list_functions()
        function_metadata = {}
        
        for func_name in functions:
            metadata = self.rpc_server.executor.get_function_metadata(func_name)
            function_metadata[func_name] = metadata
        
        response = {
            "functions": functions,
            "function_metadata": function_metadata,
            "total_count": len(functions)
        }
        self._send_json_response(200, response)
    
    def _handle_execute_request(self):
        """Handle function execution requests."""
        try:
            # Update stats
            self.rpc_server.stats["requests_handled"] += 1
            
            # Read request body
            content_length = int(self.headers.get('Content-Length', 0))
            if content_length == 0:
                self._send_error_response(400, "Missing request body")
                return
            
            body = self.rfile.read(content_length)
            request_data = json.loads(body.decode('utf-8'))
            
            # Parse RPC request
            rpc_request = RPCRequest.from_dict(request_data)
            
            # Validate request
            validation_errors = validate_rpc_request(rpc_request)
            if validation_errors:
                error_msg = "Validation errors: " + ", ".join(validation_errors)
                rpc_response = RPCResponse.create_error_response(
                    rpc_request.request_id, error_msg, ResponseStatus.INVALID_REQUEST
                )
                self._send_json_response(400, rpc_response.to_dict())
                return
            
            # Execute function
            execution_result = self.rpc_server.executor.execute_function(
                rpc_request.function_name,
                rpc_request.input_data,
                rpc_request.timeout_seconds
            )
            
            # Update function execution stats
            self.rpc_server.stats["functions_executed"] += 1
            
            # Create response
            if execution_result.success:
                rpc_response = RPCResponse.create_success_response(
                    rpc_request.request_id,
                    result=execution_result.output,
                    execution_result=execution_result
                )
                status_code = 200
            else:
                # Check if it's a function not found error
                if "not found" in execution_result.error_message.lower():
                    rpc_response = RPCResponse.create_error_response(
                        rpc_request.request_id,
                        execution_result.error_message,
                        ResponseStatus.NOT_FOUND
                    )
                    status_code = 404
                else:
                    rpc_response = RPCResponse.create_error_response(
                        rpc_request.request_id,
                        execution_result.error_message,
                        ResponseStatus.ERROR
                    )
                    status_code = 500
            
            self._send_json_response(status_code, rpc_response.to_dict())
            
        except json.JSONDecodeError:
            self.rpc_server.stats["errors_encountered"] += 1
            self._send_error_response(400, "Invalid JSON in request body")
        except Exception as e:
            self.rpc_server.stats["errors_encountered"] += 1
            self.rpc_server.logger.error(f"Error executing function: {e}")
            self._send_error_response(500, f"Execution error: {str(e)}")
    
    def _handle_ping_request(self):
        """Handle ping requests."""
        try:
            # Update stats
            self.rpc_server.stats["requests_handled"] += 1
            
            # Read request body
            content_length = int(self.headers.get('Content-Length', 0))
            if content_length > 0:
                body = self.rfile.read(content_length)
                request_data = json.loads(body.decode('utf-8'))
                rpc_request = RPCRequest.from_dict(request_data)
                request_id = rpc_request.request_id
            else:
                request_id = "ping-" + str(int(time.time()))
            
            # Create ping response
            rpc_response = RPCResponse.create_success_response(
                request_id,
                result={
                    "message": "pong",
                    "node_id": self.rpc_server.node_info.node_id,
                    "timestamp": time.time()
                }
            )
            
            self._send_json_response(200, rpc_response.to_dict())
            
        except Exception as e:
            self.rpc_server.stats["errors_encountered"] += 1
            self.rpc_server.logger.error(f"Error handling ping: {e}")
            self._send_error_response(500, f"Ping error: {str(e)}")
    
    def _send_json_response(self, status_code: int, data: Dict[str, Any]):
        """Send a JSON response."""
        response_body = json.dumps(data, default=str, indent=2)
        
        self.send_response(status_code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(response_body)))
        self.send_header('Access-Control-Allow-Origin', '*')  # Enable CORS
        self.end_headers()
        
        self.wfile.write(response_body.encode('utf-8'))
    
    def _send_error_response(self, status_code: int, message: str):
        """Send an error response."""
        error_data = {
            "error": message,
            "status_code": status_code,
            "timestamp": time.time()
        }
        self._send_json_response(status_code, error_data)


class RPCServer:
    """
    HTTP server that provides RPC endpoints for remote function execution.
    """
    
    def __init__(self,
                 node_info: NodeInfo,
                 executor: LocalExecutor,
                 host: str = "0.0.0.0",
                 port: Optional[int] = None):
        """
        Initialize the RPC server.
        
        Args:
            node_info: Information about this node
            executor: Local executor for running functions
            host: Host to bind the server to
            port: Port to bind the server to (uses node_info.port if None)
        """
        self.node_info = node_info
        self.executor = executor
        self.host = host
        self.port = port or node_info.port
        
        self.server: Optional[HTTPServer] = None
        self.server_thread: Optional[threading.Thread] = None
        self.is_running = False
        
        # Set up logging
        self.logger = logging.getLogger(f"{__name__}.{node_info.node_id[:8]}")
        
        # Statistics
        self.stats = {
            "requests_handled": 0,
            "functions_executed": 0,
            "errors_encountered": 0,
            "start_time": None
        }
    
    def start(self) -> bool:
        """
        Start the RPC server.
        
        Returns:
            True if started successfully, False otherwise
        """
        if self.is_running:
            self.logger.warning("RPC server is already running")
            return True
        
        try:
            # Create custom request handler with reference to this server
            def handler_factory(*args, **kwargs):
                return RPCRequestHandler(self, *args, **kwargs)
            
            # Create HTTP server
            self.server = HTTPServer((self.host, self.port), handler_factory)
            
            # Start server in a separate thread
            self.server_thread = threading.Thread(
                target=self._run_server,
                name=f"rpc-server-{self.node_info.node_id[:8]}",
                daemon=True
            )
            
            self.is_running = True
            self.stats["start_time"] = time.time()
            self.server_thread.start()
            
            self.logger.info(f"RPC server started on {self.host}:{self.port}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to start RPC server: {e}")
            self.is_running = False
            return False
    
    def stop(self, timeout: float = 5.0) -> bool:
        """
        Stop the RPC server.
        
        Args:
            timeout: Maximum time to wait for server to stop
            
        Returns:
            True if stopped successfully, False otherwise
        """
        if not self.is_running:
            return True
        
        try:
            self.is_running = False
            
            if self.server:
                self.server.shutdown()
                self.server.server_close()
            
            if self.server_thread and self.server_thread.is_alive():
                self.server_thread.join(timeout)
            
            self.logger.info("RPC server stopped")
            return True
            
        except Exception as e:
            self.logger.error(f"Error stopping RPC server: {e}")
            return False
    
    def _run_server(self):
        """Run the HTTP server (internal method)."""
        try:
            self.server.serve_forever()
        except Exception as e:
            if self.is_running:  # Only log if not intentionally stopped
                self.logger.error(f"RPC server error: {e}")
        finally:
            self.is_running = False
    
    def get_stats(self) -> Dict[str, Any]:
        """Get server statistics."""
        stats = self.stats.copy()
        if stats["start_time"]:
            stats["uptime_seconds"] = time.time() - stats["start_time"]
        else:
            stats["uptime_seconds"] = 0
        
        stats["is_running"] = self.is_running
        stats["endpoint"] = f"{self.host}:{self.port}"
        
        return stats
    
    def get_endpoint_url(self, path: str = "") -> str:
        """
        Get the full URL for an endpoint.
        
        Args:
            path: Optional path to append
            
        Returns:
            Full URL string
        """
        base_url = f"http://{self.node_info.ip_address}:{self.port}"
        if path:
            if not path.startswith('/'):
                path = '/' + path
            return base_url + path
        return base_url