"""
RPC Client for Remote Function Calls

This module provides a client for making RPC calls to remote nodes
for distributed function execution.
"""

import json
import logging
import time
import uuid
from typing import Dict, Any, Optional, List, Tuple
import urllib.request
import urllib.parse
import urllib.error
from dataclasses import asdict

from .rpc_protocol import (
    RPCRequest, RPCResponse, RequestType, ResponseStatus,
    NodeEndpoint, RPCError, RPCTimeoutError, ConnectionError,
    validate_rpc_request
)
from ..core.function_interface import ExecutionResult
from ..networking.node_info import NodeInfo


class RPCClient:
    """
    Client for making RPC calls to remote nodes.
    """
    
    def __init__(self,
                 default_timeout: float = 30.0,
                 max_retries: int = 3,
                 retry_delay: float = 1.0):
        """
        Initialize the RPC client.
        
        Args:
            default_timeout: Default timeout for requests in seconds
            max_retries: Maximum number of retry attempts
            retry_delay: Delay between retries in seconds
        """
        self.default_timeout = default_timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
        # Set up logging
        self.logger = logging.getLogger(__name__)
        
        # Statistics
        self.stats = {
            "requests_sent": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "total_response_time": 0.0,
            "retry_attempts": 0
        }
    
    def execute_function(self,
                        endpoint: NodeEndpoint,
                        function_name: str,
                        input_data: Any = None,
                        timeout_seconds: Optional[float] = None,
                        metadata: Optional[Dict[str, Any]] = None) -> ExecutionResult:
        """
        Execute a function on a remote node.
        
        Args:
            endpoint: Target node endpoint
            function_name: Name of the function to execute
            input_data: Input data for the function
            timeout_seconds: Execution timeout (uses default if None)
            metadata: Optional metadata for the request
            
        Returns:
            ExecutionResult from the remote execution
            
        Raises:
            RPCError: If the RPC call fails
            RPCTimeoutError: If the request times out
            ConnectionError: If connection to remote node fails
        """
        request_id = str(uuid.uuid4())
        timeout = timeout_seconds or self.default_timeout
        
        # Create RPC request
        rpc_request = RPCRequest.create_execute_request(
            function_name=function_name,
            input_data=input_data,
            source_node_id=metadata.get("source_node_id", "") if metadata else "",
            target_node_id=metadata.get("target_node_id", "") if metadata else "",
            timeout_seconds=timeout
        )
        
        # Validate request
        validation_errors = validate_rpc_request(rpc_request)
        if validation_errors:
            error_msg = "Invalid request: " + ", ".join(validation_errors)
            return ExecutionResult(
                success=False,
                output=None,
                error_message=error_msg,
                execution_time=0.0
            )
        
        start_time = time.time()
        
        try:
            # Send request with retries
            rpc_response = self._send_request_with_retry(
                endpoint, "/rpc/execute", rpc_request, timeout
            )
            
            execution_time = time.time() - start_time
            
            # Convert RPC response to ExecutionResult
            if rpc_response.status == ResponseStatus.SUCCESS:
                # If the response includes execution_result, use it
                if rpc_response.execution_result:
                    result = rpc_response.execution_result
                    result.execution_time_ms = execution_time * 1000  # Convert to milliseconds
                    return result
                else:
                    # Create ExecutionResult from response data
                    return ExecutionResult(
                        success=True,
                        output=rpc_response.result,
                        error_message=None,
                        execution_time_ms=execution_time * 1000
                    )
            else:
                return ExecutionResult(
                    success=False,
                    output=None,
                    error_message=rpc_response.error_message or "Remote execution failed",
                    execution_time_ms=execution_time * 1000
                )
                
        except (RPCTimeoutError, ConnectionError) as e:
            execution_time = time.time() - start_time
            return ExecutionResult(
                success=False,
                output=None,
                error_message=str(e),
                execution_time_ms=execution_time * 1000
            )
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"Unexpected error in execute_function: {e}")
            return ExecutionResult(
                success=False,
                output=None,
                error_message=f"Unexpected error: {str(e)}",
                execution_time_ms=execution_time * 1000
            )
    
    def ping_node(self,
                  endpoint: NodeEndpoint,
                  timeout_seconds: Optional[float] = None,
                  metadata: Optional[Dict[str, Any]] = None) -> Tuple[bool, float, Optional[str]]:
        """
        Ping a remote node to check if it's alive.
        
        Args:
            endpoint: Target node endpoint
            timeout_seconds: Ping timeout (uses default if None)
            metadata: Optional metadata for the request
            
        Returns:
            Tuple of (success, response_time, error_message)
        """
        request_id = str(uuid.uuid4())
        timeout = timeout_seconds or min(self.default_timeout, 10.0)  # Shorter timeout for pings
        
        # Create ping request
        rpc_request = RPCRequest.create_ping_request(
            source_node_id=metadata.get("source_node_id", "") if metadata else "",
            target_node_id=metadata.get("target_node_id", "") if metadata else ""
        )
        
        start_time = time.time()
        
        try:
            rpc_response = self._send_request_with_retry(
                endpoint, "/rpc/ping", rpc_request, timeout
            )
            
            response_time = time.time() - start_time
            
            if rpc_response.status == ResponseStatus.SUCCESS:
                return True, response_time, None
            else:
                return False, response_time, rpc_response.error_message
                
        except Exception as e:
            response_time = time.time() - start_time
            return False, response_time, str(e)
    
    def get_node_status(self,
                       endpoint: NodeEndpoint,
                       timeout_seconds: Optional[float] = None) -> Optional[Dict[str, Any]]:
        """
        Get status information from a remote node.
        
        Args:
            endpoint: Target node endpoint
            timeout_seconds: Request timeout (uses default if None)
            
        Returns:
            Node status dictionary or None if failed
        """
        timeout = timeout_seconds or self.default_timeout
        url = f"http://{endpoint.host}:{endpoint.port}/status"
        
        try:
            response_data = self._send_http_request("GET", url, None, timeout)
            return response_data
            
        except Exception as e:
            self.logger.error(f"Failed to get node status from {endpoint}: {e}")
            return None
    
    def list_remote_functions(self,
                            endpoint: NodeEndpoint,
                            timeout_seconds: Optional[float] = None) -> Optional[Dict[str, Any]]:
        """
        List available functions on a remote node.
        
        Args:
            endpoint: Target node endpoint
            timeout_seconds: Request timeout (uses default if None)
            
        Returns:
            Functions information dictionary or None if failed
        """
        timeout = timeout_seconds or self.default_timeout
        url = f"http://{endpoint.host}:{endpoint.port}/functions"
        
        try:
            response_data = self._send_http_request("GET", url, None, timeout)
            return response_data
            
        except Exception as e:
            self.logger.error(f"Failed to list functions from {endpoint}: {e}")
            return None
    
    def health_check(self,
                    endpoint: NodeEndpoint,
                    timeout_seconds: Optional[float] = None) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Perform a health check on a remote node.
        
        Args:
            endpoint: Target node endpoint
            timeout_seconds: Request timeout (uses default if None)
            
        Returns:
            Tuple of (is_healthy, health_data)
        """
        timeout = timeout_seconds or min(self.default_timeout, 10.0)
        url = f"http://{endpoint.host}:{endpoint.port}/health"
        
        try:
            response_data = self._send_http_request("GET", url, None, timeout)
            is_healthy = response_data.get("status") == "healthy"
            return is_healthy, response_data
            
        except Exception as e:
            self.logger.debug(f"Health check failed for {endpoint}: {e}")
            return False, None
    
    def _send_request_with_retry(self,
                               endpoint: NodeEndpoint,
                               path: str,
                               rpc_request: RPCRequest,
                               timeout: float) -> RPCResponse:
        """
        Send an RPC request with retry logic.
        
        Args:
            endpoint: Target endpoint
            path: URL path
            rpc_request: RPC request to send
            timeout: Request timeout
            
        Returns:
            RPC response
            
        Raises:
            RPCError: If all retry attempts fail
        """
        url = f"http://{endpoint.host}:{endpoint.port}{path}"
        request_data = rpc_request.to_dict()
        
        last_exception = None
        
        for attempt in range(self.max_retries + 1):
            try:
                if attempt > 0:
                    self.stats["retry_attempts"] += 1
                    self.logger.debug(f"Retry attempt {attempt} for {url}")
                    time.sleep(self.retry_delay * attempt)  # Exponential backoff
                
                response_data = self._send_http_request("POST", url, request_data, timeout)
                
                # Parse RPC response
                rpc_response = RPCResponse.from_dict(response_data)
                
                # Update statistics
                self.stats["requests_sent"] += 1
                if rpc_response.status == ResponseStatus.SUCCESS:
                    self.stats["successful_requests"] += 1
                else:
                    self.stats["failed_requests"] += 1
                
                return rpc_response
                
            except urllib.error.URLError as e:
                last_exception = ConnectionError(f"Connection failed: {e.reason}")
                self.logger.debug(f"Connection attempt {attempt + 1} failed: {e}")
                
            except urllib.error.HTTPError as e:
                if e.code >= 500:  # Server errors are retryable
                    last_exception = RPCError(f"Server error (HTTP {e.code}): {e.reason}")
                    self.logger.debug(f"Server error on attempt {attempt + 1}: {e}")
                else:  # Client errors are not retryable
                    self.stats["requests_sent"] += 1
                    self.stats["failed_requests"] += 1
                    raise RPCError(f"Client error (HTTP {e.code}): {e.reason}")
                    
            except Exception as e:
                last_exception = RPCError(f"Request failed: {str(e)}")
                self.logger.debug(f"Request attempt {attempt + 1} failed: {e}")
        
        # All retries failed
        self.stats["requests_sent"] += 1
        self.stats["failed_requests"] += 1
        
        if last_exception:
            raise last_exception
        else:
            raise RPCError("All retry attempts failed")
    
    def _send_http_request(self,
                          method: str,
                          url: str,
                          data: Optional[Dict[str, Any]],
                          timeout: float) -> Dict[str, Any]:
        """
        Send an HTTP request.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            url: Request URL
            data: Request data (for POST requests)
            timeout: Request timeout
            
        Returns:
            Response data as dictionary
            
        Raises:
            Various urllib exceptions
        """
        start_time = time.time()
        
        try:
            # Prepare request
            if data:
                request_body = json.dumps(data).encode('utf-8')
                headers = {
                    'Content-Type': 'application/json',
                    'Content-Length': str(len(request_body))
                }
            else:
                request_body = None
                headers = {}
            
            # Create request
            req = urllib.request.Request(url, data=request_body, headers=headers, method=method)
            
            # Send request
            with urllib.request.urlopen(req, timeout=timeout) as response:
                response_body = response.read().decode('utf-8')
                response_data = json.loads(response_body)
                
                # Update response time statistics
                response_time = time.time() - start_time
                self.stats["total_response_time"] += response_time
                
                return response_data
                
        except urllib.error.HTTPError as e:
            # Try to read error response
            try:
                error_body = e.read().decode('utf-8')
                error_data = json.loads(error_body)
                self.logger.debug(f"HTTP error response: {error_data}")
            except:
                pass
            raise
        
        except json.JSONDecodeError as e:
            raise RPCError(f"Invalid JSON response: {str(e)}")
        
        except Exception as e:
            response_time = time.time() - start_time
            self.stats["total_response_time"] += response_time
            raise
    
    def get_stats(self) -> Dict[str, Any]:
        """Get client statistics."""
        stats = self.stats.copy()
        
        # Calculate average response time
        if stats["successful_requests"] > 0:
            stats["average_response_time"] = (
                stats["total_response_time"] / stats["successful_requests"]
            )
        else:
            stats["average_response_time"] = 0.0
        
        # Calculate success rate
        total_requests = stats["requests_sent"]
        if total_requests > 0:
            stats["success_rate"] = stats["successful_requests"] / total_requests
        else:
            stats["success_rate"] = 0.0
        
        return stats
    
    def reset_stats(self):
        """Reset client statistics."""
        self.stats = {
            "requests_sent": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "total_response_time": 0.0,
            "retry_attempts": 0
        }
