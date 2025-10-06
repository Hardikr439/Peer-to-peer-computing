"""
RPC Protocol and Message Definitions for Remote Function Execution

This module defines the communication protocol for sending function execution
requests between nodes and receiving results.
"""

import json
import uuid
from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import Any, Dict, Optional, List
from enum import Enum

from ..core.function_interface import ExecutionResult


class RequestType(Enum):
    """Types of RPC requests."""
    EXECUTE_FUNCTION = "execute_function"
    LIST_FUNCTIONS = "list_functions"
    GET_NODE_STATUS = "get_node_status"
    PING = "ping"


class ResponseStatus(Enum):
    """RPC response status codes."""
    SUCCESS = "success"
    ERROR = "error"
    NOT_FOUND = "not_found"
    TIMEOUT = "timeout"
    INVALID_REQUEST = "invalid_request"


@dataclass
class RPCRequest:
    """
    RPC request message for remote function execution.
    """
    request_id: str
    request_type: RequestType
    timestamp: str = ""
    source_node_id: str = ""
    target_node_id: str = ""
    function_name: str = ""
    input_data: Any = None
    timeout_seconds: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Initialize fields that depend on other fields."""
        if not self.timestamp:
            self.timestamp = datetime.utcnow().isoformat()
        if not self.request_id:
            self.request_id = str(uuid.uuid4())
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        result = asdict(self)
        result['request_type'] = self.request_type.value
        return result
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), default=str, indent=2)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RPCRequest':
        """Create RPCRequest from dictionary."""
        if 'request_type' in data:
            data['request_type'] = RequestType(data['request_type'])
        return cls(**data)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'RPCRequest':
        """Create RPCRequest from JSON string."""
        return cls.from_dict(json.loads(json_str))
    
    @classmethod
    def create_execute_request(cls,
                             function_name: str,
                             input_data: Any,
                             source_node_id: str = "",
                             target_node_id: str = "",
                             timeout_seconds: Optional[float] = None) -> 'RPCRequest':
        """
        Create a function execution request.
        
        Args:
            function_name: Name of the function to execute
            input_data: Input data for the function
            source_node_id: ID of the requesting node
            target_node_id: ID of the target node
            timeout_seconds: Optional timeout for execution
            
        Returns:
            RPCRequest instance
        """
        return cls(
            request_id=str(uuid.uuid4()),
            request_type=RequestType.EXECUTE_FUNCTION,
            source_node_id=source_node_id,
            target_node_id=target_node_id,
            function_name=function_name,
            input_data=input_data,
            timeout_seconds=timeout_seconds
        )
    
    @classmethod
    def create_list_functions_request(cls,
                                    source_node_id: str = "",
                                    target_node_id: str = "") -> 'RPCRequest':
        """Create a list functions request."""
        return cls(
            request_id=str(uuid.uuid4()),
            request_type=RequestType.LIST_FUNCTIONS,
            source_node_id=source_node_id,
            target_node_id=target_node_id
        )
    
    @classmethod
    def create_ping_request(cls,
                          source_node_id: str = "",
                          target_node_id: str = "") -> 'RPCRequest':
        """Create a ping request."""
        return cls(
            request_id=str(uuid.uuid4()),
            request_type=RequestType.PING,
            source_node_id=source_node_id,
            target_node_id=target_node_id
        )


@dataclass
class RPCResponse:
    """
    RPC response message containing execution results or error information.
    """
    request_id: str
    status: ResponseStatus
    timestamp: str = ""
    result: Any = None
    error_message: str = ""
    execution_result: Optional[ExecutionResult] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Initialize fields that depend on other fields."""
        if not self.timestamp:
            self.timestamp = datetime.utcnow().isoformat()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        result = asdict(self)
        result['status'] = self.status.value
        
        # Convert ExecutionResult to dict if present
        if self.execution_result:
            result['execution_result'] = self.execution_result.to_dict()
        
        return result
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), default=str, indent=2)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'RPCResponse':
        """Create RPCResponse from dictionary."""
        if 'status' in data:
            data['status'] = ResponseStatus(data['status'])
        
        # Convert execution_result dict back to ExecutionResult if present
        if 'execution_result' in data and data['execution_result']:
            data['execution_result'] = ExecutionResult.from_dict(data['execution_result'])
        
        return cls(**data)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'RPCResponse':
        """Create RPCResponse from JSON string."""
        return cls.from_dict(json.loads(json_str))
    
    @classmethod
    def create_success_response(cls,
                              request_id: str,
                              result: Any = None,
                              execution_result: Optional[ExecutionResult] = None) -> 'RPCResponse':
        """Create a successful response."""
        return cls(
            request_id=request_id,
            status=ResponseStatus.SUCCESS,
            result=result,
            execution_result=execution_result
        )
    
    @classmethod
    def create_error_response(cls,
                            request_id: str,
                            error_message: str,
                            status: ResponseStatus = ResponseStatus.ERROR) -> 'RPCResponse':
        """Create an error response."""
        return cls(
            request_id=request_id,
            status=status,
            error_message=error_message
        )


@dataclass
class NodeEndpoint:
    """Network endpoint information for a node."""
    node_id: str
    host: str
    port: int
    protocol: str = "http"
    
    def get_url(self, path: str = "") -> str:
        """
        Get the full URL for this endpoint.
        
        Args:
            path: Optional path to append
            
        Returns:
            Full URL string
        """
        base_url = f"{self.protocol}://{self.host}:{self.port}"
        if path:
            if not path.startswith('/'):
                path = '/' + path
            return base_url + path
        return base_url
    
    def __str__(self) -> str:
        """String representation of the endpoint."""
        return f"{self.host}:{self.port}"


class RPCError(Exception):
    """Base exception for RPC operations."""
    
    def __init__(self, message: str, status: ResponseStatus = ResponseStatus.ERROR):
        super().__init__(message)
        self.status = status


class RPCTimeoutError(RPCError):
    """Exception raised when RPC requests timeout."""
    
    def __init__(self, message: str = "RPC request timed out"):
        super().__init__(message, ResponseStatus.TIMEOUT)


class RPCConnectionError(RPCError):
    """Exception raised when RPC connection fails."""
    
    def __init__(self, message: str = "Failed to connect to remote node"):
        super().__init__(message, ResponseStatus.ERROR)


class FunctionNotFoundError(RPCError):
    """Exception raised when requested function is not found."""
    
    def __init__(self, function_name: str):
        super().__init__(f"Function '{function_name}' not found", ResponseStatus.NOT_FOUND)


class ValidationError(RPCError):
    """Exception raised when request validation fails."""
    
    def __init__(self, message: str, errors: List[str] = None):
        super().__init__(message, ResponseStatus.INVALID_REQUEST)
        self.errors = errors or []


class ConnectionError(RPCError):
    """Exception raised when connection to remote node fails."""
    
    def __init__(self, message: str, endpoint: Optional[str] = None):
        super().__init__(message, ResponseStatus.ERROR)
        self.endpoint = endpoint


def validate_rpc_request(request: RPCRequest) -> List[str]:
    """
    Validate an RPC request and return list of validation errors.
    
    Args:
        request: RPCRequest to validate
        
    Returns:
        List of validation error messages (empty if valid)
    """
    errors = []
    
    if not request.request_id:
        errors.append("request_id is required")
    
    if not request.request_type:
        errors.append("request_type is required")
    
    if request.request_type == RequestType.EXECUTE_FUNCTION:
        if not request.function_name:
            errors.append("function_name is required for execute_function requests")
        if request.input_data is None:
            errors.append("input_data is required for execute_function requests")
    
    if request.timeout_seconds is not None and request.timeout_seconds <= 0:
        errors.append("timeout_seconds must be positive if specified")
    
    return errors


def create_rpc_metadata(source_node_id: str, additional_info: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Create standard RPC metadata.
    
    Args:
        source_node_id: ID of the source node
        additional_info: Additional metadata to include
        
    Returns:
        Dictionary with RPC metadata
    """
    metadata = {
        "source_node_id": source_node_id,
        "protocol_version": "1.0",
        "timestamp": datetime.utcnow().isoformat()
    }
    
    if additional_info:
        metadata.update(additional_info)
    
    return metadata