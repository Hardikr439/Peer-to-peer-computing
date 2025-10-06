"""
Core Function Interface for Serverless Distributed Function Runtime

This module defines the standard interface that all user-provided functions must adhere to.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import json
from dataclasses import dataclass, asdict
from datetime import datetime
import traceback


@dataclass
class ExecutionResult:
    """
    Standard data structure to hold function execution results.
    
    Attributes:
        output: The function's return value (must be JSON serializable)
        execution_time_ms: Time taken to execute the function in milliseconds
        success: Whether the function executed successfully
        error_message: Error message if execution failed
        timestamp: When the execution completed
        metadata: Additional metadata about execution
    """
    output: Any = None
    execution_time_ms: float = 0.0
    success: bool = True
    error_message: Optional[str] = None
    timestamp: str = ""
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.utcnow().isoformat()
        if self.metadata is None:
            self.metadata = {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary for serialization."""
        return asdict(self)
    
    def to_json(self) -> str:
        """Convert result to JSON string."""
        return json.dumps(self.to_dict(), default=str)
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ExecutionResult':
        """Create ExecutionResult from dictionary."""
        return cls(**data)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'ExecutionResult':
        """Create ExecutionResult from JSON string."""
        return cls.from_dict(json.loads(json_str))


class ServerlessFunction(ABC):
    """
    Abstract base class for serverless functions.
    
    All user-provided functions should inherit from this class and implement
    the execute method.
    """
    
    @abstractmethod
    def execute(self, input_data: Any) -> Any:
        """
        Execute the function with the given input data.
        
        Args:
            input_data: Input data for the function (must be JSON serializable)
            
        Returns:
            Function output (must be JSON serializable)
            
        Raises:
            Exception: Any exception during function execution
        """
        pass
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about this function.
        
        Returns:
            Dictionary containing function metadata
        """
        return {
            "function_name": self.__class__.__name__,
            "module": self.__class__.__module__,
        }


def validate_serializable(data: Any) -> bool:
    """
    Validate that data is JSON serializable.
    
    Args:
        data: Data to validate
        
    Returns:
        True if data is serializable, False otherwise
    """
    try:
        json.dumps(data, default=str)
        return True
    except (TypeError, ValueError):
        return False


class FunctionValidationError(Exception):
    """Exception raised when function validation fails."""
    pass


def create_error_result(error: Exception, execution_time_ms: float = 0.0) -> ExecutionResult:
    """
    Create an ExecutionResult for a failed execution.
    
    Args:
        error: The exception that occurred
        execution_time_ms: Time taken before the error occurred
        
    Returns:
        ExecutionResult with error information
    """
    return ExecutionResult(
        output=None,
        execution_time_ms=execution_time_ms,
        success=False,
        error_message=str(error),
        metadata={
            "error_type": error.__class__.__name__,
            "traceback": traceback.format_exc()
        }
    )