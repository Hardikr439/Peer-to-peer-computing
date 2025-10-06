"""
Local Execution Environment for Serverless Functions

This module provides the local driver that can load, run, and measure 
the execution time of functions conforming to the ServerlessFunction interface.
"""

import time
import importlib.util
import inspect
import json
import sys
from pathlib import Path
from typing import Any, Dict, Type, Union, Optional, List
import traceback
import threading
import queue
import signal
from contextlib import contextmanager

from .function_interface import ServerlessFunction, ExecutionResult, create_error_result, validate_serializable, FunctionValidationError


class ExecutionTimeoutError(Exception):
    """Exception raised when function execution times out."""
    pass


class FunctionLoader:
    """Loads and validates serverless functions from various sources."""
    
    @staticmethod
    def load_from_file(file_path: Union[str, Path]) -> Dict[str, Type[ServerlessFunction]]:
        """
        Load all ServerlessFunction classes from a Python file.
        
        Args:
            file_path: Path to the Python file containing functions
            
        Returns:
            Dictionary mapping function names to function classes
            
        Raises:
            FunctionValidationError: If loading or validation fails
        """
        file_path = Path(file_path)
        if not file_path.exists():
            raise FunctionValidationError(f"Function file not found: {file_path}")
        
        try:
            # Load the module from file
            spec = importlib.util.spec_from_file_location("user_functions", file_path)
            if spec is None or spec.loader is None:
                raise FunctionValidationError(f"Could not load module from {file_path}")
            
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            # Find all ServerlessFunction classes
            functions = {}
            for name, obj in inspect.getmembers(module, inspect.isclass):
                if (issubclass(obj, ServerlessFunction) and 
                    obj is not ServerlessFunction and 
                    not inspect.isabstract(obj)):
                    functions[name] = obj
            
            if not functions:
                raise FunctionValidationError(f"No valid ServerlessFunction classes found in {file_path}")
            
            return functions
            
        except Exception as e:
            if isinstance(e, FunctionValidationError):
                raise
            raise FunctionValidationError(f"Error loading functions from {file_path}: {str(e)}")
    
    @staticmethod
    def load_from_class(function_class: Type[ServerlessFunction]) -> ServerlessFunction:
        """
        Create an instance of a ServerlessFunction class.
        
        Args:
            function_class: The ServerlessFunction class to instantiate
            
        Returns:
            Instance of the function class
            
        Raises:
            FunctionValidationError: If validation fails
        """
        if not issubclass(function_class, ServerlessFunction):
            raise FunctionValidationError("Class must inherit from ServerlessFunction")
        
        if inspect.isabstract(function_class):
            raise FunctionValidationError("Cannot instantiate abstract function class")
        
        try:
            return function_class()
        except Exception as e:
            raise FunctionValidationError(f"Error instantiating function class: {str(e)}")


class LocalExecutor:
    """
    Local execution environment for serverless functions.
    
    Provides safe execution with timeout, resource monitoring, and result tracking.
    """
    
    def __init__(self, default_timeout_seconds: float = 30.0):
        """
        Initialize the local executor.
        
        Args:
            default_timeout_seconds: Default timeout for function execution
        """
        self.default_timeout = default_timeout_seconds
        self.execution_history: List[ExecutionResult] = []
        self._function_registry: Dict[str, ServerlessFunction] = {}
    
    def register_function(self, name: str, function: ServerlessFunction) -> None:
        """
        Register a function instance for later execution.
        
        Args:
            name: Name to register the function under
            function: ServerlessFunction instance
        """
        self._function_registry[name] = function
    
    def register_function_from_class(self, name: str, function_class: Type[ServerlessFunction]) -> None:
        """
        Register a function class, creating an instance.
        
        Args:
            name: Name to register the function under
            function_class: ServerlessFunction class
        """
        function_instance = FunctionLoader.load_from_class(function_class)
        self.register_function(name, function_instance)
    
    def load_and_register_from_file(self, file_path: Union[str, Path]) -> List[str]:
        """
        Load functions from a file and register them.
        
        Args:
            file_path: Path to the Python file containing functions
            
        Returns:
            List of function names that were registered
        """
        functions = FunctionLoader.load_from_file(file_path)
        registered_names = []
        
        for name, function_class in functions.items():
            function_instance = FunctionLoader.load_from_class(function_class)
            self.register_function(name, function_instance)
            registered_names.append(name)
        
        return registered_names
    
    def list_functions(self) -> List[str]:
        """Get list of registered function names."""
        return list(self._function_registry.keys())
    
    def get_function_metadata(self, function_name: str) -> Optional[Dict[str, Any]]:
        """Get metadata for a registered function."""
        if function_name not in self._function_registry:
            return None
        return self._function_registry[function_name].get_metadata()
    
    @contextmanager
    def _timeout_context(self, timeout_seconds: float):
        """Context manager for function execution timeout."""
        def timeout_handler(signum, frame):
            raise ExecutionTimeoutError(f"Function execution timed out after {timeout_seconds} seconds")
        
        # Note: signal.alarm only works on Unix-like systems
        # For Windows, we'll use threading instead
        if sys.platform == "win32":
            result_queue = queue.Queue()
            exception_queue = queue.Queue()
            
            def run_with_timeout():
                try:
                    yield
                    result_queue.put("completed")
                except Exception as e:
                    exception_queue.put(e)
            
            # For this context manager, we'll handle timeout differently
            yield
        else:
            old_handler = signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(int(timeout_seconds))
            try:
                yield
            finally:
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old_handler)
    
    def execute_function(self, 
                        function_name: str, 
                        input_data: Any, 
                        timeout_seconds: Optional[float] = None) -> ExecutionResult:
        """
        Execute a registered function with the given input data.
        
        Args:
            function_name: Name of the registered function to execute
            input_data: Input data for the function
            timeout_seconds: Timeout for execution (uses default if None)
            
        Returns:
            ExecutionResult containing the execution outcome
        """
        if function_name not in self._function_registry:
            return create_error_result(
                ValueError(f"Function '{function_name}' not found in registry")
            )
        
        function = self._function_registry[function_name]
        return self.execute_function_instance(function, input_data, timeout_seconds)
    
    def execute_function_instance(self, 
                                function: ServerlessFunction, 
                                input_data: Any, 
                                timeout_seconds: Optional[float] = None) -> ExecutionResult:
        """
        Execute a function instance with the given input data.
        
        Args:
            function: ServerlessFunction instance to execute
            input_data: Input data for the function
            timeout_seconds: Timeout for execution (uses default if None)
            
        Returns:
            ExecutionResult containing the execution outcome
        """
        if timeout_seconds is None:
            timeout_seconds = self.default_timeout
        
        # Validate input data is serializable
        if not validate_serializable(input_data):
            return create_error_result(
                ValueError("Input data must be JSON serializable")
            )
        
        start_time = time.time()
        
        try:
            # Execute with timeout (simplified for cross-platform compatibility)
            if sys.platform == "win32":
                # Windows: use threading for timeout
                result_queue = queue.Queue()
                exception_queue = queue.Queue()
                
                def execute_with_timeout():
                    try:
                        output = function.execute(input_data)
                        result_queue.put(output)
                    except Exception as e:
                        exception_queue.put(e)
                
                thread = threading.Thread(target=execute_with_timeout)
                thread.daemon = True
                thread.start()
                thread.join(timeout_seconds)
                
                if thread.is_alive():
                    # Thread is still running, timeout occurred
                    execution_time_ms = (time.time() - start_time) * 1000
                    result = create_error_result(
                        ExecutionTimeoutError(f"Function execution timed out after {timeout_seconds} seconds"),
                        execution_time_ms
                    )
                    self.execution_history.append(result)
                    return result
                
                # Check for exceptions
                if not exception_queue.empty():
                    execution_time_ms = (time.time() - start_time) * 1000
                    error = exception_queue.get()
                    result = create_error_result(error, execution_time_ms)
                    self.execution_history.append(result)
                    return result
                
                # Get result
                if not result_queue.empty():
                    output = result_queue.get()
                    execution_time_ms = (time.time() - start_time) * 1000
                else:
                    execution_time_ms = (time.time() - start_time) * 1000
                    result = create_error_result(
                        RuntimeError("Function completed but produced no output"),
                        execution_time_ms
                    )
                    self.execution_history.append(result)
                    return result
            else:
                # Unix-like systems: use signal for timeout
                with self._timeout_context(timeout_seconds):
                    output = function.execute(input_data)
                
                execution_time_ms = (time.time() - start_time) * 1000
            
            # Validate output is serializable
            if not validate_serializable(output):
                execution_time_ms = (time.time() - start_time) * 1000
                result = create_error_result(
                    ValueError("Function output must be JSON serializable"),
                    execution_time_ms
                )
                self.execution_history.append(result)
                return result
            
            result = ExecutionResult(
                output=output,
                execution_time_ms=execution_time_ms,
                success=True,
                metadata=function.get_metadata()
            )
            
        except Exception as e:
            execution_time_ms = (time.time() - start_time) * 1000
            result = create_error_result(e, execution_time_ms)
        
        # Store in execution history
        self.execution_history.append(result)
        
        return result
    
    def get_execution_history(self) -> List[ExecutionResult]:
        """Get the execution history."""
        return self.execution_history.copy()
    
    def clear_execution_history(self) -> None:
        """Clear the execution history."""
        self.execution_history.clear()
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get execution statistics."""
        if not self.execution_history:
            return {
                "total_executions": 0,
                "successful_executions": 0,
                "failed_executions": 0,
                "success_rate": 0.0,
                "average_execution_time_ms": 0.0
            }
        
        successful = [r for r in self.execution_history if r.success]
        failed = [r for r in self.execution_history if not r.success]
        
        avg_time = sum(r.execution_time_ms for r in self.execution_history) / len(self.execution_history)
        
        return {
            "total_executions": len(self.execution_history),
            "successful_executions": len(successful),
            "failed_executions": len(failed),
            "success_rate": len(successful) / len(self.execution_history),
            "average_execution_time_ms": avg_time
        }