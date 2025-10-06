"""
Example serverless functions for testing the runtime.

These functions demonstrate different types of operations and use cases.
"""

import time
import json
import math
import sys
import os
from typing import Any, Dict, List

# Add the src directory to the Python path  
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from core import ServerlessFunction


class HelloWorldFunction(ServerlessFunction):
    """Simple hello world function."""
    
    def execute(self, input_data: Any) -> str:
        """Return a personalized greeting."""
        name = input_data.get("name", "World") if isinstance(input_data, dict) else "World"
        return f"Hello, {name}!"


class MathOperationsFunction(ServerlessFunction):
    """Function that performs various mathematical operations."""
    
    def execute(self, input_data: Any) -> Dict[str, Any]:
        """Perform mathematical operations on input numbers."""
        if not isinstance(input_data, dict):
            raise ValueError("Input must be a dictionary with 'operation', 'a', and 'b' keys")
        
        operation = input_data.get("operation")
        a = input_data.get("a")
        b = input_data.get("b")
        
        if operation is None or a is None or b is None:
            raise ValueError("Must provide 'operation', 'a', and 'b' parameters")
        
        try:
            a = float(a)
            b = float(b)
        except (ValueError, TypeError):
            raise ValueError("Parameters 'a' and 'b' must be numeric")
        
        result = None
        if operation == "add":
            result = a + b
        elif operation == "subtract":
            result = a - b
        elif operation == "multiply":
            result = a * b
        elif operation == "divide":
            if b == 0:
                raise ValueError("Cannot divide by zero")
            result = a / b
        elif operation == "power":
            result = a ** b
        elif operation == "sqrt":
            if a < 0:
                raise ValueError("Cannot take square root of negative number")
            result = math.sqrt(a)
        else:
            raise ValueError(f"Unsupported operation: {operation}")
        
        return {
            "operation": operation,
            "operands": {"a": a, "b": b},
            "result": result
        }


class DataProcessingFunction(ServerlessFunction):
    """Function that processes lists of data."""
    
    def execute(self, input_data: Any) -> Dict[str, Any]:
        """Process a list of numbers and return statistics."""
        if not isinstance(input_data, dict) or "numbers" not in input_data:
            raise ValueError("Input must be a dictionary with a 'numbers' key containing a list")
        
        numbers = input_data["numbers"]
        if not isinstance(numbers, list) or not numbers:
            raise ValueError("'numbers' must be a non-empty list")
        
        try:
            numbers = [float(n) for n in numbers]
        except (ValueError, TypeError):
            raise ValueError("All items in 'numbers' must be numeric")
        
        # Calculate statistics
        total = sum(numbers)
        count = len(numbers)
        mean = total / count
        
        sorted_numbers = sorted(numbers)
        if count % 2 == 0:
            median = (sorted_numbers[count//2 - 1] + sorted_numbers[count//2]) / 2
        else:
            median = sorted_numbers[count//2]
        
        variance = sum((x - mean) ** 2 for x in numbers) / count
        std_dev = math.sqrt(variance)
        
        return {
            "count": count,
            "sum": total,
            "mean": mean,
            "median": median,
            "min": min(numbers),
            "max": max(numbers),
            "std_dev": std_dev,
            "variance": variance
        }


class SlowFunction(ServerlessFunction):
    """Function that takes time to execute (for testing timeouts)."""
    
    def execute(self, input_data: Any) -> Dict[str, Any]:
        """Sleep for a specified duration."""
        if isinstance(input_data, dict):
            sleep_time = input_data.get("sleep_seconds", 1.0)
        else:
            sleep_time = float(input_data) if input_data is not None else 1.0
        
        if sleep_time < 0:
            raise ValueError("Sleep time must be non-negative")
        
        start_time = time.time()
        time.sleep(sleep_time)
        end_time = time.time()
        
        return {
            "requested_sleep_seconds": sleep_time,
            "actual_sleep_seconds": end_time - start_time,
            "message": f"Slept for {end_time - start_time:.2f} seconds"
        }


class ErrorFunction(ServerlessFunction):
    """Function that demonstrates error handling."""
    
    def execute(self, input_data: Any) -> Any:
        """Raise different types of errors based on input."""
        if not isinstance(input_data, dict):
            raise ValueError("Input must be a dictionary with 'error_type' key")
        
        error_type = input_data.get("error_type", "value_error")
        message = input_data.get("message", "This is a test error")
        
        if error_type == "value_error":
            raise ValueError(message)
        elif error_type == "runtime_error":
            raise RuntimeError(message)
        elif error_type == "type_error":
            raise TypeError(message)
        elif error_type == "zero_division":
            return 1 / 0
        elif error_type == "key_error":
            test_dict = {}
            return test_dict["nonexistent_key"]
        else:
            return {"message": "No error raised", "error_type": error_type}


class JSONValidationFunction(ServerlessFunction):
    """Function that validates and manipulates JSON data."""
    
    def execute(self, input_data: Any) -> Dict[str, Any]:
        """Validate and process JSON-like data structures."""
        if not isinstance(input_data, dict):
            raise ValueError("Input must be a dictionary")
        
        # Validate that the input can be serialized to JSON
        try:
            json_str = json.dumps(input_data)
            parsed_back = json.loads(json_str)
        except (TypeError, ValueError) as e:
            raise ValueError(f"Input is not JSON serializable: {str(e)}")
        
        # Process the data
        result = {
            "original_data": input_data,
            "json_string": json_str,
            "json_length": len(json_str),
            "key_count": len(input_data),
            "keys": list(input_data.keys())
        }
        
        # Add type information for each value
        type_info = {}
        for key, value in input_data.items():
            type_info[key] = type(value).__name__
        
        result["type_info"] = type_info
        
        return result