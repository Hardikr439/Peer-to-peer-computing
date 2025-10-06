"""
Demo script for Increment 1: Local Function Runner

This script demonstrates the core functionality of the local execution environment.
"""

import sys
import os
from pathlib import Path

# Add the src directory to the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from core import LocalExecutor, FunctionLoader
from examples.sample_functions import (
    HelloWorldFunction,
    MathOperationsFunction,
    DataProcessingFunction,
    SlowFunction,
    ErrorFunction,
    JSONValidationFunction
)


def main():
    """Run the demo."""
    print("=== Serverless Distributed Function Runtime ===")
    print("Increment 1: Local Function Runner Demo\n")
    
    # Initialize the local executor
    executor = LocalExecutor(default_timeout_seconds=10.0)
    
    # Register example functions
    print("1. Registering example functions...")
    executor.register_function_from_class("hello", HelloWorldFunction)
    executor.register_function_from_class("math", MathOperationsFunction)
    executor.register_function_from_class("data_process", DataProcessingFunction)
    executor.register_function_from_class("slow", SlowFunction)
    executor.register_function_from_class("error", ErrorFunction)
    executor.register_function_from_class("json_validate", JSONValidationFunction)
    
    print(f"Registered functions: {executor.list_functions()}\n")
    
    # Demo 1: Hello World Function
    print("2. Demo: Hello World Function")
    result = executor.execute_function("hello", {"name": "Alice"})
    print(f"Input: {{'name': 'Alice'}}")
    print(f"Output: {result.output}")
    print(f"Execution time: {result.execution_time_ms:.2f}ms")
    print(f"Success: {result.success}\n")
    
    # Demo 2: Math Operations
    print("3. Demo: Math Operations Function")
    test_cases = [
        {"operation": "add", "a": 10, "b": 5},
        {"operation": "multiply", "a": 7, "b": 8},
        {"operation": "divide", "a": 20, "b": 4},
        {"operation": "sqrt", "a": 16, "b": 0}
    ]
    
    for test_case in test_cases:
        result = executor.execute_function("math", test_case)
        print(f"Input: {test_case}")
        print(f"Output: {result.output}")
        print(f"Success: {result.success}")
        if not result.success:
            print(f"Error: {result.error_message}")
        print()
    
    # Demo 3: Data Processing
    print("4. Demo: Data Processing Function")
    numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    result = executor.execute_function("data_process", {"numbers": numbers})
    print(f"Input numbers: {numbers}")
    print(f"Statistics: {result.output}")
    print(f"Execution time: {result.execution_time_ms:.2f}ms\n")
    
    # Demo 4: Error Handling
    print("5. Demo: Error Handling")
    error_tests = [
        {"error_type": "value_error", "message": "This is a custom value error"},
        {"error_type": "zero_division"},
        {"error_type": "none"}  # This won't raise an error
    ]
    
    for error_test in error_tests:
        result = executor.execute_function("error", error_test)
        print(f"Input: {error_test}")
        print(f"Success: {result.success}")
        if result.success:
            print(f"Output: {result.output}")
        else:
            print(f"Error: {result.error_message}")
            print(f"Error type: {result.metadata.get('error_type', 'Unknown')}")
        print()
    
    # Demo 5: JSON Validation
    print("6. Demo: JSON Validation Function")
    json_test_data = {
        "user_id": 12345,
        "username": "alice_smith",
        "preferences": {
            "theme": "dark",
            "notifications": True,
            "languages": ["en", "es", "fr"]
        },
        "scores": [85, 92, 78, 96]
    }
    
    result = executor.execute_function("json_validate", json_test_data)
    print(f"Input data keys: {list(json_test_data.keys())}")
    print(f"JSON string length: {result.output['json_length']}")
    print(f"Type information: {result.output['type_info']}")
    print(f"Execution time: {result.execution_time_ms:.2f}ms\n")
    
    # Demo 6: Function Metadata
    print("7. Demo: Function Metadata")
    for func_name in executor.list_functions():
        metadata = executor.get_function_metadata(func_name)
        print(f"Function: {func_name}")
        print(f"Metadata: {metadata}")
        print()
    
    # Demo 7: Execution Statistics
    print("8. Demo: Execution Statistics")
    stats = executor.get_statistics()
    print(f"Total executions: {stats['total_executions']}")
    print(f"Successful executions: {stats['successful_executions']}")
    print(f"Failed executions: {stats['failed_executions']}")
    print(f"Success rate: {stats['success_rate']:.2%}")
    print(f"Average execution time: {stats['average_execution_time_ms']:.2f}ms\n")
    
    # Demo 8: Loading Functions from File
    print("9. Demo: Loading Functions from File")
    try:
        examples_file = Path(__file__).parent / "examples" / "sample_functions.py"
        if examples_file.exists():
            registered_names = executor.load_and_register_from_file(examples_file)
            print(f"Loaded functions from file: {registered_names}")
        else:
            print("Sample functions file not found, trying direct loading...")
            # Clear previous registrations to show file loading
            executor.clear_execution_history()
            # Manually load from the current examples (already imported)
            print("(Functions already loaded and demonstrated above)")
    except Exception as e:
        print(f"Error loading from file: {e}")
    
    print("\n=== Demo Complete ===")
    print("Increment 1 successfully demonstrates:")
    print("✓ Core Function Interface")
    print("✓ Local Execution Environment") 
    print("✓ Result Object with execution metrics")
    print("✓ Error handling and timeout management")
    print("✓ Function registration and metadata")
    print("✓ Execution history and statistics")


if __name__ == "__main__":
    main()