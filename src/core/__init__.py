"""
Core module for Serverless Distributed Function Runtime

This module provides the foundation for executing serverless functions
locally and remotely.
"""

from .function_interface import (
    ServerlessFunction,
    ExecutionResult,
    FunctionValidationError,
    validate_serializable,
    create_error_result
)

from .local_executor import (
    LocalExecutor,
    FunctionLoader,
    ExecutionTimeoutError
)

__all__ = [
    'ServerlessFunction',
    'ExecutionResult',
    'FunctionValidationError',
    'ExecutionTimeoutError',
    'LocalExecutor',
    'FunctionLoader',
    'validate_serializable',
    'create_error_result'
]