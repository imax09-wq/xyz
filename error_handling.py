#!/usr/bin/env python3
"""
Error handling system for the ETL pipeline with retry mechanisms and contextual errors.

Part of the Sierra Chart ETL Pipeline project.
"""

import logging
import time
import traceback
import functools
import os
import sys
from typing import Callable, Any, Dict, Optional, TypeVar, List, Tuple
from dataclasses import dataclass
from enum import Enum

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl_pipeline.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("SierraChartETL")

# Define error types for better categorization
class ErrorCategory(Enum):
    # Fatal errors that should stop processing
    FATAL = "FATAL"
    # Recoverable errors that can be retried
    RECOVERABLE = "RECOVERABLE"
    # Data errors that require handling but aren't fatal
    DATA_ERROR = "DATA_ERROR"
    # Configuration errors
    CONFIG_ERROR = "CONFIG_ERROR"
    # System errors (IO, memory, etc.)
    SYSTEM_ERROR = "SYSTEM_ERROR"

@dataclass
class ErrorContext:
    """Context for error handling to provide better diagnostics"""
    component: str
    operation: str
    file_path: Optional[str] = None
    checkpoint: Optional[int] = None
    contract_id: Optional[str] = None
    additional_info: Optional[Dict[str, Any]] = None
    exception: Optional[Exception] = None
    traceback_str: Optional[str] = None

class ETLError(Exception):
    """Custom exception for ETL pipeline errors"""
    def __init__(
        self, 
        message: str, 
        category: ErrorCategory, 
        context: ErrorContext
    ):
        self.message = message
        self.category = category
        self.context = context
        super().__init__(self.message)
    
    def __str__(self):
        return (f"{self.category.value} Error: {self.message}\n"
                f"Component: {self.context.component}\n"
                f"Operation: {self.context.operation}\n"
                f"Contract: {self.context.contract_id or 'N/A'}\n"
                f"File: {self.context.file_path or 'N/A'}")

def create_error_context(component: str, operation: str, **kwargs) -> ErrorContext:
    """Helper to create error context"""
    return ErrorContext(
        component=component,
        operation=operation,
        **kwargs
    )

# Configure retry behavior
@dataclass
class RetryConfig:
    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    backoff_factor: float = 2.0

T = TypeVar('T')

def with_retry(
    retry_config: RetryConfig = RetryConfig(), 
    error_types: Tuple[type] = (Exception,),
    context_factory: Callable[..., ErrorContext] = None
) -> Callable:
    """Decorator to add retry logic to functions"""
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            attempt = 0
            last_exception = None
            
            while attempt <= retry_config.max_retries:
                try:
                    return func(*args, **kwargs)
                except error_types as e:
                    attempt += 1
                    last_exception = e
                    
                    if attempt > retry_config.max_retries:
                        # If we have context factory, create proper error context
                        if context_factory:
                            ctx = context_factory(*args, **kwargs)
                            ctx.exception = e
                            ctx.traceback_str = traceback.format_exc()
                            
                            # Create a proper ETL error
                            raise ETLError(
                                message=f"Failed after {retry_config.max_retries} retries: {str(e)}",
                                category=ErrorCategory.RECOVERABLE,
                                context=ctx
                            ) from e
                        else:
                            # No context factory, just re-raise
                            raise
                    
                    # Calculate backoff time using exponential backoff
                    delay = min(
                        retry_config.base_delay * (retry_config.backoff_factor ** (attempt - 1)),
                        retry_config.max_delay
                    )
                    
                    logger.warning(
                        f"Retry attempt {attempt}/{retry_config.max_retries} for {func.__name__} "
                        f"after error: {str(e)}. Retrying in {delay:.2f}s"
                    )
                    
                    time.sleep(delay)
            
            # This should not happen due to the raise inside the loop
            raise RuntimeError("Unexpected end of retry loop")
        
        return wrapper
    return decorator

def validate_file_exists(file_path: str, component: str, operation: str, contract_id: str = None) -> None:
    """Validate that a file exists, raising appropriate error if not"""
    if not os.path.exists(file_path):
        ctx = create_error_context(
            component=component,
            operation=operation,
            file_path=file_path,
            contract_id=contract_id
        )
        
        raise ETLError(
            message=f"File not found: {file_path}",
            category=ErrorCategory.SYSTEM_ERROR,
            context=ctx
        )

def validate_file_readable(file_path: str, component: str, operation: str, contract_id: str = None) -> None:
    """Validate that a file is readable"""
    validate_file_exists(file_path, component, operation, contract_id)
    
    if not os.access(file_path, os.R_OK):
        ctx = create_error_context(
            component=component,
            operation=operation,
            file_path=file_path,
            contract_id=contract_id
        )
        
        raise ETLError(
            message=f"File not readable: {file_path}",
            category=ErrorCategory.SYSTEM_ERROR,
            context=ctx
        )

def handle_data_error(error_msg: str, component: str, operation: str, **kwargs) -> None:
    """Handle data-related errors"""
    ctx = create_error_context(
        component=component,
        operation=operation,
        **kwargs
    )
    
    logger.error(f"Data error: {error_msg}", extra={
        "component": component,
        "operation": operation,
        "contract_id": kwargs.get("contract_id"),
        "file_path": kwargs.get("file_path")
    })
    
    raise ETLError(
        message=error_msg,
        category=ErrorCategory.DATA_ERROR,
        context=ctx
    )

# Global exception handler to log unhandled exceptions
def global_exception_handler(exc_type, exc_value, exc_traceback):
    """Handle uncaught exceptions globally"""
    if issubclass(exc_type, KeyboardInterrupt):
        # Don't capture keyboard interrupt
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    
    logger.critical("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))
    
# Install the global exception handler
sys.excepthook = global_exception_handler


