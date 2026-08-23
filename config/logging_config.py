"""
Production-Grade Logging Configuration
=======================================

Standardized logging setup for all platform components.
Provides consistent formatting, log levels, and handlers.

Usage:
    from config.logging_config import get_logger
    
    logger = get_logger(__name__)
    logger.info("Processing started")
    logger.error("An error occurred", exc_info=True)
"""

import logging
import sys
from typing import Optional


# Log format for production
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


def get_logger(
    name: str,
    level: Optional[str] = None,
    format_string: Optional[str] = None
) -> logging.Logger:
    """
    Get a configured logger instance.
    
    Args:
        name: Logger name (typically __name__ of the calling module)
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format_string: Custom format string (uses LOG_FORMAT if None)
    
    Returns:
        Configured logger instance
    
    Example:
        >>> logger = get_logger(__name__)
        >>> logger.info("Processing batch of 1000 records")
        >>> logger.error("Failed to connect to database", exc_info=True)
    """
    logger = logging.getLogger(name)
    
    # Set level
    if level:
        logger.setLevel(getattr(logging, level.upper()))
    else:
        logger.setLevel(logging.INFO)
    
    # Avoid duplicate handlers
    if not logger.handlers:
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.DEBUG)
        
        # Formatter
        formatter = logging.Formatter(
            format_string or LOG_FORMAT,
            datefmt=DATE_FORMAT
        )
        console_handler.setFormatter(formatter)
        
        logger.addHandler(console_handler)
    
    return logger


def configure_root_logger(level: str = 'INFO') -> None:
    """
    Configure the root logger for the entire application.
    
    Args:
        level: Log level for root logger
    
    Example:
        >>> configure_root_logger('INFO')
    """
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=LOG_FORMAT,
        datefmt=DATE_FORMAT,
        stream=sys.stdout
    )


# Convenience function for quick logging setup
def setup_logging(module_name: str, level: str = 'INFO') -> logging.Logger:
    """
    Quick setup for module logging.
    
    Args:
        module_name: Name of the module
        level: Log level
    
    Returns:
        Configured logger
    
    Example:
        >>> logger = setup_logging(__name__)
        >>> logger.info("Module initialized")
    """
    return get_logger(module_name, level=level)


# Module-level logger for this config file
logger = get_logger(__name__)
logger.debug("Logging configuration module loaded")
