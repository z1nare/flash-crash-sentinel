"""
Centralized Logging System for RiskBeacon

Provides structured logging with different log levels and formatters
for different services (API, Services, Dashboard, etc.)
"""
import logging
import sys
from pathlib import Path
from typing import Optional
from datetime import datetime
import os

# Log directory
LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

# Log format
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

def setup_logger(
    name: str,
    level: int = logging.INFO,
    log_to_file: bool = True,
    log_to_console: bool = True
) -> logging.Logger:
    """
    Set up a logger with file and console handlers.
    
    Args:
        name: Logger name (typically __name__)
        level: Logging level (logging.DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_to_file: Whether to log to file
        log_to_console: Whether to log to console
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Avoid duplicate handlers if logger already configured
    if logger.handlers:
        return logger
    
    logger.setLevel(level)
    
    # Formatter
    formatter = logging.Formatter(LOG_FORMAT, DATE_FORMAT)
    
    # Console handler
    if log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    # File handler
    if log_to_file:
        # Create log file with date
        log_file = LOG_DIR / f"{name.replace('.', '_')}_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    # Prevent propagation to root logger
    logger.propagate = False
    
    return logger

def get_logger(name: str) -> logging.Logger:
    """
    Get or create a logger instance.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Logger instance
    """
    logger = logging.getLogger(name)
    
    # If logger doesn't have handlers, set it up with defaults
    if not logger.handlers:
        logger = setup_logger(name)
    
    return logger

# Default loggers for different modules
def get_api_logger() -> logging.Logger:
    """Get logger for API module."""
    return get_logger("riskbeacon.api")

def get_service_logger(name: str) -> logging.Logger:
    """Get logger for service modules."""
    return get_logger(f"riskbeacon.services.{name}")

def get_controller_logger() -> logging.Logger:
    """Get logger for controller."""
    return get_logger("riskbeacon.controller")

def get_dashboard_logger() -> logging.Logger:
    """Get logger for dashboard."""
    return get_logger("riskbeacon.dashboard")

