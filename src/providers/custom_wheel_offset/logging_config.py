#!/usr/bin/env python3
"""
Centralized logging configuration for Custom Wheel Offset scraper.
Provides file-based logging with rotation and proper formatting.
"""

import logging
import logging.handlers
import os
from pathlib import Path
from datetime import datetime
from typing import Optional

# Default log directory
DEFAULT_LOG_DIR = Path("e:/scraper/data/logs")

class CustomWheelOffsetLogger:
    """Centralized logger for Custom Wheel Offset scraper with file output."""
    
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self.setup_logging()
            CustomWheelOffsetLogger._initialized = True
    
    def setup_logging(self, log_dir: Optional[Path] = None):
        """Setup logging configuration with file handlers."""
        if log_dir is None:
            log_dir = DEFAULT_LOG_DIR
        
        # Ensure log directory exists
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create timestamp for log files
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Define log files
        self.main_log_file = log_dir / f"scraper_main_{timestamp}.log"
        self.error_log_file = log_dir / f"scraper_errors_{timestamp}.log"
        self.debug_log_file = log_dir / f"scraper_debug_{timestamp}.log"
        
        # Create formatters
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        simple_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Setup root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        
        # Clear any existing handlers
        root_logger.handlers.clear()
        
        # Console handler (for immediate feedback)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(simple_formatter)
        root_logger.addHandler(console_handler)
        
        # Main log file handler (INFO and above)
        main_file_handler = logging.handlers.RotatingFileHandler(
            self.main_log_file,
            maxBytes=50*1024*1024,  # 50MB
            backupCount=5,
            encoding='utf-8'
        )
        main_file_handler.setLevel(logging.INFO)
        main_file_handler.setFormatter(detailed_formatter)
        root_logger.addHandler(main_file_handler)
        
        # Error log file handler (ERROR and above)
        error_file_handler = logging.handlers.RotatingFileHandler(
            self.error_log_file,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=3,
            encoding='utf-8'
        )
        error_file_handler.setLevel(logging.ERROR)
        error_file_handler.setFormatter(detailed_formatter)
        root_logger.addHandler(error_file_handler)
        
        # Debug log file handler (ALL levels)
        debug_file_handler = logging.handlers.RotatingFileHandler(
            self.debug_log_file,
            maxBytes=100*1024*1024,  # 100MB
            backupCount=3,
            encoding='utf-8'
        )
        debug_file_handler.setLevel(logging.DEBUG)
        debug_file_handler.setFormatter(detailed_formatter)
        root_logger.addHandler(debug_file_handler)
        
        # Log the initialization
        logging.info(f"Logging initialized - Main: {self.main_log_file}")
        logging.info(f"Logging initialized - Errors: {self.error_log_file}")
        logging.info(f"Logging initialized - Debug: {self.debug_log_file}")
    
    def get_logger(self, name: str) -> logging.Logger:
        """Get a logger instance with the specified name."""
        return logging.getLogger(name)
    
    def get_log_files(self) -> dict:
        """Return paths to all log files."""
        return {
            'main': self.main_log_file,
            'error': self.error_log_file,
            'debug': self.debug_log_file
        }


def setup_logging(log_dir: Optional[Path] = None) -> CustomWheelOffsetLogger:
    """Setup logging and return logger instance."""
    logger_instance = CustomWheelOffsetLogger()
    if log_dir:
        logger_instance.setup_logging(log_dir)
    return logger_instance


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance. Ensures logging is initialized."""
    logger_instance = CustomWheelOffsetLogger()
    return logger_instance.get_logger(name)


# Convenience function for modules
def init_module_logger(module_name: str) -> logging.Logger:
    """Initialize and return a logger for a specific module."""
    return get_logger(f"custom_wheel_offset.{module_name}")


# Initialize logging when module is imported
_logger_instance = CustomWheelOffsetLogger()