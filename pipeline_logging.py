"""
Logging configuration for the dimensional data pipeline.
"""
import logging
import os
from datetime import datetime


def setup_logger(execution_id: str, log_file_path: str = "logs/logs_dimensional_data_pipeline.txt") -> logging.Logger:
    """
    Set up a logger for the dimensional data flow.
    
    Args:
        execution_id: Unique execution ID (UUID) for tracking
        log_file_path: Path to the log file
        
    Returns:
        logging.Logger: Configured logger instance
    """
    # Create logs directory if it doesn't exist
    log_dir = os.path.dirname(log_file_path)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)
    
    # Create logger
    logger = logging.getLogger(f"dimensional_data_pipeline_{execution_id}")
    logger.setLevel(logging.INFO)
    
    # Avoid duplicate handlers
    if logger.handlers:
        return logger
    
    # Create formatter
    formatter = logging.Formatter(
        fmt='%(asctime)s | ExecutionID: %(execution_id)s | %(levelname)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # File handler
    file_handler = logging.FileHandler(log_file_path, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # Add execution_id to all log records
    old_factory = logging.getLogRecordFactory()
    
    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        record.execution_id = execution_id
        return record
    
    logging.setLogRecordFactory(record_factory)
    
    return logger
