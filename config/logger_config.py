# File: config/logger_config.py
# This file provides a centralized configuration for logging in Python applications.
# It defines a function that configures a logger with specified settings, such as log level, output format, and log file rotation.

import logging  # Provides logging functionality
import os  # For handling file system paths and directories
from logging.handlers import RotatingFileHandler  # For managing rotating log files
from typing import Optional  # For optional type hinting

def configure_logger(
    name: Optional[str] = None,  # The name of the logger; None defaults to the root logger
    log_dir: str = "./logs",  # Directory where log files will be stored
    log_file: str = "application.log",  # Name of the log file
    level: int = logging.INFO,  # Logging level (e.g., DEBUG, INFO, WARNING, ERROR)
    max_bytes: int = 10 * 1024 * 1024,  # Maximum size of a log file before rotation (default: 10 MB)
    backup_count: int = 5,  # Number of backup files to keep during log rotation
    output: str = "both",  # Where to output logs: "file", "console", or "both"
) -> logging.Logger:
    """
    Configures and returns a logger instance.

    Args:
        name (Optional[str]): Name of the logger. If None, the root logger is used.
        log_dir (str): Directory to store log files.
        log_file (str): Name of the log file.
        level (int): Logging level (e.g., logging.INFO, logging.DEBUG).
        max_bytes (int): Maximum size of the log file before rotation.
        backup_count (int): Number of backup files to keep during rotation.
        output (str): Where to send logs: "file", "console", or "both".

    Returns:
        logging.Logger: Configured logger instance.
    """
    # Calculate the absolute path to the project root directory
    # This assumes the file structure where `logger_config.py` is in the `config` folder
    # and the `logs` directory is directly under the project root
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # Define the centralized logs directory within the project root
    # This ensures all log files are stored in one place regardless of where the logger is called
    log_dir = os.path.join(project_root, "logs")

    # Ensure the logs directory exists; create it if it does not
    os.makedirs(log_dir, exist_ok=True)

    # Define the full path to the log file within the logs directory
    log_path = os.path.join(log_dir, log_file)

    # Create or retrieve the logger instance with the specified name
    logger = logging.getLogger(name)

    # Set the logging level for this logger (e.g., INFO, DEBUG)
    logger.setLevel(level)

    # Check if the logger already has handlers to avoid duplicate logs
    if not logger.handlers:
        # Define the log message format, including timestamp, logger name, level, and message
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # If output includes file logging, add a rotating file handler
        if output in {"file", "both"}:
            file_handler = RotatingFileHandler(
                log_path, maxBytes=max_bytes, backupCount=backup_count
            )
            file_handler.setLevel(level)  # Set logging level for the file handler
            file_handler.setFormatter(formatter)  # Apply the log format to the file handler
            logger.addHandler(file_handler)  # Attach the file handler to the logger

        # If output includes console logging, add a stream handler
        if output in {"console", "both"}:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(level)  # Set logging level for the console handler
            console_handler.setFormatter(formatter)  # Apply the log format to the console handler
            logger.addHandler(console_handler)  # Attach the console handler to the logger

    # Return the configured logger instance
    return logger


# Example usage
if __name__ == "__main__":
    # Configure a logger for demonstration purposes
    logger = configure_logger(
        name="example_logger",  # Name of the logger
        log_file="example.log",  # Log file name
        level=logging.DEBUG,  # Logging level set to DEBUG
        output="both"  # Log to both the console and the file
    )

    # Log example messages to test the configuration
    logger.info("This is an informational message.")  # Logs an INFO-level message
    logger.debug("This is a debug message.")  # Logs a DEBUG-level message
    logger.error("This is an error message.")  # Logs an ERROR-level message
