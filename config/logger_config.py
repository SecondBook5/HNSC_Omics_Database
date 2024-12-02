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
    try:
        # Calculate the absolute path to the project root directory
        # This ensures the logger always places logs in the correct directory
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        # Define the centralized logs directory within the project root
        log_dir = os.path.join(project_root, "logs")

        # Ensure the logs directory exists; create it if it does not
        os.makedirs(log_dir, exist_ok=True)

        # Define the full path to the log file
        log_path = os.path.join(log_dir, log_file)

        # Create or retrieve the logger instance with the specified name
        logger = logging.getLogger(name)

        # Set the logging level for the logger
        logger.setLevel(level)

        # Check if the logger already has handlers to prevent duplicate logs
        if not logger.handlers:
            # Define the log message format, including timestamp, logger name, level, and message
            formatter = logging.Formatter(
                fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )

            # If output includes file logging, configure a rotating file handler
            if output in {"file", "both"}:
                try:
                    file_handler = RotatingFileHandler(
                        log_path, maxBytes=max_bytes, backupCount=backup_count
                    )
                    file_handler.setLevel(level)  # Set the log level for the file handler
                    file_handler.setFormatter(formatter)  # Set the log format
                    logger.addHandler(file_handler)  # Add the file handler to the logger
                except Exception as e:  # Catch unexpected errors during file handler setup
                    raise RuntimeError(
                        f"Failed to configure file handler for logger: {e}"
                    ) from e

            # If output includes console logging, configure a stream handler
            if output in {"console", "both"}:
                try:
                    console_handler = logging.StreamHandler()
                    console_handler.setLevel(level)  # Set the log level for the console handler
                    console_handler.setFormatter(formatter)  # Set the log format
                    logger.addHandler(console_handler)  # Add the console handler to the logger
                except Exception as e:  # Catch unexpected errors during console handler setup
                    raise RuntimeError(
                        f"Failed to configure console handler for logger: {e}"
                    ) from e

        # Return the configured logger instance
        return logger

    except OSError as e:  # Handle issues with creating log directories or files
        raise RuntimeError(
            f"Failed to create or access log directory: {e}"
        ) from e
    except Exception as e:  # Catch all other unexpected exceptions
        raise RuntimeError(f"Unexpected error during logger configuration: {e}") from e


# Example usage
if __name__ == "__main__":
    try:
        # Configure a logger for demonstration purposes
        logger = configure_logger(
            name="example_logger",  # Name of the logger
            log_file="example.log",  # Log file name
            level=logging.DEBUG,  # Logging level set to DEBUG
            output="both",  # Log to both the console and the file
        )

        # Log example messages to test the configuration
        logger.info("This is an informational message.")  # Logs an INFO-level message
        logger.debug("This is a debug message.")  # Logs a DEBUG-level message
        logger.error("This is an error message.")  # Logs an ERROR-level message

    except RuntimeError as e:  # Catch and log errors during logger setup or usage
        print(f"Logger setup failed: {e}")
