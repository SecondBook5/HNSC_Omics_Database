import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Optional

def configure_logger(
    name: Optional[str] = None,
    log_dir: str = "./logs",
    log_file: str = "application.log",
    level: int = logging.INFO,
    max_bytes: int = 10 * 1024 * 1024,  # 10 MB
    backup_count: int = 5,
    output: str = "both",  # Options: "file", "console", "both"
) -> logging.Logger:
    """
    Configures and returns a logger instance.

    Args:
        name (Optional[str]): Name of the logger. If None, root logger is used.
        log_dir (str): Directory to store log files.
        log_file (str): Name of the log file.
        level (int): Logging level (e.g., logging.INFO, logging.DEBUG).
        max_bytes (int): Maximum size of the log file before rotation.
        backup_count (int): Number of backup files to keep during rotation.
        output (str): Where to send logs: "file", "console", or "both".

    Returns:
        logging.Logger: Configured logger instance.
    """
    # Ensure the log directory exists (if output includes file logging)
    if output in {"file", "both"}:
        os.makedirs(log_dir, exist_ok=True)

    # Create the full path for the log file
    log_path = os.path.join(log_dir, log_file)

    # Configure the logger
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid adding duplicate handlers
    if not logger.handlers:
        # Define the log message format
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Add a file handler if needed
        if output in {"file", "both"}:
            file_handler = RotatingFileHandler(
                log_path, maxBytes=max_bytes, backupCount=backup_count
            )
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        # Add a console handler if needed
        if output in {"console", "both"}:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(level)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

    return logger

# Example usage
if __name__ == "__main__":
    # Configure a logger with both console and file output
    logger = configure_logger(
        name="example_logger",
        log_dir="./logs",
        log_file="example.log",
        level=logging.DEBUG,
        output="both"
    )
    # Test the logger
    logger.info("This is an informational message.")
    logger.debug("This is a debug message.")
    logger.error("This is an error message.")
