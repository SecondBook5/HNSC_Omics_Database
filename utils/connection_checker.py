# File: utils/connection_checker.py
# This script provides functionality to check connections to PostgreSQL and MongoDB databases
# with retry logic to handle transient connection issues. It uses connection functions from db_config.py,
# keeping configuration centralized and enhancing modularity.

import time  # Import the time module for delays between retry attempts
import logging  # Import the logging module to log connection status
from sqlalchemy.exc import OperationalError  # Import OperationalError to handle PostgreSQL connection issues
from pymongo.errors import ConnectionFailure  # Import ConnectionFailure to handle MongoDB connection issues
from config.db_config import get_postgres_engine, get_mongo_client, get_session_context, get_mongo_context
# Import connection methods from db_config to handle both PostgreSQL and MongoDB connections
from sqlalchemy import text  # Import text to execute raw SQL commands
import os  # Import os module to access environment variables


class DatabaseConnectionError(Exception):
    """
    Custom exception for database connection failures.

    Attributes:
        db_type (str): Type of the database that caused the failure (e.g., 'PostgreSQL', 'MongoDB').
    """

    def __init__(self, db_type, message="Database connection failed"):
        """
        Initializes the custom exception with database type and error message.

        Args:
            db_type (str): The type of the database that caused the failure.
            message (str): Description of the connection failure (default: "Database connection failed").
        """
        self.db_type = db_type
        super().__init__(f"{message}: {db_type}")


# Configure logging settings
log_level = logging.DEBUG if os.getenv("DEBUG", "False").lower() == "true" else logging.INFO
log_file = os.getenv("LOG_FILE", None)  # Optional log file location from environment variables
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=log_file if log_file else None  # Log to a file if specified, otherwise log to the console
)


class DatabaseConnectionChecker:
    """
    Checks the connection to PostgreSQL and MongoDB databases with retry logic.
    This class provides methods to verify connections to PostgreSQL and MongoDB,
    including retry mechanisms to handle transient connection failures.
    """

    def __init__(self, retries: int = 3, delay: int = 2) -> None:
        """
        Initializes the DatabaseConnectionChecker with retry parameters.

        Args:
            retries (int): Maximum number of retry attempts.
            delay (int): Delay (in seconds) between retry attempts.
        """
        self.retries: int = retries  # Set the number of retries for failed connections
        self.delay: int = delay  # Set the delay (in seconds) between each retry attempt

    def check_postgresql_connection(self, retries: int = None, delay: int = None) -> bool:
        """
        Attempts to connect to PostgreSQL using the engine from db_config with retry logic.

        Args:
            retries (int, optional): Number of retry attempts (overrides the default if specified).
            delay (int, optional): Delay between retries (overrides the default if specified).

        Returns:
            bool: True if the connection is successful, False otherwise.

        Raises:
            DatabaseConnectionError: If the connection fails after the maximum number of retries.
        """
        retries = retries or self.retries
        delay = delay or self.delay
        for attempt in range(1, retries + 1):  # Loop for the specified number of retries
            try:
                engine = get_postgres_engine()
                if engine is None:
                    logging.error("PostgreSQL engine could not be created. Check environment variables.")
                    return False
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                logging.info("PostgreSQL connection successful.")
                return True
            except OperationalError as e:  # Handle operational errors during connection
                logging.warning(
                    f"Attempt {attempt}: PostgreSQL connection failed. Error: {e}. Retrying in {delay} seconds..."
                )
                time.sleep(delay)  # Delay for the specified time before retrying
        raise DatabaseConnectionError("PostgreSQL")  # Raise a custom exception if all retries fail

    def check_postgresql_context(self, retries: int = None, delay: int = None) -> bool:
        """
        Attempts to connect to PostgreSQL using the context manager from db_config.

        Args:
            retries (int, optional): Number of retry attempts (overrides the default if specified).
            delay (int, optional): Delay between retries (overrides the default if specified).

        Returns:
            bool: True if the connection is successful, False otherwise.

        Raises:
            DatabaseConnectionError: If the connection fails after the maximum number of retries.
        """
        retries = retries or self.retries
        delay = delay or self.delay
        for attempt in range(1, retries + 1):  # Loop for the specified number of retries
            try:
                with get_session_context() as session:
                    session.execute(text("SELECT 1"))  # Execute a simple query to validate the connection
                logging.info("PostgreSQL context-managed connection successful.")
                return True
            except OperationalError as e:  # Handle operational errors during connection
                logging.warning(
                    f"Attempt {attempt}: PostgreSQL context-managed connection failed. Error: {e}. Retrying in {delay} seconds..."
                )
                time.sleep(delay)  # Delay for the specified time before retrying
        raise DatabaseConnectionError("PostgreSQL (context-managed)")  # Raise a custom exception if all retries fail

    def check_mongodb_connection(self, retries: int = None, delay: int = None) -> bool:
        """
        Attempts to connect to MongoDB using the client from db_config with retry logic.

        Args:
            retries (int, optional): Number of retry attempts (overrides the default if specified).
            delay (int, optional): Delay between retries (overrides the default if specified).

        Returns:
            bool: True if the connection is successful, False otherwise.

        Raises:
            DatabaseConnectionError: If the connection fails after the maximum number of retries.
        """
        retries = retries or self.retries
        delay = delay or self.delay
        for attempt in range(1, retries + 1):  # Loop for the specified number of retries
            try:
                client = get_mongo_client()
                if client is None:
                    logging.error("MongoDB client could not be created. Check environment variables.")
                    return False
                client.admin.command('ping')  # Send a ping command to validate the connection
                logging.info("MongoDB connection successful.")
                return True
            except ConnectionFailure as e:  # Handle connection failures
                logging.warning(
                    f"Attempt {attempt}: MongoDB connection failed. Error: {e}. Retrying in {delay} seconds..."
                )
                time.sleep(delay)  # Delay for the specified time before retrying
        raise DatabaseConnectionError("MongoDB")  # Raise a custom exception if all retries fail

    def check_mongodb_context(self, retries: int = None, delay: int = None) -> bool:
        """
        Attempts to connect to MongoDB using the context manager from db_config.

        Args:
            retries (int, optional): Number of retry attempts (overrides the default if specified).
            delay (int, optional): Delay between retries (overrides the default if specified).

        Returns:
            bool: True if the connection is successful, False otherwise.

        Raises:
            DatabaseConnectionError: If the connection fails after the maximum number of retries.
        """
        retries = retries or self.retries
        delay = delay or self.delay
        for attempt in range(1, retries + 1):  # Loop for the specified number of retries
            try:
                with get_mongo_context() as client:
                    client.admin.command('ping')  # Send a ping command to validate the connection
                logging.info("MongoDB context-managed connection successful.")
                return True
            except ConnectionFailure as e:  # Handle connection failures
                logging.warning(
                    f"Attempt {attempt}: MongoDB context-managed connection failed. Error: {e}. Retrying in {delay} seconds..."
                )
                time.sleep(delay)  # Delay for the specified time before retrying
        raise DatabaseConnectionError("MongoDB (context-managed)")  # Raise a custom exception if all retries fail

    def check_all_connections(self):
        """
        Checks all database connections (PostgreSQL, MongoDB, and their context-managed versions).

        Returns:
            bool: True if all connections are successful, False otherwise.
        """
        results = {
            "postgresql": self.check_postgresql_connection(),
            "postgresql_context": self.check_postgresql_context(),
            "mongodb": self.check_mongodb_connection(),
            "mongodb_context": self.check_mongodb_context(),
        }
        for db, status in results.items():
            logging.info(f"{db} connection status: {'SUCCESS' if status else 'FAILED'}")
        return all(results.values())  # Return True only if all connections are successful


if __name__ == "__main__":
    # Initialize the DatabaseConnectionChecker with retry parameters from environment variables
    checker = DatabaseConnectionChecker(
        retries=int(os.getenv("DB_RETRIES", 3)),  # Default retries to 3 if not set
        delay=int(os.getenv("DB_DELAY", 2))  # Default delay to 2 seconds if not set
    )

    # Run all connection checks and log the results
    try:
        if checker.check_all_connections():
            logging.info("All database connections (including context-managed) are successful.")
        else:
            logging.error("Some database connections failed.")
    except DatabaseConnectionError as e:
        logging.critical(f"Critical failure: {e}")
