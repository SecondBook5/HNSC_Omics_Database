# File: utils/connection_checker.py
# This script provides functionality to check connections to PostgreSQL and MongoDB databases
# with retry logic, enhanced error handling, exponential backoff for retries, and environment variable validation.

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
        self.db_type = db_type  # Store the database type that caused the error
        super().__init__(f"{message}: {db_type}")  # Format the error message


# Configure logging settings
log_level = logging.DEBUG if os.getenv("DEBUG", "False").lower() == "true" else logging.INFO
log_file = os.getenv("LOG_FILE", None)  # Optional log file location from environment variables
logging.basicConfig(
    level=log_level,  # Set the log level based on the environment variable
    format='%(asctime)s - %(levelname)s - %(message)s',  # Define the log format
    filename=log_file if log_file else None  # Log to a file if specified, otherwise log to the console
)


def get_env_variable(key: str, default=None):
    """
    Fetches and validates an environment variable.

    Args:
        key (str): Environment variable key to fetch.
        default: Default value if the variable is not set.

    Returns:
        str: Value of the environment variable.

    Raises:
        RuntimeError: If the environment variable is missing and no default is provided.
    """
    value = os.getenv(key, default)  # Retrieve the environment variable value
    if value is None:  # Check if the value is None and no default is provided
        raise RuntimeError(f"Environment variable '{key}' is required but not set.")  # Raise an error
    return value  # Return the environment variable value


class DatabaseConnectionChecker:
    """
    Checks the connection to PostgreSQL and MongoDB databases with retry logic and exponential backoff.
    """

    def __init__(self, retries: int = 3, delay: int = 2) -> None:
        """
        Initializes the DatabaseConnectionChecker with retry parameters.

        Args:
            retries (int): Maximum number of retry attempts.
            delay (int): Delay (in seconds) between retry attempts.
        """
        self.retries = max(retries, 1)  # Ensure retries are at least 1
        self.delay = max(delay, 0)  # Ensure delay is non-negative

    def check_postgresql_connection(self) -> bool:
        """
        Attempts to connect to PostgreSQL using the engine from db_config with retry logic.

        Returns:
            bool: True if the connection is successful, False otherwise.

        Raises:
            DatabaseConnectionError: If the connection fails after the maximum number of retries.
        """
        for attempt in range(1, self.retries + 1):  # Loop for the specified number of retries
            try:
                engine = get_postgres_engine()  # Retrieve the PostgreSQL engine
                if engine is None:  # Check if the engine was not created successfully
                    logging.error("PostgreSQL engine could not be created. Check environment variables.")
                    return False
                with engine.connect() as conn:  # Use the engine to establish a connection
                    conn.execute(text("SELECT 1"))  # Execute a simple query to validate the connection
                logging.info("PostgreSQL connection successful.")  # Log success
                return True  # Return True for successful connection
            except OperationalError as e:  # Handle operational errors during connection
                logging.warning(
                    f"Attempt {attempt}: PostgreSQL connection failed. Error: {e}. Retrying in {self.delay * (2 ** (attempt - 1))} seconds..."
                )
                time.sleep(self.delay * (2 ** (attempt - 1)))  # Apply exponential backoff
        raise DatabaseConnectionError("PostgreSQL")  # Raise a custom exception if all retries fail

    def check_mongodb_connection(self) -> bool:
        """
        Attempts to connect to MongoDB using the client from db_config with retry logic.

        Returns:
            bool: True if the connection is successful, False otherwise.

        Raises:
            DatabaseConnectionError: If the connection fails after the maximum number of retries.
        """
        for attempt in range(1, self.retries + 1):  # Loop for the specified number of retries
            try:
                client = get_mongo_client()  # Retrieve the MongoDB client
                if client is None:  # Check if the client was not created successfully
                    logging.error("MongoDB client could not be created. Check environment variables.")
                    return False
                client.admin.command('ping')  # Send a ping command to validate the connection
                logging.info("MongoDB connection successful.")  # Log success
                return True  # Return True for successful connection
            except ConnectionFailure as e:  # Handle connection failures
                logging.warning(
                    f"Attempt {attempt}: MongoDB connection failed. Error: {e}. Retrying in {self.delay * (2 ** (attempt - 1))} seconds..."
                )
                time.sleep(self.delay * (2 ** (attempt - 1)))  # Apply exponential backoff
        raise DatabaseConnectionError("MongoDB")  # Raise a custom exception if all retries fail

    def check_all_connections(self):
        """
        Checks all database connections (PostgreSQL and MongoDB).

        Returns:
            bool: True if all connections are successful, False otherwise.
        """
        results = {
            "postgresql": self.check_postgresql_connection(),
            "mongodb": self.check_mongodb_connection(),
        }
        for db, status in results.items():  # Iterate over connection results
            logging.info(f"{db} connection status: {'SUCCESS' if status else 'FAILED'}")  # Log status for each database
        return all(results.values())  # Return True only if all connections are successful


if __name__ == "__main__":
    # Initialize the DatabaseConnectionChecker with retry parameters from environment variables
    checker = DatabaseConnectionChecker(
        retries=int(get_env_variable("DB_RETRIES", 3)),  # Get retries from environment variable with default 3
        delay=int(get_env_variable("DB_DELAY", 2))  # Get delay from environment variable with default 2 seconds
    )

    # Run all connection checks and log the results
    try:
        if checker.check_all_connections():  # Check all connections
            logging.info("All database connections are successful.")  # Log success
        else:
            logging.error("Some database connections failed.")  # Log failure
    except DatabaseConnectionError as e:  # Handle connection errors
        logging.critical(f"Critical failure: {e}")  # Log critical failure
