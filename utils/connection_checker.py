# HNSC_Omics_Database/utils/connection_checker.py
# This script provides functionality to check connections to PostgreSQL and MongoDB databases
# with retry logic to handle transient connection issues. It uses connection functions from db_config.py,
# keeping configuration centralized and enhancing modularity.

import time  # Import time module to enable delays between retry attempts
import logging  # Import logging module for structured logging of connection status
from sqlalchemy.exc import OperationalError  # Import OperationalError to catch PostgreSQL connection errors
from pymongo.errors import ConnectionFailure  # Import ConnectionFailure to catch MongoDB connection errors
from db.db_config import get_postgres_engine, get_mongo_client  # Import connection functions from db_config
from sqlalchemy import text

# Configure the logging settings
# Set the logging level to INFO to show general info and warning/error messages in a consistent format
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


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
        # Set the number of retries to attempt on failure
        self.retries: int = retries
        # Set the delay (in seconds) between each retry attempt
        self.delay: int = delay

    def check_postgresql_connection(self) -> bool:
        """
        Attempts to connect to PostgreSQL using the engine from db_config with retry logic.

        Returns:
            bool: True if the connection is successful, False otherwise.
        """
        # Loop over the range of retries, starting from 1 up to the specified number of attempts
        for attempt in range(1, self.retries + 1):
            try:
                # Get the PostgreSQL engine from db_config, which uses environment variables
                engine = get_postgres_engine()

                # Check if get_postgres_engine returned None, which indicates a setup issue
                if engine is None:
                    # Log an error and return False if the engine is None, as we can't proceed
                    logging.error("PostgreSQL engine could not be created. Check environment variables.")
                    return False

                # Connect to PostgreSQL and execute a simple test query to confirm connectivity
                with engine.connect() as conn:
                    # Run a basic SQL command "SELECT 1" to validate the connection
                    conn.execute(text("SELECT 1"))

                # Log success if the connection was established and return True
                logging.info("PostgreSQL connection successful.")
                return True

            # Catch any OperationalError that occurs during the connection attempt
            except OperationalError as e:
                # Log a warning with the error message and attempt number, then delay for retry
                logging.warning(
                    f"Attempt {attempt}: PostgreSQL connection failed. Error: {e}. Retrying in {self.delay} seconds...")
                time.sleep(self.delay)  # Wait before retrying

        # Log an error if all attempts fail and return False to indicate connection failure
        logging.error("PostgreSQL connection failed after maximum retries.")
        return False

    def check_mongodb_connection(self) -> bool:
        """
        Attempts to connect to MongoDB using the client from db_config with retry logic.

        Returns:
            bool: True if the connection is successful, False otherwise.
        """
        # Loop over the range of retries, starting from 1 up to the specified number of attempts
        for attempt in range(1, self.retries + 1):
            try:
                # Get the MongoDB client from db_config, which uses environment variables
                client = get_mongo_client()

                # Check if get_mongo_client returned None, which indicates a setup issue
                if client is None:
                    # Log an error and return False if the client is None, as we can't proceed
                    logging.error("MongoDB client could not be created. Check environment variables.")
                    return False

                # Send a ping command to MongoDB to validate the connection
                client.admin.command('ping')

                # Log success if the connection was established and return True
                logging.info("MongoDB connection successful.")
                return True

            # Catch any ConnectionFailure that occurs during the connection attempt
            except ConnectionFailure as e:
                # Log a warning with the error message and attempt number, then delay for retry
                logging.warning(
                    f"Attempt {attempt}: MongoDB connection failed. Error: {e}. Retrying in {self.delay} seconds...")
                time.sleep(self.delay)  # Wait before retrying

        # Log an error if all attempts fail and return False to indicate connection failure
        logging.error("MongoDB connection failed after maximum retries.")
        return False


# Only run the following code if this script is executed directly
if __name__ == "__main__":
    # Initialize the DatabaseConnectionChecker with default retry parameters (3 retries, 2-second delay)
    checker = DatabaseConnectionChecker()

    # Attempt to check PostgreSQL connection and log the result
    checker.check_postgresql_connection()

    # Attempt to check MongoDB connection and log the result
    checker.check_mongodb_connection()
