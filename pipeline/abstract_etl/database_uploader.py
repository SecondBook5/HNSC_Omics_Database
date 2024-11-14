# File: pipeline/abstract_etl/database_uploader.py

from abc import ABC, abstractmethod  # Import ABC and abstractmethod for abstract class definition
from typing import Dict, Union, Any  # Import typing for type annotations
import logging                       # Import logging for logging information
from utils.connection_checker import DatabaseConnectionChecker  # Import connection checker for defensive checks

# Initialize logger
logger = logging.getLogger(__name__)  # Set up logger for this module
logging.basicConfig(level=logging.INFO)  # Set logging level to INFO


class DatabaseUploader(ABC):
    """
    Abstract base class for uploading metadata to databases.
    Ensures that each subclass defines specific connection, upload, and disconnection logic.
    Uses DatabaseConnectionChecker for connection validation.
    """

    def __init__(self, connection_params: Dict[str, Any], retries: int = 3, delay: int = 2) -> None:
        # Initialize connection parameters for database configuration
        self.connection_params = connection_params
        # Initialize DatabaseConnectionChecker to handle connectivity checks
        self.connection_checker = DatabaseConnectionChecker(retries=retries, delay=delay)

    def connect(self) -> None:
        """
        Connect to the database and check connection stability using DatabaseConnectionChecker.

        Raises:
            ConnectionError: If unable to establish a stable database connection.
        """
        # Attempt to establish a connection to the database
        if self._connect():
            # Verify the connection with connection checker to ensure stability
            if not self._check_connection():
                # Raise error if connection verification fails
                raise ConnectionError("Failed to establish a stable database connection.")
        else:
            # Raise error if connection setup fails
            raise ConnectionError("Database connection setup failed.")

    def _check_connection(self) -> bool:
        """
        Validate database connectivity based on database type.

        Returns:
            bool: True if the connection is stable, False otherwise.
        """
        # Check if database type is PostgreSQL
        if "postgres" in self.connection_params.get("db_type", "").lower():
            return self.connection_checker.check_postgresql_connection()
        # Check if database type is MongoDB
        elif "mongo" in self.connection_params.get("db_type", "").lower():
            return self.connection_checker.check_mongodb_connection()
        else:
            # Log a warning for unsupported database types
            logger.warning("Unsupported database type for connection checking.")
            return False

    @abstractmethod
    def _connect(self) -> bool:
        """
        Abstract method to set up the database connection.
        Must be implemented in each subclass.

        Returns:
            bool: True if connection setup succeeds, False otherwise.
        """
        pass

    @abstractmethod
    def upload_metadata(self, metadata: Dict[str, Union[str, Dict]]) -> None:
        """
        Abstract method to upload metadata to the database.

        Args:
            metadata (Dict[str, Union[str, Dict]]): The metadata to upload.

        Raises:
            ValueError: If metadata structure is invalid.
        """
        pass

    def disconnect(self) -> None:
        """
        Close the database connection gracefully.

        Raises:
            ConnectionError: If disconnection fails.
        """
        try:
            # Call abstract disconnection method implemented in subclass
            self._disconnect()
            # Log successful disconnection
            logger.info("Database connection closed successfully.")
        except Exception as e:
            # Raise error if disconnection fails
            raise ConnectionError(f"Failed to disconnect from database: {e}")

    @abstractmethod
    def _disconnect(self) -> None:
        """
        Abstract method to close the database connection.
        Must be implemented in each subclass.
        """
        pass
