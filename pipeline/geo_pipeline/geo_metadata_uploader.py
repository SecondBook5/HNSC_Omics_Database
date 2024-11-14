# File: pipeline/geo_pipeline/geo_metadata_uploader.py

from pipeline.abstract_etl.database_uploader import DatabaseUploader  # Import the base class for database uploaders
from typing import Dict, Union  # For type hints
import psycopg2  # Import for PostgreSQL database connection
from psycopg2 import sql, OperationalError, DatabaseError  # For SQL command building and connection errors
from utils.connection_checker import DatabaseConnectionChecker  # For checking stable connections


class GeoMetadataUploader(DatabaseUploader):
    """
    GEO metadata uploader for PostgreSQL, extending the DatabaseUploader base class.
    This uploader handles connection establishment, metadata upload, and connection cleanup.

    Attributes:
        connection_params (Dict[str, str]): Connection parameters for PostgreSQL.
        debug (bool): If True, enables debug-level logging.
    """

    def __init__(self, connection_params: Dict[str, str], retries: int = 3, delay: int = 2,
                 debug: bool = False) -> None:
        """
        Initialize the uploader with database connection parameters, retry settings, and optional debug mode.

        Args:
            connection_params (Dict[str, str]): Parameters for PostgreSQL connection.
            retries (int): Number of retry attempts for connecting (default is 3).
            delay (int): Delay (in seconds) between retries (default is 2).
            debug (bool): Enables debug logging if set to True.
        """
        # Initialize the base uploader with connection parameters
        super().__init__(connection_params)

        # Create an instance of DatabaseConnectionChecker with retry logic
        self.connection_checker = DatabaseConnectionChecker(retries=retries, delay=delay)

        # Set the debug flag for optional detailed logging
        self.debug = debug

        # Initialize connection and cursor attributes to None for defensive checks
        self.connection = None
        self.cursor = None

        # Establish the connection immediately upon instantiation
        self._connect()

    def _connect(self) -> bool:
        """
        Establishes a connection to PostgreSQL with retry logic for stability.

        Returns:
            bool: True if connection is successful, False otherwise.

        Raises:
            ConnectionError: Raised if unable to connect after all retries.
        """
        # Check if PostgreSQL is reachable and stable using retry logic
        if not self.connection_checker.check_postgresql_connection():
            raise ConnectionError("PostgreSQL connection failed after retries.")

        # Attempt to create the connection
        try:
            # Establish connection to PostgreSQL using provided parameters
            self.connection = psycopg2.connect(**self.connection_params)

            # Create a cursor for executing SQL commands
            self.cursor = self.connection.cursor()

            # Log successful connection if debug mode is enabled
            if self.debug:
                print("[DEBUG] PostgreSQL connection established.")

            return True
        except OperationalError as e:
            # Raise an error if connection setup fails
            raise ConnectionError(f"Failed to establish connection to PostgreSQL: {e}")

    def upload_metadata(self, metadata: Dict[str, Dict[str, Union[str, None]]]) -> None:
        """
        Uploads extracted metadata to PostgreSQL by inserting records.

        Args:
            metadata (Dict[str, Dict[str, Union[str, None]]]): Metadata with table names as keys and data fields as values.

        Raises:
            ValueError: Raised if the upload process encounters SQL errors or invalid data.
        """
        # Validate metadata as a non-empty dictionary
        if not metadata or not isinstance(metadata, dict):
            raise ValueError("Invalid metadata provided for upload. Must be a non-empty dictionary.")

        # Iterate through each table and its associated fields in the metadata
        for table, fields in metadata.items():
            # Validate each field dictionary for correct type and contents
            if not fields or not isinstance(fields, dict):
                raise ValueError(f"Invalid fields for table '{table}': Must be a non-empty dictionary.")

            # Prepare SQL insertion query
            try:
                # Construct SQL INSERT query using placeholders for safe data insertion
                insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                    sql.Identifier(table),  # Dynamically set table name
                    sql.SQL(', ').join(map(sql.Identifier, fields.keys())),  # Dynamically set column names
                    sql.SQL(', ').join(sql.Placeholder() * len(fields))  # Placeholder for each column value
                )

                # Execute the SQL insert with values from the field dictionary
                self.cursor.execute(insert_query, tuple(fields.values()))

                # Commit the transaction to save the inserted record
                self.connection.commit()

                # Log success message if debug mode is enabled
                if self.debug:
                    print(f"[DEBUG] Successfully inserted data into '{table}': {fields}")
            except DatabaseError as e:
                # Roll back transaction if an error occurs
                self.connection.rollback()

                # Log error in debug mode
                if self.debug:
                    print(f"[DEBUG] Failed to insert data into '{table}': {fields}. Error: {e}")

                # Raise error to alert calling functions
                raise ValueError(f"Failed to upload metadata to table '{table}': {e}")

    def _disconnect(self) -> None:
        """
        Closes the connection and cursor to PostgreSQL, releasing resources.

        Raises:
            ConnectionError: If errors occur during disconnection.
        """
        # Check if the cursor is initialized and close it if present
        if hasattr(self, 'cursor') and self.cursor:
            try:
                self.cursor.close()
                if self.debug:
                    print("[DEBUG] PostgreSQL cursor closed.")
            except Exception as e:
                # Raise error if cursor closing fails
                raise ConnectionError(f"Error closing cursor: {e}")

        # Check if the connection is initialized and close it if present
        if hasattr(self, 'connection') and self.connection:
            try:
                self.connection.close()
                if self.debug:
                    print("[DEBUG] PostgreSQL connection closed.")
            except Exception as e:
                # Raise error if connection closing fails
                raise ConnectionError(f"Error closing connection to PostgreSQL: {e}")

