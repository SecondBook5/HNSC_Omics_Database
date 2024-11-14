# File: pipeline/geo_pipeline/geo_metadata_uploader.py

# Import the base class for database uploaders to extend with custom functionality
from pipeline.abstract_etl.database_uploader import DatabaseUploader
# Import necessary typing for specifying data types in function arguments and return values
from typing import Dict, Union
# Import psycopg2 components for SQL commands and database error handling
from psycopg2 import sql, OperationalError, DatabaseError
# Import SQLAlchemy Engine for flexible database connection handling
from sqlalchemy.engine import Engine


class GeoMetadataUploader(DatabaseUploader):
    """
    GEO metadata uploader for PostgreSQL, extending the DatabaseUploader base class.
    This uploader handles connection establishment, metadata upload, and connection cleanup.

    Attributes:
        engine (Engine): SQLAlchemy Engine for managing PostgreSQL connections.
        debug (bool): If True, enables debug-level logging.
    """

    def __init__(self, engine: Engine, debug: bool = False) -> None:
        """
        Initialize the uploader with an SQLAlchemy Engine for PostgreSQL and optional debug mode.

        Args:
            engine (Engine): SQLAlchemy Engine object for connecting to PostgreSQL.
            debug (bool): Enables debug logging if set to True.
        """
        # Call the parent DatabaseUploader constructor with an empty dictionary for compatibility
        super().__init__(connection_params={})
        # Store the SQLAlchemy Engine for database connection management
        self.engine = engine
        # Set the debug flag to enable detailed logging if requested
        self.debug = debug
        # Initialize connection and cursor as None for defensive checks
        self.connection = None
        self.cursor = None
        # Establish the database connection upon instantiation
        self._connect()

    def _connect(self) -> bool:
        """
        Establishes a connection to PostgreSQL using the SQLAlchemy Engine.

        Returns:
            bool: True if connection is successful, False otherwise.

        Raises:
            ConnectionError: Raised if unable to connect.
        """
        try:
            # Use the engine to get a raw psycopg2-compatible connection
            self.connection = self.engine.raw_connection()
            # Create a cursor for executing SQL commands within the connection
            self.cursor = self.connection.cursor()

            # Log successful connection if debug mode is enabled
            if self.debug:
                print("[DEBUG] PostgreSQL connection established using SQLAlchemy Engine.")

            return True  # Indicate successful connection setup
        except OperationalError as e:
            # Raise a ConnectionError if the connection setup fails
            raise ConnectionError(f"Failed to establish connection to PostgreSQL: {e}")

    def upload_metadata(self, metadata: Dict[str, Dict[str, Union[str, None]]]) -> None:
        """
        Uploads extracted metadata to PostgreSQL by inserting records into the appropriate tables.

        Args:
            metadata (Dict[str, Dict[str, Union[str, None]]]): Metadata with table names as keys and data fields as values.

        Raises:
            ValueError: Raised if SQL errors or invalid data occur during the upload.
        """
        # Validate metadata to ensure it is a non-empty dictionary
        if not metadata or not isinstance(metadata, dict):
            raise ValueError("Invalid metadata provided for upload. Must be a non-empty dictionary.")

        # Iterate through each table and its fields in the metadata dictionary
        for table, fields in metadata.items():
            # Validate each field dictionary to ensure correct type and content
            if not fields or not isinstance(fields, dict):
                raise ValueError(f"Invalid fields for table '{table}': Must be a non-empty dictionary.")

            # Prepare SQL insertion query to insert records into the table
            try:
                # Dynamically construct SQL INSERT query with placeholders for data insertion
                insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                    sql.Identifier(table),  # Set table name dynamically
                    sql.SQL(', ').join(map(sql.Identifier, fields.keys())),  # Set column names dynamically
                    sql.SQL(', ').join(sql.Placeholder() * len(fields))  # Placeholders for column values
                )

                # Execute the SQL insertion using values from the fields dictionary
                self.cursor.execute(insert_query, tuple(fields.values()))
                # Commit the transaction to save changes in the database
                self.connection.commit()

                # Log success message if debug mode is enabled
                if self.debug:
                    print(f"[DEBUG] Successfully inserted data into '{table}': {fields}")
            except DatabaseError as e:
                # Roll back transaction in case of errors to prevent partial data insertion
                self.connection.rollback()

                # Log detailed error if debug mode is enabled
                if self.debug:
                    print(f"[DEBUG] Failed to insert data into '{table}': {fields}. Error: {e}")

                # Raise ValueError to notify the calling code of the failed upload
                raise ValueError(f"Failed to upload metadata to table '{table}': {e}")

    def _disconnect(self) -> None:
        """
        Closes the connection and cursor to PostgreSQL, releasing resources.

        Raises:
            ConnectionError: If errors occur during disconnection.
        """
        # Check if the cursor exists and close it if initialized
        if hasattr(self, 'cursor') and self.cursor:
            try:
                self.cursor.close()
                if self.debug:
                    print("[DEBUG] PostgreSQL cursor closed.")
            except Exception as e:
                # Raise ConnectionError if cursor closing fails
                raise ConnectionError(f"Error closing cursor: {e}")

        # Check if the connection exists and close it if initialized
        if hasattr(self, 'connection') and self.connection:
            try:
                self.connection.close()
                if self.debug:
                    print("[DEBUG] PostgreSQL connection closed.")
            except Exception as e:
                # Raise ConnectionError if connection closing fails
                raise ConnectionError(f"Error closing connection to PostgreSQL: {e}")
