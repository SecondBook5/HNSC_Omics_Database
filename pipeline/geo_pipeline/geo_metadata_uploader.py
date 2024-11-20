import logging
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from config.db_config import get_postgres_engine, get_session_context
from db.schema.metadata_schema import DatasetSeriesMetadata, DatasetSampleMetadata, GeoMetadataLog
from utils.exceptions import MissingForeignKeyError  # Import the custom exception
from typing import List, Dict

# Configure logger for the uploader
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GeoMetadataUploader:
    """
    Handles uploading GEO metadata (series, sample) and logging operations to the database.
    """

    def __init__(self) -> None:
        """
        Initializes the uploader with database configurations.
        """
        self.engine = get_postgres_engine()  # Access PostgreSQL engine for database operations

    def upload_series_metadata(self, series_metadata: List[Dict]) -> None:
        """
        Uploads series metadata to the database.

        Args:
            series_metadata (List[Dict]): List of dictionaries containing series metadata.

        Raises:
            ValueError: If the series_metadata list is empty.
            SQLAlchemyError: If a database error occurs during the upload.
        """
        # Ensure the input is not empty
        if not series_metadata:
            raise ValueError("Series metadata list cannot be empty.")

        try:
            # Use a session context for database operations
            with get_session_context() as session:
                for series in series_metadata:
                    # Perform an upsert (insert or do nothing if the row exists)
                    insert_query = insert(DatasetSeriesMetadata).values(series).on_conflict_do_nothing()
                    session.execute(insert_query)  # Execute the upsert query
                session.commit()  # Commit the transaction to save changes
                logger.info(f"Successfully inserted series metadata for {len(series_metadata)} entries.")
        except SQLAlchemyError as e:
            # Log and raise database errors
            logger.error(f"Database error during series metadata upload: {e}")
            raise

    def upload_sample_metadata(self, sample_metadata: List[Dict]) -> None:
        """
        Uploads sample metadata to the database. Ensures that corresponding series exist.

        Args:
            sample_metadata (List[Dict]): List of dictionaries containing sample metadata.

        Raises:
            MissingForeignKeyError: If referenced series do not exist.
            ValueError: If the sample_metadata list is empty.
            SQLAlchemyError: If a database error occurs during the upload.
        """
        # Ensure the input is not empty
        if not sample_metadata:
            raise ValueError("Sample metadata list cannot be empty.")

        try:
            # Use a session context for database operations
            with get_session_context() as session:
                # Extract all SeriesIDs from the sample metadata
                series_ids = {sample["SeriesID"] for sample in sample_metadata}

                # Query the database to find existing SeriesIDs
                existing_series = session.query(DatasetSeriesMetadata.SeriesID).filter(
                    DatasetSeriesMetadata.SeriesID.in_(series_ids)
                ).all()
                # Convert query results to a set of SeriesIDs
                existing_series_ids = {row.SeriesID for row in existing_series}

                # Find missing SeriesIDs by subtracting found SeriesIDs from the input
                missing_series_ids = series_ids - existing_series_ids

                # Raise a custom exception if any SeriesIDs are missing
                if missing_series_ids:
                    raise MissingForeignKeyError(
                        missing_keys=missing_series_ids,
                        foreign_key_name="SeriesID"
                    )

                # Perform upsert for each sample metadata entry
                for sample in sample_metadata:
                    insert_query = insert(DatasetSampleMetadata).values(sample).on_conflict_do_nothing()
                    session.execute(insert_query)  # Execute the upsert query
                session.commit()  # Commit the transaction to save changes
                logger.info(f"Successfully inserted sample metadata for {len(sample_metadata)} entries.")
        except SQLAlchemyError as e:
            # Log and raise database errors
            logger.error(f"Database error during sample metadata upload: {e}")
            raise
        except MissingForeignKeyError as e:
            # Log and re-raise custom validation errors
            logger.error(f"Validation error: {e}")
            raise

    def log_metadata_operation(self, geo_id: str, status: str, message: str, file_names: List[str] = None) -> None:
        """
        Logs an operation's status for a specific GEO ID.

        Args:
            geo_id (str): The GEO series or sample ID being logged.
            status (str): The status of the operation (e.g., 'downloaded', 'processed').
            message (str): A detailed message or description of the operation.
            file_names (List[str], optional): List of file names related to the GEO ID.

        Raises:
            ValueError: If 'geo_id' or 'status' is not provided.
            SQLAlchemyError: If a database error occurs during the log operation.
        """
        # Validate required inputs
        if not geo_id or not status:
            raise ValueError("Both 'geo_id' and 'status' are required for logging.")

        # Construct the log entry
        log_entry = {
            "geo_id": geo_id,
            "status": status,
            "message": message,
            "file_names": file_names,
        }

        try:
            # Use a session context for database operations
            with get_session_context() as session:
                # Perform an upsert for the log entry
                insert_query = insert(GeoMetadataLog).values(log_entry).on_conflict_do_nothing()
                session.execute(insert_query)
                session.commit()  # Commit the transaction to save the log entry
                logger.info(f"Log entry created for GEO ID '{geo_id}' with status '{status}'.")
        except SQLAlchemyError as e:
            # Log and raise database errors
            logger.error(f"Database error during log entry creation: {e}")
            raise
