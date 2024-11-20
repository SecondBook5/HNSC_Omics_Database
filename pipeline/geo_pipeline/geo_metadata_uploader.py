import logging
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from config.db_config import get_postgres_engine, get_session_context
from db.schema.metadata_schema import DatasetSeriesMetadata, DatasetSampleMetadata, GeoMetadataLog
from utils.exceptions import MissingForeignKeyError
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
        # Initialize the PostgreSQL engine for database operations
        self.engine = get_postgres_engine()

    def upload_series_metadata(self, session, series_metadata: List[Dict]) -> None:
        """
        Uploads series metadata to the database.

        Args:
            session: The database session to use for the operation.
            series_metadata (List[Dict]): List of dictionaries containing series metadata.

        Raises:
            ValueError: If the series_metadata list is empty.
            SQLAlchemyError: If a database error occurs during the upload.
        """
        # Validate that the series_metadata list is not empty
        if not series_metadata:
            raise ValueError("Series metadata list cannot be empty.")

        try:
            # Iterate over each series entry in the metadata
            for series in series_metadata:
                # Create an upsert query to insert the series data or do nothing if it already exists
                insert_query = insert(DatasetSeriesMetadata).values(series).on_conflict_do_nothing()
                # Execute the query within the provided session
                session.execute(insert_query)
            # Commit the transaction to save the changes
            session.commit()
            # Log a success message with the number of entries inserted
            logger.info(f"Successfully inserted series metadata for {len(series_metadata)} entries.")
        except SQLAlchemyError as e:
            # Log and re-raise the error if a database issue occurs
            logger.error(f"Database error during series metadata upload: {e}")
            raise

    def upload_sample_metadata(self, session, sample_metadata: List[Dict]) -> None:
        """
        Uploads sample metadata to the database. Ensures that corresponding series exist.

        Args:
            session: The database session to use for the operation.
            sample_metadata (List[Dict]): List of dictionaries containing sample metadata.

        Raises:
            MissingForeignKeyError: If referenced series do not exist.
            ValueError: If the sample_metadata list is empty.
            SQLAlchemyError: If a database error occurs during the upload.
        """
        # Validate that the sample_metadata list is not empty
        if not sample_metadata:
            raise ValueError("Sample metadata list cannot be empty.")

        try:
            # Extract the unique SeriesIDs from the sample metadata
            series_ids = {sample["SeriesID"] for sample in sample_metadata}
            # Query the database for existing SeriesIDs in the DatasetSeriesMetadata table
            existing_series = session.query(DatasetSeriesMetadata.SeriesID).filter(
                DatasetSeriesMetadata.SeriesID.in_(series_ids)
            ).all()
            # Convert the query results to a set of existing SeriesIDs
            existing_series_ids = {row.SeriesID for row in existing_series}
            # Identify missing SeriesIDs by subtracting existing IDs from the input IDs
            missing_series_ids = series_ids - existing_series_ids

            # Raise a custom exception if any SeriesIDs are missing
            if missing_series_ids:
                raise MissingForeignKeyError(
                    missing_keys=missing_series_ids,
                    foreign_key_name="SeriesID"
                )

            # Iterate over each sample entry in the metadata
            for sample in sample_metadata:
                # Create an upsert query to insert the sample data or do nothing if it already exists
                insert_query = insert(DatasetSampleMetadata).values(sample).on_conflict_do_nothing()
                # Execute the query within the provided session
                session.execute(insert_query)
            # Commit the transaction to save the changes
            session.commit()
            # Log a success message with the number of entries inserted
            logger.info(f"Successfully inserted sample metadata for {len(sample_metadata)} entries.")
        except SQLAlchemyError as e:
            # Log and re-raise the error if a database issue occurs
            logger.error(f"Database error during sample metadata upload: {e}")
            raise
        except MissingForeignKeyError as e:
            # Log and re-raise the custom validation error
            logger.error(f"Validation error: {e}")
            raise

    def log_metadata_operation(
        self, session, geo_id: str, status: str, message: str, file_names: List[str] = None
    ) -> None:
        """
        Logs an operation's status for a specific GEO ID.

        Args:
            session: The database session to use for the operation.
            geo_id (str): The GEO series or sample ID being logged.
            status (str): The status of the operation (e.g., 'downloaded', 'processed').
            message (str): A detailed message or description of the operation.
            file_names (List[str], optional): List of file names related to the GEO ID.

        Raises:
            ValueError: If 'geo_id' or 'status' is not provided.
            SQLAlchemyError: If a database error occurs during the log operation.
        """
        # Validate that geo_id and status are provided
        if not geo_id or not status:
            raise ValueError("Both 'geo_id' and 'status' are required for logging.")

        # Construct the log entry dictionary
        log_entry = {
            "geo_id": geo_id,
            "status": status,
            "message": message,
            "file_names": file_names,
        }

        try:
            # Create an upsert query to insert the log entry or do nothing if it already exists
            insert_query = insert(GeoMetadataLog).values(log_entry).on_conflict_do_nothing()
            # Execute the query within the provided session
            session.execute(insert_query)
            # Commit the transaction to save the log entry
            session.commit()
            # Log a success message indicating the log entry was created
            logger.info(f"Log entry created for GEO ID '{geo_id}' with status '{status}'.")
        except SQLAlchemyError as e:
            # Log and re-raise the error if a database issue occurs
            logger.error(f"Database error during log entry creation: {e}")
            raise
