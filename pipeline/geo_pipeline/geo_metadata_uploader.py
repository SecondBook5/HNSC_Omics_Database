# File: pipeline/geo_pipeline/geo_metadata_uploader.py

from sqlalchemy.dialects.postgresql import insert  # For database upsert queries
from sqlalchemy.exc import SQLAlchemyError  # For handling database errors
from config.db_config import get_postgres_engine, get_session_context  # For database configuration
from db.schema.metadata_schema import DatasetSeriesMetadata, DatasetSampleMetadata, GeoMetadataLog  # Database schema
from utils.exceptions import MissingForeignKeyError  # Custom exception for validation errors
from typing import List, Dict  # For type hinting
from config.logger_config import configure_logger  # Centralized logger configuration

# Initialize the centralized logger for the uploader
logger = configure_logger(name="GeoMetadataUploader", log_file="geo_metadata_uploader.log")

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
        if not series_metadata:
            logger.error("Series metadata list cannot be empty.")
            raise ValueError("Series metadata list cannot be empty.")

        try:
            for series in series_metadata:
                insert_query = insert(DatasetSeriesMetadata).values(series).on_conflict_do_nothing()
                session.execute(insert_query)
            session.commit()
            logger.info(f"Successfully inserted series metadata for {len(series_metadata)} entries.")
        except SQLAlchemyError as e:
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
        if not sample_metadata:
            logger.error("Sample metadata list cannot be empty.")
            raise ValueError("Sample metadata list cannot be empty.")

        try:
            series_ids = {sample["SeriesID"] for sample in sample_metadata}
            existing_series = session.query(DatasetSeriesMetadata.SeriesID).filter(
                DatasetSeriesMetadata.SeriesID.in_(series_ids)
            ).all()
            existing_series_ids = {row.SeriesID for row in existing_series}
            missing_series_ids = series_ids - existing_series_ids

            if missing_series_ids:
                logger.error(f"Missing foreign keys for SeriesID: {missing_series_ids}")
                raise MissingForeignKeyError(missing_keys=missing_series_ids, foreign_key_name="SeriesID")

            for sample in sample_metadata:
                insert_query = insert(DatasetSampleMetadata).values(sample).on_conflict_do_nothing()
                session.execute(insert_query)
            session.commit()
            logger.info(f"Successfully inserted sample metadata for {len(sample_metadata)} entries.")
        except SQLAlchemyError as e:
            logger.error(f"Database error during sample metadata upload: {e}")
            raise
        except MissingForeignKeyError as e:
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
        if not geo_id or not status:
            logger.error("Both 'geo_id' and 'status' are required for logging.")
            raise ValueError("Both 'geo_id' and 'status' are required for logging.")

        log_entry = {
            "geo_id": geo_id,
            "status": status,
            "message": message,
            "file_names": file_names,
        }

        try:
            insert_query = insert(GeoMetadataLog).values(log_entry).on_conflict_do_nothing()
            session.execute(insert_query)
            session.commit()
            logger.info(f"Log entry created for GEO ID '{geo_id}' with status '{status}'.")
        except SQLAlchemyError as e:
            logger.error(f"Database error during log entry creation: {e}")
            raise
