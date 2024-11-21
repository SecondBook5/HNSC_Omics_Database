# File: pipeline/geo_pipeline/geo_file_handler.py

import os  # For file and directory operations
import zipfile  # For compressing files into ZIP archives
from datetime import date  # For timestamping log entries
from typing import List, Optional  # For type hinting
from sqlalchemy.dialects.postgresql import insert  # For upsert database operations
from sqlalchemy.exc import SQLAlchemyError  # For handling SQLAlchemy-specific exceptions
from config.db_config import get_session_context  # For database session management
from db.schema.metadata_schema import GeoMetadataLog  # For database schema access
from config.logger_config import configure_logger  # For centralized logging configuration

# Initialize a centralized logger for the file handler
logger = configure_logger(name="GeoFileHandler", log_file="geo_file_handler.log")


class GeoFileHandler:
    """
    Handles GEO file operations, including initialization, logging, and cleanup.
    """

    def __init__(self, geo_ids_file: Optional[str], output_dir: str, compress_files: bool = False, logger=None):
        """
        Initializes the file handler.

        Args:
            geo_ids_file (Optional[str]): Path to the file containing GEO IDs (optional).
            output_dir (str): Directory where files are downloaded or stored.
            compress_files (bool): If True, compress files instead of deleting them.
            logger: Logger instance for logging operations (default: create a new logger).
        """
        # Store the path to the GEO IDs file
        self.geo_ids_file = geo_ids_file
        # Define the output directory for file operations
        self.output_dir = output_dir
        # Specify whether to compress files instead of deleting them
        self.compress_files = compress_files
        # Use the provided logger or create a new one
        self.logger = logger or configure_logger(name="GeoFileHandler", log_file="geo_file_handler.log")

    def initialize_log_table(self) -> None:
        """
        Initializes the geo_metadata_log table by marking all GEO IDs as 'not_downloaded.'
        """
        try:
            # Ensure the GEO IDs file is provided
            if not self.geo_ids_file:
                raise ValueError("GEO IDs file must be provided for batch initialization.")

            # Ensure the GEO IDs file exists
            if not os.path.exists(self.geo_ids_file):
                raise FileNotFoundError(f"GEO IDs file not found: {self.geo_ids_file}")

            # Read GEO IDs from the file and strip whitespace
            with open(self.geo_ids_file, "r") as file:
                geo_ids = [line.strip() for line in file if line.strip()]

            # Ensure the file contains at least one GEO ID
            if not geo_ids:
                raise ValueError("No GEO IDs found in the provided file.")

            # Log the initialization process
            self.logger.info(f"Initializing log table for {len(geo_ids)} GEO IDs.")

            # Insert GEO IDs into the database log table
            with get_session_context() as session:
                for geo_id in geo_ids:
                    log_entry = {
                        "GeoID": geo_id,
                        "Status": "not_downloaded",
                        "Message": "Pending download.",
                        "FileNames": [],
                        "Timestamp": date.today(),
                    }
                    # Use an upsert query to avoid duplicates
                    insert_query = insert(GeoMetadataLog).values(log_entry).on_conflict_do_nothing()
                    session.execute(insert_query)
                session.commit()

            # Log completion of initialization
            self.logger.info("Log table initialization complete.")
        except Exception as e:
            # Log any errors during initialization
            self.logger.error(f"Failed to initialize log table: {e}")
            raise

    def log_download(self, geo_id: str, file_names: List[str]) -> None:
        """
        Logs the download of files for a specific GEO ID and verifies that the file names exist.

        Args:
            geo_id (str): The GEO ID being logged.
            file_names (List[str]): List of downloaded file names.
        """
        try:
            # Validate that all files in file_names exist in the specified GEO directory
            for file_name in file_names:
                full_path = os.path.join(self.output_dir, geo_id, file_name)
                if not os.path.exists(full_path):
                    raise FileNotFoundError(f"Expected file {full_path} does not exist.")

            # Log the download operation
            self.logger.info(f"Logging download for GEO ID {geo_id} with files: {file_names}.")
            with get_session_context() as session:
                # Insert or update the log entry for the GEO ID
                update_query = insert(GeoMetadataLog).values(
                    GeoID=geo_id,
                    Status="downloaded",
                    Message="Files downloaded successfully.",
                    FileNames=file_names,
                    Timestamp=date.today(),
                ).on_conflict_do_update(
                    index_elements=["GeoID"],
                    set_={
                        "Status": "downloaded",
                        "Message": "Files downloaded successfully.",
                        "FileNames": file_names,
                        "Timestamp": date.today(),
                    }
                )
                session.execute(update_query)
                session.commit()
            # Log successful update of the download log
            self.logger.info(f"Download log updated for GEO ID {geo_id}.")
        except Exception as e:
            # Log any errors during the logging process
            self.logger.error(f"Failed to log download for GEO ID {geo_id}: {e}")
            raise

    def log_processed(self, geo_id: str) -> None:
        """
        Logs the processing/upload of metadata for a specific GEO ID.

        Args:
            geo_id (str): The GEO ID being logged.
        """
        try:
            # Log the start of the processing operation
            self.logger.info(f"Logging processing/upload for GEO ID {geo_id}.")
            with get_session_context() as session:
                # Insert or update the log entry to mark the GEO ID as processed
                update_query = insert(GeoMetadataLog).values(
                    GeoID=geo_id,
                    Status="processed",
                    Message="Metadata uploaded successfully.",
                    Timestamp=date.today(),
                ).on_conflict_do_update(
                    index_elements=["GeoID"],
                    set_={
                        "Status": "processed",
                        "Message": "Metadata uploaded successfully.",
                        "Timestamp": date.today(),
                    }
                )
                session.execute(update_query)
                session.commit()
            # Log successful update of the processing log
            self.logger.info(f"Processing log updated for GEO ID {geo_id}.")
        except Exception as e:
            # Log any errors during the processing log update
            self.logger.error(f"Failed to log processing for GEO ID {geo_id}: {e}")
            raise

    def clean_files(self, geo_id: str) -> None:
        """
        Cleans up downloaded files for a specific GEO ID by either compressing or deleting them.
        Verifies that the cleanup was successful.

        Args:
            geo_id (str): The GEO ID whose files are being cleaned.
        """
        # Construct the full path to the GEO ID's directory
        geo_dir = os.path.join(self.output_dir, geo_id)

        # If the directory does not exist, log a warning and skip cleanup
        if not os.path.exists(geo_dir):
            self.logger.warning(f"No files found for GEO ID {geo_id}. Skipping cleanup.")
            return

        try:
            if self.compress_files:
                # Compress the directory into a ZIP archive
                zip_path = f"{geo_dir}.zip"
                with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                    for root, _, files in os.walk(geo_dir):
                        for file in files:
                            full_path = os.path.join(root, file)
                            arcname = os.path.relpath(full_path, start=self.output_dir)
                            zipf.write(full_path, arcname=arcname)
                # Log successful compression
                self.logger.info(f"Compressed files for GEO ID {geo_id} into {zip_path}.")

                # Validate compression and delete the original files
                if not os.path.exists(zip_path):
                    raise RuntimeError(f"Failed to create zip file for GEO ID {geo_id}.")
                for root, _, files in os.walk(geo_dir):
                    for file in files:
                        os.remove(os.path.join(root, file))
                os.rmdir(geo_dir)
            else:
                # Delete files and directory directly
                for root, _, files in os.walk(geo_dir):
                    for file in files:
                        os.remove(os.path.join(root, file))
                os.rmdir(geo_dir)

                # Validate deletion
                if os.path.exists(geo_dir):
                    raise RuntimeError(f"Failed to delete directory for GEO ID {geo_id}.")

            # Log successful cleanup
            self.logger.info(f"Cleaned up files for GEO ID {geo_id} successfully.")
        except Exception as e:
            # Log any errors during the cleanup process
            self.logger.error(f"Failed to clean files for GEO ID {geo_id}: {e}")
            raise
