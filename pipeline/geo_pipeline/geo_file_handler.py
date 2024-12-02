# File: pipeline/geo_pipeline/geo_file_handler.py

import logging  # For logging operations
import os  # For file and directory operations
import zipfile  # For compressing files into ZIP archives
from datetime import date  # For timestamping log entries
from typing import List, Optional  # For type hinting
from sqlalchemy.dialects.postgresql import insert  # For upsert database operations
from sqlalchemy.exc import SQLAlchemyError  # For handling SQLAlchemy-specific exceptions
from config.db_config import get_session_context  # For database session management
from db.schema.geo_metadata_schema import GeoMetadataLog  # For database schema access
from config.logger_config import configure_logger  # For centralized logging configuration


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
            logger: Logger instance for logging operations (default: centralized logger).
        """
        # Validate that the output directory path is not empty
        if not output_dir:
            raise ValueError("Output directory path cannot be empty.")

        # Validate that the GEO IDs file exists if provided
        if geo_ids_file and not os.path.isfile(geo_ids_file):
            raise ValueError(f"Invalid GEO IDs file path: {geo_ids_file}")

        # Store the GEO IDs file path
        self.geo_ids_file = geo_ids_file

        # Ensure the output directory exists or create it
        os.makedirs(output_dir, exist_ok=True)

        # Store the output directory path
        self.output_dir = output_dir

        # Store whether files should be compressed instead of deleted
        self.compress_files = compress_files

        # Configure a logger instance for logging operations
        self.logger = logger or configure_logger(
            name="GeoFileHandler",
            log_file="geo_file_handler.log",
            level=logging.INFO,
            output="both"
        )

        # Log the initialization of the handler
        self.logger.info(f"Initialized GeoFileHandler with output_dir={output_dir}, compress_files={compress_files}")

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

            # Read GEO IDs from the file, stripping whitespace from each line
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

            # Log successful completion of the initialization process
            self.logger.info("Log table initialization complete.")
        except Exception as e:
            # Log and re-raise any errors during initialization
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
            # Construct the GEO directory path
            geo_dir = os.path.join(self.output_dir, geo_id)

            # Ensure the GEO directory exists
            if not os.path.exists(geo_dir):
                raise FileNotFoundError(f"Directory for GEO ID {geo_id} does not exist: {geo_dir}")

            # Validate that all files in file_names exist in the specified GEO directory
            validated_files = []
            for file_name in file_names:
                full_path = os.path.join(geo_dir, file_name)
                if os.path.isfile(full_path):  # Ensure it's a valid file, not a directory
                    validated_files.append(file_name)
                else:
                    self.logger.warning(f"File {file_name} for GEO ID {geo_id} does not exist.")

            # If no valid files are found, raise an exception
            if not validated_files:
                raise FileNotFoundError(f"No valid files found for GEO ID {geo_id} in directory {geo_dir}.")

            # Log the download operation
            self.logger.info(f"Logging download for GEO ID {geo_id} with files: {validated_files}")

            # Insert or update the log entry for the GEO ID in the database
            with get_session_context() as session:
                update_query = insert(GeoMetadataLog).values(
                    GeoID=geo_id,
                    Status="downloaded",
                    Message="Files downloaded successfully.",
                    FileNames=validated_files,
                    Timestamp=date.today(),
                ).on_conflict_do_update(
                    index_elements=["GeoID"],
                    set_={
                        "Status": "downloaded",
                        "Message": "Files downloaded successfully.",
                        "FileNames": validated_files,
                        "Timestamp": date.today(),
                    }
                )
                session.execute(update_query)
                session.commit()

            # Log successful update of the download log
            self.logger.info(f"Download log updated for GEO ID {geo_id}.")
        except Exception as e:
            # Log and re-raise any errors during the logging process
            self.logger.error(f"Failed to log download for GEO ID {geo_id}: {e}")
            raise

    def log_processed(self, geo_id: str) -> None:
        """
        Logs the processing or upload of metadata for a specific GEO ID.

        Args:
            geo_id (str): The GEO ID being logged.
        """
        try:
            # Log the start of the processing operation
            self.logger.info(f"Logging processing/upload for GEO ID {geo_id}.")

            # Use a database session to update the log entry for the GEO ID
            with get_session_context() as session:
                # Insert or update the log entry to mark the GEO ID as processed
                update_query = insert(GeoMetadataLog).values(
                    GeoID=geo_id,
                    Status="processed",
                    Message="Metadata uploaded successfully.",
                    Timestamp=date.today()
                ).on_conflict_do_update(
                    index_elements=["GeoID"],
                    set_={
                        "Status": "processed",
                        "Message": "Metadata uploaded successfully.",
                        "Timestamp": date.today()
                    }
                )
                # Execute the query and commit changes to the database
                session.execute(update_query)
                session.commit()

            # Log successful update of the processing log
            self.logger.info(f"Processing log updated for GEO ID {geo_id}.")
        except Exception as e:
            # Log and re-raise any errors during the logging process
            self.logger.error(f"Failed to log processing for GEO ID {geo_id}: {e}")
            raise

    def clean_files(self, geo_id: str) -> None:
        """
        Cleans up downloaded files for a specific GEO ID by compressing or deleting them.

        Args:
            geo_id (str): The GEO ID whose files are being cleaned.
        """
        # Construct the full path to the GEO ID's directory
        geo_dir = os.path.join(self.output_dir, geo_id)

        # Debugging: Log the compress_files value
        self.logger.debug(f"Compress files option is set to: {self.compress_files}")

        if not os.path.exists(geo_dir):
            self.logger.warning(f"No files found for GEO ID {geo_id}. Skipping cleanup.")
            return

        try:
            if self.compress_files:
                # Create a ZIP archive of the directory
                zip_path = f"{geo_dir}.zip"
                self.logger.debug(f"Attempting to create ZIP file at {zip_path}")
                with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                    for root, _, files in os.walk(geo_dir):
                        for file in files:
                            full_path = os.path.join(root, file)
                            arcname = os.path.relpath(full_path, start=self.output_dir)
                            zipf.write(full_path, arcname=arcname)
                self.logger.info(f"Compressed files for GEO ID {geo_id} into {zip_path}")

                # Validate the ZIP file creation
                if not os.path.exists(zip_path):
                    raise RuntimeError(f"Failed to create ZIP file for GEO ID {geo_id}")

            # Remove all files in the directory
            for root, _, files in os.walk(geo_dir):
                for file in files:
                    os.remove(os.path.join(root, file))
            os.rmdir(geo_dir)

            # Validate directory deletion
            if os.path.exists(geo_dir):
                raise RuntimeError(f"Failed to delete directory for GEO ID {geo_id}")

            # Log successful cleanup
            self.logger.info(f"Cleaned up files for GEO ID {geo_id} successfully.")
        except Exception as e:
            # Log and re-raise any errors during the cleanup process
            self.logger.error(f"Failed to clean files for GEO ID {geo_id}: {e}")
            raise
