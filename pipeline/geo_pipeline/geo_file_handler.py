# File: pipeline/geo_pipeline/geo_file_handler.py

import os
import json
import zipfile
from datetime import date
from typing import List, Optional
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from config.db_config import get_session_context
from db.schema.metadata_schema import GeoMetadataLog
from config.logger_config import configure_logger

# Initialize centralized logger
logger = configure_logger(name="GeoFileHandler", log_file="geo_file_handler.log")


class GeoFileHandler:
    """
    Handles GEO file operations, including initialization, logging, and cleanup.
    """

    def __init__(self, geo_ids_file: Optional[str], output_dir: str, compress_files: bool = False):
        """
        Initializes the file handler.

        Args:
            geo_ids_file (Optional[str]): Path to the file containing GEO IDs (can be None for single ID handling).
            output_dir (str): Directory where files are downloaded or stored.
            compress_files (bool): If True, compress files instead of deleting them.
        """
        self.geo_ids_file = geo_ids_file
        self.output_dir = output_dir
        self.compress_files = compress_files

    def initialize_log_table(self) -> None:
        """
        Initializes the geo_metadata_log table by marking all GEO IDs as 'not_downloaded.'
        """
        try:
            if not self.geo_ids_file:
                raise ValueError("GEO IDs file must be provided for batch initialization.")

            if not os.path.exists(self.geo_ids_file):
                raise FileNotFoundError(f"GEO IDs file not found: {self.geo_ids_file}")

            # Read GEO IDs from the file
            with open(self.geo_ids_file, "r") as file:
                geo_ids = [line.strip() for line in file if line.strip()]

            if not geo_ids:
                raise ValueError("No GEO IDs found in the provided file.")

            logger.info(f"Initializing log table for {len(geo_ids)} GEO IDs.")

            with get_session_context() as session:
                for geo_id in geo_ids:
                    log_entry = {
                        "GeoID": geo_id,
                        "Status": "not_downloaded",
                        "Message": "Pending download.",
                        "FileNames": [],
                        "Timestamp": date.today(),
                    }
                    insert_query = insert(GeoMetadataLog).values(log_entry).on_conflict_do_nothing()
                    session.execute(insert_query)
                session.commit()
            logger.info("Log table initialization complete.")
        except Exception as e:
            logger.error(f"Failed to initialize log table: {e}")
            raise

    def log_download(self, geo_id: str, file_names: List[str]) -> None:
        """
        Logs the download of files for a specific GEO ID.

        Args:
            geo_id (str): The GEO ID being logged.
            file_names (List[str]): List of downloaded file names.
        """
        try:
            logger.info(f"Logging download for GEO ID {geo_id} with files: {file_names}.")
            with get_session_context() as session:
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
            logger.info(f"Download log updated for GEO ID {geo_id}.")
        except Exception as e:
            logger.error(f"Failed to log download for GEO ID {geo_id}: {e}")
            raise

    def log_processed(self, geo_id: str) -> None:
        """
        Logs the processing/upload of metadata for a specific GEO ID.

        Args:
            geo_id (str): The GEO ID being logged.
        """
        try:
            logger.info(f"Logging processing/upload for GEO ID {geo_id}.")
            with get_session_context() as session:
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
            logger.info(f"Processing log updated for GEO ID {geo_id}.")
        except Exception as e:
            logger.error(f"Failed to log processing for GEO ID {geo_id}: {e}")
            raise

    def clean_files(self, geo_id: str) -> None:
        """
        Cleans up downloaded files for a specific GEO ID.

        Args:
            geo_id (str): The GEO ID whose files should be cleaned.
        """
        try:
            geo_dir = os.path.join(self.output_dir, geo_id)
            if not os.path.exists(geo_dir):
                logger.warning(f"No files found for GEO ID {geo_id}. Skipping cleanup.")
                return

            if self.compress_files:
                # Compress the directory into a zip file
                zip_path = f"{geo_dir}.zip"
                with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                    for root, _, files in os.walk(geo_dir):
                        for file in files:
                            full_path = os.path.join(root, file)
                            arcname = os.path.relpath(full_path, start=self.output_dir)
                            zipf.write(full_path, arcname=arcname)
                logger.info(f"Compressed files for GEO ID {geo_id} into {zip_path}.")
                # Delete the original directory
                for root, _, files in os.walk(geo_dir):
                    for file in files:
                        os.remove(os.path.join(root, file))
                os.rmdir(geo_dir)
            else:
                # Delete the files and directory
                for root, _, files in os.walk(geo_dir):
                    for file in files:
                        os.remove(os.path.join(root, file))
                os.rmdir(geo_dir)
                logger.info(f"Deleted files for GEO ID {geo_id}.")
        except Exception as e:
            logger.error(f"Failed to clean files for GEO ID {geo_id}: {e}")
            raise
