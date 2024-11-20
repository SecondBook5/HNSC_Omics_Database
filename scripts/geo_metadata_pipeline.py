# File: scripts/geo_metadata_pipeline.py

"""
GEO Metadata Pipeline

This script defines and executes a pipeline for processing GEO (Gene Expression Omnibus) metadata.
The pipeline automates the following tasks:

1. **Database Connection Validation**:
    - Ensures that the pipeline has a valid connection to the PostgreSQL database before proceeding.
    - Uses a `DatabaseConnectionChecker` for defensive programming to prevent pipeline failures due to connection issues.

2. **GEO ID Initialization**:
    - Reads GEO IDs from an input file (`geo_ids.txt`) and initializes their status in the database as `not-downloaded`.
    - Logs each GEO ID in the `geo_metadata_log` table with a status message.

3. **Metadata Download**:
    - Downloads GEO files (e.g., XML files) for each GEO ID from the NCBI FTP server.
    - Ensures the downloaded files are saved to a specified directory (`raw_metadata`).

4. **Metadata Extraction**:
    - Extracts metadata from the downloaded GEO files using a predefined template (`geo_tag_template.json`).
    - Metadata includes information about series and samples for each GEO ID.

5. **Metadata Upload**:
    - Uploads the extracted series and sample metadata to their respective database tables:
        - `dataset_series_metadata`: Stores series-level metadata.
        - `dataset_sample_metadata`: Stores sample-level metadata.
    - Ensures data integrity by respecting database constraints, such as foreign key relationships.

6. **Logging**:
    - Logs the status of each operation (e.g., downloaded, extracted, processed, or failed) in the `geo_metadata_log` table.
    - Provides detailed error logging for debugging and audit purposes.

7. **Parallel Processing**:
    - Uses a `ThreadPoolExecutor` for parallel execution of tasks, allowing multiple GEO IDs to be processed simultaneously.

8. **Defensive Programming**:
    - Implements robust error handling and custom exceptions (e.g., `MissingForeignKeyError`) to manage edge cases and ensure pipeline reliability.

### Pipeline Workflow:
- Read GEO IDs from the input file.
- Verify database connections.
- Initialize GEO IDs in the database.
- For each GEO ID:
    - Download the associated file.
    - Extract series and sample metadata.
    - Upload the metadata to the database.
    - Log the operation status.
- Execute the above steps in parallel for efficiency.

### Configuration:
- Log files, output directories, and metadata templates are configured at the beginning of the script.
- The database schema and connection details are managed separately through the `db_config.py` module.

### Usage:
- Run this script directly to process GEO IDs from the `geo_ids.txt` file.
- Ensure all configurations, including database connections and file paths, are properly set up before execution.

"""

# Import necessary modules for pipeline execution
import os  # For handling file and directory operations
import logging  # For structured logging of pipeline events
from pipeline.geo_pipeline.geo_metadata_downloader import GeoMetadataDownloader  # For downloading GEO files
from pipeline.geo_pipeline.geo_metadata_extractor import GeoMetadataExtractor  # For extracting metadata
from pipeline.geo_pipeline.geo_metadata_uploader import GeoMetadataUploader  # For uploading metadata to the database
from config.db_config import get_session_context  # For managing database session
from utils.connection_checker import DatabaseConnectionChecker  # For validating database connections
from utils.exceptions import MissingForeignKeyError  # Custom exception for missing foreign key errors
from concurrent.futures import ThreadPoolExecutor  # For parallel execution of tasks

# ---------------- Configuration ----------------

# Define the base directory of the script
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

# Define the directory for log files
LOG_DIR = os.path.join(BASE_DIR, "../logs")

# Define the path for the log file
LOG_FILE = os.path.join(LOG_DIR, "geo_metadata_pipeline.log")

# Define the directory for output metadata files
OUTPUT_DIR = os.path.join(BASE_DIR, "../resources/data/metadata/geo_metadata/raw_metadata")

# Define the path to the file containing GEO IDs
GEO_IDS_FILE = os.path.join(BASE_DIR, "../resources/geo_ids.txt")

# Define the path to the metadata extraction template
EXTRACTION_TEMPLATE = os.path.join(BASE_DIR, "../resources/geo_tag_template.json")

# Ensure the log and output directories exist, creating them if necessary
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Configure logging for the pipeline
logging.basicConfig(
    level=logging.INFO,  # Set log level to INFO
    filename=LOG_FILE,  # Log messages will be written to this file
    filemode="w",  # Overwrite the log file on each run
    format="%(asctime)s - %(levelname)s - %(message)s"  # Define the log message format
)
logger = logging.getLogger("geo_metadata_pipeline")  # Create a logger for this script

# ---------------- GEO Metadata Pipeline ----------------


class GeoMetadataPipeline:
    """
    Pipeline for processing GEO metadata.
    Handles downloading, extraction, logging, and uploading metadata.
    """

    def __init__(self, geo_ids: list):
        # Initialize the list of GEO IDs to process
        self.geo_ids = geo_ids

        # Create an instance of the downloader for downloading GEO files
        self.downloader = GeoMetadataDownloader(output_dir=OUTPUT_DIR, debug=True, logger=logger)

        # Create an instance of the uploader for uploading metadata to the database
        self.uploader = GeoMetadataUploader()

        # Define the path to the metadata extraction template
        self.extractor_template = EXTRACTION_TEMPLATE

        # Create an instance of the connection checker for validating database connections
        self.connection_checker = DatabaseConnectionChecker()

    def check_connections(self):
        """Ensures database connections are valid before proceeding."""
        # Check the PostgreSQL connection using the connection checker
        if not self.connection_checker.check_postgresql_connection():
            # Raise an error if the database connection fails
            raise RuntimeError("PostgreSQL connection failed. Aborting pipeline.")
        # Log success if the database connection is valid
        logger.info("Database connections verified.")

    def log_operation(self, geo_id, status, message, file_names=None):
        """Logs operation status to the geo_metadata_log table."""
        try:
            # Use a database session to log the operation
            with get_session_context() as session:
                self.uploader.log_metadata_operation(session, geo_id, status, message, file_names)
        except Exception as e:
            # Log an error if the operation logging fails
            logger.error(f"Failed to log operation for GEO ID {geo_id}: {e}")
            raise

    def upload_geo_ids(self):
        """Uploads GEO IDs to the database with initial status 'not-downloaded'."""
        # Create a list of dictionaries for each GEO ID with an initial 'not-downloaded' status
        geo_metadata = [{"geo_id": geo_id, "status": "not-downloaded"} for geo_id in self.geo_ids]

        try:
            # Open a session for interacting with the database
            with get_session_context() as session:
                # Iterate over the prepared GEO metadata
                for entry in geo_metadata:
                    # Extract the 'geo_id' for logging purposes
                    geo_id = entry["geo_id"]
                    # Log the operation to the database for the current GEO ID
                    self.uploader.log_metadata_operation(
                        session=session,
                        geo_id=geo_id,
                        status="not-downloaded",
                        message="Initializing GEO ID"
                    )
            # Log a success message indicating all GEO IDs were uploaded
            logger.info("GEO IDs uploaded successfully.")
        except Exception as e:
            # Log a critical error if uploading GEO IDs fails
            logger.critical(f"Failed to upload GEO IDs: {e}")
            # Re-raise the exception to ensure proper error handling
            raise

    def download_and_extract(self, geo_id: str):
        """
        Downloads and extracts metadata for a GEO ID.
        Logs and uploads extracted metadata to the database.
        """
        try:
            # Step 1: Attempt to download the GEO file for the given GEO ID
            file_path = self.downloader.download_file(geo_id)
            # Check if the downloaded file exists; raise an error if not
            if not file_path or not os.path.exists(file_path):
                raise RuntimeError(f"File not found after download for GEO ID {geo_id}.")

            # Step 2: Log the successful download operation in the database
            with get_session_context() as session:
                self.uploader.log_metadata_operation(
                    session=session,  # Provide the active database session
                    geo_id=geo_id,  # Specify the GEO ID being logged
                    status="downloaded",  # Set the operation status to 'downloaded'
                    message=f"File downloaded to {file_path}",  # Log the file path
                    file_names=[file_path]  # Include the downloaded file's name
                )

            # Step 3: Initialize the metadata extractor for the downloaded file
            extractor = GeoMetadataExtractor(
                file_path=file_path,  # Provide the path to the downloaded file
                template_path=self.extractor_template,  # Specify the extraction template
                debug_mode=True  # Enable debugging mode for detailed logs
            )
            # Parse the metadata from the downloaded file
            metadata = extractor.parse()

            # Step 4: Log the successful metadata extraction operation
            with get_session_context() as session:
                self.uploader.log_metadata_operation(
                    session=session,  # Provide the active database session
                    geo_id=geo_id,  # Specify the GEO ID being logged
                    status="extracted",  # Set the operation status to 'extracted'
                    message="Metadata extracted successfully."  # Log the success message
                )

            # Step 5: Extract series and sample metadata from the parsed data
            series_metadata = metadata.get("series", [])  # Extract series metadata, default to an empty list
            sample_metadata = metadata.get("samples", [])  # Extract sample metadata, default to an empty list

            # Step 6: Upload the extracted metadata to the database
            with get_session_context() as session:
                # Upload series metadata if any is present
                if series_metadata:
                    self.uploader.upload_series_metadata(session, series_metadata)
                # Upload sample metadata if any is present
                if sample_metadata:
                    self.uploader.upload_sample_metadata(session, sample_metadata)

            # Log a success message for the entire process of the current GEO ID
            logger.info(f"Successfully processed GEO ID {geo_id}.")
        except MissingForeignKeyError as mfe:
            # Handle errors where referenced foreign keys are missing in the database
            logger.error(f"Foreign key error while processing GEO ID {geo_id}: {mfe}")
            # Log the failure in the database
            with get_session_context() as session:
                self.uploader.log_metadata_operation(
                    session=session,
                    geo_id=geo_id,
                    status="failed",
                    message=str(mfe)
                )
        except Exception as e:
            # Handle general errors during the download or extraction process
            logger.error(f"Failed to process GEO ID {geo_id}: {e}")
            # Log the failure in the database
            with get_session_context() as session:
                self.uploader.log_metadata_operation(
                    session=session,
                    geo_id=geo_id,
                    status="failed",
                    message=str(e)
                )
            # Re-raise the exception to ensure proper error handling
            raise

    def execute_pipeline(self):
        """Executes the pipeline for all GEO IDs in parallel."""
        # Use a ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor() as executor:
            # Submit tasks for downloading and extracting metadata for each GEO ID
            futures = {executor.submit(self.download_and_extract, geo_id): geo_id for geo_id in self.geo_ids}

            # Process the results of each future as they complete
            for future in futures:
                geo_id = futures[future]
                try:
                    # Retrieve the result of the future to detect any errors
                    future.result()
                except Exception as e:
                    # Log an error if processing fails for a specific GEO ID
                    logger.error(f"Error processing GEO ID {geo_id}: {e}")


# ---------------- Execution ----------------

if __name__ == "__main__":
    try:
        # Create an instance of the DatabaseConnectionChecker
        checker = DatabaseConnectionChecker()

        # Verify PostgreSQL connection before proceeding
        if not checker.check_postgresql_connection():
            raise RuntimeError("Database connection failed. Aborting pipeline execution.")

        # Check if the GEO IDs file exists
        if not os.path.exists(GEO_IDS_FILE):
            raise FileNotFoundError(f"GEO IDs file not found: {GEO_IDS_FILE}")

        # Read GEO IDs from the file
        with open(GEO_IDS_FILE, "r") as f:
            geo_ids = [line.strip() for line in f if line.strip()]

        # Raise an error if no GEO IDs are found
        if not geo_ids:
            raise ValueError("No GEO IDs found in the file.")

        # Log the total number of GEO IDs to process
        logger.info(f"Processing {len(geo_ids)} GEO IDs.")

        # Create an instance of the pipeline with the GEO IDs
        pipeline = GeoMetadataPipeline(geo_ids=geo_ids)

        # Check database connections
        pipeline.check_connections()

        # Upload GEO IDs to the database
        pipeline.upload_geo_ids()

        # Execute the pipeline for processing the GEO IDs
        pipeline.execute_pipeline()

    except Exception as e:
        # Log a critical error if the pipeline fails
        logger.critical(f"Pipeline execution failed: {e}")
        exit(1)
