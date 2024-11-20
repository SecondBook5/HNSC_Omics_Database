# Import necessary modules for pipeline execution
import os  # For handling file and directory operations
import logging  # For structured logging of pipeline events
from pipeline.geo_pipeline.geo_metadata_downloader import GeoMetadataDownloader  # For downloading GEO files
from pipeline.geo_pipeline.geo_metadata_etl import GeoMetadataExtractor  # For extracting metadata
from pipeline.geo_pipeline.geo_metadata_uploader import GeoMetadataUploader  # For uploading metadata to the database
from config.db_config import get_session_context  # For managing database session
from utils.connection_checker import DatabaseConnectionChecker  # For validating database connections
from utils.exceptions import MissingForeignKeyError  # Custom exception for missing foreign key errors
from concurrent.futures import ThreadPoolExecutor  # For parallel execution of tasks
from db.schema.metadata_schema import DatasetSeriesMetadata, DatasetSampleMetadata  # For database models

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
            raise RuntimeError("PostgreSQL connection failed. Aborting pipeline.")
        logger.info("Database connections verified.")

    def log_operation(self, geo_id, status, message, file_names=None):
        """Logs operation status to the geo_metadata_log table."""
        try:
            # Open a database session and log the operation
            with get_session_context() as session:
                self.uploader.log_metadata_operation(session, geo_id, status, message, file_names)
        except Exception as e:
            # Log an error if the operation logging fails
            logger.error(f"Failed to log operation for GEO ID {geo_id}: {e}")
            raise

    def validate_upload(self, geo_id, series_metadata, sample_metadata):
        """
        Validates that all extracted metadata has been uploaded to the database.
        """
        try:
            # Open a database session to validate uploaded data
            with get_session_context() as session:
                # Validate that each series in the metadata exists in the database
                for series in series_metadata:
                    series_id = series["SeriesID"]
                    result = session.query(DatasetSeriesMetadata).filter_by(SeriesID=series_id).first()
                    if not result:
                        raise RuntimeError(f"Series metadata for SeriesID {series_id} not found in the database.")

                # Validate that each sample in the metadata exists in the database
                for sample in sample_metadata:
                    sample_id = sample["SampleID"]
                    result = session.query(DatasetSampleMetadata).filter_by(SampleID=sample_id).first()
                    if not result:
                        raise RuntimeError(f"Sample metadata for SampleID {sample_id} not found in the database.")

            # Log validation success
            logger.info(f"Validation successful for GEO ID {geo_id}.")
        except Exception as e:
            # Log and raise validation errors
            logger.error(f"Validation failed for GEO ID {geo_id}: {e}")
            raise

    def download_extract_upload(self, geo_id: str):
        """
        Processes a single GEO ID through download, extraction, and upload.
        """
        try:
            # Step 1: Download the GEO file for the given GEO ID
            file_path = self.downloader.download_file(geo_id)
            if not file_path or not os.path.exists(file_path):
                raise RuntimeError(f"File not found after download for GEO ID {geo_id}.")
            # Log the successful download operation
            self.log_operation(geo_id, "downloaded", f"File downloaded to {file_path}", [file_path])

            # Step 2: Extract metadata from the downloaded file
            extractor = GeoMetadataExtractor(file_path=file_path, template_path=self.extractor_template, debug_mode=True)
            metadata = extractor.parse()
            # Log the successful metadata extraction operation
            self.log_operation(geo_id, "extracted", "Metadata extracted successfully.")

            # Step 3: Extract series and sample metadata
            series_metadata = metadata.get("series", [])
            sample_metadata = metadata.get("samples", [])

            # Step 4: Upload extracted metadata to the database
            with get_session_context() as session:
                if series_metadata:
                    self.uploader.upload_series_metadata(series_metadata)
                if sample_metadata:
                    self.uploader.upload_sample_metadata(sample_metadata)

            # Step 5: Validate that metadata has been uploaded successfully
            self.validate_upload(geo_id, series_metadata, sample_metadata)

            # Log overall success for processing the GEO ID
            self.log_operation(geo_id, "uploaded", "Metadata uploaded and validated successfully.")
            logger.info(f"Successfully processed GEO ID {geo_id}.")
        except MissingForeignKeyError as mfe:
            # Handle foreign key errors during processing
            logger.error(f"Foreign key error while processing GEO ID {geo_id}: {mfe}")
            self.log_operation(geo_id, "failed", f"Foreign key error: {mfe}")
        except Exception as e:
            # Handle general errors during processing
            logger.error(f"Failed to process GEO ID {geo_id}: {e}")
            self.log_operation(geo_id, "failed", str(e))
            raise

    def execute_pipeline(self):
        """Executes the pipeline for all GEO IDs in parallel."""
        # Use a ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor() as executor:
            # Submit download, extract, and upload tasks for each GEO ID
            futures = {executor.submit(self.download_extract_upload, geo_id): geo_id for geo_id in self.geo_ids}
            for future in futures:
                geo_id = futures[future]
                try:
                    # Process the result of each future to check for errors
                    future.result()
                except Exception as e:
                    # Log an error for any GEO ID that fails processing
                    logger.error(f"Error processing GEO ID {geo_id}: {e}")


# ---------------- Execution ----------------

if __name__ == "__main__":
    try:
        # Create an instance of the database connection checker
        checker = DatabaseConnectionChecker()

        # Verify that PostgreSQL connection is available
        if not checker.check_postgresql_connection():
            raise RuntimeError("Database connection failed. Aborting pipeline execution.")

        # Check if the GEO IDs file exists
        if not os.path.exists(GEO_IDS_FILE):
            raise FileNotFoundError(f"GEO IDs file not found: {GEO_IDS_FILE}")

        # Read the GEO IDs from the file
        with open(GEO_IDS_FILE, "r") as f:
            geo_ids = [line.strip() for line in f if line.strip()]

        # Raise an error if the GEO IDs list is empty
        if not geo_ids:
            raise ValueError("No GEO IDs found in the file.")

        # Log the number of GEO IDs to process
        logger.info(f"Processing {len(geo_ids)} GEO IDs.")

        # Create an instance of the pipeline and execute it
        pipeline = GeoMetadataPipeline(geo_ids=geo_ids)

        # Check database connections
        pipeline.check_connections()
        pipeline.execute_pipeline()
    except Exception as e:
        # Log a critical error if the pipeline fails
        logger.critical(f"Pipeline execution failed: {e}")
        exit(1)
