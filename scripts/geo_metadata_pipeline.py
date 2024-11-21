# Import necessary modules for pipeline execution
import os  # For file and directory operations
from pipeline.geo_pipeline.geo_metadata_downloader import GeoMetadataDownloader  # For downloading GEO metadata
from pipeline.geo_pipeline.geo_metadata_etl import GeoMetadataETL  # For extracting metadata from files
from pipeline.geo_pipeline.geo_file_handler import GeoFileHandler  # For handling file-related operations
from config.db_config import get_session_context  # For managing database sessions
from utils.connection_checker import DatabaseConnectionChecker  # For checking database connections
from utils.exceptions import MissingForeignKeyError  # Custom exception for missing foreign key errors
from concurrent.futures import ThreadPoolExecutor  # For parallel task execution
from config.logger_config import configure_logger  # Centralized logger configuration
from db.schema.metadata_schema import DatasetSeriesMetadata, DatasetSampleMetadata  # Database models for validation

# ---------------- Configuration ----------------

# Define the base directory of the script
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

# Define the directory for log files
LOG_DIR = os.path.join(BASE_DIR, "../logs")

# Define the directory for output metadata files
OUTPUT_DIR = os.path.join(BASE_DIR, "../resources/data/metadata/geo_metadata/raw_metadata")

# Define the path to the file containing GEO IDs
GEO_IDS_FILE = os.path.join(BASE_DIR, "../resources/geo_ids.txt")

# Define the path to the metadata extraction template
EXTRACTION_TEMPLATE = os.path.join(BASE_DIR, "../resources/geo_tag_template.json")

# Ensure that the log and output directories exist, creating them if necessary
os.makedirs(LOG_DIR, exist_ok=True)  # Create the log directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)  # Create the output directory if it doesn't exist

# Configure centralized logger for the pipeline
logger = configure_logger(
    name="GeoMetadataPipeline",  # Logger name
    log_dir=LOG_DIR,  # Directory for log files
    log_file="geo_metadata_pipeline.log",  # Log file name
    level="INFO",  # Logging level
    output="both"  # Log to both console and file
)


# ---------------- GEO Metadata Pipeline ----------------


class GeoMetadataPipeline:
    """
    Pipeline for processing GEO metadata.
    Handles downloading, extraction, logging, and cleanup of metadata.
    """

    def __init__(self, geo_ids: list):
        # Validate that geo_ids is a non-empty list
        if not geo_ids or not isinstance(geo_ids, list):
            raise ValueError("geo_ids must be a non-empty list of GEO IDs.")
        self.geo_ids = geo_ids  # Assign the list of GEO IDs to an instance variable

        # Validate that the extraction template exists
        if not os.path.exists(EXTRACTION_TEMPLATE):
            raise FileNotFoundError(f"Metadata extraction template not found: {EXTRACTION_TEMPLATE}")

        # Initialize a file handler for managing GEO files
        self.file_handler = GeoFileHandler(geo_ids_file=GEO_IDS_FILE, output_dir=OUTPUT_DIR, compress_files=True)

        # Initialize the downloader for GEO metadata files
        try:
            self.downloader = GeoMetadataDownloader(output_dir=OUTPUT_DIR, debug=True, file_handler=self.file_handler)
        except Exception as e:
            logger.critical(f"Failed to initialize GeoMetadataDownloader: {e}")
            raise

        # Initialize the connection checker for validating database connections
        self.connection_checker = DatabaseConnectionChecker()

    def check_connections(self):
        """Ensures database connections are valid before proceeding."""
        try:
            # Verify the PostgreSQL connection
            if not self.connection_checker.check_postgresql_connection():
                raise RuntimeError("PostgreSQL connection failed. Aborting pipeline.")
            logger.info("Database connections verified.")  # Log successful connection verification
        except Exception as e:
            # Log critical error if the connection check fails
            logger.critical(f"Database connection check failed: {e}")
            raise

    def download_extract_upload(self, geo_id: str):
        """
        Processes a single GEO ID through download, extraction, logging, and cleanup.
        """
        # Validate that geo_id is a non-empty string
        if not geo_id or not isinstance(geo_id, str):
            logger.error("Invalid GEO ID provided. GEO ID must be a non-empty string.")
            raise ValueError("GEO ID must be a non-empty string.")

        try:
            # Step 1: Download the GEO metadata file
            file_path = self.downloader.download_file(geo_id)
            if not file_path or not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found after download for GEO ID {geo_id}.")
            logger.info(f"Successfully downloaded file for GEO ID {geo_id}.")

            # Step 2: Extract and upload metadata from the file
            extractor = GeoMetadataETL(
                file_path=file_path,
                template_path=EXTRACTION_TEMPLATE,
                debug_mode=True,
                file_handler=self.file_handler
            )
            extractor.parse_and_stream()  # Parse and upload metadata to the database
            logger.info(f"Successfully extracted and uploaded metadata for GEO ID {geo_id}.")

            # Step 3: Clean up the downloaded files
            self.file_handler.clean_files(geo_id)
            logger.info(f"Cleaned up files for GEO ID {geo_id}.")  # Log cleanup success

        except MissingForeignKeyError as mfe:
            # Handle missing foreign key errors
            logger.error(f"Foreign key error while processing GEO ID {geo_id}: {mfe}")
            self.file_handler.log_processed(geo_id)  # Log that processing was completed
            self.file_handler.log_download(geo_id, [f"Error: {mfe}"])  # Log the error
        except FileNotFoundError as fnf:
            # Handle file-related errors
            logger.error(f"File operation failed for GEO ID {geo_id}: {fnf}")
            self.file_handler.log_download(geo_id, [f"Error: {fnf}"])
        except ValueError as ve:
            # Handle invalid values
            logger.error(f"Value error encountered for GEO ID {geo_id}: {ve}")
            self.file_handler.log_download(geo_id, [f"Error: {ve}"])
        except Exception as e:
            # Handle any other unexpected errors
            logger.error(f"Unexpected error processing GEO ID {geo_id}: {e}")
            self.file_handler.log_download(geo_id, [f"Error: {e}"])

    def execute_pipeline(self):
        """Executes the pipeline for all GEO IDs in parallel."""
        try:
            # Initialize the log table for GEO metadata
            self.file_handler.initialize_log_table()

            # Use a ThreadPoolExecutor to process GEO IDs in parallel
            with ThreadPoolExecutor() as executor:
                # Submit each GEO ID as a separate task
                futures = {executor.submit(self.download_extract_upload, geo_id): geo_id for geo_id in self.geo_ids}
                for future in futures:
                    geo_id = futures[future]  # Retrieve the GEO ID for the current task
                    try:
                        future.result()  # Wait for the task to complete
                    except Exception as e:
                        # Log any errors encountered during processing
                        logger.error(f"Error processing GEO ID {geo_id}: {e}")
        except Exception as e:
            # Log critical errors that occur during the pipeline execution
            logger.critical(f"Pipeline execution encountered a critical failure: {e}")
            raise


# ---------------- Execution ----------------

if __name__ == "__main__":
    try:
        # Initialize the connection checker
        checker = DatabaseConnectionChecker()

        # Verify that PostgreSQL connection is available
        if not checker.check_postgresql_connection():
            raise RuntimeError("Database connection failed. Aborting pipeline execution.")

        # Ensure the GEO IDs file exists
        if not os.path.exists(GEO_IDS_FILE):
            raise FileNotFoundError(f"GEO IDs file not found: {GEO_IDS_FILE}")

        # Read the GEO IDs from the file
        with open(GEO_IDS_FILE, "r") as f:
            geo_ids = [line.strip() for line in f if line.strip()]

        # Ensure the GEO IDs list is not empty
        if not geo_ids:
            raise ValueError("No GEO IDs found in the file.")

        logger.info(f"Processing {len(geo_ids)} GEO IDs.")  # Log the number of GEO IDs

        # Initialize the pipeline with the list of GEO IDs
        pipeline = GeoMetadataPipeline(geo_ids=geo_ids)

        # Check database connections before execution
        pipeline.check_connections()

        # Execute the pipeline
        pipeline.execute_pipeline()

    except MissingForeignKeyError as mfe:
        logger.critical(f"Foreign key error encountered: {mfe}")
    except FileNotFoundError as fnf_error:
        logger.critical(f"File not found: {fnf_error}")
    except ValueError as value_error:
        logger.critical(f"Value error: {value_error}")
    except RuntimeError as runtime_error:
        logger.critical(f"Runtime error: {runtime_error}")
    except Exception as e:
        # Log any unexpected errors during execution
        logger.critical(f"Unexpected error during pipeline execution: {e}")
        exit(1)
