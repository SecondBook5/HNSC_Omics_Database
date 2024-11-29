# File: scripts/geo_metadata_pipeline.py

# Import necessary modules for pipeline execution
import logging  # For logging pipeline information
import os  # For file and directory operations
import time
import psutil
import json
from pipeline.geo_pipeline.geo_metadata_downloader import GeoMetadataDownloader  # For downloading GEO metadata
from pipeline.geo_pipeline.geo_metadata_etl import GeoMetadataETL  # For extracting metadata from files
from pipeline.geo_pipeline.geo_file_handler import GeoFileHandler  # For handling file-related operations
from config.db_config import get_session_context  # For managing database sessions
from utils.connection_checker import DatabaseConnectionChecker  # For checking database connections
from utils.exceptions import MissingForeignKeyError  # Custom exception for missing foreign key errors
from concurrent.futures import ThreadPoolExecutor  # For parallel task execution
from config.logger_config import configure_logger  # Centralized logger configuration
from db.schema.geo_metadata_schema import GeoSeriesMetadata, GeoSampleMetadata  # Database models for validation
from pipeline.geo_pipeline.geo_classifier import DataTypeDeterminer  # Correct import for DataTypeDeterminer

# ---------------- Configuration ----------------

# Define the base directory of the script
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

# Define the directory for output metadata files
OUTPUT_DIR = os.path.join(BASE_DIR, "../resources/metadata/geo_metadata/raw_metadata")

# Define the path to the file containing GEO IDs
GEO_IDS_FILE = os.path.join(BASE_DIR, "../resources/geo_ids.txt")

# Define the path to the metadata extraction template
EXTRACTION_TEMPLATE = os.path.join(BASE_DIR, "../resources/geo_tag_template.json")

# Ensure that the log and output directories exist, creating them if necessary
os.makedirs(OUTPUT_DIR, exist_ok=True)  # Create the output directory if it doesn't exist


# ---------------- GEO Metadata Pipeline ----------------


class GeoMetadataPipeline:
    """
    Pipeline for processing GEO metadata.
    Handles downloading, extraction, logging, and cleanup of metadata.
    """
    stats: dict[str, float]

    def __init__(self, geo_ids: list):
        """
        Initializes the GeoMetadataPipeline with GEO IDs and configures the logger.

        Args:
            geo_ids (list): A list of GEO IDs to process.

        Raises:
            ValueError: If geo_ids is not a non-empty list.
            FileNotFoundError: If the extraction template does not exist.
        """
        # Validate GEO IDs
        if not geo_ids or not isinstance(geo_ids, list):
            raise ValueError("geo_ids must be a non-empty list of GEO IDs.")
        self.geo_ids = geo_ids

        # Ensure the extraction template exists
        if not os.path.exists(EXTRACTION_TEMPLATE):
            raise FileNotFoundError(f"Metadata extraction template not found: {EXTRACTION_TEMPLATE}")

        # Initialize the logger specific to this pipeline instance
        self.logger = configure_logger(
            name="GeoMetadataPipeline",
            log_file="geo_metadata_pipeline.log",
            level=logging.INFO,
            output="both"
        )

        # Log initialization of the pipeline
        self.logger.info("Initializing the GeoMetadataPipeline.")

        # Initialize the file handler
        self.file_handler = GeoFileHandler(
            geo_ids_file=GEO_IDS_FILE,
            output_dir=OUTPUT_DIR,
            compress_files=True,
            logger=self.logger  # Pass the logger to the file handler
        )

        # Initialize the metadata downloader
        try:
            self.downloader = GeoMetadataDownloader(
                output_dir=OUTPUT_DIR,
                debug=True,
                file_handler=self.file_handler
            )
            self.logger.info("GeoMetadataDownloader initialized successfully.")
        except Exception as e:
            self.logger.critical(f"Failed to initialize GeoMetadataDownloader: {e}")
            raise

        # Initialize the database connection checker
        self.connection_checker = DatabaseConnectionChecker()

        # Initialize statistics for the pipeline
        self.stats = {"total_series": 0, "total_samples": 0, "runtime": 0, "memory_usage": 0, "cpu_usage": 0}
        self.failed_geo_ids = {}  # Dictionary to store failures for individual GEO IDs
# ------------------ Validation Methods ------------------
    def check_connections(self):
        """
        Ensures database connections are valid before proceeding.

        Raises:
            RuntimeError: If the PostgreSQL connection check fails.
        """
        try:
            # Verify PostgreSQL connection
            if not self.connection_checker.check_postgresql_connection():
                raise RuntimeError("PostgreSQL connection failed. Aborting pipeline.")
            self.logger.info("Database connections verified.")
        except Exception as e:
            self.logger.critical(f"Database connection check failed: {e}")
            raise

    def validate_download(self, geo_id: str, file_path: str) -> None:
        """
        Validates that the file was downloaded successfully.

        Args:
            geo_id (str): GEO ID of the downloaded file.
            file_path (str): Path to the downloaded file.

        Raises:
            FileNotFoundError: If the file does not exist after download.
        """
        # Ensure the file exists
        if not file_path or not os.path.exists(file_path):
            raise FileNotFoundError(f"Validation failed: File not found after download for GEO ID {geo_id}.")
        self.logger.info(f"Download validation successful for GEO ID {geo_id}.")

    def validate_metadata_upload(self, geo_id: str) -> None:
        """
        Validates that metadata has been uploaded successfully to the database.

        Args:
            geo_id (str): GEO ID of the metadata.

        Raises:
            RuntimeError: If SeriesID or associated samples are missing from the database.
        """
        try:
            with get_session_context() as session:
                # Check if the SeriesID exists in the database
                series_exists = session.query(GeoSeriesMetadata).filter_by(SeriesID=geo_id).first() is not None
                # Check if any Samples are associated with the SeriesID
                samples_exist = session.query(GeoSampleMetadata).filter_by(SeriesID=geo_id).count() > 0

                if not series_exists or not samples_exist:
                    raise RuntimeError(
                        f"Metadata validation failed: Missing SeriesID or Samples for GEO ID {geo_id}."
                    )
        except Exception as e:
            self.logger.error(f"Error during metadata upload validation for GEO ID {geo_id}: {e}")
            raise
        self.logger.info(f"Metadata upload validation successful for GEO ID {geo_id}.")

    def validate_cleanup(self, geo_id: str) -> None:
        """
        Validates that files have been cleaned successfully.

        Args:
            geo_id (str): GEO ID for which cleanup validation is performed.

        Raises:
            RuntimeError: If cleanup validation fails.
        """
        geo_dir = os.path.join(OUTPUT_DIR, geo_id)
        zip_file = f"{geo_dir}.zip"

        # Check if the directory still exists or if the zip file was not created
        if os.path.exists(geo_dir) or (self.file_handler.compress_files and not os.path.exists(zip_file)):
            raise RuntimeError(f"Validation failed: Cleanup incomplete for GEO ID {geo_id}.")
        self.logger.info(f"Cleanup validation successful for GEO ID {geo_id}.")
#----------------------------- Utility Methods ---------------------------------
    def log_resource_usage(self):
        """
        Logs the memory and CPU usage of the pipeline and updates stats.

        Updates:
            - Memory usage (in MB)
            - Current CPU utilization
        """
        # Fetch memory usage in megabytes
        memory_usage = psutil.Process(os.getpid()).memory_info().rss / (1024 ** 2)
        # Fetch CPU usage percentage
        cpu_usage = psutil.cpu_percent(interval=1)

        # Log memory and CPU usage
        self.logger.info(f"Memory usage: {memory_usage:.2f} MB")
        self.logger.info(f"CPU usage: {cpu_usage:.2f}%")

        # Update statistics
        self.stats["memory_usage"] = memory_usage
        self.stats["cpu_usage"] = int(cpu_usage)

    def generate_summary_report(self):
        """
        Generates a summary report of the pipeline's execution statistics.

        Includes:
            - Total series processed
            - Total samples processed
            - Runtime in seconds
            - Memory usage (in MB)
        """
        # Define the path for the summary JSON file
        summary_path = os.path.join(OUTPUT_DIR, "pipeline_summary.json")
        series_sample_counts = {}

        try:
            with get_session_context() as session:
                # Query database for per-series sample counts
                series_sample_counts = {
                    series.SeriesID: session.query(GeoSampleMetadata)
                    .filter_by(SeriesID=series.SeriesID)
                    .count()
                    for series in session.query(GeoSeriesMetadata).all()
                }

            # Add per-series sample counts to stats
            self.stats["series_sample_counts"] = series_sample_counts
            self.stats["total_series"] = len(series_sample_counts)
            self.stats["total_samples"] = sum(series_sample_counts.values())

            # Debug log for sample counts
            self.logger.debug(f"Series sample counts: {series_sample_counts}")

        except Exception as e:
            self.logger.error(f"Error generating per-series counts for summary: {e}")

        # Save statistics as a JSON file
        with open(summary_path, "w") as summary_file:
            json.dump(self.stats, summary_file, indent=4)

        # Log summary generation
        self.logger.info(f"Pipeline summary saved to {summary_path}")
# -------------------------Processing Methods -----------------------------------
    def download_extract_upload(self, geo_id: str):
        """
        Processes a single GEO ID through download, extraction, logging, and cleanup.

        Args:
            geo_id (str): GEO ID to process.

        Updates:
            - Increments total_series and total_samples stats.
            - Logs failures in `failed_geo_ids`.

        Raises:
            ValueError: If the GEO ID is invalid.
            Exception: For unexpected errors during processing.
        """
        # Validate that geo_id is a non-empty string
        if not geo_id or not isinstance(geo_id, str):
            self.logger.error("Invalid GEO ID provided. GEO ID must be a non-empty string.")
            raise ValueError("GEO ID must be a non-empty string.")

        try:
            # Step 1: Download the GEO metadata file
            file_paths = self.downloader.download_file(geo_id)

            # Log the downloaded file names
            self.file_handler.log_download(geo_id, [os.path.basename(path) for path in file_paths])

            # Validate each downloaded file path
            for file_path in file_paths:
                self.validate_download(geo_id, file_path)

            # Step 2: Extract and upload metadata for each file
            total_processed_samples = 0
            for file_path in file_paths:
                extractor = GeoMetadataETL(
                    file_path=file_path,
                    template_path=EXTRACTION_TEMPLATE,
                    debug_mode=True,
                    file_handler=self.file_handler
                )
                processed_samples = extractor.parse_and_stream()
                total_processed_samples += processed_samples

            # Validate that metadata was uploaded for the GEO ID
            self.validate_metadata_upload(geo_id)

            # Increment stats
            self.stats["total_series"] += 1
            self.stats["total_samples"] += total_processed_samples

            # Step 3: Run the DataTypeDeterminer to determine and update data types
            determiner = DataTypeDeterminer(geo_id)
            determiner.process()

            # Step 4: Clean up downloaded files
            self.file_handler.clean_files(geo_id)
            self.validate_cleanup(geo_id)

        except Exception as e:
            self.failed_geo_ids[geo_id] = f"UnexpectedError: {str(e)}"
            self.logger.error(f"Unexpected error processing GEO ID {geo_id}: {e}")
            raise

    def execute_pipeline(self):
        """
        Executes the pipeline for all GEO IDs in parallel and tracks runtime.

        Updates:
            - Tracks total runtime of the pipeline.
            - Logs resource usage upon completion.
        """
        start_time = time.time()  # Start timer for runtime tracking

        try:
            # Initialize the log table for GEO metadata
            self.file_handler.initialize_log_table()

            # Use a ThreadPoolExecutor to process GEO IDs in parallel
            with ThreadPoolExecutor() as executor:
                # Submit each GEO ID as a separate task
                futures = {executor.submit(self.download_extract_upload, geo_id): geo_id for geo_id in self.geo_ids}
                for future in futures:
                    geo_id = futures[future]
                    try:
                        future.result()  # Wait for task completion
                    except Exception as e:
                        self.logger.error(f"Error processing GEO ID {geo_id}: {e}")

        except Exception as e:
            self.logger.critical(f"Pipeline execution encountered a critical failure: {e}")
            raise

        # Track runtime after pipeline execution
        end_time = time.time()
        self.stats["runtime"] = end_time - start_time

        # Log resource usage
        self.log_resource_usage()

        # Generate a summary report
        self.generate_summary_report()


# ---------------- Global Logger Initialization ----------------
logger = configure_logger(name="GeoMetadataPipeline_Main", log_file="geo_metadata_pipeline.log")

# ---------------- Execution ----------------
if __name__ == "__main__":
    try:
        # Initialize the connection checker
        checker = DatabaseConnectionChecker()

        # Verify that PostgreSQL connection is available
        if not checker.check_postgresql_connection():
            logger.critical("Database connection failed. Aborting pipeline execution.")
            raise RuntimeError("Database connection failed.")

        # Ensure the GEO IDs file exists
        if not os.path.exists(GEO_IDS_FILE):
            logger.critical(f"GEO IDs file not found: {GEO_IDS_FILE}")
            raise FileNotFoundError(f"GEO IDs file not found: {GEO_IDS_FILE}")

        # Read the GEO IDs from the file
        with open(GEO_IDS_FILE, "r") as f:
            geo_ids = [line.strip() for line in f if line.strip()]

        # Ensure the GEO IDs list is not empty
        if not geo_ids:
            logger.critical("No GEO IDs found in the file.")
            raise ValueError("No GEO IDs found in the file.")

        logger.info(f"Processing {len(geo_ids)} GEO IDs.")

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
        logger.critical(f"Critical failure: {e}")
        exit(1)
