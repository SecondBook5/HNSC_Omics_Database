# File: scripts/geo_metadata_pipeline.py

# Import necessary modules for pipeline execution
import logging
import os
import time
import psutil
import json
from pipeline.geo_pipeline.geo_metadata_downloader import GeoMetadataDownloader
from pipeline.geo_pipeline.geo_metadata_etl import GeoMetadataETL
from pipeline.geo_pipeline.geo_file_handler import GeoFileHandler
from config.db_config import get_session_context
from utils.connection_checker import DatabaseConnectionChecker
from utils.exceptions import MissingForeignKeyError
from concurrent.futures import ThreadPoolExecutor
from config.logger_config import configure_logger
from db.schema.geo_metadata_schema import GeoSeriesMetadata, GeoSampleMetadata
from pipeline.geo_pipeline.geo_classifier import DataTypeDeterminer

# ---------------- Configuration ----------------

# Define the base directory of the script
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

# Define the directory for output metadata files
OUTPUT_DIR = os.path.join(BASE_DIR, "../resources/metadata/geo_metadata/raw_metadata")

# Define the path to the file containing GEO IDs
GEO_IDS_FILE = os.path.join(BASE_DIR, "../resources/geo_ids.txt")

# Define the path to the metadata extraction template
EXTRACTION_TEMPLATE = os.path.join(BASE_DIR, "../resources/geo_tag_template.json")

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ---------------- GEO Metadata Pipeline ----------------
class GeoMetadataPipeline:
    """
    Pipeline for processing GEO metadata with options for parallel and batch processing.
    """
    stats: dict[str, float]

    def __init__(self, geo_ids: list, parallel: bool = True, batch_size: int = None):
        """
        Initializes the GeoMetadataPipeline with options for parallel and batch processing.

        Args:
            geo_ids (list): A list of GEO IDs to process.
            parallel (bool): Whether to enable parallel processing.
            batch_size (int): Number of GEO IDs to process in a single batch. Only used if parallel is False.

        Raises:
            ValueError: If geo_ids is not a non-empty list.
            FileNotFoundError: If the extraction template does not exist.
        """
        # Validate that geo_ids is a list and not empty
        if not geo_ids or not isinstance(geo_ids, list):
            raise ValueError("geo_ids must be a non-empty list of GEO IDs.")
        self.geo_ids = geo_ids  # Assign GEO IDs to an instance variable
        self.parallel = parallel  # Store the parallel processing option
        self.batch_size = batch_size  # Store the batch size if provided

        # Validate that the metadata extraction template exists
        if not os.path.exists(EXTRACTION_TEMPLATE):
            raise FileNotFoundError(f"Metadata extraction template not found: {EXTRACTION_TEMPLATE}")

        # Configure the logger for this pipeline instance
        self.logger = configure_logger(
            name="GeoMetadataPipeline",
            log_file="geo_metadata_pipeline.log",
            level=logging.INFO,
            output="both"
        )

        # Log initialization of the pipeline
        self.logger.info("Initializing the GeoMetadataPipeline.")

        # Initialize the file handler for managing files and logs
        self.file_handler = GeoFileHandler(
            geo_ids_file=GEO_IDS_FILE,
            output_dir=OUTPUT_DIR,
            compress_files=True,
            logger=self.logger
        )

        # Initialize the downloader for GEO metadata files
        try:
            self.downloader = GeoMetadataDownloader(
                output_dir=OUTPUT_DIR,
                debug=True,
                file_handler=self.file_handler
            )
            self.logger.info("GeoMetadataDownloader initialized successfully.")
        except Exception as e:
            # Log and raise critical error if downloader initialization fails
            self.logger.critical(f"Failed to initialize GeoMetadataDownloader: {e}")
            raise

        # Initialize the database connection checker
        self.connection_checker = DatabaseConnectionChecker()

        # Initialize stats for tracking pipeline performance
        self.stats = {"total_series": 0, "total_samples": 0, "runtime": 0, "memory_usage": 0, "cpu_usage": 0}
        # Dictionary for recording failures during processing
        self.failed_geo_ids = {}

    # ------------------ Validation Methods ------------------

    def check_connections(self):
        """
        Ensures database connections are valid before proceeding.

        Raises:
            RuntimeError: If the PostgreSQL connection check fails.
        """
        try:
            # Check if PostgreSQL connection is available
            if not self.connection_checker.check_postgresql_connection():
                raise RuntimeError("PostgreSQL connection failed. Aborting pipeline.")
            # Log successful connection verification
            self.logger.info("Database connections verified.")
        except Exception as e:
            # Log and raise critical error for connection failure
            self.logger.critical(f"Database connection check failed: {e}")
            raise

    def validate_download(self, geo_id: str, file_path: str):
        """
        Validates that the file was downloaded successfully.

        Args:
            geo_id (str): GEO ID of the downloaded file.
            file_path (str): Path to the downloaded file.

        Raises:
            FileNotFoundError: If the file does not exist after download.
            RuntimeError: If the file is empty.
        """
        # Check if the file path exists
        if not file_path or not os.path.exists(file_path):
            raise FileNotFoundError(f"Validation failed: File not found after download for GEO ID {geo_id}.")
        # Check if the file is empty
        if os.path.getsize(file_path) == 0:
            raise RuntimeError(f"Validation failed: File is empty for GEO ID {geo_id}.")
        # Log successful validation
        self.logger.info(f"Download validation successful for GEO ID {geo_id}.")

    def validate_metadata_upload(self, geo_id: str) -> None:
        """
        Validates that metadata has been uploaded successfully to the database.
        For SuperSeries, ensures SubSeries and their samples are also present.

        Args:
            geo_id (str): GEO ID of the metadata.

        Raises:
            RuntimeError: If SeriesID or associated samples are missing from the database.
        """
        try:
            with get_session_context() as session:
                # Fetch the Series metadata
                series = session.query(GeoSeriesMetadata).filter_by(SeriesID=geo_id).one_or_none()

                # If series does not exist, raise an error
                if not series:
                    raise RuntimeError(f"Validation failed: SeriesID {geo_id} not found in GeoSeriesMetadata.")

                # For SuperSeries, ensure that subseries samples exist
                if series.Summary and "superseries" in series.Summary.lower():
                    self.logger.info(f"Validating SubSeries samples for SuperSeries {geo_id}.")
                    related_datasets = series.RelatedDatasets or []
                    subseries_ids = [
                        dataset["target"] for dataset in related_datasets
                        if dataset.get("type", "").lower().startswith("superseries of")
                    ]

                    for subseries_id in subseries_ids:
                        # Check if the SubSeries has samples
                        samples_exist = session.query(GeoSampleMetadata).filter_by(SeriesID=subseries_id).count() > 0
                        if not samples_exist:
                            raise RuntimeError(
                                f"Validation failed: Missing samples for SubSeries {subseries_id} referenced by {geo_id}."
                            )
                else:
                    # Check if the Series has samples
                    samples_exist = session.query(GeoSampleMetadata).filter_by(SeriesID=geo_id).count() > 0
                    if not samples_exist:
                        raise RuntimeError(f"Validation failed: Missing samples for SeriesID {geo_id}.")

        except Exception as e:
            self.logger.error(f"Error during metadata upload validation for GEO ID {geo_id}: {e}")
            raise
        self.logger.info(f"Metadata upload validation successful for GEO ID {geo_id}.")

    # -------------------------Processing Methods -----------------------------------

    def download_extract_upload(self, geo_id: str):
        """
        Processes a single GEO ID through download, extraction, logging, classification, and cleanup.

        Args:
            geo_id (str): GEO ID to process.

        Updates:
            - Increments total_series and total_samples stats.
            - Logs failures in `failed_geo_ids`.

        Raises:
            ValueError: If the GEO ID is invalid.
            Exception: For unexpected errors during processing.
        """
        # Ensure GEO ID is valid
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
                # Skip .txt files
                if file_path.endswith(".txt"):
                    self.logger.warning(f"Skipping unsupported file type: {file_path}")
                    continue

                self.validate_download(geo_id, file_path)

            # Step 2: Extract and upload metadata for each file
            total_processed_samples = 0
            for file_path in file_paths:
                # Skip .txt files
                if file_path.endswith(".txt"):
                    self.logger.warning(f"Skipping unsupported file type: {file_path}")
                    continue

                try:
                    # Initialize the ETL processor for the file
                    extractor = GeoMetadataETL(
                        file_path=file_path,
                        template_path=EXTRACTION_TEMPLATE,
                        debug_mode=True,
                        file_handler=self.file_handler
                    )
                    # Parse and stream the file, updating processed sample count
                    processed_samples = extractor.parse_and_stream()
                    total_processed_samples += processed_samples
                except Exception as etl_error:
                    # Log and skip errors during extraction for a specific file
                    self.logger.error(f"ETL failed for file {file_path}: {etl_error}")
                    continue

            # Step 3: Validate metadata upload to the database
            self.validate_metadata_upload(geo_id)

            # Update pipeline statistics for processed series and samples
            self.stats["total_series"] += 1
            self.stats["total_samples"] += total_processed_samples

            # Step 4: Run DataTypeDeterminer to classify and update data types
            determiner = DataTypeDeterminer(geo_id)
            determiner.process()

            # Step 5: Clean up downloaded files and validate cleanup
            self.file_handler.clean_files(geo_id)
            self.validate_cleanup(geo_id)

        except Exception as e:
            # Record failure in the failed_geo_ids dictionary and log the error
            self.failed_geo_ids[geo_id] = f"UnexpectedError: {str(e)}"
            self.logger.error(f"Unexpected error processing GEO ID {geo_id}: {e}")
            raise

    def execute_pipeline(self):
        """
        Executes the pipeline for all GEO IDs with options for parallel and batch processing.

        Updates:
            - Tracks total runtime of the pipeline.
            - Logs resource usage upon completion.
        """
        start_time = time.time()  # Start timer for runtime tracking

        try:
            # Initialize the log table for GEO metadata
            self.file_handler.initialize_log_table()

            if self.parallel:
                # Use ThreadPoolExecutor for parallel processing of GEO IDs
                with ThreadPoolExecutor() as executor:
                    # Submit each GEO ID as a separate task
                    futures = {executor.submit(self.download_extract_upload, geo_id): geo_id for geo_id in self.geo_ids}
                    for future in futures:
                        geo_id = futures[future]
                        try:
                            future.result()  # Wait for task completion
                        except Exception as e:
                            # Log errors for individual GEO IDs
                            self.logger.error(f"Error processing GEO ID {geo_id}: {e}")
            else:
                # Process GEO IDs sequentially, optionally in batches
                batch_size = self.batch_size or len(self.geo_ids)
                for i in range(0, len(self.geo_ids), batch_size):
                    batch = self.geo_ids[i:i + batch_size]
                    for geo_id in batch:
                        try:
                            self.download_extract_upload(geo_id)
                        except Exception as e:
                            # Log errors for individual GEO IDs
                            self.logger.error(f"Error processing GEO ID {geo_id}: {e}")

        except Exception as e:
            # Log and raise critical errors during pipeline execution
            self.logger.critical(f"Pipeline execution encountered a critical failure: {e}")
            raise

        # Track runtime after pipeline execution
        end_time = time.time()
        self.stats["runtime"] = end_time - start_time

        # Log resource usage
        self.log_resource_usage()

        # Generate a summary report
        self.generate_summary_report()

        # Retry failed GEO IDs if any
        if self.failed_geo_ids:
            self.logger.warning(f"Retrying {len(self.failed_geo_ids)} failed GEO IDs.")
            self.retry_failed_ids()

    def retry_failed_ids(self, retries: int = 2):
        """
        Retries failed GEO IDs up to a specified number of times.

        Args:
            retries (int): Maximum number of retry attempts.
        """
        for attempt in range(1, retries + 1):
            self.logger.info(f"Retry attempt {attempt} for failed GEO IDs.")
            failed_ids = list(self.failed_geo_ids.keys())  # Get the current list of failed IDs
            for geo_id in failed_ids:
                try:
                    # Retry processing the GEO ID
                    self.download_extract_upload(geo_id)
                    self.failed_geo_ids.pop(geo_id)  # Remove from failed list if successful
                except Exception as e:
                    # Log retry failure for this GEO ID
                    self.logger.warning(f"Retry failed for GEO ID {geo_id}: {e}")
            if not self.failed_geo_ids:  # Exit early if no more failures
                self.logger.info("All GEO IDs processed successfully after retries.")
                break
        if self.failed_geo_ids:
            self.logger.error(f"Final failed GEO IDs after retries: {self.failed_geo_ids}")

# ----------------------------- Utility Methods ---------------------------------

    def log_resource_usage(self):
        """
        Logs the memory and CPU usage of the pipeline and updates stats.

        Updates:
            - Memory usage (in MB)
            - Current CPU utilization
        """
        try:
            # Get the process's current memory usage in MB
            memory_usage = psutil.Process(os.getpid()).memory_info().rss / (1024 ** 2)
            # Get the current CPU utilization percentage (averaged over 1 second)
            cpu_usage = psutil.cpu_percent(interval=1)

            # Log the resource usage
            self.logger.info(f"Memory usage: {memory_usage:.2f} MB")
            self.logger.info(f"CPU usage: {cpu_usage:.2f}%")

            # Update statistics
            self.stats["memory_usage"] = memory_usage
            self.stats["cpu_usage"] = int(cpu_usage)
        except Exception as e:
            # Log any errors during resource tracking
            self.logger.error(f"Error tracking resource usage: {e}")

    def generate_summary_report(self):
        """
        Generates a summary report of the pipeline's execution statistics.

        Includes:
            - Total series processed
            - Total samples processed
            - Runtime in seconds
            - Memory usage (in MB)
            - CPU usage (percentage)
        """
        try:
            # Define the path for the summary report JSON file
            summary_path = os.path.join(OUTPUT_DIR, "pipeline_summary.json")

            # Initialize a dictionary to store per-series sample counts
            series_sample_counts = {}

            # Query the database for sample counts per series
            with get_session_context() as session:
                # Fetch sample counts for each series in the database
                series_sample_counts = {
                    series.SeriesID: session.query(GeoSampleMetadata)
                    .filter_by(SeriesID=series.SeriesID)
                    .count()
                    for series in session.query(GeoSeriesMetadata).all()
                }

            # Update stats with series and sample counts
            self.stats["series_sample_counts"] = series_sample_counts
            self.stats["total_series"] = len(series_sample_counts)
            self.stats["total_samples"] = sum(series_sample_counts.values())

            # Debug log to verify per-series sample counts
            self.logger.debug(f"Series sample counts: {series_sample_counts}")

            # Save the statistics dictionary to the summary report file
            with open(summary_path, "w") as summary_file:
                json.dump(self.stats, summary_file, indent=4)

            # Log the successful generation of the summary report
            self.logger.info(f"Pipeline summary saved to {summary_path}")
        except Exception as e:
            # Log any errors that occur during summary report generation
            self.logger.error(f"Error generating pipeline summary report: {e}")

    def validate_cleanup(self, geo_id: str) -> None:
        """
        Validates that files have been cleaned successfully.

        Args:
            geo_id (str): GEO ID for which cleanup validation is performed.

        Raises:
            RuntimeError: If cleanup validation fails due to leftover files or directories.
        """
        try:
            # Construct the path to the directory associated with the GEO ID
            geo_dir = os.path.join(OUTPUT_DIR, geo_id)
            # Construct the path to the zip file for the GEO ID
            zip_file = f"{geo_dir}.zip"

            # Check if the directory still exists (it should have been deleted after compression)
            if os.path.exists(geo_dir):
                raise RuntimeError(f"Validation failed: Directory '{geo_dir}' still exists after cleanup.")

            # If file compression is enabled, ensure the zip file exists
            if self.file_handler.compress_files and not os.path.exists(zip_file):
                raise RuntimeError(f"Validation failed: Zip file '{zip_file}' not found for GEO ID {geo_id}.")

            # Log successful cleanup validation
            self.logger.info(f"Cleanup validation successful for GEO ID {geo_id}.")
        except Exception as e:
            # Log the error encountered during cleanup validation
            self.logger.error(f"Error during cleanup validation for GEO ID {geo_id}: {e}")
            raise




# ---------------- Execution ----------------

# ----------------------------- Main Execution ---------------------------------

if __name__ == "__main__":
    # Initialize the global logger for the pipeline
    logger = configure_logger(name="GeoMetadataPipeline_Main", log_file="geo_metadata_pipeline.log")
    try:
        # Verify PostgreSQL database connection
        logger.info("Initializing database connection checker.")
        checker = DatabaseConnectionChecker()

        # Verify that PostgreSQL connection is available
        if not checker.check_postgresql_connection():
            logger.critical("Database connection failed. Aborting pipeline execution.")
            raise RuntimeError("Database connection failed.")

        # Ensure the GEO IDs file exists
        if not os.path.exists(GEO_IDS_FILE):
            logger.critical(f"GEO IDs file not found: {GEO_IDS_FILE}")
            raise FileNotFoundError(f"GEO IDs file not found: {GEO_IDS_FILE}")

        # Read GEO IDs from the file
        with open(GEO_IDS_FILE, "r") as f:
            geo_ids = [line.strip() for line in f if line.strip()]

        # Validate the GEO IDs list is not empty
        if not geo_ids:
            logger.critical("No GEO IDs found in the file.")
            raise ValueError("No GEO IDs found in the file.")

        # Log the number of GEO IDs to be processed
        logger.info(f"Processing {len(geo_ids)} GEO IDs.")

        # Initialize the pipeline with GEO IDs
        pipeline = GeoMetadataPipeline(geo_ids=geo_ids)

        # Set pipeline options (these could be configured dynamically or through arguments)
        pipeline.parallel = True  # Enable parallel processing
        pipeline.batch_size = 10  # Process in batches of 10 if not running in parallel

        # Check database connections before starting the pipeline
        pipeline.check_connections()

        # Execute the pipeline
        pipeline.execute_pipeline()

    except MissingForeignKeyError as mfe:
        # Handle foreign key errors specifically
        logger.critical(f"Foreign key error encountered: {mfe}")
    except FileNotFoundError as fnf_error:
        # Handle missing files
        logger.critical(f"File not found: {fnf_error}")
    except ValueError as value_error:
        # Handle invalid input errors
        logger.critical(f"Value error: {value_error}")
    except RuntimeError as runtime_error:
        # Handle runtime-specific errors
        logger.critical(f"Runtime error: {runtime_error}")
    except Exception as e:
        # Handle unexpected errors gracefully
        logger.critical(f"Critical failure: {e}")
        exit(1)
