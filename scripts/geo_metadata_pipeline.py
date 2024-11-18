# File: scripts/geo_metadata_pipeline.py

# Import necessary classes for downloading and extracting GEO metadata
from pipeline.geo_pipeline.geo_metadata_downloader import GeoMetadataDownloader  # Downloader class
from pipeline.geo_pipeline.geo_metadata_extractor import GeoMetadataExtractor  # Extractor class
from utils.parallel_processing import ParallelProcessor  # Base class for parallel processing

# ---------------- Configuration ----------------

# Import necessary modules
import os
import logging

# Get the absolute path of the script's directory
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

# Define the directory for log files relative to the script's directory
LOG_DIR = os.path.join(BASE_DIR, "../logs")
LOG_FILE = os.path.join(LOG_DIR, "geo_metadata_pipeline.log")

# Ensure the log directory exists, creating it if necessary
os.makedirs(LOG_DIR, exist_ok=True)

# Configure the logger for this script
logger = logging.getLogger("geo_metadata_pipeline")  # Create a logger with a specific name
if not logger.hasHandlers():  # Avoid adding duplicate handlers
    # Define a handler that writes logs to the specified file
    file_handler = logging.FileHandler(LOG_FILE)
    # Define the log format
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    # Set the formatter for the file handler
    file_handler.setFormatter(formatter)
    # Add the handler to the logger
    logger.addHandler(file_handler)
    # Set the logging level (use DEBUG for detailed logs)
    logger.setLevel(logging.INFO)

    # Flush the file handler to ensure logs are written immediately
    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            handler.flush()

# Print the location of the log file for verification
print(f"Logger is writing to: {LOG_FILE}")

# Log an initial message to confirm logging is set up correctly
logger.info("Logging setup complete. Starting pipeline.")

# Flush the logger after the initial setup message
for handler in logger.handlers:
    if isinstance(handler, logging.FileHandler):
        handler.flush()

# Debugging logger configuration
print(f"Logger handlers: {logger.handlers}")
if not os.path.exists(LOG_FILE):
    print(f"Log file does not exist: {LOG_FILE}")
    print(f"Attempting to create log directory: {LOG_DIR}")
    os.makedirs(LOG_DIR, exist_ok=True)

# File writing test
try:
    with open(LOG_FILE, "a") as test_log:
        test_log.write("Test log entry\n")
        print(f"Successfully wrote to log file: {LOG_FILE}")
except Exception as e:
    print(f"Failed to write to log file: {e}")

# Define the output directory for downloaded GEO metadata
OUTPUT_DIR = os.path.join(BASE_DIR, "../resources/data/metadata/geo_metadata/raw_metadata")

# Define the file path for the list of GEO IDs to process
GEO_IDS_FILE = os.path.join(BASE_DIR, "../resources/geo_ids.txt")

# Define the path to the template for metadata extraction
EXTRACTION_TEMPLATE = os.path.join(BASE_DIR, "../resources/geo_tag_template.json")

# Ensure the output directory exists, creating it if necessary
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ---------------- GEO Metadata Pipeline ----------------

class GeoMetadataPipeline(ParallelProcessor):
    """
    Pipeline for downloading and extracting metadata from GEO datasets.

    This class integrates downloading and metadata extraction functionalities
    to process GEO datasets in parallel, ensuring both tasks happen sequentially per resource.
    """

    def __init__(self, geo_ids: list):
        """
        Initializes the GeoMetadataPipeline with GEO IDs.

        Args:
            geo_ids (list): List of GEO series IDs to process.
        """
        # Initialize the base class for parallel processing with GEO IDs and output directory
        super().__init__(resource_ids=geo_ids, output_dir=OUTPUT_DIR)

        # Create an instance of the downloader with the specified output directory
        self.downloader = GeoMetadataDownloader(output_dir=OUTPUT_DIR, debug=True, logger= logger)

        # Store the path to the metadata extraction template
        self.extractor_template = EXTRACTION_TEMPLATE

    def download_resource(self, geo_id: str) -> str:
        """
        Downloads the GEO dataset for a given GEO ID.

        Args:
            geo_id (str): GEO series ID to download.

        Returns:
            str: Path to the extracted XML file.

        Raises:
            RuntimeError: If the download fails or the extracted file is not found.
        """
        try:
            # Log the start of the download process for this GEO ID
            logger.info(f"Starting download for GEO ID: {geo_id}")

            # Use the downloader to download and extract the GEO dataset
            extracted_path = self.downloader.download_file(geo_id)

            # Verify that the extracted file exists
            if not extracted_path or not os.path.exists(extracted_path):
                raise RuntimeError(f"Download failed for GEO ID: {geo_id}")

            # Log success and return the path to the extracted file
            logger.info(f"Download and extraction successful for GEO ID: {geo_id}")
            return extracted_path
        except Exception as e:
            # Log any errors encountered during the download process
            logger.error(f"Error during download for GEO ID {geo_id}: {e}")
            raise

    def process_resource(self, file_path: str) -> None:
        """
        Processes the downloaded GEO XML file.

        Args:
            file_path (str): Path to the extracted XML file.
        """
        try:
            # Log the start of metadata extraction for this file
            logger.info(f"Starting metadata extraction for file: {file_path}")

            # Create an instance of the extractor and parse the file
            extractor = GeoMetadataExtractor(
                file_path=file_path,
                template_path=self.extractor_template,
                debug_mode=True,
                verbose_mode=False
            )
            extractor.parse()

            # Log success after processing the metadata
            logger.info(f"Successfully processed metadata for file: {file_path}")
        except Exception as e:
            # Log any errors encountered during metadata extraction
            logger.error(f"Error during metadata extraction for file {file_path}: {e}")
            raise

    def download_and_process(self, geo_id: str) -> None:
        """
        Combines downloading and processing for a GEO ID.

        Args:
            geo_id (str): GEO series ID.
        """
        try:
            # Download the GEO dataset and get the path to the extracted file
            file_path = self.download_resource(geo_id)

            # Process the metadata from the downloaded file
            self.process_resource(file_path)
        except Exception as e:
            # Log any errors encountered during the combined download and processing
            logger.error(f"Error in pipeline for GEO ID {geo_id}: {e}")
            raise


# ---------------- Execution ----------------

if __name__ == "__main__":
    try:
        # Ensure the GEO IDs file exists
        if not os.path.exists(GEO_IDS_FILE):
            raise FileNotFoundError(f"GEO IDs file not found: {GEO_IDS_FILE}")

        # Read GEO series IDs from the input file, stripping whitespace and skipping empty lines
        with open(GEO_IDS_FILE, "r") as f:
            geo_ids = [line.strip() for line in f if line.strip()]

        # Check if the list of GEO IDs is empty
        if not geo_ids:
            raise ValueError("No GEO IDs found in the file.")

        # Log the number of GEO IDs to be processed
        logger.info(f"Processing {len(geo_ids)} GEO IDs.")
    except Exception as e:
        # Log any critical errors related to reading the GEO IDs file and exit
        logger.critical(f"Failed to initialize pipeline: {e}")
        exit(1)

    try:
        # Initialize the pipeline with the list of GEO IDs
        pipeline = GeoMetadataPipeline(geo_ids=geo_ids)

        # Execute the pipeline with parallel processing
        pipeline.execute(pipeline.download_and_process)
    except Exception as e:
        # Log any critical errors during pipeline execution and exit
        logger.critical(f"Pipeline execution failed: {e}")
        exit(1)
