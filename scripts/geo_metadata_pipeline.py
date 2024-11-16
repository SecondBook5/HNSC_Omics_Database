# File: scripts/geo_metadata_pipeline.py
from pipeline.geo_pipeline.geo_metadata_downloader import GeoMetadataDownloader  # Downloader class
from pipeline.geo_pipeline.geo_metadata_extractor import GeoMetadataExtractor  # Extractor class
from utils.parallel_processing import ParallelProcessor  # Base class for parallel processing


import os
import logging

# Define log file relative to the current script's directory
LOG_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../logs"))
LOG_FILE = os.path.join(LOG_DIR, "geo_metadata_pipeline.log")

# Ensure the logs directory exists
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# Set up logging configuration
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.DEBUG,  # Set the logging level to DEBUG or INFO as needed
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Test log entry to verify setup
logging.info("Logging setup complete. Starting pipeline.")


# ---------------- Configuration ----------------
# Get the base directory of the script for consistent paths
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

# Paths for the pipeline
OUTPUT_DIR = os.path.join(BASE_DIR, "../resources/data/metadata/geo_metadata")  # Directory for downloaded data
GEO_IDS_FILE = os.path.join(BASE_DIR, "../resources/geo_ids.txt")  # File containing GEO IDs to process
EXTRACTION_TEMPLATE = os.path.join(BASE_DIR, "../resources/geo_tag_template.json")  # Template for metadata extraction


# ---------------- Initialization ----------------
# Ensure required directories exist
os.makedirs(os.path.dirname(OUTPUT_DIR), exist_ok=True)  # Create output directory if it doesn't exist
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)  # Create log directory if it doesn't exist

# Configure logging to output to a file
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,  # Use DEBUG for more detailed logs
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="w"  # Overwrites the log file; use 'a' to append to it
)
logger = logging.getLogger("geo_metadata_pipeline")  # Create a logger for the pipeline


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
        # Initialize the base class for parallel processing
        super().__init__(resource_ids=geo_ids, output_dir=OUTPUT_DIR)

        # Initialize the downloader instance
        self.downloader = GeoMetadataDownloader(output_dir=OUTPUT_DIR, debug=True)

        # Store the template path for the extractor
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
            # Log the start of the download
            logger.info(f"Starting download for GEO ID: {geo_id}")

            # Attempt to download and extract the file
            extracted_path = self.downloader.download_file(geo_id)

            # Ensure the extracted file exists
            if not extracted_path or not os.path.exists(extracted_path):
                raise RuntimeError(f"Download failed for GEO ID: {geo_id}")

            # Log success and return the path
            logger.info(f"Download and extraction successful for GEO ID: {geo_id}")
            return extracted_path
        except Exception as e:
            # Log and raise any errors encountered during downloading
            logger.error(f"Error during download for GEO ID {geo_id}: {e}")
            raise

    def process_resource(self, file_path: str) -> None:
        """
        Processes the downloaded GEO XML file.

        Args:
            file_path (str): Path to the extracted XML file.

        Returns:
            None
        """
        try:
            # Log the start of metadata extraction
            logger.info(f"Starting metadata extraction for file: {file_path}")

            # Create an extractor instance and parse the file
            extractor = GeoMetadataExtractor(
                file_path=file_path,
                template_path=self.extractor_template,
                debug_mode=True,
                verbose_mode=False
            )
            extractor.parse()

            # Log success after processing
            logger.info(f"Successfully processed metadata for file: {file_path}")
        except Exception as e:
            # Log and raise any errors encountered during extraction
            logger.error(f"Error during metadata extraction for file {file_path}: {e}")
            raise

    def download_and_process(self, geo_id: str) -> None:
        """
        Combines downloading and processing for a GEO ID.

        Args:
            geo_id (str): GEO series ID.

        Returns:
            None
        """
        try:
            # Download the resource and get its path
            file_path = self.download_resource(geo_id)

            # Process the downloaded file
            self.process_resource(file_path)
        except Exception as e:
            # Log errors encountered during the combined operation
            logger.error(f"Error in pipeline for GEO ID {geo_id}: {e}")
            raise


# ---------------- Execution ----------------
if __name__ == "__main__":
    try:
        # Ensure the GEO IDs file exists
        if not os.path.exists(GEO_IDS_FILE):
            raise FileNotFoundError(f"GEO IDs file not found: {GEO_IDS_FILE}")

        # Read GEO series IDs from the input file
        with open(GEO_IDS_FILE, "r") as f:
            geo_ids = [line.strip() for line in f if line.strip()]
        if not geo_ids:
            raise ValueError("No GEO IDs found in the file.")

        # Log the number of GEO IDs to process
        logger.info(f"Processing {len(geo_ids)} GEO IDs.")
    except Exception as e:
        # Log critical errors related to reading the GEO IDs file
        logger.critical(f"Failed to read GEO IDs: {e}")
        exit(1)

    try:
        # Initialize the pipeline with GEO IDs
        pipeline = GeoMetadataPipeline(geo_ids=geo_ids)

        # Execute the pipeline with parallel processing
        pipeline.execute(pipeline.download_and_process)
    except Exception as e:
        # Log critical errors related to the pipeline execution
        logger.critical(f"Pipeline execution failed: {e}")
        exit(1)
