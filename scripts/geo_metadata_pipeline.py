# File: scripts/geo_metadata_pipeline.py

# Import necessary classes for downloading, extracting, and uploading GEO metadata
from pipeline.geo_pipeline.geo_metadata_downloader import GeoMetadataDownloader  # Downloader class
from pipeline.geo_pipeline.geo_metadata_extractor import GeoMetadataExtractor  # Extractor class
from pipeline.geo_pipeline.geo_metadata_uploader import GeoMetadataUploader  # Uploader class
from config.db_config import get_postgres_engine  # Database engine configuration
from utils.parallel_processing import ParallelProcessor  # Base class for parallel processing

# ---------------- Configuration ----------------

import os
import logging

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

LOG_DIR = os.path.join(BASE_DIR, "../logs")
LOG_FILE = os.path.join(LOG_DIR, "geo_metadata_pipeline.log")
OUTPUT_DIR = os.path.join(BASE_DIR, "../resources/data/metadata/geo_metadata/raw_metadata")
GEO_IDS_FILE = os.path.join(BASE_DIR, "../resources/geo_ids.txt")
EXTRACTION_TEMPLATE = os.path.join(BASE_DIR, "../resources/geo_tag_template.json")

DB_ENGINE = get_postgres_engine()  # Get PostgreSQL engine
TABLE_NAME = "geo_metadata_log"

# Ensure necessary directories exist
os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Configure logger
logging.basicConfig(level=logging.INFO, filename=LOG_FILE, filemode="w", format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("geo_metadata_pipeline")

# ---------------- GEO Metadata Pipeline ----------------

class GeoMetadataPipeline(ParallelProcessor):
    """
    Pipeline for processing GEO metadata.
    Handles GEO ID uploads, downloading, and metadata extraction.
    """

    def __init__(self, geo_ids: list):
        """
        Initializes the GeoMetadataPipeline with GEO IDs.
        """
        super().__init__(resource_ids=geo_ids, output_dir=OUTPUT_DIR)

        # Initialize components
        self.downloader = GeoMetadataDownloader(output_dir=OUTPUT_DIR, debug=True, logger=logger)
        self.uploader = GeoMetadataUploader(engine=DB_ENGINE, table_name=TABLE_NAME)
        self.extractor_template = EXTRACTION_TEMPLATE

    def upload_geo_ids(self):
        """
        Upload GEO IDs to the database with initial status 'not-downloaded'.
        """
        try:
            geo_metadata = [{"geo_id": geo_id, "status": "not-downloaded"} for geo_id in self.resource_ids]
            logger.info("Uploading GEO IDs to the database...")
            self.uploader.upload_metadata(geo_metadata)
            logger.info("GEO IDs uploaded successfully.")
        except Exception as e:
            logger.critical(f"Failed to upload GEO IDs: {e}")
            raise

    def download_resource(self, geo_id: str) -> str:
        """
        Downloads the GEO dataset for a given GEO ID.
        """
        try:
            logger.info(f"Starting download for GEO ID: {geo_id}")
            extracted_path = self.downloader.download_file(geo_id)
            if not extracted_path or not os.path.exists(extracted_path):
                raise RuntimeError(f"Download failed for GEO ID: {geo_id}")

            # Update status in the database
            self.uploader.update_status(geo_id, "downloaded")
            logger.info(f"Download successful for GEO ID: {geo_id}")
            return extracted_path
        except Exception as e:
            logger.error(f"Error during download for GEO ID {geo_id}: {e}")
            raise

    def process_resource(self, file_path: str) -> None:
        """
        Processes the downloaded GEO XML file.
        """
        try:
            logger.info(f"Starting metadata extraction for file: {file_path}")
            extractor = GeoMetadataExtractor(
                file_path=file_path,
                template_path=self.extractor_template,
                debug_mode=True,
                verbose_mode=False
            )
            extractor.parse()
            logger.info(f"Successfully processed metadata for file: {file_path}")
        except Exception as e:
            logger.error(f"Error during metadata extraction for file {file_path}: {e}")
            raise

    def download_and_process(self, geo_id: str) -> None:
        """
        Combines downloading and processing for a GEO ID.
        """
        try:
            file_path = self.download_resource(geo_id)
            self.process_resource(file_path)
        except Exception as e:
            logger.error(f"Error in pipeline for GEO ID {geo_id}: {e}")
            raise

# ---------------- Execution ----------------

if __name__ == "__main__":
    try:
        if not os.path.exists(GEO_IDS_FILE):
            raise FileNotFoundError(f"GEO IDs file not found: {GEO_IDS_FILE}")

        with open(GEO_IDS_FILE, "r") as f:
            geo_ids = [line.strip() for line in f if line.strip()]

        if not geo_ids:
            raise ValueError("No GEO IDs found in the file.")

        logger.info(f"Processing {len(geo_ids)} GEO IDs.")
    except Exception as e:
        logger.critical(f"Failed to initialize pipeline: {e}")
        exit(1)

    try:
        pipeline = GeoMetadataPipeline(geo_ids=geo_ids)
        pipeline.upload_geo_ids()
        pipeline.execute(pipeline.download_and_process)
    except Exception as e:
        logger.critical(f"Pipeline execution failed: {e}")
        exit(1)
