import os
import re
import json
import logging
from retry import retry
import traceback
from utils.connection_checker import DatabaseConnectionChecker
from utils.validate_tags import validate_tags
from pipeline.geo_pipeline.geo_metadata_downloader import GeoMetadataDownloader
from pipeline.geo_pipeline.geo_metadata_extractor import GeoMetadataExtractor
from pipeline.geo_pipeline.geo_metadata_uploader import GeoMetadataUploader
from utils.parallel_processing import GEODataProcessor
from config.db_config import get_postgres_engine
from db.schema.metadata_schema import DatasetSeriesMetadata, DatasetSampleMetadata
from utils.xml_tree_parser import parse_and_populate_xml_tree

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Define the project root directory based on the current file's known structure
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Define paths for resources and configurations, relative to the project root
GEO_IDS_FILE = os.path.join(ROOT_DIR, "resources/geo_ids.txt")
TAGS_TEMPLATE_FILE = os.path.join(ROOT_DIR, "resources/geo_tag_template.json")
OUTPUT_DIR = os.path.join(ROOT_DIR, "resources/data/metadata/geo_metadata")

GEO_ID_PATTERN = re.compile(r"^GSE\d{3,}$")

def initialize_database() -> None:
    try:
        engine = get_postgres_engine()
        DatasetSeriesMetadata.metadata.create_all(engine)
        DatasetSampleMetadata.metadata.create_all(engine)
        logger.info("Database tables initialized successfully.")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        raise RuntimeError("Failed to initialize database tables") from e

def load_geo_ids(file_path: str) -> list:
    try:
        with open(file_path, 'r') as f:
            geo_ids = [line.strip() for line in f if line.strip()]

        invalid_ids = [geo_id for geo_id in geo_ids if not GEO_ID_PATTERN.match(geo_id)]
        if invalid_ids:
            raise ValueError(f"Invalid GEO ID format for IDs: {invalid_ids}")

        logger.info(f"Loaded {len(geo_ids)} GEO IDs from {file_path}.")
        return geo_ids
    except (FileNotFoundError, ValueError) as e:
        logger.error(f"Error loading GEO IDs: {e}")
        raise

def load_and_validate_fields(file_path: str) -> dict:
    try:
        with open(file_path, 'r') as f:
            fields_to_extract = json.load(f)

        required_keys = {"Sample", "Series"}
        missing_keys = required_keys - fields_to_extract.keys()
        if missing_keys:
            raise ValueError(f"Missing required tags in extraction fields: {missing_keys}")

        logger.info(f"Loaded fields from {file_path}.")
        return fields_to_extract
    except (FileNotFoundError, json.JSONDecodeError, ValueError) as e:
        logger.error(f"Error loading fields from JSON: {e}")
        raise

@retry(tries=3, delay=2)
def safe_download_file(downloader: GeoMetadataDownloader, geo_id: str) -> str:
    try:
        file_path = downloader.download_file(geo_id)
        if not file_path:
            logger.error(f"Failed to download GEO ID: {geo_id}")
            return None
        return file_path
    except Exception as e:
        logger.error(f"Download failed for GEO ID {geo_id}: {e}")
        logger.debug(traceback.format_exc())
        return None

def process_geo_id_wrapper(geo_id: str, downloader, extractor, uploader, fields_to_extract):
    """Wrapper function for processing GEO IDs in parallel."""
    process_geo_id(geo_id, downloader, extractor, uploader, fields_to_extract)

def process_geo_id(geo_id: str, downloader: GeoMetadataDownloader, extractor: GeoMetadataExtractor,
                   uploader: GeoMetadataUploader, fields_to_extract: dict) -> None:
    logger.info(f"Processing GEO ID: {geo_id}")
    try:
        file_path = safe_download_file(downloader, geo_id)
        if not file_path:
            return

        xml_tree = parse_and_populate_xml_tree(file_path, fields_to_extract)
        if not validate_tags(xml_tree, fields_to_extract):
            logger.error(f"Validation failed for GEO ID {geo_id}. Skipping.")
            return

        metadata = extractor.extract_metadata(file_path)
        if metadata is None:
            logger.error(f"Metadata extraction failed for GEO ID {geo_id}.")
            return

        uploader.upload_metadata(metadata)
        logger.info(f"Metadata successfully uploaded for GEO ID: {geo_id}")

    except Exception as e:
        logger.error(f"Error processing GEO ID {geo_id}: {e}")
        logger.debug(traceback.format_exc())

def main():

    uploader = None  # Initialize to avoid referencing before assignment

    try:
        initialize_database()
        geo_ids = load_geo_ids(GEO_IDS_FILE)
        fields_to_extract = load_and_validate_fields(TAGS_TEMPLATE_FILE)

        downloader = GeoMetadataDownloader(OUTPUT_DIR)
        extractor = GeoMetadataExtractor(fields_to_extract, debug=True)

        connection_checker = DatabaseConnectionChecker()
        if not connection_checker.check_postgresql_connection():
            logger.error("PostgreSQL connection could not be established. Exiting.")
            return

        uploader = GeoMetadataUploader(engine=get_postgres_engine(), debug=True)

        geo_processor = GEODataProcessor(geo_ids, OUTPUT_DIR, extractor)
        geo_processor.execute(lambda geo_id: process_geo_id_wrapper(geo_id, downloader, extractor, uploader, fields_to_extract))

    except Exception as e:
        logger.critical(f"Pipeline encountered a critical error: {e}")
        logger.debug(traceback.format_exc())

    finally:
        if uploader:
            uploader.disconnect()
        logger.info("Pipeline completed. All resources closed.")

if __name__ == "__main__":
    main()


