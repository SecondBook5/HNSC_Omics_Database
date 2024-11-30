from sqlalchemy.exc import SQLAlchemyError, IntegrityError, DataError
from config.db_config import get_session_context
from config.logger_config import configure_logger
from db.schema.cptac_metadata_schema import CptacMetadata
import cptac
import traceback

# Configure logger to handle logs for debugging and monitoring
logger = configure_logger(name="cptac_metadata_extractor", log_file="cptac_metadata_extractor.log")


class CptacMetadataExtractor:
    """
    A class to handle metadata extraction and database upload for CPTAC datasets.
    """

    def __init__(self, cancer_dataset_name: str):
        """
        Initialize the extractor with the cancer dataset name.
        Args:
            cancer_dataset_name (str): The name of the cancer dataset to process.
        """
        if not cancer_dataset_name or not isinstance(cancer_dataset_name, str):
            raise ValueError("Invalid cancer dataset name provided. It must be a non-empty string.")
        self.cancer_dataset_name = cancer_dataset_name
        self.cancer_data = None
        self.errors = []  # Track combinations with processing errors

    def load_dataset(self):
        """
        Load the cancer dataset dynamically using the CPTAC package.
        """
        logger.info(f"Loading {self.cancer_dataset_name} dataset...")
        try:
            self.cancer_data = getattr(cptac, self.cancer_dataset_name)()
            logger.info(f"{self.cancer_dataset_name} dataset loaded successfully!")
        except AttributeError:
            logger.error(f"The dataset '{self.cancer_dataset_name}' is not available in CPTAC.")
            raise
        except Exception as e:
            logger.error(f"Unexpected error while loading the dataset '{self.cancer_dataset_name}': {e}")
            raise

    def get_data_sources(self):
        """
        Retrieve available data types and sources for the loaded dataset.
        Returns:
            DataFrame: Table of available data types and their sources.
        """
        logger.info("Listing available data types and sources...")
        try:
            data_sources = self.cancer_data.list_data_sources()
            if data_sources.empty:
                logger.warning("No data sources found for the dataset.")
            return data_sources
        except AttributeError as e:
            logger.error(f"Error accessing 'list_data_sources' method: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error listing data sources: {e}")
            raise

    def extract_metadata(self, data_sources):
        """
        Extract metadata for all data types and sources from the dataset sequentially.
        Args:
            data_sources (DataFrame): Table of available data types and sources.
        Returns:
            list[dict]: List of metadata entries.
        """
        if data_sources is None or data_sources.empty:
            logger.warning("No data sources available for metadata extraction.")
            return []

        metadata_entries = []

        # Sequential processing for debugging
        for _, row in data_sources.iterrows():
            for source in row.get("Available sources", []):
                if isinstance(row.get("Available sources"), list):
                    try:
                        result = self.extract_data_type_source(row["Data type"], source)
                        if result:
                            metadata_entries.append(result)
                            logger.info(f"Successfully processed: {result['data_type']} - {result['source']}")
                    except Exception as e:
                        logger.error(f"Error processing {row['Data type']} - {source}: {e}")
                        self.errors.append((row["Data type"], source))
                        traceback.print_exc()

        return metadata_entries

    def extract_data_type_source(self, data_type, source):
        """
        Extract metadata for a specific data type and source.
        Args:
            data_type (str): Data type (e.g., 'transcriptomics').
            source (str): Data source (e.g., 'bcm').
        Returns:
            dict: Metadata entry with data type, source, samples, and features.
        """
        if not data_type or not source:
            raise ValueError("Both 'data_type' and 'source' must be provided.")

        logger.info(f"Processing Data Type: '{data_type}', Source: '{source}'")
        try:
            df = self.cancer_data.get_dataframe(data_type, source)
            if df.empty:
                logger.warning(f"No data available for '{data_type}' from '{source}'.")
                return None

            logger.debug(f"DataFrame Columns for {data_type} - {source}: {df.columns}")
            logger.debug(f"DataFrame Head for {data_type} - {source}:\n{df.head()}")

            return {
                "data_type": data_type,
                "source": source,
                "num_samples": len(df),
                "num_features": len(df.columns),
                "sample_names": df.index.tolist()[:10],
                "feature_names": df.columns.tolist()[:10],
                "description": f"Dataset for {data_type} from {source}",
            }
        except Exception as e:
            logger.error(f"Unexpected error processing '{data_type}' from '{source}': {e}")
            self.errors.append((data_type, source))
            traceback.print_exc()
            return None

    @staticmethod
    def upload_metadata_to_db(metadata_entries):
        """
        Upload metadata entries to the database.
        Args:
            metadata_entries (list[dict]): Metadata entries to upload.
        """
        if not metadata_entries:
            logger.warning("No metadata entries available to upload.")
            return

        logger.info("Starting database upload...")
        with get_session_context() as session:
            for entry in metadata_entries:
                try:
                    if not entry.get("data_type") or not entry.get("source"):
                        logger.error("Missing required fields: 'data_type' or 'source'")
                        continue

                    metadata_entry = CptacMetadata(
                        data_type=entry["data_type"],
                        source=entry["source"],
                        num_samples=entry["num_samples"],
                        num_features=entry["num_features"],
                        description=entry["description"],
                        preview_samples=entry["sample_names"],
                        preview_features=entry["feature_names"],
                    )
                    session.add(metadata_entry)
                    session.commit()

                    logger.info(f"Uploaded metadata for {entry['data_type']} - {entry['source']}")
                except (IntegrityError, DataError, SQLAlchemyError) as e:
                    logger.error(f"Database error while uploading '{entry['data_type']}' from '{entry['source']}': {e}")
                    session.rollback()
                except Exception as e:
                    logger.error(f"Unexpected error during database upload for '{entry['data_type']}' from '{entry['source']}': {e}")
                    session.rollback()
        logger.info("Database upload complete.")

    def run(self):
        """
        Execute the metadata extraction and upload pipeline.
        """
        try:
            self.load_dataset()
            data_sources = self.get_data_sources()
            metadata_entries = self.extract_metadata(data_sources)
            if not metadata_entries:
                logger.warning("No metadata extracted. Exiting.")
                return
            self.upload_metadata_to_db(metadata_entries)
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")

        if self.errors:
            logger.warning("The following data type-source combinations encountered errors:")
            for error in self.errors:
                logger.warning(f" - {error}")
            print("\nErrors during processing:")
            print(self.errors)


# Run the pipeline
if __name__ == "__main__":
    extractor = CptacMetadataExtractor(cancer_dataset_name="Hnscc")
    extractor.run()
