# File: pipeline/cptac_pipeline/cptac_metadata_extractor.py

# Import necessary modules for database handling, logging, and error management
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, DataError  # Handle SQLAlchemy-specific database errors
from sqlalchemy.dialects.postgresql import insert  # Use PostgreSQL-specific insert statements for conflict resolution
from config.db_config import get_session_context  # Provide a context manager for database sessions
from config.logger_config import configure_logger  # Configure centralized logging for debugging and monitoring
from db.schema.cptac_metadata_schema import CptacMetadata  # Import the database schema for CPTAC metadata
import cptac  # Import the CPTAC library to access cancer datasets
import traceback  # Use traceback to print stack traces for debugging errors

# Set up a logger to capture and store logs in a specified file for tracking and debugging
logger = configure_logger(name="cptac_metadata_extractor", log_file="cptac_metadata_extractor.log")


class CptacMetadataExtractor:
    """
    Class to handle metadata extraction and upload for CPTAC datasets.
    Includes methods for data extraction, processing, and database upload.
    """

    def __init__(self, cancer_dataset_name: str):
        """
        Initialize the metadata extractor with the name of the cancer dataset.

        Args:
            cancer_dataset_name (str): The name of the cancer dataset to process.
        """
        # Validate that the provided cancer dataset name is a non-empty string
        if not cancer_dataset_name or not isinstance(cancer_dataset_name, str):
            raise ValueError("Invalid cancer dataset name provided. It must be a non-empty string.")

        # Store the dataset name as an instance attribute
        self.cancer_dataset_name = cancer_dataset_name

        # Initialize the dataset object as None (will be loaded later)
        self.cancer_data = None

        # Create a list to track errors for specific data type-source combinations
        self.errors = []

    def load_dataset(self):
        """
        Load the specified cancer dataset using the CPTAC library.
        """
        # Log the start of the dataset loading process
        logger.info(f"Loading dataset: {self.cancer_dataset_name}")
        try:
            # Dynamically load the dataset using the dataset name
            self.cancer_data = getattr(cptac, self.cancer_dataset_name)()

            # Log successful loading of the dataset
            logger.info(f"Successfully loaded dataset: {self.cancer_dataset_name}")
        except AttributeError:
            # Log an error if the dataset name is not available in the CPTAC library
            logger.error(f"The dataset '{self.cancer_dataset_name}' is not available in CPTAC.")
            raise
        except Exception as e:
            # Catch and log any unexpected errors during the dataset loading process
            logger.error(f"Unexpected error while loading dataset '{self.cancer_dataset_name}': {e}")
            raise

    def get_data_sources(self):
        """
        Retrieve a DataFrame of available data types and their sources for the loaded dataset.

        Returns:
            DataFrame: A DataFrame containing data types and sources.
        """
        # Log the start of the data sources retrieval process
        logger.info("Listing available data types and sources...")
        try:
            # Call the list_data_sources method from the dataset object to get available sources
            data_sources = self.cancer_data.list_data_sources()

            # Log a warning if no data sources are found
            if data_sources.empty:
                logger.warning("No data sources found for the dataset.")

            # Return the retrieved data sources
            return data_sources
        except AttributeError as e:
            # Log an error if the method is not found in the dataset object
            logger.error(f"Error accessing 'list_data_sources' method: {e}")
            raise
        except Exception as e:
            # Catch and log any unexpected errors during the data source retrieval process
            logger.error(f"Unexpected error listing data sources: {e}")
            raise

    def extract_metadata(self, data_sources):
        """
        Extract metadata for all data types and sources from the dataset.

        Args:
            data_sources (DataFrame): A DataFrame of available data types and sources.

        Returns:
            list[dict]: A list of extracted metadata entries.
        """
        # Validate that data sources are available
        if data_sources is None or data_sources.empty:
            logger.warning("No data sources available for metadata extraction.")
            return []

        # Initialize a list to store extracted metadata entries
        metadata_entries = []

        # Explicit test for 'medical_history' dataset from 'mssm' source
        try:
            logger.info("Explicitly testing 'medical_history' from 'mssm'.")
            medical_history_metadata = self.extract_data_type_source("medical_history", "mssm")
            if medical_history_metadata:
                metadata_entries.append(medical_history_metadata)
                logger.info(
                    f"Explicitly processed: {medical_history_metadata['data_type']} - {medical_history_metadata['source']}")
        except Exception as e:
            logger.error(f"Error during explicit 'medical_history' processing: {e}")
            self.errors.append(("medical_history", "mssm"))
            traceback.print_exc()

        # Iterate over each row in the data sources DataFrame
        for _, row in data_sources.iterrows():
            # Iterate over each source for the current data type
            for source in row.get("Available sources", []):
                try:
                    # Extract metadata for the current data type and source
                    metadata = self.extract_data_type_source(row["Data type"], source)

                    # If metadata was successfully extracted, add it to the list
                    if metadata:
                        metadata_entries.append(metadata)
                        logger.info(f"Processed successfully: {metadata['data_type']} - {metadata['source']}")
                except Exception as e:
                    # Log any errors and add the combination to the errors list
                    logger.error(f"Error processing {row['Data type']} - {source}: {e}")
                    self.errors.append((row["Data type"], source))
                    traceback.print_exc()

        # Return the list of extracted metadata entries
        return metadata_entries

    def extract_data_type_source(self, data_type: str, source: str):
        """
        Extract metadata for a specific data type and source.

        Args:
            data_type (str): The type of data (e.g., 'proteomics').
            source (str): The source of the data (e.g., 'bcm').

        Returns:
            dict: A dictionary containing metadata details, or None if extraction fails.
        """
        # Validate that both data type and source are provided
        if not data_type or not source:
            raise ValueError("Both 'data_type' and 'source' must be provided.")

        # Log the start of the metadata extraction process for the given type and source
        logger.info(f"Processing: Data Type = '{data_type}', Source = '{source}'")
        try:
            # Retrieve the data for the specified type and source as a DataFrame
            df = self.cancer_data.get_dataframe(data_type, source)

            # If the DataFrame is empty, log a warning and return None
            if df.empty:
                logger.warning(f"No data available for '{data_type}' from '{source}'.")
                return None

            # Extract metadata details and return them as a dictionary
            return {
                "data_type": data_type,
                "source": source,
                "num_samples": len(df),
                "num_features": len(df.columns),
                "sample_names": df.index.tolist()[:10],  # Preview the first 10 sample names
                "feature_names": df.columns.tolist()[:10],  # Preview the first 10 feature names
                "description": f"Dataset for {data_type} from {source}",
            }
        except Exception as e:
            # Log any errors during the metadata extraction process and return None
            logger.error(f"Unexpected error processing '{data_type}' from '{source}': {e}")
            self.errors.append((data_type, source))
            traceback.print_exc()
            return None

    @staticmethod
    def upload_metadata_to_db(metadata_entries: list[dict]):
        """
        Upload extracted metadata entries to the database with conflict resolution.

        Args:
            metadata_entries (list[dict]): A list of metadata entries to upload.
        """
        # Validate that there are metadata entries to upload
        if not metadata_entries:
            logger.warning("No metadata entries available for upload.")
            return

        # Log the start of the database upload process
        logger.info("Starting metadata upload to the database...")
        with get_session_context() as session:
            # Iterate over each metadata entry
            for entry in metadata_entries:
                try:
                    # Validate that required fields are present in the metadata entry
                    if not entry.get("data_type") or not entry.get("source"):
                        logger.error("Missing required fields: 'data_type' or 'source'. Skipping entry.")
                        continue

                    # Use ON CONFLICT to handle duplicate entries gracefully
                    stmt = insert(CptacMetadata).values(
                        data_type=entry["data_type"],
                        source=entry["source"],
                        num_samples=entry["num_samples"],
                        num_features=entry["num_features"],
                        description=entry["description"],
                        preview_samples=entry["sample_names"],
                        preview_features=entry["feature_names"],
                    ).on_conflict_do_nothing()  # Skip insertion if conflict occurs

                    # Execute the statement and commit
                    session.execute(stmt)
                    session.commit()
                    logger.info(f"Uploaded metadata for {entry['data_type']} - {entry['source']}, "
                                f"or skipped if it already exists.")
                except (IntegrityError, DataError, SQLAlchemyError) as e:
                    # Handle database-specific errors, log them, and roll back the transaction
                    logger.error(f"Database error for '{entry['data_type']}' - '{entry['source']}': {e}")
                    session.rollback()
                except Exception as e:
                    # Handle unexpected errors, log them, and roll back the transaction
                    logger.error(f"Unexpected error during upload: {e}")
                    session.rollback()
        logger.info("Database upload complete.")


    def run(self):
        """
        Execute the metadata extraction and upload pipeline.
        """
        try:
            # Load the dataset and retrieve available data sources
            self.load_dataset()
            data_sources = self.get_data_sources()

            # Extract metadata for all available data sources
            metadata_entries = self.extract_metadata(data_sources)

            # Upload the extracted metadata to the database
            self.upload_metadata_to_db(metadata_entries)
        except Exception as e:
            # Log any critical errors during pipeline execution
            logger.error(f"Pipeline execution failed: {e}")

        # Log errors encountered during processing
        if self.errors:
            logger.warning("Errors encountered for the following data type-source combinations:")
            for error in self.errors:
                logger.warning(f" - {error}")
            print("\nErrors during processing:")
            print(self.errors)


# Main execution block
if __name__ == "__main__":
    # Initialize the extractor with the specified cancer dataset name
    extractor = CptacMetadataExtractor(cancer_dataset_name="Hnscc")

    # Run the metadata extraction and upload pipeline
    extractor.run()
