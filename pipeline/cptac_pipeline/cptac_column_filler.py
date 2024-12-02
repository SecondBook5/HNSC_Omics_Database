# File: pipeline/cptac_pipeline/cptac_column_filler.py

import json
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert
from typing import List, Dict, Optional
import traceback
import cptac
import logging

from db.schema.cptac_metadata_schema import CptacMetadata, CptacColumns
from config.db_config import get_session_context
from config.logger_config import configure_logger
from utils.data_structures.hashmap import HashMap

# Configure the logger
logger = configure_logger(
    name="CptacColumnsFiller",
    log_file="cptac_column_filler.log",
    level=logging.DEBUG,
    output="both"
)


class CptacColumnsFiller:
    """
    Class to populate the CptacColumns table with grouped column metadata for datasets in the CptacMetadata table.
    """

    def __init__(self, cancer_dataset_name: str):
        if not cancer_dataset_name:
            logger.error("cancer_dataset_name cannot be empty.")
            raise ValueError("cancer_dataset_name cannot be empty.")
        self.cancer_dataset_name = cancer_dataset_name
        self.cancer_data = None
        self.hashmap = HashMap()
        logger.info(f"Initialized for dataset: {self.cancer_dataset_name}")

    def load_dataset(self) -> None:
        """
        Load the CPTAC cancer dataset using the given name.
        """
        try:
            self.cancer_data = getattr(cptac, self.cancer_dataset_name)()
            logger.info(f"Dataset {self.cancer_dataset_name} successfully loaded.")
        except AttributeError as e:
            logger.error(f"Dataset '{self.cancer_dataset_name}' is not available in the CPTAC package.")
            raise RuntimeError(f"Dataset '{self.cancer_dataset_name}' is not available in the CPTAC package.") from e
        except Exception as e:
            logger.error(f"Unexpected error while loading dataset '{self.cancer_dataset_name}': {e}")
            raise RuntimeError(f"Unexpected error while loading dataset '{self.cancer_dataset_name}'.") from e

    def get_cleaned_grouped_columns(self, data_type: str, source: str) -> Dict[str, List[str]]:
        """
        Retrieve and clean grouped column metadata for a specific data type and source.

        Args:
            data_type (str): The data type (e.g., "clinical").
            source (str): The source of the data (e.g., "mssm").

        Returns:
            Dict[str, List[str]]: A dictionary of grouped column names.
        """
        if not data_type or not source:
            logger.error("data_type and source must be provided.")
            raise ValueError("data_type and source must be provided.")

        try:
            df = self.cancer_data.get_dataframe(data_type, source)
            if df.empty:
                logger.warning(f"No data available for {data_type} - {source}.")
                return {}

            # Replace NaN with None for JSON compatibility
            df = df.where(pd.notnull(df), None)

            # Convert the columns to a list for JSON serialization
            return {data_type: df.columns.tolist()}
        except AttributeError as e:
            logger.error(f"Dataset does not contain data type '{data_type}' or source '{source}'.")
            raise RuntimeError(f"Dataset does not contain data type '{data_type}' or source '{source}'.") from e
        except Exception as e:
            logger.error(f"Error retrieving columns for {data_type} - {source}: {e}")
            raise RuntimeError(f"Error retrieving columns for {data_type} - {source}.") from e

    def populate_columns_table(self) -> None:
        """
        Populate the CptacColumns table with grouped column metadata for each dataset in CptacMetadata.
        """
        with get_session_context() as session:
            try:
                metadata_entries: List[CptacMetadata] = session.query(CptacMetadata).all()

                if not metadata_entries:
                    logger.warning("No entries found in the CptacMetadata table.")
                    return

                for metadata in metadata_entries:
                    try:
                        grouped_columns = self.get_cleaned_grouped_columns(metadata.data_type, metadata.source)
                        if not grouped_columns:
                            logger.warning(f"No columns found for {metadata.data_type} - {metadata.source}")
                            continue

                        # Serialize grouped columns to JSON string
                        column_data_json = json.dumps(grouped_columns)

                        stmt = insert(CptacColumns).values(
                            dataset_id=metadata.id,
                            column_data=column_data_json,
                            data_type=metadata.data_type,
                            source=metadata.source,  # Include source field
                            description=metadata.description,
                        ).on_conflict_do_nothing()
                        session.execute(stmt)

                        self.hashmap.put(metadata.id, grouped_columns)
                        logger.info(f"Inserted grouped columns for dataset ID {metadata.id} into CptacColumns table.")

                    except RuntimeError as e:
                        logger.error(f"Skipping dataset {metadata.id} due to error: {e}")
                        continue

                session.commit()
                logger.info("CptacColumns table populated successfully.")

            except SQLAlchemyError as e:
                session.rollback()
                logger.error(f"Database error while populating CptacColumns table: {e}")
                raise RuntimeError("Database error while populating CptacColumns table.") from e
            except Exception as e:
                session.rollback()
                logger.error(f"Unexpected error during CptacColumns table population: {e}")
                raise RuntimeError("Unexpected error during CptacColumns table population.") from e

    def lookup_in_hashmap(self, dataset_id: int) -> Optional[Dict[str, List[str]]]:
        """
        Perform an in-memory lookup for grouped column metadata using the HashMap.

        Args:
            dataset_id (int): The ID of the dataset to look up.

        Returns:
            Dict[str, List[str]]: The grouped column metadata if found, otherwise None.
        """
        try:
            if dataset_id is None:
                raise ValueError("dataset_id cannot be None.")

            result = self.hashmap.get(dataset_id)
            if result is None:
                logger.warning(f"No data found in HashMap for dataset ID {dataset_id}.")
                return None
            logger.info(f"Lookup successful for dataset ID {dataset_id}: {result}")
            return result

        except Exception as e:
            logger.error(f"Error during HashMap lookup for dataset ID {dataset_id}: {e}")
            raise RuntimeError(f"Error during HashMap lookup for dataset ID {dataset_id}.") from e


# Main Execution
if __name__ == "__main__":
    try:
        filler = CptacColumnsFiller(cancer_dataset_name="Hnscc")
        filler.load_dataset()
        filler.populate_columns_table()

        # Example HashMap lookup
        dataset_id = 1  # Replace with valid ID for testing
        grouped_columns = filler.lookup_in_hashmap(dataset_id)
        logger.info(f"Grouped columns for dataset ID {dataset_id}: {grouped_columns}")

    except Exception as e:
        logger.critical(f"Execution failed: {e}")
