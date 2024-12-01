from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert
from typing import List, Dict
import traceback

# Import the schema
from db.schema.cptac_metadata_schema import CptacMetadata, CptacColumns
from config.db_config import get_session_context  # Provides a session context manager

# Import CPTAC package
import cptac


class CptacColumnsFiller:
    """
    Class to populate the CptacColumns table with column names for datasets in the CptacMetadata table.
    """

    def __init__(self, cancer_dataset_name: str):
        """
        Initialize the filler with a specific cancer dataset name.

        Args:
            cancer_dataset_name (str): The name of the cancer dataset to process.
        """
        self.cancer_dataset_name = cancer_dataset_name
        self.cancer_data = None

    def load_dataset(self):
        """
        Load the CPTAC cancer dataset using the given name.
        """
        try:
            self.cancer_data = getattr(cptac, self.cancer_dataset_name)()
            print(f"Dataset {self.cancer_dataset_name} successfully loaded.")
        except AttributeError as e:
            print(f"Dataset '{self.cancer_dataset_name}' is not available in the CPTAC package. {e}")
            raise
        except Exception as e:
            print(f"Unexpected error while loading dataset '{self.cancer_dataset_name}': {e}")
            raise

    def get_column_names(self, data_type: str, source: str) -> List[str]:
        """
        Retrieve the column names for a specific data type and source.

        Args:
            data_type (str): The data type (e.g., "clinical").
            source (str): The source of the data (e.g., "mssm").

        Returns:
            List[str]: A list of column names.
        """
        try:
            df = self.cancer_data.get_dataframe(data_type, source)
            return df.columns.tolist()
        except Exception as e:
            print(f"Error retrieving columns for {data_type} - {source}: {e}")
            traceback.print_exc()
            return []

    def populate_columns_table(self):
        """
        Populate the CptacColumns table by retrieving column names for each dataset in the CptacMetadata table.
        """
        with get_session_context() as session:  # Database session
            try:
                # Fetch all datasets in the CptacMetadata table
                metadata_entries: List[CptacMetadata] = session.query(CptacMetadata).all()

                for metadata in metadata_entries:
                    # Fetch the columns for this dataset
                    column_names = self.get_column_names(metadata.data_type, metadata.source)
                    if not column_names:
                        print(f"No columns found for {metadata.data_type} - {metadata.source}")
                        continue

                    # Insert columns into CptacColumns table
                    for column_name in column_names:
                        stmt = insert(CptacColumns).values(
                            metadata_id=metadata.id,
                            column_name=column_name
                        ).on_conflict_do_nothing()  # Avoid duplicate entries
                        session.execute(stmt)

                    session.commit()
                    print(f"Added columns for {metadata.data_type} - {metadata.source} to CptacColumns table.")

            except SQLAlchemyError as e:
                print(f"Database error while populating CptacColumns table: {e}")
                session.rollback()
            except Exception as e:
                print(f"Unexpected error: {e}")
                traceback.print_exc()
                session.rollback()


# Main Execution
if __name__ == "__main__":
    # Initialize the filler for the HNSCC dataset
    filler = CptacColumnsFiller(cancer_dataset_name="Hnscc")

    try:
        # Load the dataset
        filler.load_dataset()

        # Populate the columns table
        filler.populate_columns_table()
    except Exception as e:
        print(f"Execution failed: {e}")
