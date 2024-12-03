import os
import zipfile
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from config.db_config import get_session_context
from db.schema.microarray_schema import PlatformAnnotation, MicroarrayData
import logging
from config.logger_config import configure_logger

# Configure logger
logger = configure_logger(
    name="Microarray_Processing",
    log_file="microarray_processing.log",
    level=logging.DEBUG,
    output="both"
)


class SimplifiedMicroarrayProcessor:
    """
    Processes and populates microarray data (GPL and GSM) into the database for the simplified schema.
    """

    def __init__(self, session: Session):
        """
        Initializes the processor with a database session.

        Args:
            session (Session): SQLAlchemy database session.
        """
        self.session = session

    def load_gpl_file(self, gpl_file_path: str) -> pd.DataFrame:
        """
        Loads the GPL file into a DataFrame.

        Args:
            gpl_file_path (str): Path to the GPL file.

        Returns:
            pd.DataFrame: DataFrame containing platform annotations.
        """
        try:
            gpl_df = pd.read_csv(
                gpl_file_path,
                sep="\t",
                header=None,
                usecols=[0, 1, 2],
                names=["ProbeID", "GeneSymbol", "Description"],
            )
            logger.info(f"Loaded GPL file with {len(gpl_df)} rows.")
            return gpl_df
        except Exception as e:
            logger.error(f"Failed to load GPL file: {gpl_file_path}. Error: {e}")
            raise

    def populate_platform_annotation(self, gpl_df: pd.DataFrame) -> None:
        """
        Populates the PlatformAnnotation table using the GPL DataFrame.

        Args:
            gpl_df (pd.DataFrame): DataFrame containing platform annotations.
        """
        try:
            data_batch = [
                PlatformAnnotation(
                    ProbeID=row["ProbeID"],
                    GeneSymbol=row["GeneSymbol"],
                    Description=row["Description"],
                )
                for _, row in gpl_df.iterrows()
            ]
            self.session.bulk_save_objects(data_batch)
            self.session.commit()
            logger.info(f"Successfully inserted {len(data_batch)} rows into PlatformAnnotation.")
        except SQLAlchemyError as e:
            self.session.rollback()
            logger.error(f"Failed to populate PlatformAnnotation table: {e}")
            raise

    def load_gsm_file(self, gsm_file_path: str) -> pd.DataFrame:
        """
        Loads a GSM file into a DataFrame.

        Args:
            gsm_file_path (str): Path to the GSM file.

        Returns:
            pd.DataFrame: DataFrame containing sample expression data.
        """
        try:
            gsm_df = pd.read_csv(
                gsm_file_path,
                sep="\t",
                header=None,
                names=["ProbeID", "ExpressionValue"],
            )
            logger.info(f"Loaded GSM file with {len(gsm_df)} rows.")
            return gsm_df
        except Exception as e:
            logger.error(f"Failed to load GSM file: {gsm_file_path}. Error: {e}")
            raise

    def populate_microarray_data(self, gsm_df: pd.DataFrame, sample_id: str, series_id: str) -> None:
        """
        Populates the MicroarrayData table using the GSM DataFrame for a specific sample.

        Args:
            gsm_df (pd.DataFrame): DataFrame containing sample expression data.
            sample_id (str): Sample ID.
            series_id (str): Series ID.
        """
        try:
            data_batch = [
                MicroarrayData(
                    SampleID=sample_id,
                    ProbeID=row["ProbeID"],
                    SeriesID=series_id,
                    ExpressionValue=row["ExpressionValue"],
                )
                for _, row in gsm_df.iterrows()
            ]
            self.session.bulk_save_objects(data_batch)
            self.session.commit()
            logger.info(f"Successfully inserted {len(data_batch)} rows for SampleID: {sample_id}.")
        except SQLAlchemyError as e:
            self.session.rollback()
            logger.error(f"Failed to insert data for SampleID '{sample_id}': {e}")

    def process_microarray_data(self, zip_file_path: str, series_id: str) -> None:
        """
        Processes microarray data from a ZIP file and populates the database.

        Args:
            zip_file_path (str): Path to the ZIP file.
            series_id (str): Series ID.
        """
        extract_to = "../../resources/data/raw/GEO"
        os.makedirs(extract_to, exist_ok=True)

        try:
            # Step 1: Extract ZIP file
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
            logger.info(f"Extracted ZIP file: {zip_file_path}")

            # Step 2: Locate and process GPL file
            nested_directory = os.path.join(extract_to, series_id)
            gpl_file_path = os.path.join(nested_directory, "GPL570-tbl-1.txt")
            gpl_df = self.load_gpl_file(gpl_file_path)

            # Upload ProbeIDs first to avoid foreign key constraints
            self.populate_platform_annotation(gpl_df)

            # Step 3: Process GSM files
            for file_name in os.listdir(nested_directory):
                if file_name.startswith("GSM") and file_name.endswith(".txt"):
                    gsm_file_path = os.path.join(nested_directory, file_name)
                    sample_id = file_name.split("-")[0]  # Extract SampleID from the file name
                    try:
                        gsm_df = self.load_gsm_file(gsm_file_path)
                        self.populate_microarray_data(gsm_df, sample_id, series_id)
                    except Exception as e:
                        logger.error(f"Error processing GSM file '{file_name}' for SampleID '{sample_id}': {e}")
        except Exception as e:
            logger.error(f"Error processing microarray data for SeriesID '{series_id}': {e}")
        finally:
            self.session.close()
            logger.info("Session closed.")


if __name__ == "__main__":
    # Define parameters
    zip_file_path = "../../resources/metadata/geo_metadata/raw_metadata/GSE41613.zip"
    series_id = "GSE41613"

    # Run the processor
    with get_session_context() as session:
        processor = SimplifiedMicroarrayProcessor(session)
        processor.process_microarray_data(zip_file_path, series_id)
