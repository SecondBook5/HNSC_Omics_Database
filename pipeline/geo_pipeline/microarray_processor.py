import os
import zipfile
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from config.db_config import get_session_context
from db.schema.microarray_schema import PlatformAnnotation, MicroarrayData
from db.mapping_table import MappingTable
import logging
from config.logger_config import configure_logger

# Configure logger
logger = configure_logger(
    name="Microarray_Processing",
    log_file="microarray_processing.log",
    level=logging.DEBUG,
    output="both"
)


class MicroarrayProcessor:
    """
    Processes and populates microarray data (GPL and GSM) into the database.
    """

    def __init__(self, session: Session):
        """
        Initializes the processor with a database session.

        Args:
            session (Session): SQLAlchemy database session.
        """
        self.session = session

    def get_mapper_id(self, gene_symbol: str) -> int:
        """
        Retrieve or create a mapper ID for a given gene symbol.

        Args:
            gene_symbol (str): Gene symbol to map.

        Returns:
            int: The mapper ID.
        """
        try:
            # Check if the mapper entry exists
            mapper_entry = self.session.query(MappingTable).filter_by(gene_symbol=gene_symbol).first()
            if mapper_entry:
                return mapper_entry.id

            # Create a new mapper entry if it doesn't exist
            new_mapper_entry = MappingTable(gene_symbol=gene_symbol)
            self.session.add(new_mapper_entry)
            self.session.commit()
            logger.info(f"Created new mapper entry for gene symbol: {gene_symbol}")
            return new_mapper_entry.id
        except SQLAlchemyError as e:
            self.session.rollback()
            logger.error(f"Failed to retrieve or create mapper ID for {gene_symbol}: {e}")
            raise

    def load_gpl_table(self, gpl_file_path: str) -> pd.DataFrame:
        """
        Loads the GPL annotation table into a DataFrame.

        Args:
            gpl_file_path (str): Path to the GPL file.

        Returns:
            pd.DataFrame: DataFrame containing platform annotations.
        """
        try:
            # Load the file without specifying headers to inspect its structure
            initial_df = pd.read_csv(gpl_file_path, sep="\t", header=None)
            num_columns = initial_df.shape[1]
            logger.info(f"File '{gpl_file_path}' has {num_columns} columns.")

            # Define column names based on the actual number of columns
            column_names = [
                "ProbeID", "GeneSymbol", "GeneTitle", "RefSeqIDs",
                "ChromosomalLocation", "UniGeneCluster", "TargetDescription",
                "GO_BP", "GO_CC", "GO_MF"
            ]
            column_names = column_names[:num_columns]  # Adjust column names to match available columns

            gpl_df = pd.read_csv(
                gpl_file_path,
                sep="\t",
                header=None,
                names=column_names
            )

            # Process GO terms into structured JSONB if those columns exist
            if "GO_BP" in gpl_df and "GO_CC" in gpl_df and "GO_MF" in gpl_df:
                gpl_df["GO_Terms"] = gpl_df.apply(
                    lambda row: {
                        "biological_process": row["GO_BP"].split(" /// ") if pd.notnull(row["GO_BP"]) else [],
                        "cellular_component": row["GO_CC"].split(" /// ") if pd.notnull(row["GO_CC"]) else [],
                        "molecular_function": row["GO_MF"].split(" /// ") if pd.notnull(row["GO_MF"]) else [],
                    },
                    axis=1,
                )
            else:
                gpl_df["GO_Terms"] = None
            return gpl_df

        except Exception as e:
            logger.error(f"Failed to load GPL file: {gpl_file_path}. Error: {e}")
            raise

    def populate_platform_annotation(self, gpl_df: pd.DataFrame, series_id: str, platform_id: str) -> None:
        """
        Populates the PlatformAnnotation table using the GPL DataFrame.

        Args:
            gpl_df (pd.DataFrame): GPL table DataFrame.
            series_id (str): Series ID.
            platform_id (str): Platform ID (e.g., GPL570).
        """
        for _, row in gpl_df.iterrows():
            # Extract and clean GeneSymbol
            raw_gene_symbol = row.get("GeneSymbol", None)
            if pd.notnull(raw_gene_symbol):
                # Clean the gene symbol by extracting the primary symbol
                cleaned_gene_symbol = raw_gene_symbol.split(" /// ")[0].strip()
            else:
                cleaned_gene_symbol = None

            mapper_id = None  # Default to None if no matching ID is found
            if cleaned_gene_symbol:
                try:
                    # Attempt to retrieve a mapper ID; if it fails, log and move on
                    mapper_entry = (
                        self.session.query(MappingTable)
                        .filter_by(gene_symbol=cleaned_gene_symbol)
                        .first()
                    )
                    if mapper_entry:
                        mapper_id = mapper_entry.id
                    else:
                        logger.info(
                            f"No mapper ID found for GeneSymbol: {cleaned_gene_symbol}. Leaving mapper_id as None.")
                except Exception as e:
                    logger.error(f"Error retrieving mapper ID for GeneSymbol '{cleaned_gene_symbol}': {e}")

            # Create PlatformAnnotation object
            annotation = PlatformAnnotation(
                PlatformID=platform_id,
                SeriesID=series_id,
                ProbeID=row["ProbeID"],
                GeneSymbol=cleaned_gene_symbol,
                GeneTitle=row.get("GeneTitle", None),
                ProteinIDs={"RefSeqIDs": row["RefSeqIDs"].split(" /// ") if pd.notnull(row["RefSeqIDs"]) else []},
                GO_Terms=row.get("GO_Terms", None),
                ChromosomalLocation=row.get("ChromosomalLocation", None),
                TargetDescription=row.get("TargetDescription", None),
                AdditionalAnnotations={"UniGeneCluster": row["UniGeneCluster"]},
                mapper_id=mapper_id,  # Allow mapper_id to be None
            )
            try:
                self.session.merge(annotation)  # Upsert to avoid duplicates
            except Exception as e:
                logger.error(f"Failed to insert PlatformAnnotation for GeneSymbol '{cleaned_gene_symbol}': {e}")
        logger.info("PlatformAnnotation table populated.")

    def process_gsm_files(self, gsm_folder: str, series_id: str) -> None:
        """
        Processes GSM files and populates the MicroarrayData table.

        Args:
            gsm_folder (str): Path to the folder containing GSM files.
            series_id (str): Series ID.
        """
        for file_name in os.listdir(gsm_folder):
            if file_name.startswith("GSM") and file_name.endswith(".txt"):
                gsm_file_path = os.path.join(gsm_folder, file_name)
                sample_id = file_name.split("-")[0]  # Extract SampleID from the file name

                logger.info(f"Processing GSM file: {file_name}")

                # Load GSM file
                gsm_df = pd.read_csv(
                    gsm_file_path,
                    sep="\t",
                    header=None,
                    names=["ProbeID", "ExpressionValue"]
                )

                # Populate MicroarrayData table
                for _, row in gsm_df.iterrows():
                    # Verify ProbeID exists in PlatformAnnotation
                    probe_annotation = (
                        self.session.query(PlatformAnnotation)
                        .filter(
                            PlatformAnnotation.ProbeID == row["ProbeID"],
                            PlatformAnnotation.SeriesID == series_id
                        )
                        .first()
                    )
                    if not probe_annotation:
                        logger.warning(
                            f"ProbeID {row['ProbeID']} not found in PlatformAnnotation for SeriesID {series_id}. Skipping.")
                        continue

                    # Insert into MicroarrayData
                    microarray_data = MicroarrayData(
                        SampleID=sample_id,
                        ProbeID=row["ProbeID"],
                        SeriesID=series_id,
                        ExpressionValue=row["ExpressionValue"]
                    )
                    self.session.add(microarray_data)

        logger.info("MicroarrayData table populated.")

    def process_microarray_data(self, zip_file_path: str, series_id: str, platform_id: str) -> None:
        """
        Processes microarray data from a ZIP file and populates the database.

        Args:
            zip_file_path (str): Path to the ZIP file.
            series_id (str): Series ID.
            platform_id (str): Platform ID (e.g., GPL570).
        """
        extract_to = "../../resources/data/raw/GEO"
        os.makedirs(extract_to, exist_ok=True)

        # Step 1: Extract ZIP file
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        logger.info(f"Extracted ZIP file: {zip_file_path}")

        # Adjust for nested directory (e.g., "GSE41613" folder)
        nested_directory = os.path.join(extract_to, series_id)

        # Step 2: Locate and Process GPL file
        gpl_file_path = os.path.join(nested_directory, f"{platform_id}-tbl-1.txt")
        gpl_df = self.load_gpl_table(gpl_file_path)
        self.populate_platform_annotation(gpl_df, series_id, platform_id)

        # Step 3: Process GSM files
        self.process_gsm_files(nested_directory, series_id)

        # Commit changes to the database
        self.session.commit()
        logger.info("Database updated with microarray data.")


if __name__ == "__main__":
    # Define parameters
    zip_file_path = "../../resources/metadata/geo_metadata/raw_metadata/GSE41613.zip"
    series_id = "GSE41613"
    platform_id = "GPL570"

    # Run the processor
    with get_session_context() as session:
        processor = MicroarrayProcessor(session)
        processor.process_microarray_data(zip_file_path, series_id, platform_id)
