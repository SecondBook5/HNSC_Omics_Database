import traceback
import re
import pandas as pd
import logging
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import cast, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Session
from config.db_config import get_session_context
from db.orm_models.cptac_omics_model import ProteomicsData, PhosphoproteomicsData, TranscriptomicsData
from db.mapping_table import MappingTable
from config.logger_config import configure_logger
from db.schema.cptac_metadata_schema import CptacColumns, CptacMetadata, CptacMetadataLog
import cptac

# Configure logger
logger = configure_logger(
    name="CPTACDataIngestor",
    log_file="cptac_data_ingestor.log",
    level=logging.DEBUG,
    output="both"
)

# Define regex patterns for Ensembl IDs
ENSEMBL_GENE_REGEX = re.compile(r"^ENSG\d{11}(\.\d+)?$")
ENSEMBL_PROTEIN_REGEX = re.compile(r"^ENSP\d{11}(\.\d+)?$")
ENSEMBL_TRANSCRIPT_REGEX = re.compile(r"^ENST\d{11}(\.\d+)?$")


class CPTACDataIngestor:
    """
    Handles ingestion of CPTAC data (proteomics, phosphoproteomics, transcriptomics, etc.) into the database.
    """

    def __init__(self, cancer_dataset_name: str):
        if not cancer_dataset_name:
            logger.error("Cancer dataset name must be provided.")
            raise ValueError("Cancer dataset name must be provided.")
        self.cancer_dataset_name = cancer_dataset_name
        self.cancer_data = None

    def load_dataset(self):
        """
        Loads the cancer dataset using the CPTAC library.
        """
        try:
            self.cancer_data = getattr(cptac, self.cancer_dataset_name)()
            logger.info(f"Loaded CPTAC dataset: {self.cancer_dataset_name}")
        except AttributeError as e:
            logger.error(f"Dataset '{self.cancer_dataset_name}' not found in CPTAC: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to load dataset {self.cancer_dataset_name}: {e}")
            raise

    def preprocess_data(self, df: pd.DataFrame, data_type: str, source: str) -> pd.DataFrame:
        """
        Preprocess the data by flattening rows into sample-feature pairs and extracting metadata.
        Adjusts dynamically for differences in dataset structures.

        Args:
            df (pd.DataFrame): Original DataFrame from CPTAC.
            data_type (str): The data type (e.g., 'proteomics', 'phosphoproteomics', 'transcriptomics').
            source (str): Data source (e.g., 'washu', 'broad', 'bcm').

        Returns:
            pd.DataFrame: Transformed DataFrame with rows for each sample-feature pair.
        """
        # Reset index to expose 'Patient_ID' if needed
        if df.index.name == 'Patient_ID':
            df.reset_index(inplace=True)

        # Flatten MultiIndex columns if present
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [
                "|".join(filter(None, map(str, col))).strip()
                for col in df.columns
            ]

        # Dynamically extract metadata based on data type
        if data_type == "proteomics":
            metadata = df.iloc[:2, :].T.reset_index()
            metadata.columns = ["feature", "Name", "Database_ID"]
        elif data_type == "phosphoproteomics":
            metadata = df.iloc[:4, :].T.reset_index()
            metadata.columns = ["feature", "Name", "Site", "Peptide", "Database_ID"]
        elif data_type == "transcriptomics":
            if source in ["washu", "bcm"]:
                metadata = df.iloc[:2, :].T.reset_index()
                metadata.columns = ["feature", "Name", "Database_ID"]
            elif source == "broad":
                metadata = df.iloc[:3, :].T.reset_index()
                metadata.columns = ["feature", "Name", "Transcript_ID", "Database_ID"]
            else:
                logger.error(f"Unsupported transcriptomics source: {source}")
                raise ValueError(f"Unsupported source: {source}")
        else:
            logger.error(f"Unsupported data type for metadata extraction: {data_type}")
            raise ValueError(f"Unsupported data type: {data_type}")

        # Log metadata structure for debugging
        logger.debug(f"Metadata structure for {data_type} ({source}): {metadata.head()}")

        # Extract quantification data (skip metadata rows)
        quantification_data = df.iloc[len(metadata.columns):, :].copy()
        quantification_data.reset_index(inplace=True)

        # Melt quantification data
        melted_df = quantification_data.melt(
            id_vars=["Patient_ID"],
            var_name="feature",
            value_name="quantification"
        )

        # Filter out invalid features (e.g., "index")
        melted_df = melted_df[melted_df["feature"] != "index"]

        # Merge with metadata
        final_df = melted_df.merge(metadata, on="feature", how="left")

        # Rename 'Patient_ID' to 'sample_id' for consistency
        final_df.rename(columns={"Patient_ID": "sample_id"}, inplace=True)

        # Drop rows with missing metadata or quantifications
        final_df.dropna(subset=["Name", "quantification"], inplace=True)

        # Log final DataFrame structure
        logger.info(f"Preprocessed {data_type} ({source}) DataFrame structure: {final_df.head()}")
        return final_df

    def get_relevant_metadata_entry(self, session, data_type: str) -> CptacMetadata:
        """
        Retrieves a metadata entry for the specified data type.
        """
        try:
            metadata_entry = (
                session.query(CptacMetadata)
                .filter(CptacMetadata.data_type == data_type)
                .first()
            )
            if not metadata_entry:
                raise ValueError(f"No metadata entry found for data type {data_type}")
            return metadata_entry
        except Exception as e:
            logger.error(f"Error retrieving metadata entry for data type {data_type}: {e}")
            raise

    def get_column_mappings(self, session, metadata_entry):
        """
        Retrieve column mappings for the specified metadata entry.
        """
        try:
            column_mappings = (
                session.query(CptacColumns)
                .filter(CptacColumns.dataset_id == metadata_entry.id)
                .all()
            )
            if not column_mappings:
                raise ValueError(f"No column mappings found for metadata entry {metadata_entry.id}")

            return {column.data_type: column.column_data for column in column_mappings}
        except Exception as e:
            logger.error(f"Failed to retrieve column mappings for metadata entry {metadata_entry.id}: {e}")
            raise

    def preload_sample_ids(self, session, data_type: str, source: str) -> set:
        """
        Preload existing SampleIDs from the CptacMetadataLog for a specific data type and source.
        """
        try:
            existing_samples = session.query(CptacMetadataLog.SampleID).filter_by(
                DataType=data_type,
                Source=source,
                Status="uploaded"
            ).all()
            return {sample[0] for sample in existing_samples}
        except Exception as e:
            logger.error(f"Failed to preload SampleIDs: {e}")
            return set()

    def log_cptac_upload(self, session, sample_id, data_type, source, status, message=None):
        """
        Logs the status of CPTAC data upload for a specific sample.
        """
        try:
            log_entry = CptacMetadataLog(
                SampleID=sample_id,
                DataType=data_type,
                Source=source,
                Status=status,
                Message=message
            )
            session.add(log_entry)
            session.commit()
        except Exception as e:
            logger.error(f"Failed to log upload for sample {sample_id}: {e}")
            session.rollback()

    def upload_proteomics_data(
            self,
            session: Session,
            sample_id: str,
            sample_data: pd.DataFrame,
            metadata_entry: CptacMetadata,
            column_mappings: dict,
            mapper_table
    ):
        """
        Uploads proteomics data for a specific sample into the database.
        """
        try:
            # Validate sample_id
            if not isinstance(sample_id, str):
                logger.error(f"Invalid sample_id: {sample_id}. Skipping.")
                return

            batch = []
            for _, row in sample_data.iterrows():
                feature_name = row.get("feature")
                quant_value = row.get("quantification")

                # Validate feature name and quantification
                if not feature_name or quant_value is None:
                    logger.warning(f"Invalid data for feature {feature_name}. Skipping row.")
                    continue

                # Map feature to Ensembl IDs
                ensembl_gene_id = column_mappings.get(feature_name)
                if not ensembl_gene_id:
                    logger.warning(f"Feature {feature_name} not found in column mappings. Skipping row.")
                    continue

                ensembl_protein_id = ensembl_gene_id if ENSEMBL_PROTEIN_REGEX.match(ensembl_gene_id) else None

                # Get Mapper ID
                mapper_id = self.get_mapper_id(session, mapper_table, feature_name)
                if not mapper_id:
                    logger.warning(f"Mapper ID not found for feature {feature_name}. Skipping row.")
                    continue

                # Build data entry
                data_entry = ProteomicsData(
                    sample_id=sample_id,
                    protein_name=feature_name,
                    ensembl_gene_id=ensembl_gene_id,
                    ensembl_protein_id=ensembl_protein_id,
                    quantification={metadata_entry.source: {"value": quant_value}},
                    mapper_id=mapper_id,
                )
                batch.append(data_entry)

            # Check if batch is empty
            if not batch:
                logger.warning(f"No data to insert for proteomics sample {sample_id}. Skipping.")
                return

            # Bulk insert and commit
            session.bulk_save_objects(batch)
            session.commit()

            # Log successful upload
            self.log_cptac_upload(session, sample_id, metadata_entry.data_type, metadata_entry.source, "uploaded")
            logger.info(f"Proteomics data for sample {sample_id} successfully uploaded.")

        except Exception as e:
            # Log failed upload
            logger.error(f"Failed to upload proteomics data for sample {sample_id}: {e}")
            self.log_cptac_upload(session, sample_id, metadata_entry.data_type, metadata_entry.source, "failed", str(e))

    def upload_phosphoproteomics_data(
            self,
            session: Session,
            sample_id: str,
            sample_data: pd.DataFrame,
            metadata_entry: CptacMetadata,
            column_mappings: dict,
            mapper_table
    ):
        """
        Uploads phosphoproteomics data for a specific sample into the database.
        """
        try:
            # Validate sample_id
            if not isinstance(sample_id, str):
                logger.error(f"Invalid sample_id: {sample_id}. Skipping.")
                return

            batch = []
            for _, row in sample_data.iterrows():
                feature_name = row.get("feature")
                quant_value = row.get("quantification")

                # Validate feature name and quantification
                if not feature_name or quant_value is None:
                    logger.warning(f"Invalid data for feature {feature_name}. Skipping row.")
                    continue

                # Map feature to Ensembl IDs
                ensembl_gene_id = column_mappings.get(feature_name)
                if not ensembl_gene_id:
                    logger.warning(f"Feature {feature_name} not found in column mappings. Skipping row.")
                    continue

                ensembl_protein_id = ensembl_gene_id if ENSEMBL_PROTEIN_REGEX.match(ensembl_gene_id) else None

                # Extract phosphoproteomics-specific fields
                phosphorylation_site = row.get("Site", None)
                peptide = row.get("Peptide", None)

                # Get Mapper ID
                mapper_id = self.get_mapper_id(session, mapper_table, feature_name)
                if not mapper_id:
                    logger.warning(f"Mapper ID not found for feature {feature_name}. Skipping row.")
                    continue

                # Build data entry
                data_entry = PhosphoproteomicsData(
                    sample_id=sample_id,
                    phosphoprotein_name=feature_name,
                    phosphorylation_site=phosphorylation_site,
                    peptide=peptide,
                    ensembl_gene_id=ensembl_gene_id,
                    ensembl_protein_id=ensembl_protein_id,
                    quantification={metadata_entry.source: {"value": quant_value}},
                    mapper_id=mapper_id,
                )
                batch.append(data_entry)

            # Check if batch is empty
            if not batch:
                logger.warning(f"No data to insert for phosphoproteomics sample {sample_id}. Skipping.")
                return

            # Bulk insert and commit
            session.bulk_save_objects(batch)
            session.commit()

            # Log successful upload
            self.log_cptac_upload(session, sample_id, metadata_entry.data_type, metadata_entry.source, "uploaded")
            logger.info(f"Phosphoproteomics data for sample {sample_id} successfully uploaded.")

        except Exception as e:
            # Log failed upload
            logger.error(f"Failed to upload phosphoproteomics data for sample {sample_id}: {e}")
            self.log_cptac_upload(session, sample_id, metadata_entry.data_type, metadata_entry.source, "failed", str(e))

    def upload_transcriptomics_data(
            self,
            session: Session,
            sample_id: str,
            sample_data: pd.DataFrame,
            metadata_entry: CptacMetadata,
            column_mappings: dict,
            mapper_table
    ):
        """
        Uploads transcriptomics data for a specific sample into the database.
        """
        try:
            # Validate sample_id
            if not isinstance(sample_id, str):
                logger.error(f"Invalid sample_id: {sample_id}. Skipping.")
                return

            batch = []
            for _, row in sample_data.iterrows():
                feature_name = row.get("feature")
                quant_value = row.get("quantification")

                # Validate feature name and quantification
                if not feature_name or quant_value is None:
                    logger.warning(f"Invalid data for feature {feature_name}. Skipping row.")
                    continue

                # Map feature to Ensembl IDs
                ensembl_gene_id = column_mappings.get(feature_name)
                if not ensembl_gene_id:
                    logger.warning(f"Feature {feature_name} not found in column mappings. Skipping row.")
                    continue

                ensembl_transcript_id = ensembl_gene_id if ENSEMBL_TRANSCRIPT_REGEX.match(ensembl_gene_id) else None

                # Get Mapper ID
                mapper_id = self.get_mapper_id(session, mapper_table, feature_name)
                if not mapper_id:
                    logger.warning(f"Mapper ID not found for feature {feature_name}. Skipping row.")
                    continue

                # Build data entry
                data_entry = TranscriptomicsData(
                    sample_id=sample_id,
                    transcript_name=feature_name,
                    ensembl_gene_id=ensembl_gene_id,
                    ensembl_transcript_id=ensembl_transcript_id,
                    quantification={metadata_entry.source: {"value": quant_value}},
                    mapper_id=mapper_id,
                )
                batch.append(data_entry)

            # Check if batch is empty
            if not batch:
                logger.warning(f"No data to insert for transcriptomics sample {sample_id}. Skipping.")
                return

            # Bulk insert and commit
            session.bulk_save_objects(batch)
            session.commit()

            # Log successful upload
            self.log_cptac_upload(session, sample_id, metadata_entry.data_type, metadata_entry.source, "uploaded")
            logger.info(f"Transcriptomics data for sample {sample_id} successfully uploaded.")

        except Exception as e:
            # Log failed upload
            logger.error(f"Failed to upload transcriptomics data for sample {sample_id}: {e}")
            self.log_cptac_upload(session, sample_id, metadata_entry.data_type, metadata_entry.source, "failed", str(e))

    def get_mapper_id(self, session: Session, mapper_table, feature: str) -> int:
        """
        Retrieves or creates a mapper ID for a given feature.

        Args:
            session (Session): Database session.
            mapper_table: ORM model for the mapping table.
            feature (str): Feature name (e.g., "A1BG|ENSG00000121410.12").

        Returns:
            int: Mapper ID.
        """
        try:
            # Initialize mapping components
            ensembl_gene_id = None
            ensembl_transcript_id = None
            ensembl_protein_id = None
            gene_symbol = None

            # Parse feature into components
            if "|" in feature:
                parts = feature.split("|")
                gene_symbol = parts[0]  # Assume first part is gene symbol
                if len(parts) > 1 and ENSEMBL_GENE_REGEX.match(parts[1]):
                    ensembl_gene_id = parts[1]
                if len(parts) > 2 and ENSEMBL_TRANSCRIPT_REGEX.match(parts[2]):
                    ensembl_transcript_id = parts[2]
                if len(parts) > 3 and ENSEMBL_PROTEIN_REGEX.match(parts[3]):
                    ensembl_protein_id = parts[3]
            else:
                gene_symbol = feature  # Treat entire feature as gene_symbol if no delimiter is present

            # Check if the mapper entry already exists
            mapper_entry = session.query(mapper_table).filter(mapper_table.gene_symbol == gene_symbol).first()
            if mapper_entry:
                return mapper_entry.id

            # Create a new mapper entry
            new_mapper_entry = mapper_table(
                gene_symbol=gene_symbol,
                ensembl_gene_id=ensembl_gene_id,
                ensembl_transcript_id=ensembl_transcript_id,
                ensembl_protein_id=ensembl_protein_id,
            )
            session.add(new_mapper_entry)
            session.commit()

            return new_mapper_entry.id

        except Exception as e:
            logger.error(f"Failed to retrieve or create mapper ID for {feature}: {e}")
            session.rollback()
            raise

    def ingest_data(self):
        """
        Main ingestion process for the dataset.
        Loads the dataset, preprocesses data, and uploads it to the database.
        """
        try:
            # Step 1: Load the dataset
            self.load_dataset()

            with get_session_context() as session:
                # Step 2: Process each data type
                for data_type, upload_function in [
                    ("proteomics", self.upload_proteomics_data),
                    ("phosphoproteomics", self.upload_phosphoproteomics_data),
                    ("transcriptomics", self.upload_transcriptomics_data),
                ]:
                    # Step 2.1: Retrieve metadata and column mappings
                    metadata_entry = self.get_relevant_metadata_entry(session, data_type)
                    column_mappings = self.get_column_mappings(session, metadata_entry)

                    # Step 2.2: Preload uploaded samples
                    uploaded_samples = self.preload_sample_ids(session, metadata_entry.data_type, metadata_entry.source)
                    logger.info(f"Preloaded {len(uploaded_samples)} uploaded samples for {data_type}.")

                    # Step 2.3: Retrieve and preprocess data
                    try:
                        df = self.cancer_data.get_dataframe(metadata_entry.data_type, metadata_entry.source)
                        if "Patient_ID" not in df.index:
                            raise ValueError(f"'Patient_ID' index missing in {data_type} dataset.")
                        df = self.preprocess_data(df, metadata_entry.data_type, metadata_entry.source)
                    except Exception as e:
                        logger.error(f"Failed to preprocess {data_type} data: {e}")
                        continue

                    # Step 2.4: Validate preprocessed data
                    if "sample_id" not in df.columns:
                        logger.error(f"'sample_id' column missing in preprocessed {data_type} data. Skipping.")
                        continue
                    if df["sample_id"].isnull().any():
                        logger.warning(f"Null values found in 'sample_id' for {data_type}. Dropping invalid rows.")
                        df = df.dropna(subset=["sample_id"])
                    unique_sample_ids = df["sample_id"].unique()
                    logger.info(f"Unique sample_ids in {data_type} dataset: {unique_sample_ids}")

                    # Step 2.5: Process each sample
                    for sample_id, sample_data in df.groupby("sample_id"):
                        # Validate sample_id
                        if not isinstance(sample_id, str):
                            logger.error(f"Invalid sample_id: {sample_id}. Skipping.")
                            continue
                        if sample_id in uploaded_samples:
                            logger.info(f"Sample {sample_id} already uploaded. Skipping.")
                            continue
                        if sample_data.empty:
                            logger.warning(f"No data available for sample_id: {sample_id}. Skipping.")
                            continue

                        # Upload sample data
                        try:
                            upload_function(
                                session=session,
                                sample_id=sample_id,
                                sample_data=sample_data,
                                metadata_entry=metadata_entry,
                                column_mappings=column_mappings,
                                mapper_table=MappingTable
                            )
                        except Exception as e:
                            logger.error(f"Failed to upload {data_type} data for sample {sample_id}: {e}")
                            continue

            logger.info("Ingestion process completed successfully.")
        except Exception as e:
            logger.critical(f"Ingestion process failed: {e}")
            traceback.print_exc()


if __name__ == "__main__":
    ingestor = CPTACDataIngestor(cancer_dataset_name="Hnscc")
    ingestor.ingest_data()
