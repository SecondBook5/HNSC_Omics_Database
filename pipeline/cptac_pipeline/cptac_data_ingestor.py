import traceback
import pandas as pd
import logging
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from config.db_config import get_session_context
from db.orm_models.proteomics_object import Proteomics
from db.mapping_table import MappingTable
from config.logger_config import configure_logger
from db.schema.cptac_metadata_schema import CptacColumns, CptacMetadata, CptacMetadataLog
import cptac

# Configure logger
logger = configure_logger(
    name="CPTACQuantificationUploader",
    log_file="cptac_quantification_uploader.log",
    level=logging.DEBUG,
    output="both"
)

class CPTACDataIngestor:
    """
    Handles ingestion of proteomics data into the database.
    """

    def __init__(self, cancer_dataset_name: str):
        if not cancer_dataset_name:
            logger.error("Cancer dataset name must be provided.")
            raise ValueError("Cancer dataset name must be provided.")
        self.cancer_dataset_name = cancer_dataset_name
        self.cancer_data = None

    def load_dataset(self):
        """
        Load the dataset using the CPTAC library.
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

    def preprocess_data(self, df: pd.DataFrame, metadata_entry) -> pd.DataFrame:
        """
        Preprocess the dataframe by:
        1. Flattening rows into sample-protein pairs.
        2. Removing pairs with NaN quantification.
        3. Logging dropped proteins and samples.
        """
        # Initial shape
        initial_samples = df.shape[0]
        initial_proteins = df.shape[1]

        # Flatten the DataFrame to long format
        melted_df = df.melt(
            var_name="protein",
            value_name="quantification",
            ignore_index=False
        ).reset_index()

        # Rename the original index to 'sample_id' for consistency
        if "index" in melted_df.columns:
            melted_df.rename(columns={"index": "sample_id"}, inplace=True)
        elif "Patient_ID" in melted_df.columns:
            melted_df.rename(columns={"Patient_ID": "sample_id"}, inplace=True)
        else:
            logger.error("Neither 'index' nor 'Patient_ID' found for renaming to 'sample_id'.")
            raise KeyError("Missing required column for renaming to 'sample_id'.")

        # Debugging: Verify the DataFrame structure
        logger.debug(f"Flattened DataFrame structure: {melted_df.head()}")

        # Initial count of sample-protein pairs
        initial_pairs = len(melted_df)

        # Remove NaN quantifications
        dropped_pairs = melted_df[melted_df["quantification"].isna()]
        num_pairs_dropped = len(dropped_pairs)
        melted_df = melted_df.dropna(subset=["quantification"])

        # Log dropped pairs
        if not dropped_pairs.empty:
            dropped_file = f"dropped_pairs_{metadata_entry.data_type}_{metadata_entry.source}.csv"
            dropped_pairs.to_csv(dropped_file, index=False)
            logger.info(f"Dropped {num_pairs_dropped} sample-protein pairs. Details saved in {dropped_file}.")

        # Log cleaning stats
        logger.info(f"Initial samples: {initial_samples}")
        logger.info(f"Initial proteins: {initial_proteins}")
        logger.info(f"Initial sample-protein pairs: {initial_pairs}")
        logger.info(f"Remaining sample-protein pairs: {len(melted_df)}")
        logger.info(f"Dropped sample-protein pairs: {num_pairs_dropped}")

        return melted_df

    def get_relevant_metadata_entry(self, session, data_type: str) -> CptacMetadata:
        """
        Retrieve a single metadata entry for the specified data type.
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
        Log the status of CPTAC data upload for a specific sample.
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

    def get_mapper_id(self, session: Session, mapper_table, feature: str) -> int:
        """
        Retrieve or create a mapper ID for a given feature.
        """
        try:
            mapper_entry = session.query(mapper_table).filter(mapper_table.gene_symbol == feature).first()
            if mapper_entry:
                return mapper_entry.id

            new_mapper_entry = mapper_table(gene_symbol=feature)
            session.add(new_mapper_entry)
            session.commit()
            return new_mapper_entry.id
        except Exception as e:
            logger.error(f"Failed to retrieve or create mapper ID for {feature}: {e}")
            session.rollback()
            raise

    def upload_sample_with_logging(self, session, sample_id, sample_data, orm_model, metadata_entry, column_mappings, mapper_table):
        """
        Upload a single sample and log the operation in the metadata log.
        """
        try:
            batch = []
            for _, row in sample_data.iterrows():
                protein_name = row["protein"]
                quant_value = row["quantification"]

                ensembl_id = column_mappings.get(protein_name, None)
                ensembl_gene_id, ensembl_protein_id = None, None
                if ensembl_id and ensembl_id.startswith("ENSP"):
                    ensembl_protein_id = ensembl_id
                elif ensembl_id and ensembl_id.startswith("ENSG"):
                    ensembl_gene_id = ensembl_id

                data_entry = {
                    "sample_id": sample_id,
                    "protein_name": protein_name,
                    "ensembl_gene_id": ensembl_gene_id,
                    "ensembl_protein_id": ensembl_protein_id,
                    "quantification": {metadata_entry.source: {"value": quant_value}},
                    "data_type": metadata_entry.data_type,
                    "description": f"Quantification of {metadata_entry.data_type} for dataset {metadata_entry.source}.",
                }

                data_entry["mapper_id"] = self.get_mapper_id(session, mapper_table, protein_name)
                batch.append(data_entry)

            # Perform bulk insert
            stmt = insert(orm_model).values(batch).on_conflict_do_nothing()
            session.execute(stmt)
            session.commit()

            # Log successful upload
            self.log_cptac_upload(session, sample_id, metadata_entry.data_type, metadata_entry.source, "uploaded")
            logger.info(f"Sample {sample_id} successfully uploaded.")
        except Exception as e:
            logger.error(f"Failed to upload data for sample {sample_id}: {e}")
            self.log_cptac_upload(session, sample_id, metadata_entry.data_type, metadata_entry.source, "failed", str(e))

    def ingest_data(self):
        """
        Main ingestion process for a single data type and source.
        """
        try:
            self.load_dataset()

            with get_session_context() as session:
                metadata_entry = self.get_relevant_metadata_entry(session, "proteomics")
                column_mappings = self.get_column_mappings(session, metadata_entry)

                # Preload already uploaded SampleIDs
                uploaded_samples = self.preload_sample_ids(session, metadata_entry.data_type, metadata_entry.source)
                logger.info(f"Preloaded {len(uploaded_samples)} uploaded samples.")

                # Retrieve and preprocess DataFrame for proteomics data
                df = self.cancer_data.get_dataframe(metadata_entry.data_type, metadata_entry.source)
                df = self.preprocess_data(df, metadata_entry)

                # Chunk data by sample and process each sample independently
                for sample_id, sample_data in df.groupby("sample_id"):
                    if sample_id in uploaded_samples:
                        logger.info(f"Sample {sample_id} already uploaded. Skipping.")
                        continue

                    self.upload_sample_with_logging(session, sample_id, sample_data, Proteomics, metadata_entry, column_mappings, MappingTable)

            logger.info("Ingestion completed successfully.")
        except Exception as e:
            logger.critical(f"Ingestion process failed: {e}")
            traceback.print_exc()


# Main Execution
if __name__ == "__main__":
    ingestor = CPTACDataIngestor(cancer_dataset_name="Hnscc")
    ingestor.ingest_data()
