import traceback
import logging
import json
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from config.db_config import get_session_context
from db.orm_models.proteomics_object import Proteomics
from db.mapping_table import MappingTable
from config.logger_config import configure_logger
from db.schema.cptac_metadata_schema import CptacColumns, CptacMetadataLog
import cptac

# Configure logger
logger = configure_logger(
    name="CptacDataAppender",
    log_file="cptac_data_appender.log",
    level=logging.DEBUG,
    output="both"
)

class CptacDataAppender:
    """
    Generic CPTAC Data Appender to add quantification data to the Proteomics table.
    Ensures data integrity and merges quantifications from multiple sources.
    """

    def __init__(self, cancer_dataset_name: str, data_type: str, source_name: str):
        if not cancer_dataset_name or not data_type or not source_name:
            logger.error("Cancer dataset name, data_type, and source_name must be provided.")
            raise ValueError("All parameters must be provided.")
        self.cancer_dataset_name = cancer_dataset_name
        self.data_type = data_type
        self.source_name = source_name.lower()
        self.cancer_data = None

    def log_append_status(self, session: Session, sample_id: str, status: str, message: str = None):
        """
        Log the status of an append operation.
        """
        try:
            log_entry = CptacMetadataLog(
                SampleID=sample_id,
                DataType=self.data_type,
                Source=self.source_name,
                Status=status,
                Message=message
            )
            session.add(log_entry)
            session.commit()
        except Exception as e:
            logger.error(f"Failed to log append status for sample {sample_id}: {e}")
            session.rollback()

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

    def preprocess_data(self, df):
        """
        Flatten and clean the DataFrame for appending, using the logic from the ingestor.
        """
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

        # Remove NaN quantifications
        melted_df.dropna(subset=["quantification"], inplace=True)

        logger.info(f"Preprocessed data: {len(melted_df)} sample-protein pairs ready for appending.")
        return melted_df

    def preload_sample_protein_pairs(self, session: Session) -> set:
        """
        Preload existing sample-protein pairs from the Proteomics table.
        """
        try:
            existing_pairs = session.query(Proteomics.sample_id, Proteomics.protein_name).all()
            return {(pair.sample_id, pair.protein_name) for pair in existing_pairs}
        except Exception as e:
            logger.error(f"Error preloading sample-protein pairs: {e}")
            return set()

    import json

    def append_sample(self, session: Session, sample_id: str, sample_data, column_mappings, existing_pairs):
        """
        Append a single sample's data to the database.
        """
        try:
            batch = []
            for _, row in sample_data.iterrows():
                protein_name = row["protein"]
                quant_value = row["quantification"]

                # Check if this sample-protein pair already exists
                if (sample_id, protein_name) in existing_pairs:
                    logger.info(f"Sample {sample_id}, Protein {protein_name} already exists. Appending quantification.")
                    session.execute(
                        text("""
                            UPDATE proteomics
                            SET 
                                quantification = jsonb_set(
                                    quantification,
                                    ARRAY[:source_name],
                                    to_jsonb(:quant_value::TEXT),
                                    true
                                ),
                                description = CONCAT(description, :description_update)
                            WHERE sample_id = :sample_id AND protein_name = :protein_name;
                        """),
                        {
                            "source_name": self.source_name,
                            "quant_value": json.dumps({"value": quant_value}),  # Serialize quant_value to JSON
                            "description_update": f", and {self.source_name}",
                            "sample_id": sample_id,
                            "protein_name": protein_name,
                        }
                    )
                else:
                    # Prepare new data entry
                    ensembl_id = column_mappings.get(protein_name)
                    ensembl_gene_id = ensembl_protein_id = None

                    if ensembl_id and ensembl_id.startswith("ENSP"):
                        ensembl_protein_id = ensembl_id
                    elif ensembl_id and ensembl_id.startswith("ENSG"):
                        ensembl_gene_id = ensembl_id

                    data_entry = {
                        "sample_id": sample_id,
                        "protein_name": protein_name,
                        "ensembl_gene_id": ensembl_gene_id,
                        "ensembl_protein_id": ensembl_protein_id,
                        "quantification": {self.source_name: {"value": quant_value}},
                        "description": f"Quantification of {self.data_type} for dataset {self.source_name}.",
                    }
                    batch.append(data_entry)

            if batch:
                stmt = insert(Proteomics).values(batch).on_conflict_do_nothing()
                session.execute(stmt)

            session.commit()
            self.log_append_status(session, sample_id, "appended")
            logger.info(f"Successfully appended data for sample {sample_id}.")
        except Exception as e:
            session.rollback()
            self.log_append_status(session, sample_id, "failed", str(e))
            logger.error(f"Failed to append data for sample {sample_id}: {e}")

    def append_data(self):
        """
        Main appending process for the new source.
        """
        try:
            self.load_dataset()

            with get_session_context() as session:
                # Preload existing sample-protein pairs
                existing_pairs = self.preload_sample_protein_pairs(session)

                # Retrieve and preprocess DataFrame for proteomics data
                df = self.cancer_data.get_dataframe(self.data_type, self.source_name)
                df = self.preprocess_data(df)

                # Process data sample-by-sample
                for sample_id, sample_data in df.groupby("sample_id"):
                    self.append_sample(session, sample_id, sample_data, {}, existing_pairs)

            logger.info("Data appending completed successfully.")
        except Exception as e:
            logger.critical(f"Data appending failed: {e}")
            traceback.print_exc()


# Main Execution
if __name__ == "__main__":
    appender = CptacDataAppender(cancer_dataset_name="Hnscc", data_type="proteomics", source_name="umich")
    appender.append_data()
