import traceback
import pandas as pd
import logging
from typing import List
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.postgresql import insert
from config.db_config import get_session_context
from db.orm_models.proteomics_object import Proteomics
from db.orm_models.phosphoproteomics_object import Phosphoproteomics
from db.mapping_table import MappingTable
from config.logger_config import configure_logger
from db.schema.cptac_metadata_schema import CptacColumns, CptacMetadata
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
    Handles ingestion of proteomics and phosphoproteomics data into the database.
    """

    def __init__(self, cancer_dataset_name: str):
        if not cancer_dataset_name:
            logger.error("Cancer dataset name must be provided.")
            raise ValueError("Cancer dataset name must be provided.")
        self.cancer_dataset_name = cancer_dataset_name
        self.cancer_data = None

    def load_dataset(self):
        try:
            self.cancer_data = getattr(cptac, self.cancer_dataset_name)()
            logger.info(f"Loaded CPTAC dataset: {self.cancer_dataset_name}")
        except AttributeError as e:
            logger.error(f"Dataset '{self.cancer_dataset_name}' not found in CPTAC: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to load dataset {self.cancer_dataset_name}: {e}")
            raise

    @staticmethod
    def get_relevant_metadata_entries(session, relevant_types: List[str]) -> List[CptacMetadata]:
        try:
            metadata_entries = (
                session.query(CptacMetadata)
                .filter(CptacMetadata.data_type.in_(relevant_types))
                .all()
            )
            if not metadata_entries:
                logger.warning("No metadata entries found for specified data types.")
            return metadata_entries
        except Exception as e:
            logger.error(f"Error retrieving metadata entries: {e}")
            raise

    @staticmethod
    def get_column_mappings(session, metadata_entry):
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

    def process_and_upload_data(self, session, metadata_entry, mapper_table):
        """
        Process and upload data for proteomics and phosphoproteomics.

        Args:
            session: The database session.
            metadata_entry: A single metadata entry from the database.
            mapper_table: The mapping table ORM model.
        """
        data_type = metadata_entry.data_type
        source = metadata_entry.source

        logger.info(f"Processing {data_type} from {source}.")

        try:
            # Retrieve column mappings for the metadata entry
            column_mappings = self.get_column_mappings(session, metadata_entry)

            # Retrieve the quantification DataFrame
            df = self.cancer_data.get_dataframe(data_type, source)

            # Limit to one sample for debugging
            if not df.empty:
                logger.info(f"Original dataset for {data_type} from {source} has {len(df)} samples.")
                df = df.iloc[:1]  # Limit to the first sample
                logger.info(f"Using only the first sample for {data_type} from {source}.")
            else:
                logger.warning(f"No data extracted for {data_type} from {source}. Skipping.")
                return

            # Debug: Log details about the extracted DataFrame
            logger.debug(f"Extracted DataFrame for {data_type} from {source} - Columns: {df.columns}")
            logger.debug(f"First row of extracted DataFrame:\n{df.iloc[0]}")
            logger.debug(f"Column mappings: {column_mappings}")

        except Exception as e:
            logger.error(f"Failed to retrieve data for {data_type} from {source}: {e}")
            return

        # Determine ORM model and process according to data type
        if data_type == "proteomics":
            orm_model = Proteomics
            self.upload_proteomics_data(session, df, orm_model, metadata_entry, mapper_table, column_mappings)
        elif data_type == "phosphoproteomics":
            orm_model = Phosphoproteomics
            self.upload_phosphoproteomics_data(session, df, orm_model, metadata_entry, mapper_table, column_mappings)
        else:
            logger.warning(f"Unknown data type {data_type}. Skipping.")
            return

    def upload_proteomics_data(
            self,
            session,
            df,
            orm_model,
            metadata_entry,
            mapper_table,
            column_mappings,
    ):
        """
        Upload data specific to proteomics by flattening quantifications for each sample-protein pair.

        Args:
            session: SQLAlchemy session.
            df: DataFrame containing proteomics data.
            orm_model: ORM model for proteomics data.
            metadata_entry: Metadata entry for the dataset.
            mapper_table: Mapping table ORM model.
            column_mappings: Column mappings for metadata entry.
        """
        try:
            for sample_id, row in df.iterrows():
                quantification_data = row.to_dict()

                for protein_info, quant_value in quantification_data.items():
                    try:
                        # Parse protein and Ensembl IDs
                        if isinstance(protein_info, tuple):
                            protein_name, ensembl_id = protein_info
                        else:
                            protein_name = protein_info
                            ensembl_id = None

                        # Determine Ensembl IDs based on prefix
                        ensembl_gene_id, ensembl_protein_id = None, None
                        if ensembl_id and ensembl_id.startswith("ENSP"):
                            ensembl_protein_id = ensembl_id
                            ensembl_gene_id = column_mappings.get("ensembl_gene_id")
                        elif ensembl_id and ensembl_id.startswith("ENSG"):
                            ensembl_gene_id = ensembl_id
                            ensembl_protein_id = column_mappings.get("ensembl_protein_id")

                        # Skip invalid quantification
                        if pd.isna(quant_value):
                            logger.debug(
                                f"Skipping protein {protein_name} in sample {sample_id} due to NaN quantification.")
                            continue

                        # Prepare the data entry
                        data_entry = {
                            "sample_id": sample_id,
                            "protein_name": protein_name,
                            "ensembl_gene_id": ensembl_gene_id,
                            "ensembl_protein_id": ensembl_protein_id,
                            "quantification": {metadata_entry.source: {"value": quant_value}},
                            "data_type": metadata_entry.data_type,
                            "description": metadata_entry.description,
                        }

                        # Add mapper_id
                        try:
                            data_entry["mapper_id"] = self.get_mapper_id(session, mapper_table, protein_name)
                        except Exception as e:
                            logger.error(f"Failed to map protein {protein_name} for sample {sample_id}: {e}")
                            continue

                        # Insert into the database
                        stmt = insert(orm_model).values(data_entry).on_conflict_do_nothing()
                        session.execute(stmt)

                    except Exception as e:
                        logger.error(f"Error processing protein {protein_info} in sample {sample_id}: {e}")
                        continue

            session.commit()
            logger.info(f"Successfully uploaded proteomics data for {metadata_entry.source}.")

        except Exception as e:
            session.rollback()
            logger.error(f"Failed to upload proteomics data for {metadata_entry.source}: {e}")

    def upload_phosphoproteomics_data(
            self,
            session,
            df,
            orm_model,
            metadata_entry,
            mapper_table,
            column_mappings,
    ):
        """
        Upload data specific to phosphoproteomics by flattening quantifications for each sample-protein pair.

        Args:
            session: SQLAlchemy session.
            df: DataFrame containing phosphoproteomics data.
            orm_model: ORM model for phosphoproteomics data.
            metadata_entry: Metadata entry for the dataset.
            mapper_table: Mapping table ORM model.
            column_mappings: Column mappings for metadata entry.
        """
        try:
            for sample_id, row in df.iterrows():
                quantification_data = row.to_dict()

                for protein_info, quant_value in quantification_data.items():
                    try:
                        # Parse protein and Ensembl IDs
                        if isinstance(protein_info, tuple):
                            phosphoprotein_name, phosphorylation_site, sequence_window, ensembl_id = protein_info
                        else:
                            phosphoprotein_name = protein_info
                            phosphorylation_site, sequence_window, ensembl_id = None, None, None

                        # Determine Ensembl IDs based on prefix
                        ensembl_gene_id, ensembl_protein_id = None, None
                        if ensembl_id and ensembl_id.startswith("ENSP"):
                            ensembl_protein_id = ensembl_id
                            ensembl_gene_id = column_mappings.get("ensembl_gene_id")
                        elif ensembl_id and ensembl_id.startswith("ENSG"):
                            ensembl_gene_id = ensembl_id
                            ensembl_protein_id = column_mappings.get("ensembl_protein_id")

                        # Skip invalid quantification
                        if pd.isna(quant_value):
                            logger.debug(f"Skipping phosphoprotein {phosphoprotein_name} in sample {sample_id}.")
                            continue

                        # Prepare the data entry
                        data_entry = {
                            "sample_id": sample_id,
                            "phosphoprotein_name": phosphoprotein_name,
                            "phosphorylation_site": phosphorylation_site,
                            "sequence_window": sequence_window,
                            "ensembl_gene_id": ensembl_gene_id,
                            "ensembl_protein_id": ensembl_protein_id,
                            "quantification": {metadata_entry.source: {"value": quant_value}},
                            "data_type": metadata_entry.data_type,
                            "description": metadata_entry.description,
                        }

                        # Add mapper_id
                        try:
                            data_entry["mapper_id"] = self.get_mapper_id(session, mapper_table, phosphoprotein_name)
                        except Exception as e:
                            logger.error(f"Failed to map phosphoprotein {phosphoprotein_name}: {e}")
                            continue

                        # Insert into the database
                        stmt = insert(orm_model).values(data_entry).on_conflict_do_nothing()
                        session.execute(stmt)

                    except Exception as e:
                        logger.error(f"Error processing phosphoprotein {protein_info}: {e}")
                        continue

            session.commit()
            logger.info(f"Successfully uploaded phosphoproteomics data for {metadata_entry.source}.")

        except Exception as e:
            session.rollback()
            logger.error(f"Failed to upload phosphoproteomics data for {metadata_entry.source}: {e}")

    @staticmethod
    def get_mapper_id(session, mapper_table, feature: str) -> int:
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

    def ingest_data(self):
        """
        Main method to ingest data into the database.
        """
        try:
            # Load the CPTAC dataset
            self.load_dataset()

            with get_session_context() as session:
                # Get metadata entries for relevant data types
                relevant_types = ["proteomics", "phosphoproteomics"]
                metadata_entries = self.get_relevant_metadata_entries(session, relevant_types)

                # Process each metadata entry individually
                for metadata_entry in metadata_entries:
                    try:
                        self.process_and_upload_data(session, metadata_entry, MappingTable)
                    except Exception as e:
                        logger.error(f"Error processing metadata entry {metadata_entry.id}: {e}")
                        traceback.print_exc()
                        continue

            logger.info("Ingestion completed successfully.")
        except Exception as e:
            logger.critical(f"Ingestion process failed: {e}")
            traceback.print_exc()


# Main Execution
if __name__ == "__main__":
    ingestor = CPTACDataIngestor(cancer_dataset_name="Hnscc")
    ingestor.ingest_data()
