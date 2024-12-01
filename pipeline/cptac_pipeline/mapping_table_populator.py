import json
from sqlalchemy.exc import SQLAlchemyError
from config.db_config import get_session_context
from config.logger_config import configure_logger
from db.mapping_table import MappingTable
from db.schema.cptac_metadata_schema import CptacColumns  # Assuming these ORM models exist
import traceback

# Configure logger
logger = configure_logger(name="MappingTablePopulator", log_file="mapping_table.log", output="both")


def load_proteomics_data_from_db() -> tuple[list, list]:
    """
    Load and separate proteomics data for umich and bcm sources from the cptac_columns table.

    Returns:
        tuple[list, list]: Parsed proteomics data for umich and bcm.
    """
    try:
        with get_session_context() as session:
            # Query proteomics data from cptac_columns
            proteomics_results = session.query(CptacColumns.column_data, CptacColumns.description).filter(
                CptacColumns.data_type == "proteomics"
            ).all()

            if not proteomics_results:
                raise ValueError("No proteomics data found in the cptac_columns table.")

            # Separate data by description (umich and bcm sources)
            umich_data, bcm_data = [], []
            for column_data, description in proteomics_results:
                parsed_data = json.loads(column_data)
                if "proteomics" in parsed_data:
                    if "umich" in description.lower():
                        umich_data.extend(parsed_data["proteomics"])
                    elif "bcm" in description.lower():
                        bcm_data.extend(parsed_data["proteomics"])

            logger.info(f"Successfully loaded {len(umich_data)} rows from umich proteomics data.")
            logger.info(f"Successfully loaded {len(bcm_data)} rows from bcm proteomics data.")
            return umich_data, bcm_data

    except SQLAlchemyError as e:
        logger.error(f"Database error while fetching proteomics data: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding proteomics column_data JSON: {e}")
        raise


def populate_mapping_table(umich_data: list, bcm_data: list, batch_size: int = 500):
    """
    Populate the mapping table with umich data first, then map bcm data.

    Args:
        umich_data (list): List of tuples (gene_id, ensembl_protein_id) for umich.
        bcm_data (list): List of tuples (gene_id, ensembl_gene_id) for bcm.
        batch_size (int): Number of rows to process in each batch.
    """
    try:
        with get_session_context() as session:
            # Step 1: Load umich data in batches
            logger.info("Processing umich data...")
            for start in range(0, len(umich_data), batch_size):
                batch = umich_data[start:start + batch_size]

                for entry in batch:
                    try:
                        if len(entry) != 2:
                            logger.warning(f"Unexpected umich entry: {entry}")
                            continue

                        gene_id, ensembl_protein_id = entry

                        if not gene_id or not ensembl_protein_id:
                            logger.warning(f"Skipping invalid umich entry: {gene_id}, {ensembl_protein_id}")
                            continue

                        mapping_entry = MappingTable(
                            gene_id=gene_id,
                            ensembl_protein_id=ensembl_protein_id,
                            gene_symbol=gene_id,  # Use gene_id as fallback for gene_symbol
                        )
                        session.merge(mapping_entry)  # Add or update rows
                    except Exception as e:
                        logger.error(f"Error processing umich entry {entry}: {e}")
                        traceback.print_exc()
                session.commit()
                logger.info(f"Processed {start + len(batch)} / {len(umich_data)} umich entries.")

            # Step 2: Load bcm data in batches and map onto umich data
            logger.info("Processing bcm data...")
            for start in range(0, len(bcm_data), batch_size):
                batch = bcm_data[start:start + batch_size]

                for entry in batch:
                    try:
                        if len(entry) != 2:
                            logger.warning(f"Unexpected bcm entry: {entry}")
                            continue

                        gene_id, ensembl_gene_id = entry

                        if not gene_id or not ensembl_gene_id:
                            logger.warning(f"Skipping invalid bcm entry: {gene_id}, {ensembl_gene_id}")
                            continue

                        existing_entry = session.query(MappingTable).filter_by(gene_id=gene_id).first()
                        if existing_entry:
                            # Update existing row
                            existing_entry.ensembl_gene_id = ensembl_gene_id
                        else:
                            # Insert new row
                            new_entry = MappingTable(
                                gene_id=gene_id,
                                ensembl_gene_id=ensembl_gene_id,
                                gene_symbol=gene_id,  # Use gene_id as fallback for gene_symbol
                            )
                            session.add(new_entry)
                    except Exception as e:
                        logger.error(f"Error processing bcm entry {entry}: {e}")
                        traceback.print_exc()
                session.commit()
                logger.info(f"Processed {start + len(batch)} / {len(bcm_data)} bcm entries.")

            logger.info("Mapping table population completed successfully.")

    except SQLAlchemyError as e:
        logger.error(f"Database error during mapping table population: {e}")
        traceback.print_exc()
    except Exception as e:
        logger.error(f"Unexpected error during mapping table population: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    try:
        logger.info("Starting MappingTable population for proteomics...")

        # Load proteomics data from the database
        umich_data, bcm_data = load_proteomics_data_from_db()

        # Populate the mapping table
        populate_mapping_table(umich_data, bcm_data)

        logger.info("MappingTable population for proteomics completed successfully.")

    except Exception as e:
        logger.error(f"Critical error in main execution: {e}")
