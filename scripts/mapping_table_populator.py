import json
import re
from sqlalchemy.exc import SQLAlchemyError
from config.db_config import get_session_context
from config.logger_config import configure_logger
from db.mapping_table import MappingTable
from db.schema.cptac_metadata_schema import CptacColumns
import traceback

# Configure logger
logger = configure_logger(name="MappingTablePopulator", log_file="mapping_table.log", output="both")

# Regular expressions for strict validation
ENSEMBL_GENE_REGEX = re.compile(r"^ENSG\d{11}(\.\d+)?$")
ENSEMBL_PROTEIN_REGEX = re.compile(r"^ENSP\d{11}(\.\d+)?$")
ENSEMBL_TRANSCRIPT_REGEX = re.compile(r"^ENST\d{11}(\.\d+)?$")


def load_data_by_type(data_type: str) -> dict:
    """
    Load data from the database for a specific data type and categorize by source.

    Args:
        data_type (str): Type of data to query (e.g., "proteomics", "phosphoproteomics", "transcriptomics").

    Returns:
        dict: Dictionary with keys as sources and values as lists of parsed data entries.
    """
    try:
        with get_session_context() as session:
            results = session.query(CptacColumns.column_data, CptacColumns.description).filter(
                CptacColumns.data_type == data_type
            ).all()

            if not results:
                raise ValueError(f"No {data_type} data found in the cptac_columns table.")

            data_by_source = {}
            for column_data, description in results:
                try:
                    parsed_data = json.loads(column_data)
                    if data_type in parsed_data:
                        source = description.lower()
                        if source not in data_by_source:
                            data_by_source[source] = []
                        data_by_source[source].extend(parsed_data[data_type])
                except json.JSONDecodeError as e:
                    logger.error(f"Error decoding JSON for {data_type}: {e}")
                    continue

            for source, data in data_by_source.items():
                logger.info(f"Loaded {len(data)} rows of {data_type} data from {source}.")
            return data_by_source

    except SQLAlchemyError as e:
        logger.error(f"Database error while fetching {data_type} data: {e}")
        traceback.print_exc()
        return {}


def parse_entry(entry: list, data_type: str) -> dict:
    """
    Parse a single entry based on the data type and validate Ensembl IDs and gene_symbol.

    Args:
        entry (list): Data entry (e.g., list of strings).
        data_type (str): Type of data (e.g., "proteomics", "phosphoproteomics", "transcriptomics").

    Returns:
        dict: Parsed entry as a dictionary for the mapping table, or None if gene_symbol is missing.
    """
    parsed = {"gene_id": entry[0]}  # First field is always gene_id

    try:
        # Extract and validate gene_symbol
        if len(entry) > 0:
            gene_symbol = entry[0]  # Assume the first element is the gene symbol
            if gene_symbol:
                parsed["gene_symbol"] = gene_symbol
            else:
                logger.warning(f"Entry missing gene_symbol: {entry}. Skipping.")
                return None  # Skip if gene_symbol is missing

        if data_type == "proteomics" and len(entry) > 1:
            ensembl_id = entry[1]
            if ENSEMBL_PROTEIN_REGEX.match(ensembl_id):
                parsed["ensembl_protein_id"] = ensembl_id

        elif data_type == "phosphoproteomics" and len(entry) > 3:
            ensembl_id = entry[3]
            if ENSEMBL_PROTEIN_REGEX.match(ensembl_id):
                parsed["ensembl_protein_id"] = ensembl_id

        elif data_type == "transcriptomics":
            if len(entry) == 2:
                ensembl_id = entry[1]
                if ENSEMBL_TRANSCRIPT_REGEX.match(ensembl_id):
                    parsed["ensembl_transcript_id"] = ensembl_id
            elif len(entry) == 3:
                ensembl_gene_id = entry[2]
                ensembl_transcript_id = entry[1]
                if ENSEMBL_GENE_REGEX.match(ensembl_gene_id):
                    parsed["ensembl_gene_id"] = ensembl_gene_id
                if ENSEMBL_TRANSCRIPT_REGEX.match(ensembl_transcript_id):
                    parsed["ensembl_transcript_id"] = ensembl_transcript_id

        return parsed
    except IndexError:
        logger.warning(f"Malformed entry: {entry}. Skipping.")
        return None
    except Exception as e:
        logger.error(f"Unexpected error while parsing entry {entry}: {e}")
        traceback.print_exc()
        return None


def populate_mapping_table(data_by_source: dict, data_type: str, batch_size: int = 500):
    """
    Populate the mapping table with data in batches.

    Args:
        data_by_source (dict): Dictionary of data categorized by source.
        data_type (str): Type of data being processed.
        batch_size (int): Number of rows to process in each batch.
    """
    try:
        with get_session_context() as session:
            total_inserted = 0
            total_skipped = 0

            for source, data in data_by_source.items():
                logger.info(f"Processing {len(data)} rows of {data_type} data from {source}...")
                for start in range(0, len(data), batch_size):
                    batch = data[start:start + batch_size]
                    for entry in batch:
                        parsed_entry = parse_entry(entry, data_type)
                        if not parsed_entry:
                            total_skipped += 1
                            continue
                        try:
                            mapping_entry = MappingTable(**parsed_entry)
                            session.merge(mapping_entry)  # Add or update rows
                        except Exception as e:
                            logger.error(f"Error processing entry {parsed_entry}: {e}")
                            traceback.print_exc()

                    session.commit()
                    total_inserted += len(batch)
                    logger.info(f"Committed {total_inserted} / {len(data)} entries for {source}.")

            logger.info(f"Successfully inserted or updated {total_inserted} entries into the mapping table.")
            logger.info(f"Skipped {total_skipped} entries due to missing gene_symbol.")

    except SQLAlchemyError as e:
        logger.error(f"Database error during mapping table population for {data_type}: {e}")
        traceback.print_exc()
    except Exception as e:
        logger.error(f"Unexpected error during mapping table population for {data_type}: {e}")
        traceback.print_exc()



if __name__ == "__main__":
    try:
        logger.info("Starting MappingTable population...")

        # Load data for each type
        for data_type in ["proteomics", "phosphoproteomics", "transcriptomics"]:
            data_by_source = load_data_by_type(data_type)
            if data_by_source:
                populate_mapping_table(data_by_source, data_type)

        logger.info("MappingTable population completed successfully.")

    except Exception as e:
        logger.error(f"Critical error in main execution: {e}")
        traceback.print_exc()
