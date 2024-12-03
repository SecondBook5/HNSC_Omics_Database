from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from config.db_config import get_session_context
from config.logger_config import configure_logger
import logging

# Configure logger
logger = configure_logger(
    name="MapperQuery",
    log_file="mapper_query.log",
    level=logging.DEBUG,
    output="both"
)

class MapperQuery:
    """
    Handles efficient, chunked updates to the Proteomics table with Ensembl IDs.
    """

    def update_proteomics_with_ensembl_ids(self, chunk_size: int = 10000):
        """
        Updates the Proteomics table in chunks to optimize performance and resource usage.
        Args:
            chunk_size (int): Number of rows to process per chunk.
        """
        try:
            with get_session_context() as session:
                # Log start of update process
                logger.info("Starting chunked update of Proteomics table with Ensembl IDs.")

                # Get the total number of rows requiring updates
                total_rows_stmt = text("""
                    SELECT COUNT(*) 
                    FROM proteomics AS p
                    JOIN mapping_table AS m ON p.mapper_id = m.id
                    WHERE p.ensembl_gene_id IS NULL OR p.ensembl_protein_id IS NULL;
                """)
                total_rows = session.execute(total_rows_stmt).scalar()

                if total_rows == 0:
                    logger.info("No rows require updates. Exiting.")
                    return

                logger.info(f"Total rows to update: {total_rows}")

                # Update in chunks using a CTE
                offset = 0
                while offset < total_rows:
                    logger.info(f"Processing chunk with offset {offset} and chunk size {chunk_size}.")
                    try:
                        update_stmt = text("""
                            WITH cte AS (
                                SELECT p.id AS proteomics_id, m.ensembl_gene_id, m.ensembl_protein_id
                                FROM proteomics AS p
                                JOIN mapping_table AS m ON p.mapper_id = m.id
                                WHERE p.ensembl_gene_id IS NULL OR p.ensembl_protein_id IS NULL
                                ORDER BY p.id
                                LIMIT :chunk_size OFFSET :offset
                            )
                            UPDATE proteomics
                            SET 
                                ensembl_gene_id = COALESCE(proteomics.ensembl_gene_id, cte.ensembl_gene_id),
                                ensembl_protein_id = COALESCE(proteomics.ensembl_protein_id, cte.ensembl_protein_id)
                            FROM cte
                            WHERE proteomics.id = cte.proteomics_id;
                        """)
                        result = session.execute(update_stmt, {"chunk_size": chunk_size, "offset": offset})
                        session.commit()

                        # Log rows updated in this chunk
                        logger.info(f"Updated {result.rowcount} rows in this chunk.")
                    except SQLAlchemyError as chunk_error:
                        logger.error(f"Error updating chunk at offset {offset}: {chunk_error}")
                        session.rollback()  # Rollback only this chunk, not the entire operation

                    offset += chunk_size

                logger.info("Chunked update of Proteomics table completed successfully.")

        except SQLAlchemyError as e:
            logger.error(f"SQLAlchemy Error during update: {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            raise

# Main Execution
if __name__ == "__main__":
    mapper_query = MapperQuery()
    mapper_query.update_proteomics_with_ensembl_ids(chunk_size=10000)
