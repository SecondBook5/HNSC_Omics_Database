import logging
from sqlalchemy import Table, MetaData, Column, String, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from typing import List
from config.db_config import get_postgres_engine

# Configure logger
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class GeoMetadataUploader:
    """
    Handles uploading GEO IDs to the database and updating their status.

    Attributes:
        engine: SQLAlchemy engine for database interactions.
        table_name (str): Name of the table to store GEO metadata.
    """

    def __init__(self, engine, table_name: str) -> None:
        """
        Initializes the uploader with a SQLAlchemy engine and table name.

        Args:
            engine: SQLAlchemy engine for database interactions.
            table_name (str): The name of the table to upload GEO metadata.
        """
        self.engine = engine
        self.table_name = table_name
        self.metadata = MetaData()
        self.geo_metadata_table = Table(self.table_name, self.metadata, autoload_with=self.engine)

    def upload_metadata(self, geo_metadata: List[dict]) -> None:
        """
        Uploads GEO metadata to the database and sets their status to 'not-downloaded'.

        Args:
            geo_metadata (List[dict]): List of GEO metadata dictionaries to upload.

        Raises:
            ValueError: If geo_metadata list is empty or contains invalid entries.
        """
        if not geo_metadata:
            raise ValueError("Metadata list cannot be empty.")

        try:
            with self.engine.begin() as conn:  # Use a single transaction
                # Perform an upsert operation (insert or do nothing if the row exists)
                insert_query = insert(self.geo_metadata_table).values(geo_metadata).on_conflict_do_nothing()
                result = conn.execute(insert_query)
                logger.info(f"Inserted {result.rowcount} rows into the table '{self.table_name}'.")

                # Validation: Check if all GEO IDs were inserted
                geo_ids_to_check = [item["geo_id"] for item in geo_metadata]
                validation_query = select(self.geo_metadata_table.c.geo_id).where(
                    self.geo_metadata_table.c.geo_id.in_(geo_ids_to_check)
                )
                inserted_geo_ids = [row.geo_id for row in conn.execute(validation_query)]
                missing_geo_ids = set(geo_ids_to_check) - set(inserted_geo_ids)

                if missing_geo_ids:
                    logger.warning(f"Some GEO IDs were not inserted: {missing_geo_ids}")
                else:
                    logger.info("All GEO IDs successfully uploaded and validated.")
        except SQLAlchemyError as e:
            logger.error(f"Database error during GEO metadata upload: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during GEO metadata upload: {e}")
            raise

    def update_status(self, geo_id: str, status: str) -> None:
        """
        Updates the status of a specific GEO ID in the database.

        Args:
            geo_id (str): The GEO series ID to update.
            status (str): The new status to set (e.g., 'downloaded').

        Raises:
            ValueError: If the GEO ID or status is invalid.
        """
        if not geo_id or not geo_id.startswith("GSE"):
            raise ValueError("Invalid GEO ID format.")
        if not status:
            raise ValueError("Status cannot be empty.")

        try:
            with self.engine.begin() as conn:  # Use a single transaction
                # Perform the update query
                update_query = update(self.geo_metadata_table).where(
                    self.geo_metadata_table.c.geo_id == geo_id
                ).values(status=status)
                result = conn.execute(update_query)

                logger.debug(f"Executed SQL: {update_query}")
                logger.debug(f"Parameters: GEO ID = {geo_id}, Status = {status}")

                if result.rowcount > 0:
                    logger.info(f"Status for GEO ID {geo_id} updated to '{status}'.")
                else:
                    logger.warning(f"GEO ID {geo_id} does not exist in the database. No rows updated.")
        except SQLAlchemyError as e:
            logger.error(f"Database error during status update for GEO ID {geo_id}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during status update for GEO ID {geo_id}: {e}")
            raise


