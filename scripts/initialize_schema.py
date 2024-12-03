# File: initialize_schema.py
"""
This script initializes the database schema by creating tables defined in various schema and ORM model files,
including geo_metadata_schema.py, cptac_metadata_schema.py, and others.
"""

import sys
import os
from typing import List
from sqlalchemy.exc import SQLAlchemyError
from config.db_config import get_postgres_engine, Base  # Import engine creation function
from db.schema.geo_metadata_schema import GeoSeriesMetadata, GeoSampleMetadata, GeoMetadataLog
from db.schema.cptac_metadata_schema import CptacMetadata, CptacColumns
from db.orm_models.proteomics_object import Proteomics
from db.orm_models.phosphoproteomics_object import Phosphoproteomics
from db.mapping_table import MappingTable

def initialize_tables(models: List[type]) -> None:
    """
    Initializes the tables in the PostgreSQL database by creating the schema
    for the provided list of SQLAlchemy models.

    Args:
        models (List[type]): List of SQLAlchemy ORM models or Base classes to initialize.

    Raises:
        RuntimeError: If an error occurs during schema initialization.
    """
    try:
        # Get the PostgreSQL engine
        engine = get_postgres_engine()

        # Iterate through models and create tables for each
        print("Initializing database schema...")
        for model in models:
            model.metadata.create_all(bind=engine)
            print(f"Initialized schema for model: {model.__tablename__}")

        print("Database schema initialized successfully.")

    except SQLAlchemyError as e:
        print(f"SQLAlchemy error occurred during initialization: {e}")
        raise RuntimeError("Database initialization failed.") from e

    except Exception as e:
        print(f"Unexpected error occurred: {e}")
        raise RuntimeError("An unexpected error occurred during database initialization.") from e


if __name__ == "__main__":
    # Add the project root directory to sys.path
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    # Define all ORM models to initialize
    models_to_initialize = [
        GeoSeriesMetadata,
        GeoSampleMetadata,
        GeoMetadataLog,
        CptacMetadata,
        CptacColumns,
        Proteomics,
        Phosphoproteomics,
        MappingTable
    ]

    # Call the initialization function
    initialize_tables(models_to_initialize)
