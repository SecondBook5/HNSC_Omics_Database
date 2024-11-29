# File: initialize_schema.py
"""
This script initializes the database schema by creating tables defined in geo_metadata_schema.py.
"""
import sys
import os

# Add the project root directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.db_config import Base, get_postgres_engine  # Now this should work
from db.schema.geo_metadata_schema import GeoSeriesMetadata, GeoSampleMetadata, GeoMetadataLog  # Import models

def initialize_tables() -> None:
    """
    Initializes the tables in the PostgreSQL database.
    Creates tables defined in geo_metadata_schema.py if they do not already exist.
    """
    try:
        # Get the PostgreSQL engine
        engine = get_postgres_engine()

        # Create all tables defined by the Base class
        print("Initializing database schema...")
        Base.metadata.create_all(bind=engine)
        print("Database schema initialized successfully.")

    except Exception as e:
        print(f"Failed to initialize database schema: {e}")

if __name__ == "__main__":
    # Call the initialization function
    initialize_tables()
