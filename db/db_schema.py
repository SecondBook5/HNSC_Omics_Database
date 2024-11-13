# db/postgresql_schema.py
from sqlalchemy import Column, String, Integer, Text, JSON
from sqlalchemy.ext.declarative import declarative_base
from config.db_config import get_postgres_engine

# Define the SQLAlchemy base class for ORM models
Base = declarative_base()


# Define DATA_MAPPING table
class DataMapping(Base):
    """
    The DataMapping table catalogs relationships between PostgreSQL tables
    and MongoDB collections, defining mappings for data integration.

    Advanced Database Concepts:
    - Data Mapping: Connects structured (PostgreSQL) and semi-structured (MongoDB) data for integration.
    - Normalization: Separate mapping table creates modularity and avoids redundancy.
    """
    __tablename__ = 'data_mapping'

    # Primary key for the data mapping table
    MappingID = Column(Integer, primary_key=True)
    # Type of data being mapped (e.g., RNA-Seq, Single-cell RNA-Seq)
    DataType = Column(String, nullable=False)
    # PostgreSQL table name involved in the mapping
    PostgresTable = Column(String)
    # MongoDB collection name involved in the mapping
    MongoDBCollection = Column(String)
    # Detailed description of the integration process
    IntegrationDetails = Column(Text)
    # Additional metadata in JSON format, flexible for study or project-specific info
    AdditionalMetadata = Column(JSON)


# Define ENTITY_MAPPING table
class EntityMapping(Base):
    """
    The EntityMapping table defines field-specific mappings between PostgreSQL
    and MongoDB entities, enabling field-level data integration.

    Advanced Database Concepts:
    - Entity Mapping: Defines relationships between fields across databases.
    - Foreign Key and Cross-Database Relationship: Links fields for unified query capabilities.
    """
    __tablename__ = 'entity_mapping'

    # Primary key for entity mapping table
    MappingID = Column(Integer, primary_key=True)
    # Source table/collection in PostgreSQL or MongoDB
    SourceTable = Column(String, nullable=False)
    # Target table/collection in PostgreSQL or MongoDB
    TargetTable = Column(String, nullable=False)
    # Foreign key field in PostgreSQL tables
    ForeignKey = Column(String)
    # MongoDB field for NoSQL collections
    MongoField = Column(String)
    # Type of integration (e.g., Direct, Derived, Aggregated)
    IntegrationType = Column(String, nullable=False)


# Define a function to create all tables in PostgreSQL
def create_tables():
    """
    Creates all tables defined in this file in the PostgreSQL database.

    Advanced Database Concepts:
    - ACID Compliance: Ensures data integrity during table creation.
    - Modular Schema Design: Tables defined separately for easier management and scalability.
    """
    # Initialize the PostgreSQL engine
    engine = get_postgres_engine()

    # Create all tables in the database
    Base.metadata.create_all(engine)
    print("Tables created successfully.")


# Execute table creation if the script is run directly
if __name__ == "__main__":
    create_tables()
