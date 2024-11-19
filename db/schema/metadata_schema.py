"""
This module defines the initial schema for storing GEO dataset metadata in PostgreSQL.

Overview:
    - This schema includes tables for series-level and sample-level metadata for biological datasets.
    - It focuses on essential fields needed for basic workflows such as RNA-Seq normalization.
    - Includes a logging table to track download and processing statuses for GEO datasets.

Key Concepts:
    - DatasetSeriesMetadata: Captures series-level information like dataset title, submission date,
      organism, and related publication (PubMedID).
    - DatasetSampleMetadata: Captures sample-level details such as organism, library strategy, and source type.
    - GeoMetadataLog: Tracks the success or failure of GEO metadata downloads and processing.

Why This Design:
    - Consolidates fields like GEOAccession and SeriesID to avoid redundancy.
    - Keeps the initial schema lightweight while allowing future expansions.
    - Supports efficient querying of series-sample relationships and publication links.

Future Expansions:
    - Additional fields can be added later in separate files (e.g., `series_metadata.py`, `sample_metadata.py`)
      for workflows requiring detailed metadata like `SupplementaryData` or `OverallDesign`.
"""

# Import SQLAlchemy modules for defining the schema
from sqlalchemy import Column, String, Integer, Date, Text, TIMESTAMP, ForeignKey, func
from sqlalchemy.orm import relationship  # For relationships between tables
from sqlalchemy.dialects.postgresql import ARRAY  # Import ARRAY type for PostgreSQL
from config.db_config import Base  # Import Base for ORM model inheritance


class DatasetSeriesMetadata(Base):
    """
    Captures series-level metadata for biological datasets.

    Attributes:
        SeriesID (str): Primary key, unique identifier for each series (e.g., GSE114446).
        Title (str): Descriptive title of the dataset.
        SubmissionDate (Date): Date the dataset was submitted.
        Organism (str): Organism studied in the dataset (e.g., Homo sapiens).
        PubMedID (int): PubMed ID for publications associated with the dataset.
    """
    __tablename__ = 'dataset_series_metadata'  # Name of the table in the database

    # Define the primary key and essential columns
    SeriesID = Column(String, primary_key=True)  # Unique identifier for the series (e.g., GSE114446)
    Title = Column(String, nullable=False)  # Title or name of the series
    SubmissionDate = Column(Date, nullable=False)  # Date when the series was submitted
    Organism = Column(String, nullable=False)  # Organism studied in the series
    PubMedID = Column(Integer, nullable=True)  # PubMed ID linking to a publication

    # Define the relationship to the DatasetSampleMetadata table
    Samples = relationship("DatasetSampleMetadata", back_populates="Series")  # Links series to its samples


class DatasetSampleMetadata(Base):
    """
    Captures sample-level metadata within a dataset series.

    Attributes:
        SampleID (str): Primary key, unique identifier for each sample (e.g., GSM3141829).
        SeriesID (str): Foreign key linking to the associated series.
        Organism (str): Organism studied in the sample (e.g., Homo sapiens).
        LibraryStrategy (str): Library preparation strategy (e.g., RNA-Seq).
        LibrarySource (str): Type of source material (e.g., transcriptomic).
    """
    __tablename__ = 'dataset_sample_metadata'  # Name of the table in the database

    # Define the primary key and essential columns
    SampleID = Column(String, primary_key=True)  # Unique identifier for the sample
    SeriesID = Column(String, ForeignKey('dataset_series_metadata.SeriesID'), nullable=False)  # Links to a series
    Organism = Column(String, nullable=True)  # Organism studied in the sample
    LibraryStrategy = Column(String, nullable=True)  # Strategy used for library preparation
    LibrarySource = Column(String, nullable=True)  # Source type (e.g., transcriptomic)

    # Define the relationship to the DatasetSeriesMetadata table
    Series = relationship("DatasetSeriesMetadata", back_populates="Samples")  # Links sample to its series


class GeoMetadataLog(Base):
    """
    Logs download and processing statuses for GEO metadata.

    Attributes:
        id (int): Auto-incrementing primary key for the log entry.
        geo_id (str): GEO series or sample ID being logged (e.g., GSE114446 or GSM3141829).
        status (str): Status of the operation (e.g., 'Success', 'Failure').
        message (str): Details or error messages related to the operation.
        log_time (timestamp): Timestamp when the log entry was created.
    """
    __tablename__ = 'geo_metadata_log'  # Name of the table in the database

    # Define the primary key and essential columns
    id = Column(Integer, primary_key=True, autoincrement=True)  # Auto-incrementing log ID
    geo_id = Column(String(50), nullable=False, unique = True)  # ID of the GEO series or sample
    status = Column(String(20), nullable=False)  # Status of the operation
    message = Column(Text, nullable=True)  # Additional details or error messages
    file_names = Column(ARRAY(String), nullable=True)  # List of file names related to the GEO ID
    log_time = Column(TIMESTAMP, server_default=func.now())  # Timestamp of the log entry
