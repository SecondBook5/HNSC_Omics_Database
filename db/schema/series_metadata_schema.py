"""
This module defines the expanded schema for series-level metadata.

Overview:
    - Adds detailed fields to the `DatasetSeriesMetadata` table to support advanced metadata analysis.
    - Includes supplementary data fields, overall design, platform details, and contributor information.

Purpose:
    - Expands upon the initial `DatasetSeriesMetadata` table from `metadata_schema.py`.
    - Provides fields required for workflows that depend on a richer metadata structure.

Usage:
    - This module integrates into the existing schema via migrations.
    - Works in conjunction with the `DatasetSampleMetadata` table and other related tables.

"""

# Import necessary modules from SQLAlchemy
from sqlalchemy import Column, String, Text, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship  # For defining relationships
from config.db_config import Base  # Base for ORM model inheritance


class ExpandedDatasetSeriesMetadata(Base):
    """
    Expanded version of DatasetSeriesMetadata with additional fields for detailed metadata.

    Attributes:
        SeriesID (str): Primary key, unique identifier for each series (e.g., GSE114446).
        SupplementaryData (str): Links to supplementary data files.
        OverallDesign (str): Description of the overall experimental design.
        PlatformID (str): Platform used for the series (e.g., GPL16791).
        Contributors (str): Contributors to the dataset.
        RelatedDatasets (str): Links to related datasets (e.g., BioProjects, SRAs).
    """
    __tablename__ = 'expanded_dataset_series_metadata'  # Name of the expanded table

    # Define primary key and expanded columns
    SeriesID = Column(String, ForeignKey('dataset_series_metadata.SeriesID'), primary_key=True)  # Links to SeriesID
    SupplementaryData = Column(Text, nullable=True)  # Links to supplementary data files
    OverallDesign = Column(Text, nullable=True)  # Overall experimental design description
    PlatformID = Column(String, nullable=True)  # Platform used (e.g., GPL16791)
    Contributors = Column(Text, nullable=True)  # List of dataset contributors
    RelatedDatasets = Column(Text, nullable=True)  # Links to related datasets (e.g., BioProjects, SRAs)

    # Define the relationship back to the base `DatasetSeriesMetadata` table
    Series = relationship("DatasetSeriesMetadata", back_populates="ExpandedSeries")

    # Ensure no duplicate SeriesIDs
    __table_args__ = (
        UniqueConstraint('SeriesID', name='uq_series_id_expanded'),
    )
