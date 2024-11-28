"""
This module contains the SQLAlchemy ORM models for the microarray data and platform annotations.
"""

# Import necessary SQLAlchemy modules
from sqlalchemy import Column, String, Text, Integer, Float, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

# Base class for all ORM models
Base = declarative_base()


class PlatformAnnotation(Base):
    """
    Stores platform annotation data for microarray platforms, describing the probes and associated metadata.
    """
    __tablename__ = 'platform_annotation'

    id = Column(Integer, primary_key=True, autoincrement=True)  # Unique ID for the table
    PlatformID = Column(String, nullable=False, index=True)  # Platform identifier (e.g., GPL570)
    ProbeID = Column(String, nullable=False)  # Probe identifier (e.g., 1007_s_at)
    GeneSymbol = Column(String)  # Associated gene symbol (e.g., DDR1)
    TranscriptIDs = Column(Text)  # Associated transcript IDs, stored as JSON or comma-separated
    GO_Terms = Column(Text)  # GO terms related to the probe, stored as JSON or comma-separated
    AdditionalInfo = Column(Text)  # Additional descriptive data (e.g., sequences, definitions)

    # Define a relationship with MicroarrayData for cascading operations
    microarray_data = relationship(
        "MicroarrayData",
        back_populates="platform_annotation",
        cascade="all, delete-orphan",
    )

    def __repr__(self):
        return f"<PlatformAnnotation(PlatformID={self.PlatformID}, ProbeID={self.ProbeID})>"


class MicroarrayData(Base):
    """
    Stores microarray expression data for each sample and probe.
    """
    __tablename__ = 'microarray_data'

    id = Column(Integer, primary_key=True, autoincrement=True)  # Unique ID for the table
    SampleID = Column(String, ForeignKey('dataset_sample_metadata.SampleID'), nullable=False)  # Sample identifier
    ProbeID = Column(String, ForeignKey('platform_annotation.ProbeID'), nullable=False)  # Probe ID from PlatformAnnotation
    ExpressionValue = Column(Float, nullable=False)  # Expression value for the probe
    SeriesID = Column(String, ForeignKey('dataset_series_metadata.SeriesID'), nullable=False)  # Series ID from DatasetSeriesMetadata

    # Define relationships
    platform_annotation = relationship(
        "PlatformAnnotation",
        back_populates="microarray_data",
    )

    def __repr__(self):
        return f"<MicroarrayData(SampleID={self.SampleID}, ProbeID={self.ProbeID}, ExpressionValue={self.ExpressionValue})>"
