"""
This module contains the SQLAlchemy ORM models for the microarray data and platform annotations,
optimized for indexing and flexibility with JSONB fields.
"""

# Import necessary SQLAlchemy modules
from sqlalchemy import Column, String, Text, Integer, Float, ForeignKey, Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
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
    SeriesID = Column(String, ForeignKey('geo_series_metadata.SeriesID'), nullable=False)  # Series ID to scope annotations
    ProbeID = Column(String, nullable=False)  # Probe identifier (e.g., 1007_s_at)
    GeneSymbol = Column(String)  # Associated gene symbol (e.g., DDR1)
    GeneTitle = Column(String)  # Full gene title or description
    ProteinIDs = Column(JSONB)  # JSONB field for protein IDs (e.g., RefSeq, SwissProt, UniProt)
    GO_Terms = Column(JSONB)  # JSONB field for GO terms with subfields (functions, processes, components)
    ChromosomalLocation = Column(String)  # Chromosomal location of the gene
    TargetDescription = Column(Text)  # Description of the probe target
    Sequence = Column(Text)  # Probe sequence
    AdditionalAnnotations = Column(JSONB)  # JSONB field for other miscellaneous annotations

    # Mapper Relationship
    mapper_id = Column(Integer, ForeignKey("mapping_table.id", ondelete="CASCADE"), nullable=False, index=True, doc="Foreign key to the Mapping Table.")

    # Define a relationship with MicroarrayData for cascading operations
    microarray_data = relationship(
        "MicroarrayData",
        back_populates="platform_annotation",
        cascade="all, delete-orphan",
    )

    # Define indexing and unique constraints
    __table_args__ = (
        UniqueConstraint('ProbeID', 'SeriesID', name='uq_platform_annotation_probe_series'),
        Index('ix_platform_annotation_platform_probe', 'PlatformID', 'ProbeID'),
        Index('ix_platform_annotation_series_probe', 'SeriesID', 'ProbeID'),
    )

    def __repr__(self):
        return (f"<PlatformAnnotation(PlatformID={self.PlatformID}, SeriesID={self.SeriesID}, "
                f"ProbeID={self.ProbeID})>")


class MicroarrayData(Base):
    """
    Stores microarray expression data for each sample and probe.
    """
    __tablename__ = 'microarray_data'

    id = Column(Integer, primary_key=True, autoincrement=True)  # Unique ID for the table
    SampleID = Column(String, ForeignKey('geo_sample_metadata.SampleID'), nullable=False)  # Sample identifier
    ProbeID = Column(String, nullable=False)  # Probe ID from PlatformAnnotation
    SeriesID = Column(String, nullable=False)  # Series ID from DatasetSeriesMetadata
    ExpressionValue = Column(Float, nullable=False)  # Expression value for the probe

    # Define foreign key with composite key reference
    __table_args__ = (
        ForeignKey('platform_annotation.ProbeID', ondelete='CASCADE'),
        ForeignKey('platform_annotation.SeriesID', ondelete='CASCADE'),
        Index('ix_microarray_data_sample_probe', 'SampleID', 'ProbeID'),
        Index('ix_microarray_data_series_probe', 'SeriesID', 'ProbeID'),
    )

    # Define relationships
    platform_annotation = relationship(
        "PlatformAnnotation",
        back_populates="microarray_data",
    )

    def __repr__(self):
        return (f"<MicroarrayData(SampleID={self.SampleID}, ProbeID={self.ProbeID}, "
                f"ExpressionValue={self.ExpressionValue})>")
