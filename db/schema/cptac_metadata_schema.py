"""
CPTAC Metadata Schema for HNSC Omics Database.

Overview:
    - Captures detailed metadata for datasets from the CPTAC resource.
    - Tracks dataset-level information (data type, source, number of samples/features).
    - Separately stores feature-level and sample-level metadata for modularity and scalability.

Key Features:
    - `CptacMetadata`: Main table describing datasets by type and source.
    - `CptacFeature`: Tracks features in datasets, indexed by metadata ID.
    - `CptacSample`: Tracks sample-level information, indexed by metadata ID.
    - Relationships between tables ensure flexibility and enable efficient querying.

Why This Design:
    - Normalized structure avoids redundancy while allowing granular metadata tracking.
    - Optimized for querying datasets, features, and samples independently or together.
    - Indexing ensures fast lookups for data type, source, and relationships.

Future Expansions:
    - Add versioning or description fields to track dataset updates.
    - Incorporate relationships to clinical or multi-omics datasets.
"""

from sqlalchemy import (
    Column,
    String,
    Integer,
    ForeignKey,
    JSON,
    Index,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

# Base class for SQLAlchemy ORM models
Base = declarative_base()


class CptacMetadata(Base):
    """
    Represents dataset-level metadata for CPTAC datasets.
    """
    __tablename__ = "cptac_metadata"

    # Primary Key: Unique identifier for each dataset
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Dataset-specific metadata
    data_type = Column(String, nullable=False)  # E.g., transcriptomics, proteomics
    source = Column(String, nullable=False)  # E.g., 'bcm', 'umich', 'washu'
    num_samples = Column(Integer, nullable=False)  # Number of samples in the dataset
    num_features = Column(Integer, nullable=False)  # Number of features in the dataset

    # Relationships
    features = relationship("CptacFeature", back_populates="dataset", cascade="all, delete-orphan")  # Feature-level metadata
    samples = relationship("CptacSample", back_populates="dataset", cascade="all, delete-orphan")  # Sample-level metadata

    # Constraints
    __table_args__ = (
        UniqueConstraint("data_type", "source", name="uq_data_type_source"),  # Ensure uniqueness
        Index("idx_data_type", "data_type"),  # Optimize querying by data type
        Index("idx_source", "source"),  # Optimize querying by source
    )

    def __repr__(self):
        return (
            f"<CptacMetadata(id={self.id}, data_type={self.data_type}, "
            f"source={self.source}, num_samples={self.num_samples}, num_features={self.num_features})>"
        )


class CptacFeature(Base):
    """
    Represents feature-level metadata for CPTAC datasets.
    """
    __tablename__ = "cptac_features"

    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Foreign Key: Links to dataset metadata
    metadata_id = Column(Integer, ForeignKey("cptac_metadata.id"), nullable=False)

    # Feature-specific details
    feature_name = Column(String, nullable=False)  # Name of the feature
    feature_attributes = Column(JSON, nullable=True)  # Additional attributes (e.g., gene ID, location)

    # Relationships
    dataset = relationship("CptacMetadata", back_populates="features")

    # Constraints
    __table_args__ = (
        UniqueConstraint("metadata_id", "feature_name", name="uq_metadata_feature"),  # Prevent duplicate features
        Index("idx_metadata_feature", "metadata_id", "feature_name"),  # Optimize lookups by dataset
    )

    def __repr__(self):
        return f"<CptacFeature(id={self.id}, feature_name={self.feature_name})>"


class CptacSample(Base):
    """
    Represents sample-level metadata for CPTAC datasets.
    """
    __tablename__ = "cptac_samples"

    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Foreign Key: Links to dataset metadata
    metadata_id = Column(Integer, ForeignKey("cptac_metadata.id"), nullable=False)

    # Sample-specific details
    sample_name = Column(String, nullable=False)  # Name of the sample
    sample_attributes = Column(JSON, nullable=True)  # Additional attributes (e.g., clinical data)

    # Relationships
    dataset = relationship("CptacMetadata", back_populates="samples")

    # Constraints
    __table_args__ = (
        UniqueConstraint("metadata_id", "sample_name", name="uq_metadata_sample"),  # Prevent duplicate samples
        Index("idx_metadata_sample", "metadata_id", "sample_name"),  # Optimize lookups by dataset
    )

    def __repr__(self):
        return f"<CptacSample(id={self.id}, sample_name={self.sample_name})>"
