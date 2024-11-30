"""
CPTAC Metadata Schema for HNSC Omics Database.

Overview:
    - Captures detailed metadata for datasets from the CPTAC resource.
    - Tracks dataset-level information (data type, source, number of samples/features).
    - Includes previews of features and samples for quick inspection.

Key Features:
    - `CptacMetadata`: Main table describing datasets by type and source, including metadata previews.

Why This Design:
    - Simplifies schema by consolidating previews directly into the `CptacMetadata` table.
    - Optimized for querying datasets and quickly previewing their structure.
    - Reduces storage redundancy while maintaining metadata integrity.

Future Expansions:
    - Add versioning or description fields to track dataset updates.
    - Incorporate relationships to clinical or multi-omics datasets.
"""

from sqlalchemy import (
    Column,
    String,
    Integer,
    JSON,
    Index,
    UniqueConstraint,
)
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
    description = Column(String, nullable=True)  # Optional description or version identifier

    # Previews
    preview_features = Column(JSON, nullable=True)  # First 10 feature names
    preview_samples = Column(JSON, nullable=True)  # First 10 sample names

    # Constraints
    __table_args__ = (
        UniqueConstraint("data_type", "source", name="uq_data_type_source"),  # Ensure uniqueness
        Index("idx_data_type", "data_type"),  # Optimize querying by data type
        Index("idx_source", "source"),  # Optimize querying by source
    )

    def __repr__(self):
        return (
            f"<CptacMetadata(id={self.id}, data_type={self.data_type}, "
            f"source={self.source}, num_samples={self.num_samples}, num_features={self.num_features}, "
            f"description={self.description}, preview_features={self.preview_features}, "
            f"preview_samples={self.preview_samples})>"
        )
