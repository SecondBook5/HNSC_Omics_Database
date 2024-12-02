"""
CPTAC Metadata Schema for HNSC Omics Database.

Overview:
    - Captures detailed metadata for datasets from the CPTAC resource.
    - Tracks dataset-level information (data type, source, number of samples/features).
    - Includes previews of features and samples for quick inspection.
    - Consolidates grouped column metadata into a compact JSON format to minimize row count.

Key Features:
    - `CptacMetadata`: Main table describing datasets by type and source, including metadata previews.
    - `CptacColumns`: Stores grouped column metadata as JSON for each dataset.

Why This Design:
    - Reduces storage redundancy by consolidating grouped column data in JSON format.
    - Optimized for querying datasets and quickly previewing their structure.
    - Efficiently supports datasets with thousands of columns while maintaining metadata integrity.

Benefits:
    - Compact storage reduces the number of rows in the database.
    - JSON format allows for flexible additions and handling of nested metadata structures.
    - Prepares data for efficient use in memory-mapped data structures like hash maps.

Future Expansions:
    - Add versioning or description fields to track dataset updates.
    - Incorporate relationships to clinical or multi-omics datasets.
    - Extend column-level metadata to include nested metadata for complex data hierarchies.
"""

from sqlalchemy import (
    Column,
    String,
    Integer,
    JSON,
    ForeignKey,
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
    description = Column(String, nullable=True)  # Optional description or version identifier

    # Previews
    preview_features = Column(JSON, nullable=True)  # First 10 feature names
    preview_samples = Column(JSON, nullable=True)  # First 10 sample names

    # Relationship to CptacColumns
    column_metadata = relationship("CptacColumns", back_populates="dataset", cascade="all, delete-orphan")

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


class CptacColumns(Base):
    """
    Represents grouped column data stored as JSON for patient-related datasets.
    """
    __tablename__ = "cptac_columns"

    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Foreign Key: Link to the parent dataset metadata
    dataset_id = Column(Integer, ForeignKey("cptac_metadata.id"), nullable=False)

    # Grouped column data
    column_data = Column(JSON, nullable=False, doc="JSON object containing column groups.")  # JSON object containing column groups
    data_type = Column(String, nullable=False, doc="Data type (e.g., proteomics, clinical).")  # Data type (e.g., proteomics, clinical)
    source = Column(String, nullable=False, doc="Source of the dataset (e.g., 'umich', 'bcm').")  # New field: Source of the data
    description = Column(String, nullable=True, doc="Optional description of the dataset.")  # Optional description of the dataset

    # Relationships
    dataset = relationship("CptacMetadata", back_populates="column_metadata")

    __table_args__ = (
        Index("idx_dataset_id", "dataset_id"),  # Optimize queries by dataset ID
        Index("idx_data_type", "data_type"),  # Optimize queries by data type
        Index("idx_source", "source"),  # Optimize queries by source
    )

    def __repr__(self):
        return (
            f"<CptacColumns(id={self.id}, dataset_id={self.dataset_id}, "
            f"data_type={self.data_type}, source={self.source}, column_data={self.column_data}, "
            f"description={self.description})>"
        )
