from sqlalchemy import (
    Column,
    String,
    Integer,
    JSON,
    Index,
    UniqueConstraint,
)
from sqlalchemy.ext.declarative import declarative_base

# Base class for CPTAC data models
Base = declarative_base()


class BaseCptacDataModel(Base):
    """
    A base ORM model for CPTAC-related data tables.
    Provides a standardized schema for handling data types, sources,
    sample and feature counts, and metadata previews.
    """
    __abstract__ = True  # This class is abstract and not instantiated directly

    # Dataset-specific metadata
    id = Column(Integer, primary_key=True, autoincrement=True)  # Unique identifier
    data_type = Column(String, nullable=False)  # Type of data (e.g., transcriptomics, proteomics)
    source = Column(String, nullable=False)  # Source of the data (e.g., 'bcm', 'washu')
    num_samples = Column(Integer, nullable=False)  # Number of samples
    num_features = Column(Integer, nullable=False)  # Number of features
    description = Column(String, nullable=True)  # Optional description or notes

    # Previews for quick inspection
    preview_features = Column(JSON, nullable=True)  # Preview of feature names
    preview_samples = Column(JSON, nullable=True)  # Preview of sample names

    # Common constraints and indexing
    __table_args__ = (
        UniqueConstraint("data_type", "source", name="uq_data_type_source"),
        Index("idx_data_type", "data_type"),
        Index("idx_source", "source"),
    )

    def __repr__(self):
        return (
            f"<{self.__class__.__name__}(id={self.id}, data_type={self.data_type}, "
            f"source={self.source}, num_samples={self.num_samples}, "
            f"num_features={self.num_features}, description={self.description})>"
        )
