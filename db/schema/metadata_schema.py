"""
This module defines the schema for storing GEO dataset metadata in PostgreSQL.

Overview:
    - Tracks metadata at the series and sample levels for biological datasets.
    - Includes fields for sample counts and data types in the series metadata.
    - Maintains a logging table to track download and processing statuses.

Key Features:
    - `DatasetSeriesMetadata`: Captures series-level information like dataset title,
      sample count, data types, and related publication (PubMedID).
    - `DatasetSampleMetadata`: Captures sample-level details such as organism, library strategy, and source type.
    - `GeoMetadataLog`: Tracks the success or failure of GEO metadata downloads and processing.

Why This Design:
    - Enables querying series by sample counts or data types.
    - Supports relationships between series and samples for efficient exploration.
    - Designed for flexibility and scalability as new fields are added.

Future Expansions:
    - New fields can be added to support advanced workflows like batch corrections.
"""

# Import necessary SQLAlchemy modules
from sqlalchemy import Column, String, Text, Date, JSON, ForeignKey, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

# Base class for all ORM models
Base = declarative_base()

class DatasetSeriesMetadata(Base):
    """
    Represents metadata for GEO dataset series.
    """
    __tablename__ = 'dataset_series_metadata'

    # Primary Key: Unique identifier for the series
    SeriesID = Column(String, primary_key=True, nullable=False)

    # Descriptive fields
    Title = Column(String, nullable=True)
    SubmissionDate = Column(Date, nullable=True)
    LastUpdateDate = Column(Date, nullable=True)
    PubMedID = Column(String, nullable=True)
    Summary = Column(Text, nullable=True)
    OverallDesign = Column(Text, nullable=True)

    # Additional fields
    RelatedDatasets = Column(JSON, nullable=True)  # Stores relationships to other datasets
    SupplementaryData = Column(Text, nullable=True)  # Supplementary files or information

    # New fields
    SampleCount = Column(Integer, nullable=True, default=0)  # Number of samples in the series
    DataTypes = Column(JSON, nullable=True)  # List or JSON of data types (e.g., ["RNA-Seq", "ATAC-Seq"])

    # Relationship to samples
    Samples = relationship("DatasetSampleMetadata", back_populates="Series")

    def __repr__(self):
        return f"<DatasetSeriesMetadata(SeriesID={self.SeriesID}, SampleCount={self.SampleCount}, DataTypes={self.DataTypes})>"

class DatasetSampleMetadata(Base):
    """
    Represents metadata for GEO dataset samples.
    """
    __tablename__ = 'dataset_sample_metadata'

    # Primary Key
    SampleID = Column(String, primary_key=True, nullable=False)

    # Foreign Key: Links to series
    SeriesID = Column(String, ForeignKey('dataset_series_metadata.SeriesID'), nullable=False)

    # Descriptive fields
    Title = Column(String, nullable=True)
    SubmissionDate = Column(Date, nullable=True)
    ReleaseDate = Column(Date, nullable=True)
    LastUpdateDate = Column(Date, nullable=True)

    # Biological and experimental details
    Organism = Column(String, nullable=True)
    Source = Column(Text, nullable=True)
    Molecule = Column(Text, nullable=True)
    Characteristics = Column(JSON, nullable=True)
    ExtractProtocol = Column(Text, nullable=True)

    # Data processing and instrumentation
    DataProcessing = Column(Text, nullable=True)
    PlatformRef = Column(String, nullable=True)
    LibraryStrategy = Column(Text, nullable=True)
    LibrarySource = Column(Text, nullable=True)
    LibrarySelection = Column(Text, nullable=True)
    InstrumentModel = Column(Text, nullable=True)

    # Supplementary and additional details
    SupplementaryData = Column(Text, nullable=True)
    RelatedDatasets = Column(JSON, nullable=True)
    HybridizationProtocol = Column(Text, nullable=True)
    ScanProtocol = Column(Text, nullable=True)
    Label = Column(Text, nullable=True)
    TreatmentProtocol = Column(Text, nullable=True)
    GrowthProtocol = Column(Text, nullable=True)
    LabelProtocol = Column(Text, nullable=True)

    # Relationship to series
    Series = relationship("DatasetSeriesMetadata", back_populates="Samples")

    def __repr__(self):
        return f"<DatasetSampleMetadata(SampleID={self.SampleID}, SeriesID={self.SeriesID})>"


class GeoMetadataLog(Base):
    """
    Represents logs for GEO metadata operations.
    """
    __tablename__ = 'geo_metadata_log'

    # Primary Key: Unique log entry identifier
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Fields to track the status and details of operations
    GeoID = Column(String, nullable=False, unique=True)  # GEO Series or Sample ID
    Status = Column(String, nullable=False)  # e.g., 'downloaded', 'processed', etc.
    Message = Column(Text, nullable=True)  # Detailed message or description
    FileNames = Column(JSON, nullable=True)  # List of file names associated with the GEO ID
    Timestamp = Column(Date, nullable=False)  # Timestamp for the log entry

    def __repr__(self):
        return f"<GeoMetadataLog(GeoID={self.GeoID}, Status={self.Status})>"
