"""
This module defines the schema for storing GEO dataset metadata in PostgreSQL.

Overview:
    - Tracks metadata at the series and sample levels for biological datasets.
    - Includes fields for sample counts and data types in the series metadata.
    - Maintains a logging table to track download and processing statuses.

Key Features:
    - `GeoSeriesMetadata`: Captures series-level information like dataset title,
      sample count, data types, and related publication (PubMedID).
    - `GeoSampleMetadata`: Captures sample-level details such as organism, library strategy, and source type.
    - `GeoMetadataLog`: Tracks the success or failure of GEO metadata downloads and processing.

Why This Design:
    - Enables querying series by sample counts or data types.
    - Supports relationships between series and samples for efficient exploration.
    - Designed for flexibility and scalability as new fields are added.

Future Expansions:
    - New fields can be added to support advanced workflows like batch corrections.
"""

# Import necessary SQLAlchemy modules for schema definition
from sqlalchemy import Column, String, Text, Date, ForeignKey, Integer, Index
from config.db_config import Base
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB  # Use JSONB instead of JSON




class GeoSeriesMetadata(Base):
    """
    Represents metadata for GEO dataset series.

    This table stores high-level information about GEO dataset series, including
    descriptive details, publication information, and the number of samples and
    data types available in the series.
    """
    __tablename__ = 'geo_series_metadata'  # Name of the database table

    # Primary Key: Unique identifier for the series
    SeriesID = Column(String, primary_key=True, nullable=False)

    # Descriptive fields for the dataset series
    Title = Column(String, nullable=True)  # Title of the dataset series
    SubmissionDate = Column(Date, nullable=True)  # Original submission date
    LastUpdateDate = Column(Date, nullable=True)  # Last update date
    PubMedID = Column(String, nullable=True)  # PubMed ID of related publication
    Summary = Column(Text, nullable=True)  # Summary of the dataset
    OverallDesign = Column(Text, nullable=True)  # Description of experimental design

    # Additional fields
    RelatedDatasets = Column(JSONB, nullable=True)  # Stores relationships to other datasets
    SupplementaryData = Column(Text, nullable=True)  # Supplementary files or information

    # New fields
    SampleCount = Column(Integer, nullable=True, default=0)  # Number of samples in the series
    DataTypes = Column(JSONB, nullable=True)  # List or JSONB of data types (e.g., ["RNA-Seq", "ATAC-Seq"])

    # Relationship to sample metadata
    Samples = relationship("GeoSampleMetadata", back_populates="Series")

    # Index for efficient querying by SeriesID
    __table_args__ = (
        Index('idx_series_id', 'SeriesID'),  # Index for SeriesID column
    )

    def __repr__(self):
        """
        Provides a string representation of the object for debugging.
        """
        return f"<GeoSeriesMetadata(SeriesID={self.SeriesID}, SampleCount={self.SampleCount}, DataTypes={self.DataTypes})>"


class GeoSampleMetadata(Base):
    """
    Represents metadata for GEO dataset samples.

    This table captures detailed metadata for individual samples within a dataset
    series, including biological, experimental, and data processing details.
    """
    __tablename__ = 'geo_sample_metadata'  # Name of the database table

    # Primary Key: Unique identifier for each sample
    SampleID = Column(String, primary_key=True, nullable=False)

    # Foreign Key: Links the sample to its series
    SeriesID = Column(String, ForeignKey('geo_series_metadata.SeriesID'), nullable=False)

    # Descriptive fields for the sample
    Title = Column(String, nullable=True)  # Title or name of the sample
    SubmissionDate = Column(Date, nullable=True)  # Submission date of the sample
    ReleaseDate = Column(Date, nullable=True)  # Public release date of the sample
    LastUpdateDate = Column(Date, nullable=True)  # Last update date of the sample

    # Biological and experimental details
    Organism = Column(String, nullable=True)  # Organism name (e.g., "Homo sapiens")
    Source = Column(Text, nullable=True)  # Source of the sample (e.g., "tissue", "cell line")
    Molecule = Column(Text, nullable=True)  # Molecule studied (e.g., "RNA", "DNA")
    Characteristics = Column(JSONB, nullable=True)  # JSONB field for characteristics (e.g., {"age": 25, "sex": "male"})
    ExtractProtocol = Column(Text, nullable=True)  # Extraction protocol used

    # Data processing and instrumentation details
    DataProcessing = Column(Text, nullable=True)  # Description of data processing steps
    PlatformRef = Column(String, nullable=True)  # Platform reference (e.g., "Illumina")
    LibraryStrategy = Column(Text, nullable=True)  # Library strategy (e.g., "RNA-Seq")
    LibrarySource = Column(Text, nullable=True)  # Library source (e.g., "transcriptomic")
    LibrarySelection = Column(Text, nullable=True)  # Library selection method (e.g., "cDNA")
    InstrumentModel = Column(Text, nullable=True)  # Instrument model used (e.g., "HiSeq 4000")

    # Supplementary and additional details
    SupplementaryData = Column(Text, nullable=True)
    RelatedDatasets = Column(JSONB, nullable=True)
    HybridizationProtocol = Column(Text, nullable=True)
    ScanProtocol = Column(Text, nullable=True)
    Label = Column(Text, nullable=True)
    TreatmentProtocol = Column(Text, nullable=True)
    GrowthProtocol = Column(Text, nullable=True)
    LabelProtocol = Column(Text, nullable=True)

    # Relationship to series
    Series = relationship("GeoSeriesMetadata", back_populates="Samples")

    # Index for efficient querying by SeriesID
    __table_args__ = (
        Index('idx_sample_series_id', 'SeriesID'),  # Index for SeriesID column
    )

    def __repr__(self):
        """
        Provides a string representation of the object for debugging.
        """
        return f"<GeoSampleMetadata(SampleID={self.SampleID}, SeriesID={self.SeriesID})>"


class GeoMetadataLog(Base):
    """
    Represents logs for GEO metadata operations.

    This table tracks the status of operations performed on GEO metadata,
    such as downloads, processing, and errors.
    """
    __tablename__ = 'geo_metadata_log'  # Name of the database table

    # Primary Key: Unique identifier for each log entry
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Fields to track the status and details of operations
    GeoID = Column(String, nullable=False, unique=True)  # GEO Series or Sample ID
    Status = Column(String, nullable=False)  # Status of the operation (e.g., 'downloaded', 'processed')
    Message = Column(Text, nullable=True)  # Detailed message or error description
    FileNames = Column(JSONB, nullable=True)  # List of files associated with the operation
    Timestamp = Column(Date, nullable=False)  # Timestamp of the log entry

    # Index for efficient querying by GeoID
    __table_args__ = (
        Index('idx_geo_id', 'GeoID'),  # Index for GeoID
    )

    def __repr__(self):
        """
        Provides a string representation of the object for debugging.
        """
        return f"<GeoMetadataLog(GeoID={self.GeoID}, Status={self.Status})>"
