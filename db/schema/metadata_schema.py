from sqlalchemy import Column, String, Integer, Date, Text, ForeignKey, Index, UniqueConstraint, Boolean
from sqlalchemy.orm import relationship
from db.db_config import Base


class DatasetSeriesMetadata(Base):
    """
    DatasetSeriesMetadata captures series-level metadata for each dataset.
    Includes title, submission dates, contributors, summary, and more.

    Attributes:
        SeriesID (str): Primary key, unique identifier for each dataset series.
        Title (str): Title of the study or dataset.
        GEOAccession (str): GEO series accession number (e.g., GSE195832).
        Status (str): Publication status (e.g., Public).
        SubmissionDate (Date): Date of submission.
        LastUpdateDate (Date): Last updated date.
        PubMedID (int): Associated PubMed ID for published studies.
        Summary (str): Summary of objectives and findings.
        OverallDesign (str): Overview of the experimental design.
        SeriesType (str): Type of analysis (e.g., "Expression profiling").
        PlatformID (str): Platform used (e.g., GPL24676).
        Organism (str): Organism studied.
        Contributors (str): List of contributors.
        Samples (list): Relationship to DatasetSampleMetadata table.
    """
    __tablename__ = 'dataset_series_metadata'

    SeriesID = Column(String, primary_key=True)
    Title = Column(String, nullable=False)
    GEOAccession = Column(String, unique=True, nullable=False)
    Status = Column(String, nullable=False)
    SubmissionDate = Column(Date, nullable=False)
    LastUpdateDate = Column(Date, nullable=True)
    PubMedID = Column(Integer, nullable=True)
    Summary = Column(Text, nullable=True)
    OverallDesign = Column(Text, nullable=True)
    SeriesType = Column(String, nullable=False)
    PlatformID = Column(String, nullable=False)
    Organism = Column(String, nullable=False)
    Contributors = Column(Text, nullable=True)

    # Relationships
    Samples = relationship("DatasetSampleMetadata", back_populates="Series")

    __table_args__ = (
        UniqueConstraint('GEOAccession', name='uq_geo_accession'),
    )


class DatasetSampleMetadata(Base):
    """
    DatasetSampleMetadata captures sample-level metadata, covering clinical,
    demographic, and technical characteristics.

    Attributes:
        SampleID (str): Primary key, unique identifier.
        GEOAccession (str): GEO sample accession number (e.g., GSM123456).
        SeriesID (str): Foreign key linking to DatasetSeriesMetadata.
        Title (str): Description of the sample.
        Status (str): Publication status.
        SubmissionDate (Date): Date of submission.
        LastUpdateDate (Date): Last update date.
        SampleType (str): RNA, DNA type, etc.
        Organism (str): Organism.
        PlatformID (str): Platform used.
        Source (str): Biological source or material.
        Characteristics (str): Tissue type or descriptors.
        Protocol (str): Sample preparation protocols.
        DataProcessing (str): Description of data processing.
        SupplementaryFiles (str): URLs or paths to supplementary files.
        Age (int): Patient age.
        Gender (str): Patient gender.
        Race (str): Patient race.
        Smoking (str): Smoking history.
        AlcoholUse (str): Alcohol consumption status.
        ClinicalStage (str): Clinical stage of disease.
    """
    __tablename__ = 'dataset_sample_metadata'

    SampleID = Column(String, primary_key=True)
    GEOAccession = Column(String, unique=True, nullable=False)
    SeriesID = Column(String, ForeignKey('dataset_series_metadata.SeriesID'), nullable=False)
    Title = Column(String, nullable=True)
    Status = Column(String, nullable=False)
    SubmissionDate = Column(Date, nullable=True)
    LastUpdateDate = Column(Date, nullable=True)
    SampleType = Column(String, nullable=True)
    Organism = Column(String, nullable=True)
    PlatformID = Column(String, nullable=True)
    Source = Column(Text, nullable=True)
    Characteristics = Column(Text, nullable=True)
    Protocol = Column(Text, nullable=True)
    DataProcessing = Column(Text, nullable=True)
    SupplementaryFiles = Column(Text, nullable=True)

    # Clinical and demographic fields for added metadata support
    Age = Column(Integer, nullable=True)
    Gender = Column(String, nullable=True)
    Race = Column(String, nullable=True)
    Smoking = Column(String, nullable=True)
    AlcoholUse = Column(String, nullable=True)
    ClinicalStage = Column(String, nullable=True)

    # Relationships
    Series = relationship("DatasetSeriesMetadata", back_populates="Samples")

    __table_args__ = (
        Index('ix_geo_sample_accession', 'GEOAccession'),
    )
