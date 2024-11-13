"""
This module defines the ORM (Object Relational Mapping) models for storing metadata related to biological datasets
within a database. It utilizes SQLAlchemy to map Python classes to tables in a relational database, allowing for
structured storage and easy querying of dataset metadata, particularly for GEO datasets.

Classes:
    - DatasetSeriesMetadata: Represents series-level metadata for each dataset series, including fields such as title,
      submission dates, and experimental design. This table captures information about the dataset as a whole.

    - DatasetSampleMetadata: Represents sample-level metadata within a series, covering clinical, demographic,
      and experimental details for each individual sample. This table is linked to DatasetSeriesMetadata through
      a foreign key, allowing samples to be organized under each dataset series.

Key Concepts:
    - The 'Series' class defines high-level dataset information (e.g., GEO Series), while the 'Sample' class provides
      details for each sample within a dataset (e.g., GEO Samples). These models provide a normalized structure that
      supports efficient querying and retrieval of both series-level and sample-level metadata.
    - SQLAlchemy relationships are used to link samples to their series, enabling seamless access from series to samples
      and vice versa.
    - This setup supports a research environment where datasets from GEO or other omics sources are curated, stored,
      and analyzed, with a focus on clinical and demographic data relevant to cancer research.

Configuration:
    - The module relies on 'db.db_config' for base configuration, where the SQLAlchemy Base class is imported. The
      database engine and session configurations should be set up in 'db.db_config'.
"""

from sqlalchemy import Column, String, Integer, Date, Text, ForeignKey, Index, UniqueConstraint
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
        DatabaseName (str): Name of the database (e.g., GEO).
        DatabasePublicID (str): Public ID for the database.
        DatabaseOrganization (str): Organization responsible for the database.
        DatabaseWebLink (str): Web link to the database.
        DatabaseEmail (str): Contact email for the database.
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
    DatabaseName = Column(String, nullable=True)
    DatabasePublicID = Column(String, nullable=True)
    DatabaseOrganization = Column(String, nullable=True)
    DatabaseWebLink = Column(String, nullable=True)
    DatabaseEmail = Column(String, nullable=True)

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
        HPVIntegration (str): HPV integration status.
        HPVType (str): HPV type.
        TStage (str): Tumor stage.
        NStage (str): Node stage.
        AnatomicTNMStage (str): Anatomic TNM stage.
        TimeToDeathOrFollowUp (int): Time to death or follow-up in months.
        DiseaseStatusLastFollowup (str): Disease status at last clinical follow-up.
        PrimarySurgicalTherapy (str): If primary surgery was performed.
        PrimaryChemotherapy (str): If primary chemotherapy was administered.
        PrimaryRadiationTherapy (str): If primary radiation therapy was administered.
        Antibody (str): Antibody used for ChIP-Seq experiments.
        LibraryStrategy (str): Strategy for library preparation.
        LibrarySource(str): Omics type
        LibrarySelection(str): Region of interest specificity
        SupplementaryDataType (str): Type of supplementary data file.
        BioSampleRelation (str): External link to BioSample resource.
        SupplementaryFilesFormatAndContent (str): Details of supplementary files.
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
    Age = Column(Integer, nullable=True)
    Gender = Column(String, nullable=True)
    Race = Column(String, nullable=True)
    Smoking = Column(String, nullable=True)
    AlcoholUse = Column(String, nullable=True)
    ClinicalStage = Column(String, nullable=True)
    HPVIntegration = Column(String, nullable=True)
    HPVType = Column(String, nullable=True)
    TStage = Column(String, nullable=True)
    NStage = Column(String, nullable=True)
    AnatomicTNMStage = Column(String, nullable=True)
    TimeToDeathOrFollowUp = Column(Integer, nullable=True)
    DiseaseStatusLastFollowup = Column(String, nullable=True)
    PrimarySurgicalTherapy = Column(String, nullable=True)
    PrimaryChemotherapy = Column(String, nullable=True)
    PrimaryRadiationTherapy = Column(String, nullable=True)
    Antibody = Column(String, nullable=True)
    LibraryStrategy = Column(String, nullable=True)
    LibrarySource = Column(String, nullable=True)
    LibrarySelection = Column(String, nullable=True)
    SupplementaryDataType = Column(String, nullable=True)
    BioSampleRelation = Column(String, nullable=True)
    SupplementaryFilesFormatAndContent = Column(Text, nullable=True)

    # Relationships
    Series = relationship("DatasetSeriesMetadata", back_populates="Samples")

    __table_args__ = (
        Index('ix_geo_sample_accession', 'GEOAccession'),
    )
