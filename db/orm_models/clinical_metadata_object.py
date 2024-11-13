# orm_models/clinical_metadata_object.py

"""
File: clinical_metadata_object.py
This file defines the ORM class for the ClinicalMetadata table in the HNSC Omics Database.
The ClinicalMetadata model stores patient-related data and establishes a relationship with
ClinicalSample, linking patient data to sample data.

Advanced Database Concepts:
- Foreign Key Constraints: Links ClinicalID in ClinicalSample to ClinicalMetadata for data integrity.
- Indexing: Index on frequently queried fields like PatientID and TumorStage.
- Data Constraints: Ensures consistency with checks on fields like Gender and EventObserved.
- Data Source Tracking: Fields added to specify the original data source and dataset for each record.
"""

from sqlalchemy import Column, String, Integer, Boolean, CheckConstraint
from sqlalchemy.orm import relationship
from db.db_config import Base

class ClinicalMetadata(Base):
    """
    The ClinicalMetadata class defines the diagrams for storing patient-related information,
    with constraints to maintain data integrity and a relationship to ClinicalSample.

    Attributes:
        ClinicalID (str): Unique identifier for each clinical record (Primary Key).
        PatientID (str): Identifier for each patient, indexed for faster querying.
        Age (int): Patient age, validated to ensure it's within a logical range.
        Gender (str): Patient gender, constrained to 'M' or 'F'.
        SurvivalTime (int): Survival time in months.
        EventObserved (bool): True if an event (e.g., death) was observed, False otherwise.
        TumorStage (str): Clinical stage of the tumor.
        Treatment (str): Description of the treatment received.
        DataSource (str): Name or source of the dataset (e.g., GEO, CPTAC).
        Dataset (str): Specific dataset identifier (e.g., GSE114446, PDC000221).
        samples (list): Relationship to ClinicalSample, linking samples to clinical data.
    """

    # Specifies the table name for this ORM class in the database
    __tablename__ = 'clinical_metadata'

    # Defines the primary key for each clinical metadata record
    ClinicalID: str = Column(String, primary_key=True)

    # Defines a unique identifier for each patient and creates an index for optimized queries
    PatientID: str = Column(String, nullable=False, index=True)

    # Defines the age of the patient; ensures a non-negative integer for logical validity
    Age: int = Column(Integer, nullable=False)

    # Gender of the patient, constrained to specific values ('M' or 'F') for data consistency
    Gender: str = Column(String, nullable=False)

    # Survival time in months; can be null for cases without this information
    SurvivalTime: int = Column(Integer, nullable=True)

    # Boolean flag to indicate if an event, such as death, was observed
    EventObserved: bool = Column(Boolean, nullable=False)

    # Clinical stage of the tumor; nullable, as it may not apply to all patients
    TumorStage: str = Column(String, nullable=True)

    # Treatment description providing context on the treatment received
    Treatment: str = Column(String, nullable=True)

    # Specifies the original data source (e.g., GEO, CPTAC) for tracking data provenance
    DataSource: str = Column(String, nullable=False)

    # Indicates the specific dataset within the source (e.g., GSE114446, PDC000221)
    Dataset: str = Column(String, nullable=False)

    # Establishes a one-to-many relationship with ClinicalSample, linking clinical metadata to multiple samples
    samples = relationship("ClinicalSample", backref="clinical_metadata", cascade="all, delete-orphan")

    # Table constraints ensure data integrity with checks on fields like Gender and EventObserved
    __table_args__ = (
        # Check constraint to limit Gender values to 'M' or 'F' for data consistency
        CheckConstraint("Gender IN ('M', 'F')", name="check_gender"),

        # Check constraint to enforce that EventObserved is a boolean value
        CheckConstraint("EventObserved IN (0, 1)", name="check_event_observed"),
    )