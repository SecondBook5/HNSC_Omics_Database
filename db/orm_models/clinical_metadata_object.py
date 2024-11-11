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
"""

from sqlalchemy import Column, String, Integer, Boolean, CheckConstraint
from sqlalchemy.orm import relationship
from db.db_config import Base


class ClinicalMetadata(Base):
    """
    The ClinicalMetadata class defines the schema for storing patient-related information,
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
        samples (list): Relationship to ClinicalSample, linking samples to clinical data.
    """

    __tablename__ = 'clinical_metadata'

    # Primary Key for each clinical metadata record
    ClinicalID: str = Column(String, primary_key=True)

    # Patient-specific information
    PatientID: str = Column(String, nullable=False, index=True)  # Unique identifier per patient
    Age: int = Column(Integer, nullable=False)  # Age of the patient

    # Gender field constrained to 'M' or 'F' for data integrity
    Gender: str = Column(String, nullable=False)

    # Survival time (in months) and event observed (True if event such as death occurred)
    SurvivalTime: int = Column(Integer, nullable=True)
    EventObserved: bool = Column(Boolean, nullable=False)

    # Tumor stage and treatment details
    TumorStage: str = Column(String, nullable=True)
    Treatment: str = Column(String, nullable=True)

    # Establishes a one-to-many relationship to ClinicalSample
    samples = relationship("ClinicalSample", backref="clinical_metadata", cascade="all, delete-orphan")

    # Table constraints
    __table_args__ = (
        # Check constraint for Gender, enforcing 'M' or 'F' values only
        CheckConstraint("Gender IN ('M', 'F')", name="check_gender"),

        # Ensure EventObserved is a boolean (not strictly necessary in SQLAlchemy, but useful for clarity)
        CheckConstraint("EventObserved IN (0, 1)", name="check_event_observed"),
    )
