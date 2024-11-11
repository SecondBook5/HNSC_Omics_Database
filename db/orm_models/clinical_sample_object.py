# orm_models/clinical_sample_object.py

"""
File: clinical_sample_object.py
This file defines the ORM class for the ClinicalSample table in the HNSC Omics Database. This class
includes advanced features such as data integrity constraints, soft deletes, timestamps, and an
annotation relationship, allowing user-added insights on each sample.

Advanced Database Concepts:
- Foreign Key Constraints: Links ClinicalID to ClinicalMetadata with cascading delete for data integrity.
- Soft Deletes: A 'status' column enables logical deletes, marking records as inactive instead of removing them.
- Indexing: Composite index on ClinicalID and Platform for faster querying.
- Timestamps: Automatically tracks when records are created and updated.
- Annotation Relationship: Enables user annotations on sample records, adding interactivity to the database.
"""

from sqlalchemy import Column, String, ForeignKey, DateTime, Index, CheckConstraint, func
from sqlalchemy.orm import relationship, validates
from db.db_config import Base


class ClinicalSample(Base):
    """
    The ClinicalSample class defines the schema for storing sample-specific information with
    advanced features for data integrity, query performance, and user annotation capability.

    Attributes:
        SampleID (str): Unique identifier for each sample (Primary Key).
        ClinicalID (str): Foreign Key linking to ClinicalMetadata, with cascading delete.
        TissueType (str): Type of tissue (e.g., tumor, normal, blood), constrained to specific values.
        Platform (str): Analysis platform used (e.g., RNA-Seq, ATAC-Seq), indexed for faster queries.
        status (str): Soft delete status flag, marking records as 'active' or 'inactive'.
        created_at (DateTime): Timestamp indicating when the record was created.
        updated_at (DateTime): Timestamp indicating when the record was last updated.
        annotations (list): Relationship to the Annotation table, allowing user-provided notes.
    """

    __tablename__ = 'clinical_sample'

    # Primary key for each sample, serving as a unique identifier
    SampleID: str = Column(String, primary_key=True)

    # Foreign key linking to ClinicalMetadata, enabling cascading deletes if the linked record is removed
    ClinicalID: str = Column(String, ForeignKey('clinical_metadata.ClinicalID', ondelete="CASCADE"), nullable=False,
                             index=True)

    # Specifies the type of tissue sampled, with a constraint for allowed values to ensure data consistency
    TissueType: str = Column(String, nullable=False)

    # Specifies the analysis platform (e.g., RNA-Seq), indexed to support efficient queries
    Platform: str = Column(String, nullable=False, index=True)

    # Soft delete status column to mark records as 'active' or 'inactive', enabling logical deletion
    status: str = Column(String, default='active', nullable=False)

    # Timestamp indicating when the record was created, automatically set at creation
    created_at: DateTime = Column(DateTime, default=func.now())

    # Timestamp indicating when the record was last updated, automatically updated on modification
    updated_at: DateTime = Column(DateTime, default=func.now(), onupdate=func.now())

    # Establishes a relationship to the Annotation table, allowing for annotations on this sample
    annotations = relationship("Annotation", back_populates="clinical_sample")

    # Table arguments: composite index for efficient querying and check constraint on TissueType values
    __table_args__ = (
        # Composite index on ClinicalID and Platform for faster combined queries
        Index('ix_clinical_sample_clinical_platform', 'ClinicalID', 'Platform'),

        # Check constraint to restrict TissueType to allowed values ('tumor', 'normal', 'blood')
        CheckConstraint("TissueType IN ('tumor', 'normal', 'blood')", name="check_tissue_type"),
    )

    @validates('TissueType')
    def validate_tissue_type(self, key: str, tissue_type: str) -> str:
        """
        Validates the TissueType field to ensure it is one of the allowed values.

        Args:
            key (str): The name of the attribute being validated.
            tissue_type (str): The tissue type value to validate.

        Returns:
            str: The validated tissue type if it passes validation.

        Raises:
            ValueError: If tissue_type is not one of the allowed values.
        """
        valid_types = {'tumor', 'normal', 'blood'}
        if tissue_type not in valid_types:
            raise ValueError(f"Invalid tissue type: {tissue_type}")
        return tissue_type
