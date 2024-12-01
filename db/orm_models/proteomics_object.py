# File: db/orm_models/proteomics_object.py

from sqlalchemy import Column, String, Integer, Float, ForeignKey, Index, UniqueConstraint
from sqlalchemy.orm import relationship
from db.schema.base_cptac_data_model import BaseCptacDataModel


class Proteomics(BaseCptacDataModel):
    """
    ORM model for Proteomics data.
    Represents patient-specific protein quantification linked to MappingTable and metadata.
    Extends BaseCptacDataModel for standardized metadata integration.
    """
    __tablename__ = "proteomics"

    # Proteomics-Specific Fields
    patient_id = Column(String, nullable=False, index=True, doc="Unique patient identifier (e.g., C3L-00006).")
    protein_name = Column(String, nullable=False, index=True, doc="Human-readable protein name (e.g., ARF5).")
    ensembl_id = Column(String, ForeignKey('mapping_table.ensembl_protein_id'), nullable=False, index=True,
                        doc="Ensembl protein ID (e.g., ENSP00000000233.5).")
    quantification = Column(Float, nullable=False, doc=(
        "Quantification value for the protein in the patient sample. "
        "For detailed information on the quantification methodology, refer to the CPTAC protocol: "
        "'A Broad-Scale Quantitative Proteomic Analysis' (https://doi.org/10.1038/s41596-018-0006-9)."
    ))

    # Relationships
    mapping = relationship("MappingTable", backref="proteomics_entries", uselist=False,
                           doc="Reference to MappingTable for additional metadata.")
    dataset_metadata = relationship("CptacMetadata", backref="proteomics_data", uselist=False,
                                     doc="Reference to dataset metadata for this table.")

    # Table-Level Constraints
    __table_args__ = (
        UniqueConstraint("patient_id", "protein_name", name="uq_patient_protein"),
        Index("idx_protein_name", "protein_name"),
        Index("idx_patient_protein", "patient_id", "protein_name")
    )

    def __repr__(self):
        return (f"<Proteomics(patient_id={self.patient_id}, protein_name={self.protein_name}, "
                f"ensembl_id={self.ensembl_id}, quantification={self.quantification})>")
