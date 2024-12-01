# File: db/orm_models/phosphoproteomics_object.py

from sqlalchemy import Column, String, Float, ForeignKey, Index, UniqueConstraint
from sqlalchemy.orm import relationship
from db.schema.base_cptac_data_model import BaseCptacDataModel


class Phosphoproteomics(BaseCptacDataModel):
    """
    ORM model for Phosphoproteomics data.
    Represents patient-specific phosphoprotein quantification linked to MappingTable and metadata.
    Extends BaseCptacDataModel for standardized metadata integration.
    """
    __tablename__ = "phosphoproteomics"

    # Phosphoproteomics-Specific Fields
    patient_id = Column(String, nullable=False, index=True, doc="Unique patient identifier (e.g., C3L-00006).")
    phosphoprotein_name = Column(String, nullable=False, index=True, doc="Human-readable phosphoprotein name (e.g., ARF5_S192).")
    phosphorylation_site = Column(String, nullable=False, doc="The specific phosphorylation site (e.g., 'S192', 'Y705').")
    ensembl_id = Column(String, ForeignKey('mapping_table.ensembl_protein_id'), nullable=False, index=True,
                        doc="Ensembl protein ID (e.g., ENSP00000000233.5).")
    quantification = Column(Float, nullable=False, doc="Quantification value for the phosphoprotein in the patient sample.")

    # Relationships
    mapping = relationship("MappingTable", backref="phosphoproteomics_entries", uselist=False,
                           doc="Reference to MappingTable for additional metadata.")
    dataset_metadata = relationship("CptacMetadata", backref="phosphoproteomics_data", uselist=False,
                                     doc="Reference to dataset metadata for this table.")

    # Table-Level Constraints
    __table_args__ = (
        UniqueConstraint("patient_id", "phosphoprotein_name", "phosphorylation_site", name="uq_patient_phosphoprotein_site"),
        Index("idx_phosphoprotein_name", "phosphoprotein_name"),
        Index("idx_patient_phosphoprotein_site", "patient_id", "phosphoprotein_name", "phosphorylation_site")
    )

    def __repr__(self):
        return (f"<Phosphoproteomics(patient_id={self.patient_id}, phosphoprotein_name={self.phosphoprotein_name}, "
                f"phosphorylation_site={self.phosphorylation_site}, ensembl_id={self.ensembl_id}, "
                f"quantification={self.quantification})>")
