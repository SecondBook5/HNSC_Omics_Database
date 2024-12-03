from sqlalchemy import Column, Integer, String, Float, JSON, ForeignKey, UniqueConstraint, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from config.db_config import Base


class BaseOmicsData(Base):
    """
    Base ORM model for all omics data.
    Stores shared fields across all omics types.
    """
    __tablename__ = "base_omics_data"

    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Core Fields
    sample_id = Column(String, nullable=False, index=True, doc="Unique sample identifier.")
    data_type = Column(String, nullable=False, index=True, doc="Type of data (e.g., proteomics, transcriptomics).")
    quantification = Column(JSONB, nullable=False, doc="JSONB dictionary of quantification values by data source.")
    description = Column(String, nullable=True, doc="Additional description or metadata about the dataset.")

    # Relationships for child tables
    child_type = Column(String, nullable=True, doc="Subtype of the data (e.g., 'phosphoproteomics').")
    __mapper_args__ = {"polymorphic_identity": "base_omics_data", "polymorphic_on": child_type}

    # Index on polymorphic child_type for filtering
    __table_args__ = (
        Index('ix_base_omics_data_child_type', 'child_type'),
    )

    def __repr__(self):
        return f"<BaseOmicsData(id={self.id}, sample_id={self.sample_id}, data_type={self.data_type}, description={self.description})>"


class ProteomicsData(BaseOmicsData):
    """
    ORM model for proteomics data.
    Extends the base omics table for proteomics-specific fields.
    """
    __tablename__ = "proteomics_data"

    # Foreign Key to the base table
    id = Column(Integer, ForeignKey("base_omics_data.id"), primary_key=True)

    # Proteomics-Specific Fields
    protein_name = Column(String, nullable=False, index=True, doc="Name of the protein.")
    ensembl_gene_id = Column(String, nullable=True, index=True, doc="Ensembl Gene ID.")
    ensembl_protein_id = Column(String, nullable=True, index=True, doc="Ensembl Protein ID.")

    # Mapper Relationship
    mapper_id = Column(Integer, ForeignKey("mapping_table.id", ondelete="CASCADE"), nullable=False,
                       doc="Foreign key to the Mapping Table.")

    # Constraints
    __table_args__ = (
        UniqueConstraint("sample_id", "protein_name", name="uq_sample_protein"),
        Index('ix_proteomics_data_sample_protein', 'sample_id', 'protein_name')
    )
    __mapper_args__ = {"polymorphic_identity": "proteomics_data"}

    def __repr__(self):
        return (
            f"<ProteomicsData(id={self.id}, sample_id={self.sample_id}, protein_name={self.protein_name}, "
            f"ensembl_gene_id={self.ensembl_gene_id}, ensembl_protein_id={self.ensembl_protein_id}, "
            f"quantification={self.quantification}, description={self.description})>"
        )


class PhosphoproteomicsData(BaseOmicsData):
    """
    ORM model for phosphoproteomics data.
    Extends the base omics table for phosphoproteomics-specific fields.
    """
    __tablename__ = "phosphoproteomics_data"

    # Foreign Key to the base table
    id = Column(Integer, ForeignKey("base_omics_data.id"), primary_key=True)

    # Phosphoproteomics-Specific Fields
    phosphoprotein_name = Column(String, nullable=False, index=True, doc="Name of the phosphoprotein.")
    phosphorylation_site = Column(String, nullable=False, doc="The specific phosphorylation site (e.g., 'S267').")
    sequence_window = Column(String, nullable=True, doc="Phosphorylation sequence window.")
    ensembl_gene_id = Column(String, nullable=True, index=True, doc="Ensembl Gene ID, if available.")
    ensembl_protein_id = Column(String, nullable=True, index=True, doc="Ensembl Protein ID, if available.")

    # Mapper Relationship
    mapper_id = Column(Integer, ForeignKey("mapping_table.id", ondelete="CASCADE"), nullable=False,
                       doc="Foreign key to the Mapping Table.")

    # Constraints
    __table_args__ = (
        UniqueConstraint("sample_id", "phosphoprotein_name", "phosphorylation_site",
                         name="uq_sample_phosphoprotein_site"),
        Index('ix_phosphoproteomics_data_sample_protein_site', 'sample_id', 'phosphoprotein_name',
              'phosphorylation_site')
    )
    __mapper_args__ = {"polymorphic_identity": "phosphoproteomics_data"}

    def __repr__(self):
        return (
            f"<PhosphoproteomicsData(id={self.id}, sample_id={self.sample_id}, phosphoprotein_name={self.phosphoprotein_name}, "
            f"phosphorylation_site={self.phosphorylation_site}, ensembl_gene_id={self.ensembl_gene_id}, "
            f"ensembl_protein_id={self.ensembl_protein_id}, quantification={self.quantification}, description={self.description})>"
        )


class TranscriptomicsData(BaseOmicsData):
    """
    ORM model for transcriptomics data.
    Extends the base omics table for transcriptomics-specific fields.
    """
    __tablename__ = "transcriptomics_data"

    # Foreign Key to the base table
    id = Column(Integer, ForeignKey("base_omics_data.id"), primary_key=True)

    # Transcriptomics-Specific Fields
    transcript_name = Column(String, nullable=False, index=True, doc="Name of the transcript.")
    ensembl_transcript_id = Column(String, nullable=True, index=True, doc="Ensembl Transcript ID.")
    ensembl_gene_id = Column(String, nullable=True, index=True, doc="Ensembl Gene ID, if available.")

    # Mapper Relationship
    mapper_id = Column(Integer, ForeignKey("mapping_table.id", ondelete="CASCADE"), nullable=False,
                       doc="Foreign key to the Mapping Table.")

    __mapper_args__ = {"polymorphic_identity": "transcriptomics_data"}
    __table_args__ = (
        Index('ix_transcriptomics_data_sample_transcript', 'sample_id', 'transcript_name'),
    )

    def __repr__(self):
        return (
            f"<TranscriptomicsData(id={self.id}, sample_id={self.sample_id}, transcript_name={self.transcript_name}, "
            f"ensembl_transcript_id={self.ensembl_transcript_id}, ensembl_gene_id={self.ensembl_gene_id}, "
            f"quantification={self.quantification}, description={self.description})>"
        )
