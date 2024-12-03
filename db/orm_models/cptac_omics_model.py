from sqlalchemy import Column, Integer, String, ForeignKey, UniqueConstraint, Index
from sqlalchemy.dialects.postgresql import JSONB
from config.db_config import Base


class ProteomicsData(Base):
    """
    ORM model for proteomics data.
    """
    __tablename__ = "proteomics_data"

    id = Column(Integer, primary_key=True, autoincrement=True)  # Unique identifier
    sample_id = Column(String, nullable=False, index=True, doc="Unique sample identifier.")
    data_type = Column(String, default="proteomics", nullable=False, doc="Data type (proteomics).")
    description = Column(String, nullable=True, doc="Additional description or metadata about the dataset.")

    # Proteomics-Specific Fields
    protein_name = Column(String, nullable=False, index=True, doc="Name of the protein.")
    ensembl_gene_id = Column(String, nullable=True, index=True, doc="Ensembl Gene ID.")
    ensembl_protein_id = Column(String, nullable=True, index=True, doc="Ensembl Protein ID.")
    quantification = Column(JSONB, nullable=False, doc="JSONB dictionary of quantification values for the protein.")

    # Mapper Relationship
    mapper_id = Column(Integer, ForeignKey("mapping_table.id", ondelete="CASCADE"), nullable=False,
                       doc="Foreign key to the Mapping Table.")

    __table_args__ = (
        UniqueConstraint("sample_id", "protein_name", name="uq_sample_protein"),
        Index('ix_proteomics_data_sample_protein', 'sample_id', 'protein_name')
    )

    def __repr__(self):
        return (
            f"<ProteomicsData(id={self.id}, sample_id={self.sample_id}, protein_name={self.protein_name}, "
            f"ensembl_gene_id={self.ensembl_gene_id}, ensembl_protein_id={self.ensembl_protein_id}, "
            f"quantification={self.quantification}, description={self.description})>"
        )


class PhosphoproteomicsData(Base):
    """
    ORM model for phosphoproteomics data.
    """
    __tablename__ = "phosphoproteomics_data"

    id = Column(Integer, primary_key=True, autoincrement=True)  # Unique identifier
    sample_id = Column(String, nullable=False, index=True, doc="Unique sample identifier.")
    data_type = Column(String, default="phosphoproteomics", nullable=False, doc="Data type (phosphoproteomics).")
    description = Column(String, nullable=True, doc="Additional description or metadata about the dataset.")

    # Phosphoproteomics-Specific Fields
    phosphoprotein_name = Column(String, nullable=False, index=True, doc="Name of the phosphoprotein.")
    phosphorylation_site = Column(String, nullable=False, doc="The specific phosphorylation site (e.g., 'S267').")
    peptide = Column(String, nullable=True, doc="Peptide sequence.")
    ensembl_gene_id = Column(String, nullable=True, index=True, doc="Ensembl Gene ID.")
    ensembl_protein_id = Column(String, nullable=True, index=True, doc="Ensembl Protein ID.")
    quantification = Column(JSONB, nullable=False, doc="JSONB dictionary of quantification values for the phosphoprotein.")

    # Mapper Relationship
    mapper_id = Column(Integer, ForeignKey("mapping_table.id", ondelete="CASCADE"), nullable=False,
                       doc="Foreign key to the Mapping Table.")

    __table_args__ = (
        UniqueConstraint("sample_id", "phosphoprotein_name", "phosphorylation_site", name="uq_sample_phosphoprotein_site"),
        Index('ix_phosphoproteomics_data_sample_protein_site', 'sample_id', 'phosphoprotein_name', 'phosphorylation_site')
    )

    def __repr__(self):
        return (
            f"<PhosphoproteomicsData(id={self.id}, sample_id={self.sample_id}, phosphoprotein_name={self.phosphoprotein_name}, "
            f"phosphorylation_site={self.phosphorylation_site}, ensembl_gene_id={self.ensembl_gene_id}, "
            f"ensembl_protein_id={self.ensembl_protein_id}, quantification={self.quantification}, description={self.description})>"
        )


class TranscriptomicsData(Base):
    """
    ORM model for transcriptomics data.
    """
    __tablename__ = "transcriptomics_data"

    id = Column(Integer, primary_key=True, autoincrement=True)  # Unique identifier
    sample_id = Column(String, nullable=False, index=True, doc="Unique sample identifier.")
    data_type = Column(String, default="transcriptomics", nullable=False, doc="Data type (transcriptomics).")
    description = Column(String, nullable=True, doc="Additional description or metadata about the dataset.")

    # Transcriptomics-Specific Fields
    transcript_name = Column(String, nullable=False, index=True, doc="Name of the transcript.")
    ensembl_transcript_id = Column(String, nullable=True, index=True, doc="Ensembl Transcript ID.")
    ensembl_gene_id = Column(String, nullable=True, index=True, doc="Ensembl Gene ID.")
    quantification = Column(
        JSONB, nullable=False,
        doc="JSONB dictionary with quantification values for each source (e.g., {'source1': 12.34, 'source2': 45.67})."
    )

    # Mapper Relationship
    mapper_id = Column(Integer, ForeignKey("mapping_table.id", ondelete="CASCADE"), nullable=False,
                       doc="Foreign key to the Mapping Table.")

    __table_args__ = (
        Index('ix_transcriptomics_data_sample_transcript', 'sample_id', 'transcript_name'),
    )

    def __repr__(self):
        return (
            f"<TranscriptomicsData(id={self.id}, sample_id={self.sample_id}, transcript_name={self.transcript_name}, "
            f"quantification={self.quantification}, ensembl_transcript_id={self.ensembl_transcript_id}, "
            f"ensembl_gene_id={self.ensembl_gene_id}, description={self.description})>"
        )
