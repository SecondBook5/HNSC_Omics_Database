from sqlalchemy import (
    Column,
    String,
    Float,
    JSON,
    ForeignKey,
    UniqueConstraint,
    Index,
    Integer
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Phosphoproteomics(Base):
    """
    ORM model for storing phosphoproteomics data.
    Captures quantification and mapping information for phosphoprotein samples.
    """
    __tablename__ = "phosphoproteomics"

    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Core Fields
    sample_id = Column(String, nullable=False, index=True, doc="Unique sample identifier.")
    phosphoprotein_name = Column(String, nullable=False, index=True, doc="Name of the phosphoprotein (e.g., M6PR_S267).")
    phosphorylation_site = Column(String, nullable=False, doc="The specific phosphorylation site (e.g., 'S267').")
    sequence_window = Column(String, nullable=True, doc="Phosphorylation sequence window (e.g., 'DDQLGEESEERDDHL').")
    ensembl_gene_id = Column(String, nullable=True, index=True, doc="Ensembl Gene ID, if available.")
    ensembl_protein_id = Column(String, nullable=True, index=True, doc="Ensembl Protein ID, if available.")

    # Quantification Data
    quantification = Column(JSON, nullable=False, doc="JSON dictionary of quantification values by data source.")
    aggregate_quantification = Column(Float, nullable=True, doc="User-defined aggregate quantification value.")

    # Metadata Fields
    data_type = Column(String, nullable=True, index=True, doc="Type of data (e.g., phosphoproteomics).")
    description = Column(String, nullable=True, doc="Additional description or metadata about the dataset.")

    # Mapper Relationship
    mapper_id = Column(Integer, ForeignKey("mapping_table.id", ondelete="CASCADE"), nullable=False, doc="Foreign key to the Mapping Table.")

    # Table Constraints
    __table_args__ = (
        UniqueConstraint("sample_id", "phosphoprotein_name", "phosphorylation_site", name="uq_sample_phosphoprotein_site"),
    )

    def __repr__(self):
        return (
            f"<Phosphoproteomics(id={self.id}, sample_id={self.sample_id}, phosphoprotein_name={self.phosphoprotein_name}, "
            f"phosphorylation_site={self.phosphorylation_site}, sequence_window={self.sequence_window}, "
            f"ensembl_gene_id={self.ensembl_gene_id}, ensembl_protein_id={self.ensembl_protein_id}, "
            f"quantification={self.quantification}, aggregate_quantification={self.aggregate_quantification}, "
            f"data_type={self.data_type}, description={self.description}, mapper_id={self.mapper_id})>"
        )
