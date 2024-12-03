from sqlalchemy import Column, Integer, String, Float, JSON, ForeignKey, UniqueConstraint, Index
from sqlalchemy.dialects.postgresql import JSONB
from db.mapping_table import MappingTable
from config.db_config import Base


class Proteomics(Base):
    """
    ORM model for storing proteomics data.
    Captures quantification and mapping information for proteomics samples.
    """
    __tablename__ = "proteomics"

    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Core Fields
    sample_id = Column(String, nullable=False, index=True, doc="Unique identifier for the sample.")
    protein_name = Column(String, nullable=False, index=True, doc="Name of the protein or gene ID.")
    ensembl_gene_id = Column(String, nullable=True, index=True, doc="Ensembl Gene ID, if available.")
    ensembl_protein_id = Column(String, nullable=True, index=True, doc="Ensembl Protein ID, if available.")

    # Quantification Data
    quantification = Column(JSONB, nullable=False, doc="JSONB dictionary of quantification values by data source.")
    aggregate_quantification = Column(Float, nullable=True, doc="Aggregate quantification value as determined by the user.")

    # Metadata Fields
    data_type = Column(String, nullable=True, index=True, doc="Type of data (e.g., proteomics).")
    description = Column(String, nullable=True, doc="Additional description or metadata about the dataset.")

    # Mapper Relationship
    mapper_id = Column(Integer, ForeignKey("mapping_table.id", ondelete="CASCADE"), nullable=False, doc="Foreign key to the Mapping Table.")

    # Constraints and Indexes
    __table_args__ = (
        UniqueConstraint("sample_id", "protein_name", name="uq_sample_protein"),
    )

    def __repr__(self):
        return (
            f"<Proteomics(id={self.id}, sample_id={self.sample_id}, protein_name={self.protein_name}, "
            f"ensembl_gene_id={self.ensembl_gene_id}, ensembl_protein_id={self.ensembl_protein_id}, "
            f"quantification={self.quantification}, aggregate_quantification={self.aggregate_quantification}, "
            f"data_type={self.data_type}, description={self.description}, mapper_id={self.mapper_id})>"
        )
