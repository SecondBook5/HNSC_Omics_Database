from sqlalchemy import Column, String, Integer, JSON, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class MappingTable(Base):
    """
    ORM model for mapping biological identifiers.
    This table provides a universal reference for mapping identifiers across datasets.
    """
    __tablename__ = "mapping_table"

    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Core Mapping Fields
    gene_id = Column(String, nullable=False, doc="General gene ID (e.g., HGNC ID or dataset-specific ID).")
    ensembl_gene_id = Column(String, nullable=True, doc="Ensembl gene ID (e.g., ENSG00000121410.12).")
    ensembl_transcript_id = Column(String, nullable=True, doc="Ensembl transcript ID.")
    ensembl_protein_id = Column(
        String,
        nullable=True,
        unique=True,  # Unique constraint for FK compatibility
        doc="Ensembl protein ID (e.g., ENSP00000000412.3)."
    )
    uniprot_id = Column(String, nullable=True, doc="UniProt protein ID (e.g., P12345).")
    gene_symbol = Column(String, nullable=False, doc="Human-readable gene symbol (e.g., TP53).")
    alternative_symbols = Column(JSON, nullable=True, doc="List of alternative names or synonyms for the gene.")
    somatic_mutation_ids = Column(JSON, nullable=True, doc="List of somatic mutation IDs linked to this gene.")
    annotations = Column(JSON, nullable=True, doc="Additional metadata (e.g., function, pathways, etc.).")

    # Extended Annotations
    protein_name = Column(String, nullable=True, doc="Protein name (from UniProt).")
    protein_structure = Column(String, nullable=True, doc="Protein structure (linked to PDB ID).")
    pathways = Column(JSON, nullable=True, doc="List of KEGG pathways the gene is involved in.")
    go_terms = Column(JSON, nullable=True, doc="List of Gene Ontology terms associated with the gene.")
    blast_results = Column(JSON, nullable=True, doc="BLAST results (E-value, Identity) for sequence alignment.")

    # Chromosomal Information
    chromosome = Column(String, nullable=True, doc="Chromosome where the gene is located (e.g., chr1).")
    start_position = Column(Integer, nullable=True, doc="Start position of the gene on the chromosome.")
    end_position = Column(Integer, nullable=True, doc="End position of the gene on the chromosome.")
    strand = Column(String, nullable=True, doc="Strand orientation (e.g., + or -).")

    # Constraints
    __table_args__ = (
        UniqueConstraint("gene_id", "ensembl_gene_id", "uniprot_id", name="uq_gene_mapping"),
    )

    def __repr__(self):
        return (
            f"<MappingTable(id={self.id}, gene_id={self.gene_id}, ensembl_gene_id={self.ensembl_gene_id}, "
            f"uniprot_id={self.uniprot_id}, gene_symbol={self.gene_symbol})>"
        )
