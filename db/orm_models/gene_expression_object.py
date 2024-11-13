# orm_models/gene_expression_object.py

"""
File: gene_expression_object.py
This file defines the ORM class for the GeneExpression table within the HNSC Omics Database.
This class includes fields for genomic location, gene expression values, version tracking, and data integrity constraints.
Additional features include methods for data normalization and advanced indexing for efficient querying.

Advanced Database Concepts:
- Indexing: Composite index on Chromosome and GeneSymbol for optimized querying by genomic location.
- Data Constraints: Ensures StartPosition is non-negative and EndPosition is greater than StartPosition.
- Source Tracking: Allows source-specific unique constraints for data integrity.
- Data Normalization: Provides an in-class method for expression normalization.
"""

from sqlalchemy import Column, String, Integer, Float, DateTime, Index, CheckConstraint, UniqueConstraint, func
from config.db_config import Base  # Importing the Base class for SQLAlchemy ORM from the configuration


class GeneExpression(Base):
    """
    The GeneExpression class defines the diagrams for storing gene-specific information,
    including identifiers, genomic locations, expression levels, and metadata for
    source tracking, confidence, and versioning.

    Attributes:
        GeneID (str): Unique identifier for each gene (Primary Key).
        GeneSymbol (str): Standard symbol representing the gene.
        GeneName (str): Full descriptive name of the gene.
        Chromosome (str): Chromosome location of the gene.
        StartPosition (int): Genomic start position of the gene.
        EndPosition (int): Genomic end position of the gene.
        ExpressionValue (float): Measured expression level associated with the gene.
        ConfidenceScore (float): Score indicating the reliability of expression data.
        GeneType (str): Type/category of gene (e.g., protein_coding).
        Source (str): Origin of the gene expression data (e.g., GEO, TCGA).
        Version (int): Tracks updates or revisions of the gene data.
        LastUpdated (DateTime): Automatically updates timestamp upon modification.
    """

    # Define the table name within the database
    __tablename__ = 'gene_expression'

    # Primary key, representing a unique identifier for each gene
    GeneID: str = Column(String, primary_key=True)

    # Gene symbol, the short representation, indexed for faster querying
    GeneSymbol: str = Column(String, nullable=False, index=True)

    # Full gene name, optional field
    GeneName: str = Column(String, nullable=True)

    # Chromosome on which the gene is located
    Chromosome: str = Column(String, nullable=True)

    # Start position on the chromosome, constrained to be non-negative
    StartPosition: int = Column(Integer, nullable=False)

    # End position on the chromosome, must be greater than StartPosition
    EndPosition: int = Column(Integer, nullable=False)

    # Expression value of the gene, represents the intensity or amount of gene expression
    ExpressionValue: float = Column(Float, nullable=True)

    # Confidence score indicating the reliability or quality of the expression data
    ConfidenceScore: float = Column(Float, nullable=True)

    # Type of the gene (e.g., protein_coding, lincRNA)
    GeneType: str = Column(String, nullable=True)

    # Data source from which the expression information was obtained (e.g., GEO, CPTAC)
    Source: str = Column(String, nullable=True)

    # Version number to track revisions or updates to the gene data
    Version: int = Column(Integer, nullable=False, default=1)

    # Timestamp that auto-updates upon record modification, tracking the last updated time
    LastUpdated: DateTime = Column(DateTime, default=func.now(), onupdate=func.now())

    # Table arguments, setting advanced constraints and indexes
    __table_args__ = (
        # Composite index on Chromosome and GeneSymbol for efficient genomic region queries
        Index('ix_gene_expression_chromosome', 'Chromosome', 'GeneSymbol'),

        # Constraint ensuring StartPosition is non-negative, improving data integrity
        CheckConstraint("StartPosition >= 0", name="check_start_position"),

        # Constraint ensuring EndPosition is greater than StartPosition to maintain valid genomic regions
        CheckConstraint("EndPosition > StartPosition", name="check_position_order"),

        # Unique constraint to avoid duplicate records of the same GeneID from the same Source
        UniqueConstraint('GeneID', 'Source', name='uq_gene_source')
    )

    def normalize_expression(self, factor: float) -> None:
        """
        Normalize the expression value by a given factor.

        Args:
            factor (float): The normalization factor.

        Returns:
            None: Modifies the ExpressionValue in place.
        """
        # Check if ExpressionValue exists, then divides it by the given factor to normalize
        if self.ExpressionValue:
            self.ExpressionValue /= factor
