# orm_models/protein_abundance_object.py

"""
File: protein_abundance_object.py
This ORM class for the HNSC Omics Database models comprehensive protein and phosphoprotein data,
accommodating complex details from CPTAC studies, including quality metrics, quantitation methods,
and associated metadata.

Advanced Database Concepts:
- Foreign Key Constraints: Links SampleID to ClinicalSample and GeneID to Gene.
- Indexing: Indexed fields for efficient querying on SampleID, GeneID, and Dataset.
- Phosphoproteomic Data: Separate field to distinguish phosphoprotein levels.
- Supplementary Data: Links and references for external studies and supplementary files.
"""

from sqlalchemy import Column, String, ForeignKey, Float, DateTime, Boolean, Integer, func, Index
from sqlalchemy.orm import relationship
from db.db_config import Base


class ProteinAbundance(Base):
    """
    The ProteinAbundance class defines diagrams for quantitative data on protein and phosphoprotein
    levels, including detailed metadata fields for data provenance and processing information.

    Attributes:
        ProteinID (str): Unique identifier for each protein abundance record (Primary Key).
        SampleID (str): Links to ClinicalSample, associating protein data with a specific sample.
        GeneID (str): Links to Gene, linking protein data to gene-level information.
        TotalProteinAbundance (float): Quantitative value for the total protein level.
        PhosphoProteinAbundance (float): Quantitative value for phosphorylated protein level.
        QuantitationMethod (str): Method for protein quantitation (e.g., `TMT-11`).
        PeptideSpectralMatches (int): Number of peptide spectral matches (PSMs) identified.
        ProteinAssemblyQuality (str): Quality metrics for the protein assembly.
        PhosphoproteomeStatus (bool): Indicates if the protein data represents phosphorylated state.
        MassSpectraType (str): Indicates `Raw` or `Processed` for mass spectra type.
        DataSource (str): Originating data source (e.g., `CPTAC`).
        Dataset (str): Dataset accession (e.g., `PDC000222` or `PDC000221`).
        ExternalReferences (str): External study or consortium references (e.g., GDC, Imaging Archive).
        PublicationID (str): Study publication reference (e.g., DOI or PubMed ID).
        SupplementaryFiles (str): Links to supplementary files.
        created_at (DateTime): Timestamp for record creation.
        updated_at (DateTime): Timestamp for record updates.
    """

    __tablename__ = 'protein_abundance'

    # Primary Key for each record, unique identifier
    ProteinID: str = Column(String, primary_key=True)

    # Foreign Key to link protein data to specific sample and gene
    SampleID: str = Column(String, ForeignKey('clinical_sample.SampleID', ondelete="CASCADE"), nullable=False)
    GeneID: str = Column(String, ForeignKey('gene.GeneID', ondelete="CASCADE"), nullable=False)

    # Quantitative protein abundance fields
    TotalProteinAbundance: float = Column(Float, nullable=False)
    PhosphoProteinAbundance: float = Column(Float, nullable=True)

    # Quantitation method (e.g., TMT-11), Peptide Spectral Matches, and Protein Assembly Quality metrics
    QuantitationMethod: str = Column(String, nullable=True)
    PeptideSpectralMatches: int = Column(Integer, nullable=True)
    ProteinAssemblyQuality: str = Column(String, nullable=True)

    # Boolean to indicate if phosphoproteomic data is present
    PhosphoproteomeStatus: bool = Column(Boolean, default=False, nullable=True)

    # Mass Spectra type (Raw or Processed) for additional metadata on data processing
    MassSpectraType: str = Column(String, nullable=True)

    # Data Source and specific dataset
    DataSource: str = Column(String, nullable=False)
    Dataset: str = Column(String, nullable=False)

    # External references, publications, and supplementary files for full data provenance
    ExternalReferences: str = Column(String, nullable=True)
    PublicationID: str = Column(String, nullable=True)
    SupplementaryFiles: str = Column(String, nullable=True)

    # Automatic timestamps
    created_at: DateTime = Column(DateTime, default=func.now())
    updated_at: DateTime = Column(DateTime, default=func.now(), onupdate=func.now())

    # Relationships
    clinical_sample = relationship("ClinicalSample", back_populates="protein_abundance")
    gene = relationship("Gene", back_populates="protein_abundance")

    # Indexes for efficient querying on key fields
    __table_args__ = (
        Index('ix_protein_abundance_sample_gene', 'SampleID', 'GeneID'),
        Index('ix_protein_abundance_data_source', 'DataSource', 'Dataset'),
    )
