from sqlalchemy import Column, String, Float, ForeignKey, Index
from sqlalchemy.orm import relationship
from config.db_config import Base

class ATACSeqData(Base):
    """
    Represents ATAC-Seq data linked to a sample in the database.
    """
    __tablename__ = 'atac_seq_data'

    ID = Column(String, primary_key=True, nullable=False)  # Unique identifier
    SampleID = Column(String, ForeignKey('geo_sample_metadata.SampleID'), nullable=False)
    Chromosome = Column(String, nullable=False)  # Chromosome information
    Start = Column(Float, nullable=False)  # Start position of the peak
    End = Column(Float, nullable=False)  # End position of the peak
    Score = Column(Float, nullable=True)  # Score for the peak

    Sample = relationship("GeoSampleMetadata", back_populates="ATACSeqData")

    __table_args__ = (
        Index('idx_sample_id', 'SampleID'),  # Index for querying by SampleID
        Index('idx_chromosome_start', 'Chromosome', 'Start'),  # Index for range queries
    )
