from sqlalchemy import Column, String, Float, ForeignKey, Integer, Index
from config.db_config import Base
from sqlalchemy.orm import relationship



class PlatformAnnotation(Base):
    """
    Represents microarray platform annotations.
    """
    __tablename__ = 'platform_annotation'

    ProbeID = Column(String, primary_key=True, nullable=False)
    GeneSymbol = Column(String, nullable=True)
    Description = Column(String, nullable=True)

    # Relationships
    microarray_data = relationship(
        "MicroarrayData",
        back_populates="platform_annotation",
        cascade="all, delete-orphan",
    )

    def __repr__(self):
        return f"<PlatformAnnotation(ProbeID={self.ProbeID}, GeneSymbol={self.GeneSymbol})>"


class MicroarrayData(Base):
    """
    Represents microarray expression data, linked to samples and series.
    """
    __tablename__ = 'microarray_data'

    id = Column(Integer, primary_key=True, autoincrement=True)
    SampleID = Column(String, nullable=False, index=True)  # Link to Sample Metadata
    SeriesID = Column(String, nullable=False, index=True)  # Link to Series Metadata
    ProbeID = Column(String, ForeignKey("platform_annotation.ProbeID", ondelete="CASCADE"), nullable=False)
    ExpressionValue = Column(Float, nullable=False)

    # Relationships
    platform_annotation = relationship(
        "PlatformAnnotation",
        back_populates="microarray_data",
    )

    def __repr__(self):
        return f"<MicroarrayData(SampleID={self.SampleID}, SeriesID={self.SeriesID}, ProbeID={self.ProbeID}, ExpressionValue={self.ExpressionValue})>"
