# db/models.py
"""
This module ensures all ORM models are registered to the shared Base.metadata
and can be imported by Alembic for migrations or by the application.
"""

from config.db_config import Base  # Import the shared Base

# Import all ORM models to register them with Base.metadata
from db.schema.geo_metadata_schema import GeoSeriesMetadata, GeoSampleMetadata, GeoMetadataLog
from db.schema.cptac_metadata_schema import CptacMetadata, CptacColumns, CptacMetadataLog
from db.orm_models.proteomics_object import Proteomics
from db.orm_models.phosphoproteomics_object import Phosphoproteomics
from db.mapping_table import MappingTable
from db.orm_models.atac_seq_object import ATACSeqData
from db.schema.microarray_schema import MicroarrayData, PlatformAnnotation

# No explicit Base definition here. Base.metadata will automatically include
# all imported models.
