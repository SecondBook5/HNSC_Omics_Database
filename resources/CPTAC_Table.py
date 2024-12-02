from db.mapping_table import MappingTable
from db.orm_models.proteomics_object import Proteomics
from sqlalchemy.orm import relationship
from config.db_config import get_session_context

# Use the session context from db_config
with get_session_context() as session:
    # Check relationships in MappingTable
    print("Relationships in MappingTable:")
    print(MappingTable.__mapper__.relationships.keys())  # Should include 'proteomics_entries'

    # Check backref in Proteomics
    print("Backref in Proteomics:")
    print(Proteomics.mapping.property.back_populates)  # Should return 'proteomics_entries'
