# db_schema.py
# This module is intended to define the db schema for the HNSC Omics Database project.
# The module establishes tables in PostgreSQL for storing structured data (e.g., sample metadata, gene information)
# and sets up collections in MongoDB for flexible, unstructured data (e.g., single-cell RNA-Seq).
# The schema is set up first to provide a consistent framework for data ingestion and querying.
# SQLAlchemy is used for defining tables in PostgreSQL, while pymongo is used for managing MongoDB collections.

# Necessary components are imported from SQLAlchemy to define the db schema
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

# A base class is created for SQLAlchemy ORM models
Base = declarative_base()

# The Study table is defined to store metadata for each study
class Study(Base):
    # The table name is specified for PostgreSQL
    __tablename__ = 'studies'
    # The primary key column is defined for study IDs
    study_id = Column(String, primary_key=True)
    # Additional columns are specified for study title, summary, publication ID, organism, and platform
    title = Column(String)
    summary = Column(String)
    publication = Column(String)
    organism = Column(String)
    platform = Column(String)

# The Sample table is defined to store metadata for each biological sample
class Sample(Base):
    # The table name is specified for PostgreSQL
    __tablename__ = 'samples'
    # The primary key column is defined for sample IDs
    sample_id = Column(Integer, primary_key=True)
    # A foreign key column is defined to link each sample to a study ID in the Study table
    study_id = Column(String, ForeignKey('studies.study_id'))
    # Additional columns are specified for sample details such as cell line, condition, timepoint, and data type
    cell_line = Column(String)
    condition = Column(String)
    timepoint = Column(Integer)
    data_type = Column(String)

# The Gene table is defined to store information about genes
class Gene(Base):
    # The table name is specified for PostgreSQL
    __tablename__ = 'genes'
    # The primary key column is defined for gene IDs
    gene_id = Column(Integer, primary_key=True)
    # A unique column is defined for gene symbols
    gene_symbol = Column(String, unique=True)
    # A column is defined for gene descriptions or annotations
    description = Column(String)

# The Expression table is defined to store gene expression values for bulk RNA-Seq data
class Expression(Base):
    # The table name is specified for PostgreSQL
    __tablename__ = 'expression'
    # The primary key column is defined for the expression record
    id = Column(Integer, primary_key=True)
    # Foreign key columns are defined to link expression values to specific samples and genes
    sample_id = Column(Integer, ForeignKey('samples.sample_id'))
    gene_id = Column(Integer, ForeignKey('genes.gene_id'))
    # A column is defined for the expression value (float) for each gene in each sample
    expression_value = Column(Float)
    # Relationships are set up to connect expression data with samples and genes
    sample = relationship("Sample")
    gene = relationship("Gene")

# A function is defined to create the PostgreSQL schema and initialize tables
def create_postgres_schema(db_url):
    # A connection to the PostgreSQL db is established using the provided URL
    engine = create_engine(db_url)
    # All tables are created in the db according to the defined schema
    Base.metadata.create_all(engine)
    # The engine object is returned for further interaction with the db
    return engine

# MongoDB collection setup section
# MongoClient is imported from pymongo to connect and interact with MongoDB
from pymongo import MongoClient

# A function is defined to set up MongoDB collections for flexible data types (e.g., scRNA-Seq)
def setup_mongodb(db_url):
    # A connection is established to MongoDB using the provided URL
    client = MongoClient(db_url)
    # The 'hnsc_omics' db is created or connected to in MongoDB
    db = client['hnsc_omics']

    # Collections are created for single-cell RNA-Seq, ATAC-Seq, ChIP-Seq, and Spatial Transcriptomics data
    db.create_collection('scRNA_seq')      # Collection for single-cell RNA-Seq data
    db.create_collection('atac_seq')       # Collection for ATAC-Seq peaks and accessibility data
    db.create_collection('chip_seq')       # Collection for ChIP-Seq protein-binding data
    db.create_collection('spatial_transcriptomics')  # Collection for spatial transcriptomics data

    # The db object is returned for further interaction
    return db
