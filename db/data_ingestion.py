# data_ingestion.py
# This module is intended to handle data ingestion for the HNSC Omics Database project.
# Data files from the 'data/processed' directory are read, processed, and inserted into PostgreSQL and MongoDB.
# SQLAlchemy is used to insert structured data (e.g., samples and studies) into PostgreSQL, while pymongo loads flexible data into MongoDB collections.

# Necessary modules are imported for data ingestion
import pandas as pd
from sqlalchemy.orm import sessionmaker
from db_schema import Sample, Study, create_postgres_schema, setup_mongodb

# A function is defined to load sample metadata into PostgreSQL from a processed CSV file
def load_samples_to_postgres(engine, sample_data_path):
    # A new session is started for interacting with PostgreSQL
    Session = sessionmaker(bind=engine)
    session = Session()

    # Sample metadata is loaded from a CSV file into a pandas DataFrame
    sample_data = pd.read_csv(sample_data_path)
    # Each row in the DataFrame is iterated over
    for _, row in sample_data.iterrows():
        # A new Sample object is created for each row with the appropriate fields
        sample = Sample(
            sample_id=row['sample_id'],
            study_id=row['study_id'],
            cell_line=row['cell_line'],
            condition=row['condition'],
            timepoint=row['timepoint'],
            data_type=row['data_type']
        )
        # The Sample object is added to the current session
        session.add(sample)

    # The session is committed to save all changes to PostgreSQL
    session.commit()
    # A confirmation message is printed upon successful data load
    print("Sample data loaded into PostgreSQL.")

# A function is defined to load single-cell RNA-Seq data into MongoDB from a JSON file
def load_scRNA_data_to_mongo(db, scRNA_data_path):
    # Single-cell data is loaded from a JSON file into a pandas DataFrame
    scRNA_data = pd.read_json(scRNA_data_path)

    # The MongoDB collection for single-cell RNA-Seq data is accessed
    scRNA_collection = db['scRNA_seq']
    # Each row in the DataFrame is iterated over
    for _, row in scRNA_data.iterrows():
        # Each row is converted to a dictionary and inserted as a document in MongoDB
        scRNA_collection.insert_one(row.to_dict())

    # A confirmation message is printed upon successful data load
    print("Single-cell RNA data loaded into MongoDB.")
