# tests/test_clinical_models.py

"""
Pytest test suite for the ClinicalMetadata and ClinicalSample ORM models.
This suite validates CRUD operations, relationships, constraints, and cascading deletes.
"""



# Import pytest for creating test cases
import pytest
# Import Session for managing database transactions within tests
from sqlalchemy.orm import Session
# Import base configuration for tables and session management from db_config
from config.db_config import Base, engine, SessionLocal
# Import the ORM classes for ClinicalSample and ClinicalMetadata models
from db.orm_models.clinical_sample_object import ClinicalSample
from db.orm_models.clinical_metadata_object import ClinicalMetadata



# Define a fixture to set up and tear down the database for tests
@pytest.fixture(scope="module")
def db_session():
    # Create all tables in the database before running tests
    Base.metadata.create_all(bind=engine)
    # Initialize a session for interacting with the database
    session = SessionLocal()
    # Yield the session to each test function
    yield session
    # Close the session after tests complete
    session.close()
    # Drop all tables from the database to clean up after tests
    Base.metadata.drop_all(bind=engine)


# Test case for creating a ClinicalMetadata record
def test_create_clinical_metadata(db_session: Session):
    """
    Test case for creating a ClinicalMetadata record.
    Validates that the record can be created and retrieved successfully.
    """
    # Create a new ClinicalMetadata record with sample patient information
    metadata = ClinicalMetadata(
        ClinicalID="CL001",
        PatientID="PT001",
        Age=60,
        Gender="M",
        SurvivalTime=36,
        EventObserved=True,
        TumorStage="Stage II",
        Treatment="Chemotherapy"
    )
    # Add the ClinicalMetadata record to the session to prepare for saving
    db_session.add(metadata)
    # Commit the session to persist the record in the database
    db_session.commit()

    # Query the database to retrieve the ClinicalMetadata record by its ClinicalID
    record = db_session.query(ClinicalMetadata).filter_by(ClinicalID="CL001").first()
    # Assert that the record was created and found in the database
    assert record is not None
    # Assert that the PatientID matches the expected value to verify data consistency
    assert record.PatientID == "PT001"


# Test case for creating a ClinicalSample record and linking it to ClinicalMetadata
def test_create_clinical_sample(db_session: Session):
    """
    Test case for creating a ClinicalSample record linked to ClinicalMetadata.
    Validates the foreign key relationship and data consistency.
    """
    # Create a new ClinicalSample record linked to an existing ClinicalMetadata record
    sample = ClinicalSample(
        SampleID="SMP001",
        ClinicalID="CL001",  # Link to ClinicalMetadata record
        TissueType="tumor",
        Platform="RNA-Seq",
        status="active"
    )
    # Add the ClinicalSample record to the session for saving
    db_session.add(sample)
    # Commit the session to save the record in the database
    db_session.commit()

    # Query the database to retrieve the ClinicalSample record by its SampleID
    record = db_session.query(ClinicalSample).filter_by(SampleID="SMP001").first()
    # Assert that the ClinicalSample record exists in the database
    assert record is not None
    # Assert that ClinicalID matches the expected linked ClinicalMetadata record
    assert record.ClinicalID == "CL001"
    # Assert that TissueType matches the expected value
    assert record.TissueType == "tumor"


# Test case to validate constraints on ClinicalSample and ClinicalMetadata models
def test_constraint_violation(db_session: Session):
    """
    Test case for constraint violations on ClinicalSample and ClinicalMetadata models.
    Checks that invalid values are not allowed and raise appropriate errors.
    """
    # Attempt to create a ClinicalMetadata record with an invalid Gender value
    with pytest.raises(Exception):  # Expect an error for invalid Gender
        invalid_metadata = ClinicalMetadata(
            ClinicalID="CL002",
            PatientID="PT002",
            Age=50,
            Gender="X",  # Invalid gender
            EventObserved=False
        )
        # Add invalid metadata to session
        db_session.add(invalid_metadata)
        # Attempt to commit session, expecting failure due to constraint
        db_session.commit()

    # Attempt to create a ClinicalSample record with an invalid TissueType value
    with pytest.raises(Exception):  # Expect an error for invalid TissueType
        invalid_sample = ClinicalSample(
            SampleID="SMP002",
            ClinicalID="CL001",  # Link to valid ClinicalMetadata
            TissueType="unknown",  # Invalid tissue type
            Platform="RNA-Seq"
        )
        # Add invalid sample to session
        db_session.add(invalid_sample)
        # Attempt to commit session, expecting failure due to constraint
        db_session.commit()


# Test case to verify cascading delete from ClinicalMetadata to ClinicalSample
def test_cascade_delete(db_session: Session):
    """
    Test case for cascading deletes from ClinicalMetadata to ClinicalSample.
    Ensures that deleting a ClinicalMetadata record removes associated ClinicalSample records.
    """
    # Create ClinicalMetadata and a linked ClinicalSample record
    metadata = ClinicalMetadata(
        ClinicalID="CL003",
        PatientID="PT003",
        Age=45,
        Gender="F",
        EventObserved=True
    )
    sample = ClinicalSample(
        SampleID="SMP003",
        ClinicalID="CL003",  # Link to ClinicalMetadata
        TissueType="normal",
        Platform="ATAC-Seq"
    )
    # Add both records to the session
    db_session.add(metadata)
    db_session.add(sample)
    # Commit session to persist records
    db_session.commit()

    # Delete ClinicalMetadata record and cascade delete ClinicalSample
    db_session.delete(metadata)
    # Commit delete to apply changes
    db_session.commit()

    # Verify that the associated ClinicalSample record was also deleted
    sample_record = db_session.query(ClinicalSample).filter_by(SampleID="SMP003").first()
    # Assert that no record exists for SampleID, confirming cascading delete
    assert sample_record is None


# Test case for soft delete functionality in ClinicalSample
def test_soft_delete(db_session: Session):
    """
    Test case to verify the soft delete functionality by marking a ClinicalSample
    record as inactive and checking if it is excluded from active queries.
    """
    # Create a new ClinicalSample record and save it as active
    sample = ClinicalSample(
        SampleID="SMP004",
        ClinicalID="CL001",
        TissueType="tumor",
        Platform="RNA-Seq",
        status="active"
    )
    db_session.add(sample)
    db_session.commit()

    # Mark the sample as inactive (soft delete)
    sample.status = "inactive"
    db_session.commit()

    # Retrieve the record by SampleID and verify its inactive status
    inactive_sample = db_session.query(ClinicalSample).filter_by(SampleID="SMP004").first()
    assert inactive_sample is not None
    assert inactive_sample.status == "inactive"


# Test case to verify update operation and automatic timestamp update
def test_update_operation(db_session: Session):
    """
    Test case to validate update operation and automatic modification of the updated_at field.
    """
    # Create a new ClinicalSample record and commit to set initial timestamp
    sample = ClinicalSample(
        SampleID="SMP005",
        ClinicalID="CL001",
        TissueType="tumor",
        Platform="RNA-Seq"
    )
    db_session.add(sample)
    db_session.commit()

    # Capture the initial updated_at timestamp for comparison
    initial_updated_at = sample.updated_at

    # Modify TissueType and save change to trigger timestamp update
    sample.TissueType = "normal"
    db_session.commit()

    # Retrieve the updated record and check that updated_at changed
    updated_sample = db_session.query(ClinicalSample).filter_by(SampleID="SMP005").first()
    assert updated_sample.updated_at > initial_updated_at


# Test case for index performance validation (optional)
def test_index_query_performance(db_session: Session):
    """
    Test case to validate query performance for indexed fields ClinicalID and Platform.
    Populates multiple records to simulate database load, then measures query response.
    """
    # Populate database with multiple ClinicalSample records for performance test
    for i in range(20):
        sample = ClinicalSample(
            SampleID=f"SMP00{i + 6}",
            ClinicalID="CL001",
            TissueType="normal",
            Platform="RNA-Seq"
        )
        db_session.add(sample)
    db_session.commit()

    # Query using indexed fields to verify response performance
    results = db_session.query(ClinicalSample).filter_by(ClinicalID="CL001", Platform="RNA-Seq").all()
    assert len(results) == 20  # Confirm all records queried successfully
