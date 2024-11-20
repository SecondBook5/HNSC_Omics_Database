import pytest
from sqlalchemy.exc import SQLAlchemyError
from db.schema.metadata_schema import DatasetSeriesMetadata, DatasetSampleMetadata, GeoMetadataLog
from pipeline.geo_pipeline.geo_metadata_uploader import GeoMetadataUploader
from config.db_config import get_session_context
from utils.exceptions import MissingForeignKeyError  # Import the custom exception

# ---------------- Fixtures ----------------

@pytest.fixture
def uploader():
    """
    Fixture to initialize the GeoMetadataUploader for testing.
    """
    return GeoMetadataUploader()


@pytest.fixture(scope="function")
def cleanup_database():
    """
    Cleanup fixture to ensure test data is removed after each test.
    """
    try:
        with get_session_context() as session:
            # Delete any test data inserted during the tests
            session.query(DatasetSampleMetadata).filter(DatasetSampleMetadata.SeriesID == "GSE12345").delete()
            session.query(DatasetSeriesMetadata).filter(DatasetSeriesMetadata.SeriesID == "GSE12345").delete()
            session.query(GeoMetadataLog).filter(GeoMetadataLog.geo_id == "GSE12345").delete()
            session.commit()
    except SQLAlchemyError as e:
        pytest.fail(f"Failed to clean up database: {e}")

# ---------------- Test Cases ----------------

def test_upload_series_metadata(uploader, cleanup_database):
    """
    Test the upload_series_metadata method of GeoMetadataUploader using the actual database.
    """
    dummy_series = [
        {
            "SeriesID": "GSE12345",  # Unique ID for the series
            "Title": "Test Series",  # Title of the series
            "SubmissionDate": "2024-01-01",  # Submission date of the series
            "Organism": "Homo sapiens",  # Organism involved in the series
            "PubMedID": 12345678,  # PubMed ID linked to the series
        }
    ]
    try:
        # Upload the series metadata to the database
        with get_session_context() as session:
            uploader.upload_series_metadata(session, dummy_series)

        # Verify the data was inserted correctly
        with get_session_context() as session:
            result = session.query(DatasetSeriesMetadata).filter_by(SeriesID="GSE12345").first()
            assert result is not None  # Ensure the result exists
            assert result.Title == "Test Series"  # Verify the Title field
    except SQLAlchemyError as e:
        pytest.fail(f"Failed to upload series metadata: {e}")

def test_upload_sample_metadata(uploader, cleanup_database):
    """
    Test the upload_sample_metadata method of GeoMetadataUploader using the actual database.
    """
    dummy_series = [
        {
            "SeriesID": "GSE12345",  # Unique ID for the series
            "Title": "Test Series",  # Title of the series
            "SubmissionDate": "2024-01-01",  # Submission date of the series
            "Organism": "Homo sapiens",  # Organism involved in the series
            "PubMedID": 12345678,  # PubMed ID linked to the series
        }
    ]
    dummy_samples = [
        {
            "SampleID": "GSM12345",  # Unique ID for the sample
            "SeriesID": "GSE12345",  # Foreign key linking to the series
            "Organism": "Homo sapiens",  # Organism involved in the sample
            "LibraryStrategy": "RNA-Seq",  # Library strategy used for sequencing
            "LibrarySource": "transcriptomic",  # Library source type
        }
    ]
    try:
        # First upload the series metadata
        with get_session_context() as session:
            uploader.upload_series_metadata(session, dummy_series)

        # Now upload the sample metadata
        with get_session_context() as session:
            uploader.upload_sample_metadata(session, dummy_samples)

        # Verify the sample data was inserted correctly
        with get_session_context() as session:
            result = session.query(DatasetSampleMetadata).filter_by(SampleID="GSM12345").first()
            assert result is not None  # Ensure the result exists
            assert result.LibraryStrategy == "RNA-Seq"  # Verify the LibraryStrategy field
    except SQLAlchemyError as e:
        pytest.fail(f"Failed to upload sample metadata: {e}")

def test_log_metadata_operation(uploader, cleanup_database):
    """
    Test the log_metadata_operation method of GeoMetadataUploader using the actual database.
    """
    try:
        # Log a metadata operation for a GEO ID
        with get_session_context() as session:
            uploader.log_metadata_operation(
                session=session,
                geo_id="GSE12345",
                status="processed",  # Status of the operation
                message="Test log entry",  # Message describing the operation
                file_names=["test_file_1.txt", "test_file_2.txt"],  # Associated file names
            )

        # Verify the log entry was inserted correctly
        with get_session_context() as session:
            result = session.query(GeoMetadataLog).filter_by(geo_id="GSE12345").first()
            assert result is not None  # Ensure the result exists
            assert result.status == "processed"  # Verify the status field
            assert "test_file_1.txt" in result.file_names  # Verify file names
    except SQLAlchemyError as e:
        pytest.fail(f"Failed to log metadata operation: {e}")

def test_missing_foreign_key_exception(uploader, cleanup_database):
    """
    Test the upload_sample_metadata method to ensure MissingForeignKeyError is raised
    when sample metadata references a non-existent SeriesID.
    """
    dummy_samples = [
        {
            "SampleID": "GSM12345",  # Unique ID for the sample
            "SeriesID": "GSE12345",  # Non-existent SeriesID
            "Organism": "Homo sapiens",  # Organism involved in the sample
            "LibraryStrategy": "RNA-Seq",  # Library strategy used for sequencing
            "LibrarySource": "transcriptomic",  # Library source type
        }
    ]
    # Assert that the MissingForeignKeyError is raised
    with pytest.raises(MissingForeignKeyError) as excinfo:
        with get_session_context() as session:
            uploader.upload_sample_metadata(session, dummy_samples)

    # Verify that the exception contains the expected message and missing key
    assert "Missing foreign key constraint for SeriesID" in str(excinfo.value)
    assert "GSE12345" in str(excinfo.value)
