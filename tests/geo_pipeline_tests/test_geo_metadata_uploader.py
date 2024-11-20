import pytest
from sqlalchemy.exc import SQLAlchemyError
from db.schema.metadata_schema import DatasetSeriesMetadata, DatasetSampleMetadata, GeoMetadataLog
from pipeline.geo_pipeline.geo_metadata_uploader import GeoMetadataUploader
from config.db_config import get_session_context
from utils.exceptions import MissingForeignKeyError  # Import the custom exception

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

def test_upload_series_metadata(uploader, cleanup_database):
    """
    Test the upload_series_metadata method of GeoMetadataUploader using the actual database.
    """
    dummy_series = [
        {
            "SeriesID": "GSE12345",
            "Title": "Test Series",
            "SubmissionDate": "2024-01-01",
            "Organism": "Homo sapiens",
            "PubMedID": 12345678,
        }
    ]
    try:
        uploader.upload_series_metadata(dummy_series)
        # Verify the data was inserted
        with get_session_context() as session:
            result = session.query(DatasetSeriesMetadata).filter_by(SeriesID="GSE12345").first()
            assert result is not None
            assert result.Title == "Test Series"
    except SQLAlchemyError as e:
        pytest.fail(f"Failed to upload series metadata: {e}")

def test_upload_sample_metadata(uploader, cleanup_database):
    """
    Test the upload_sample_metadata method of GeoMetadataUploader using the actual database.
    """
    # Insert the dummy series metadata first
    dummy_series = [
        {
            "SeriesID": "GSE12345",
            "Title": "Test Series",
            "SubmissionDate": "2024-01-01",
            "Organism": "Homo sapiens",
            "PubMedID": 12345678,
        }
    ]
    dummy_samples = [
        {
            "SampleID": "GSM12345",
            "SeriesID": "GSE12345",
            "Organism": "Homo sapiens",
            "LibraryStrategy": "RNA-Seq",
            "LibrarySource": "transcriptomic",
        }
    ]
    try:
        # Upload series first
        uploader.upload_series_metadata(dummy_series)
        # Now upload sample metadata
        uploader.upload_sample_metadata(dummy_samples)
        # Verify the sample data was inserted
        with get_session_context() as session:
            result = session.query(DatasetSampleMetadata).filter_by(SampleID="GSM12345").first()
            assert result is not None
            assert result.LibraryStrategy == "RNA-Seq"
    except SQLAlchemyError as e:
        pytest.fail(f"Failed to upload sample metadata: {e}")

def test_log_metadata_operation(uploader, cleanup_database):
    """
    Test the log_metadata_operation method of GeoMetadataUploader using the actual database.
    """
    try:
        uploader.log_metadata_operation(
            geo_id="GSE12345",
            status="processed",
            message="Test log entry",
            file_names=["test_file_1.txt", "test_file_2.txt"],
        )
        # Verify the log entry was inserted
        with get_session_context() as session:
            result = session.query(GeoMetadataLog).filter_by(geo_id="GSE12345").first()
            assert result is not None
            assert result.status == "processed"
            assert "test_file_1.txt" in result.file_names
    except SQLAlchemyError as e:
        pytest.fail(f"Failed to log metadata operation: {e}")

def test_missing_foreign_key_exception(uploader, cleanup_database):
    """
    Test the upload_sample_metadata method to ensure MissingForeignKeyError is raised
    when sample metadata references a non-existent SeriesID.
    """
    dummy_samples = [
        {
            "SampleID": "GSM12345",
            "SeriesID": "GSE12345",  # This SeriesID does not exist in the database
            "Organism": "Homo sapiens",
            "LibraryStrategy": "RNA-Seq",
            "LibrarySource": "transcriptomic",
        }
    ]
    with pytest.raises(MissingForeignKeyError) as excinfo:
        uploader.upload_sample_metadata(dummy_samples)

    # Verify that the exception contains the expected message
    assert "Missing foreign key constraint for SeriesID" in str(excinfo.value)
    assert "GSE12345" in str(excinfo.value)
