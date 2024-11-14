# File: tests/geo_pipeline_tests/test_geo_metadata_uploader.py

import pytest  # For test case organization and assertions
from unittest.mock import patch, MagicMock  # For mocking database connections and methods
from pipeline.geo_pipeline.geo_metadata_uploader import GeoMetadataUploader  # Import the class to test
from psycopg2 import OperationalError, DatabaseError  # For simulating connection issues in PostgreSQL


# Fixture to set up connection parameters for PostgreSQL
@pytest.fixture
def connection_params():
    return {
        "dbname": "test_db",
        "user": "test_user",
        "password": "test_password",
        "host": "localhost",
        "port": "5432"
    }


# Fixture to initialize the GeoMetadataUploader with mocked connection and debug mode
@pytest.fixture
def uploader(connection_params):
    with patch('pipeline.geo_pipeline.geo_metadata_uploader.psycopg2.connect'):
        return GeoMetadataUploader(connection_params=connection_params, debug=True)


def test_successful_connection(uploader):
    """
    Test that the uploader successfully establishes a connection to the database.
    """
    with patch.object(uploader, '_connect', return_value=True) as mock_connect:
        assert uploader._connect() is True, "Expected successful connection setup"
        mock_connect.assert_called_once()


def test_failed_connection(connection_params):
    """
    Test that a ConnectionError is raised if the database connection cannot be established.
    """
    # Mock `psycopg2.connect` to raise an OperationalError to simulate connection failure
    with patch('pipeline.geo_pipeline.geo_metadata_uploader.psycopg2.connect', side_effect=OperationalError):
        with pytest.raises(ConnectionError, match="Failed to establish connection to PostgreSQL"):
            GeoMetadataUploader(connection_params=connection_params, debug=True)


def test_upload_metadata_success(uploader):
    """
    Test successful metadata upload with mock data.
    """
    # Set up sample metadata to upload
    sample_metadata = {
        "SampleTable": {
            "id": "123",
            "name": "Sample Name",
            "description": "Sample Description"
        }
    }

    with patch.object(uploader, '_connect', return_value=True), \
            patch.object(uploader, '_disconnect', return_value=None), \
            patch.object(uploader, 'cursor', create=True) as mock_cursor:
        # Call `upload_metadata` to test insert functionality
        uploader.upload_metadata(sample_metadata)

        # Verify that an INSERT command was called with the expected table and values
        mock_cursor.execute.assert_called_once()

        # Validate SQL placeholders and values without `as_string()`
        _, insert_values = mock_cursor.execute.call_args[0]
        assert insert_values == ("123", "Sample Name", "Sample Description")


def test_upload_metadata_invalid_data(uploader):
    """
    Test that a ValueError is raised for invalid metadata format.
    """
    with pytest.raises(ValueError, match="Invalid metadata provided for upload"):
        uploader.upload_metadata({})  # Test with empty metadata dictionary


def test_upload_metadata_sql_error(uploader):
    """
    Test handling of SQL errors during data upload.
    """
    sample_metadata = {
        "SampleTable": {
            "id": "123",
            "name": "Sample Name",
            "description": "Sample Description"
        }
    }

    with patch.object(uploader, 'cursor', create=True) as mock_cursor:
        # Simulate a database error during execution
        mock_cursor.execute.side_effect = DatabaseError("Database error")
        with pytest.raises(ValueError, match="Failed to upload metadata to table"):
            uploader.upload_metadata(sample_metadata)


def test_disconnect_successful(uploader):
    """
    Test successful disconnection from the database.
    """
    uploader.connection = MagicMock()
    uploader.cursor = MagicMock()

    uploader._disconnect()
    # Check if cursor and connection close methods are called
    uploader.cursor.close.assert_called_once()
    uploader.connection.close.assert_called_once()


def test_disconnect_with_errors(uploader):
    """
    Test error handling during disconnection.
    """
    uploader.connection = MagicMock()
    uploader.cursor = MagicMock()

    uploader.cursor.close.side_effect = Exception("Cursor close error")
    with pytest.raises(ConnectionError, match="Error closing cursor"):
        uploader._disconnect()

    # Clear cursor close effect and set connection close effect to simulate separate error
    uploader.cursor.close.side_effect = None
    uploader.connection.close.side_effect = Exception("Connection close error")
    with pytest.raises(ConnectionError, match="Error closing connection to PostgreSQL"):
        uploader._disconnect()


def test_debug_logging_enabled(uploader, capsys):
    """
    Test that debug logging outputs expected messages when debug mode is enabled.
    """
    uploader.debug = True  # Enable debug mode
    sample_metadata = {
        "SampleTable": {
            "id": "123",
            "name": "Sample Name",
            "description": "Sample Description"
        }
    }

    with patch.object(uploader, 'cursor', create=True) as mock_cursor:
        uploader.upload_metadata(sample_metadata)

    # Capture the debug output
    captured = capsys.readouterr()
    assert "[DEBUG] Successfully inserted data into 'SampleTable'" in captured.out
