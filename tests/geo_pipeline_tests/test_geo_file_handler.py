# File: tests/geo_pipeline_tests/test_geo_file_handler.py

import zipfile
import pytest
from unittest.mock import patch, MagicMock, call
import os
from pipeline.geo_pipeline.geo_file_handler import GeoFileHandler
from sqlalchemy.exc import SQLAlchemyError
from db.schema.geo_metadata_schema import GeoMetadataLog
from datetime import date
from sqlalchemy.dialects.postgresql import insert
from unittest.mock import mock_open

@pytest.fixture
def geo_file_handler():
    """
    Fixture to create an instance of GeoFileHandler with mock configurations.
    """
    with patch("pipeline.geo_pipeline.geo_file_handler.configure_logger") as mock_logger:
        logger = MagicMock()
        mock_logger.return_value = logger
        handler = GeoFileHandler(geo_ids_file=None, output_dir="./test_output", compress_files=False, logger=logger)
        yield handler


@patch("os.makedirs")
def test_geo_file_handler_initialization(mock_makedirs):
    """
    Test initialization of GeoFileHandler.
    """
    handler = GeoFileHandler(geo_ids_file=None, output_dir="./test_output", compress_files=True)
    assert handler.output_dir == "./test_output"
    assert handler.compress_files is True
    expected_calls = [
        call("./test_output", exist_ok=True),
        call("/mnt/c/Users/ajboo/BookAbraham/BiologicalDatabases/HNSC_Omics_Database/logs", exist_ok=True),
    ]
    mock_makedirs.assert_has_calls(expected_calls, any_order=True)


@patch("os.path.exists", side_effect=lambda path: path == "./test_geo_ids.txt")
@patch("os.path.isfile", return_value=True)
@patch("pipeline.geo_pipeline.geo_file_handler.get_session_context")
def test_initialize_log_table(mock_session_context, mock_isfile, mock_exists, geo_file_handler):
    """
    Test the initialize_log_table method.
    """
    mock_session = MagicMock()
    mock_session_context.return_value.__enter__.return_value = mock_session

    geo_file_handler.geo_ids_file = "./test_geo_ids.txt"

    # Mock file content as a list of valid GEO IDs
    file_content = ["GEO123\n", "GEO456\n"]
    with patch("builtins.open", mock_open(read_data="".join(file_content))):
        geo_file_handler.initialize_log_table()

    # Validate the inserted data
    expected_values = [
        {
            "GeoID": "GEO123",
            "Status": "not_downloaded",
            "Message": "Pending download.",
            "FileNames": [],
            "Timestamp": date.today(),
        },
        {
            "GeoID": "GEO456",
            "Status": "not_downloaded",
            "Message": "Pending download.",
            "FileNames": [],
            "Timestamp": date.today(),
        },
    ]

    actual_values = [
        call[0][0].compile().params for call in mock_session.execute.call_args_list
    ]

    for expected in expected_values:
        assert any(
            all(expected[key] == actual[key] for key in expected)
            for actual in actual_values
        )

    mock_session.commit.assert_called_once()


@patch("os.path.exists", return_value=True)
@patch("os.path.isfile", side_effect=lambda path: "file" in path)
@patch("pipeline.geo_pipeline.geo_file_handler.get_session_context")
def test_log_download(mock_session_context, mock_isfile, mock_exists, geo_file_handler):
    """
    Test the log_download method.
    """
    mock_session = MagicMock()
    mock_session_context.return_value.__enter__.return_value = mock_session

    geo_file_handler.log_download("GEO123", ["file1.txt", "file2.txt"])

    # Verify database interactions
    mock_session.execute.assert_called_once()
    mock_session.commit.assert_called_once()


@patch("os.path.exists", return_value=True)
@patch("pipeline.geo_pipeline.geo_file_handler.get_session_context")
def test_log_processed(mock_session_context, mock_exists, geo_file_handler):
    """
    Test the log_processed method.
    """
    mock_session = MagicMock()
    mock_session_context.return_value.__enter__.return_value = mock_session

    geo_file_handler.log_processed("GEO123")

    # Verify database interactions
    mock_session.execute.assert_called_once()
    mock_session.commit.assert_called_once()


@patch("os.path.exists")
@patch("os.path.isfile", return_value=True)
@patch("os.remove")
@patch("os.rmdir")
@patch("zipfile.ZipFile", autospec=True)
def test_clean_files(mock_zipfile, mock_rmdir, mock_remove, mock_isfile, mock_exists):
    """
    Test the clean_files method.
    """
    # Simulated file structure
    file_structure = [("./test_output/GEO123", [], ["file1.txt", "file2.txt"])]

    # Track deletions
    deleted_directories = set()

    def mock_exists_side_effect(path):
        if path in deleted_directories:
            return False
        if path.endswith(".zip"):
            return True  # Simulate ZIP file's existence after compression
        return True  # Simulate directory existence

    def mock_rmdir_side_effect(path):
        deleted_directories.add(path)

    mock_exists.side_effect = mock_exists_side_effect
    mock_rmdir.side_effect = mock_rmdir_side_effect

    # Create GeoFileHandler with compress_files=True
    geo_file_handler = GeoFileHandler(
        geo_ids_file=None,
        output_dir="./test_output",
        compress_files=True,  # Ensure compression logic is tested
    )

    # Mock os.walk
    with patch("os.walk", return_value=file_structure):
        geo_file_handler.clean_files("GEO123")

    # Debugging: Check all calls made to mock_zipfile
    print("mock_zipfile.call_args_list:", mock_zipfile.call_args_list)

    # Assertions for zip creation
    mock_zipfile.assert_called_once_with("./test_output/GEO123.zip", "w", zipfile.ZIP_DEFLATED)
    zip_instance = mock_zipfile.return_value.__enter__.return_value
    zip_instance.write.assert_any_call(
        os.path.join("./test_output/GEO123", "file1.txt"), arcname="GEO123/file1.txt"
    )
    zip_instance.write.assert_any_call(
        os.path.join("./test_output/GEO123", "file2.txt"), arcname="GEO123/file2.txt"
    )

    # Assertions for directory deletion
    assert "./test_output/GEO123" in deleted_directories


@patch("os.path.exists", return_value=True)
@patch("os.path.isfile", return_value=True)
def test_empty_geo_ids_file(mock_isfile, mock_exists, geo_file_handler):
    """
    Test behavior when `geo_ids_file` is empty.
    """
    # Mock file content to be empty
    with patch("builtins.open", mock_open(read_data="")):
        # Expect ValueError to be raised for empty GEO IDs file
        with pytest.raises(ValueError, match="GEO IDs file must be provided for batch initialization."):
            geo_file_handler.initialize_log_table()


@patch("os.path.exists", return_value=False)
def test_nonexistent_geo_ids_file(mock_exists, geo_file_handler):
    """
    Test behavior when `geo_ids_file` does not exist.
    """
    # Set a nonexistent file path
    geo_file_handler.geo_ids_file = "./nonexistent_geo_ids.txt"

    # Expect FileNotFoundError to be raised for nonexistent file
    with pytest.raises(FileNotFoundError, match=f"GEO IDs file not found: {geo_file_handler.geo_ids_file}"):
        geo_file_handler.initialize_log_table()
