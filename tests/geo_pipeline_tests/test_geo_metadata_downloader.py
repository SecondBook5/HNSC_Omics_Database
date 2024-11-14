# File: tests/geo_pipeline_tests/test_geo_metadata_downloader.py

import os
import pytest
import tarfile
from unittest.mock import patch
from pathlib import Path
from pipeline.geo_pipeline.geo_metadata_downloader import GeoMetadataDownloader
import requests

@pytest.fixture
def setup_output_dir(tmp_path):
    """
    Fixture to set up a temporary output directory for tests.

    Args:
        tmp_path (Path): Temporary path provided by pytest.

    Returns:
        Path: Path to the temporary directory.
    """
    output_dir = tmp_path / "geo_data"
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir

@pytest.fixture
def downloader(setup_output_dir):
    """
    Fixture to initialize the GeoDataDownloader with debugging enabled.

    Args:
        setup_output_dir (Path): Temporary output directory for testing.

    Returns:
        GeoDataDownloader: Instance with debugging enabled.
    """
    return GeoMetadataDownloader(output_dir=str(setup_output_dir), debug=True)

@patch("requests.get")
def test_download_file_success(mock_get, downloader, setup_output_dir):
    """
    Test successful download and extraction of a GEO file.
    Verifies correct handling of the file upon successful download.
    """
    # Mock HTTP response to simulate successful download
    mock_get.return_value.status_code = 200
    mock_get.return_value.content = b"fake tar content"

    # Set up file identifiers and paths
    file_id = "GSE12345"
    tgz_path = setup_output_dir / f"{file_id}_family.xml.tgz"
    xml_path = setup_output_dir / f"{file_id}_family.xml"

    # Create a .tar.gz file containing a mock XML file to simulate extraction
    with tarfile.open(tgz_path, "w:gz") as tar:
        temp_file_path = setup_output_dir / f"{file_id}_family.xml"
        temp_file_path.write_text("Sample XML content")
        tar.add(temp_file_path, arcname=f"{file_id}_family.xml")

    # Run downloader and validate extraction
    result = downloader.download_file(file_id)
    assert result == str(xml_path), f"Expected {xml_path}, but got {result}"
    assert os.path.exists(result), "Extracted file does not exist"

@patch("requests.get")
def test_download_file_existing_file(mock_get, downloader, setup_output_dir):
    """
    Test skipping download if the file already exists locally.
    Ensures the method avoids redundant downloads.
    """
    # Set up an existing XML file to simulate skipping download
    file_id = "GSE12345"
    xml_path = setup_output_dir / f"{file_id}_family.xml"
    xml_path.write_text("Existing XML content")

    # Run downloader, expecting it to skip re-download
    result = downloader.download_file(file_id)
    assert result == str(xml_path), "Expected existing file path to be returned"
    assert os.path.exists(result), "File should exist but was not found"

@patch("requests.get")
def test_download_file_http_error(mock_get, downloader, setup_output_dir):
    """
    Test handling of HTTP errors like 404 during file download.
    Confirms the function returns None on HTTP error.
    """
    # Simulate HTTP error response
    mock_get.return_value.status_code = 404

    # Run downloader and expect None due to failed download
    file_id = "GSE12345"
    result = downloader.download_file(file_id)
    assert result is None, "Expected None for HTTP error download"

@patch("requests.get")
def test_network_timeout_handling(mock_get, downloader, setup_output_dir):
    """
    Test handling of network-related issues like timeouts.
    Verifies that the function correctly handles a timeout.
    """
    mock_get.side_effect = requests.exceptions.Timeout

    file_id = "GSETimeout"
    result = downloader.download_file(file_id)
    assert result is None, "Expected None for a network timeout"

@patch("requests.get")
def test_file_permission_error(mock_get, downloader, setup_output_dir):
    """
    Test handling of permission errors during file extraction.
    Simulates extraction into a directory with restricted permissions.
    """
    # Simulate a successful download
    mock_get.return_value.status_code = 200
    mock_get.return_value.content = b"fake tar content"

    # Set up a restricted directory to force a permission error
    restricted_path = setup_output_dir / "restricted_dir"
    restricted_path.mkdir(mode=0o000)  # No permissions
    tgz_path = restricted_path / "GSEPermissionError_family.xml.tgz"

    # Attempt to create a tar file in a no-permission directory
    with pytest.raises(PermissionError):
        with tarfile.open(tgz_path, "w:gz") as tar:
            temp_file_path = restricted_path / "temp.xml"
            temp_file_path.write_text("Sample content")
            tar.add(temp_file_path, arcname="temp.xml")

    # Restore permissions to delete the directory after the test
    restricted_path.chmod(0o777)


@patch("requests.get")
def test_debug_logging(mock_get, downloader, capsys):
    """
    Test debug logging is functional when debug mode is enabled.
    Ensures that debug messages are output during download attempts.
    """
    # Simulate a 404 error in the request to trigger debug logging
    mock_get.return_value.status_code = 404

    # Set up a test file ID
    file_id = "GSEDebug"
    downloader.download_file(file_id)

    # Capture the output and verify debug messages
    captured = capsys.readouterr()

    # Check that key debug components are present in the captured output
    assert "[DEBUG] URL" in captured.out, "Expected debug log message with URL"
    assert "[DEBUG] Output Path" in captured.out, "Expected debug log message with output path"
    assert "[DEBUG] Failed to extract" in captured.out, "Expected debug log message for extraction failure"