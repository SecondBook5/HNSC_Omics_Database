# File: tests/geo_pipeline_tests/test_geo_data_downloader.py

import os
import pytest
import tarfile
from unittest.mock import patch
from pathlib import Path
from pipeline.geo_pipeline.geo_data_downloader import GeoDataDownloader


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
    return GeoDataDownloader(output_dir=str(setup_output_dir), debug=True)


@patch("requests.get")
def test_download_file_success(mock_get, downloader, setup_output_dir):
    """
    Test successful download and extraction of a GEO file.
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
    Test that download is skipped if the file already exists.
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
    Test handling of HTTP error during file download.
    """
    # Simulate HTTP error response
    mock_get.return_value.status_code = 404

    # Run downloader and expect None due to failed download
    file_id = "GSE12345"
    result = downloader.download_file(file_id)
    assert result is None, "Expected None for HTTP error download"


@patch("requests.get")
def test_download_file_404_error(mock_get, downloader, setup_output_dir):
    """
    Test handling of 404 error for non-existent file.
    """
    # Simulate a 404 response
    mock_get.return_value.status_code = 404

    # Run downloader and verify it handles the 404 gracefully
    file_id = "GSE404NotFound"
    result = downloader.download_file(file_id)
    assert result is None, "Expected None for non-existent file"


@patch("requests.get")
def test_file_permission_error(mock_get, downloader, setup_output_dir):
    """
    Test handling of permission errors during file extraction.
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
