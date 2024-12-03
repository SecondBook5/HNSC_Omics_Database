# File: tests/geo_pipeline_tests/test_geo_metadata_downloader.py

import os
import pytest
import requests
import tarfile
from unittest.mock import patch, mock_open, MagicMock
from pipeline.geo_pipeline.geo_metadata_downloader import GeoMetadataDownloader
from pipeline.geo_pipeline.geo_file_handler import GeoFileHandler  # Importing the file handler for dependency injection

# ---------------- Helper Constants ----------------
# Define the output directory for testing
OUTPUT_DIR = "./test_output"
# Sample GEO series IDs for testing
SAMPLE_IDS = ["GSE112021", "GSE112026", "GSE112023"]
# Valid and invalid URLs for testing download functionality
VALID_URL = "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE112nnn/GSE112021/miniml/GSE112021_family.xml.tgz"
INVALID_URL = "https://ftp.ncbi.nlm.nih.gov/geo/series/INVALID/miniml/INVALID.tgz"

# ---------------- Test Fixtures ----------------
@pytest.fixture
def file_handler():
    """
    Fixture to initialize a mock GeoFileHandler.
    Returns:
        GeoFileHandler: A mock file handler for dependency injection.
    """
    mock_handler = MagicMock()
    mock_handler.log_download = MagicMock()
    return mock_handler

@pytest.fixture
def downloader(file_handler):
    """
    Fixture to initialize the GeoMetadataDownloader.
    Args:
        file_handler: The mock GeoFileHandler instance.
    Returns:
        GeoMetadataDownloader: Downloader instance with file handler injected.
    """
    return GeoMetadataDownloader(output_dir=OUTPUT_DIR, file_handler=file_handler, debug=True)

@pytest.fixture
def mock_output_dir():
    """
    Fixture to create a temporary output directory for testing.
    Yields:
        str: Path to the output directory.
    """
    # Create the output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    yield OUTPUT_DIR
    # Cleanup the directory and its contents after the test
    for entry in os.listdir(OUTPUT_DIR):
        path = os.path.join(OUTPUT_DIR, entry)
        if os.path.isfile(path):
            os.remove(path)
        elif os.path.isdir(path):
            # Recursively remove directories
            for root, dirs, files in os.walk(path, topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))
            os.rmdir(path)
    os.rmdir(OUTPUT_DIR)


# ---------------- Test Cases ----------------
def test_initialization(downloader):
    """
    Test GeoMetadataDownloader initialization.
    Args:
        downloader: The downloader fixture.
    """
    assert downloader.base_url == "https://ftp.ncbi.nlm.nih.gov/geo/series"
    assert os.path.exists(OUTPUT_DIR)

def test_download_file_success(downloader, mock_output_dir):
    """
    Test successful download and extraction of a GEO file.
    """
    with patch("requests.get") as mock_get, \
         patch("builtins.open", mock_open()), \
         patch("os.makedirs", return_value=True), \
         patch("os.path.exists", side_effect=lambda path: True if path == "./test_output/GSE112021_family.xml.tgz" else False), \
         patch("tarfile.open") as mock_tar, \
         patch("os.listdir", return_value=["GSE112021_family.xml"]):

        # Mock response for GET request
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"mock tar content"
        mock_get.return_value = mock_response

        # Mock tarfile extraction
        mock_tar_instance = MagicMock()
        mock_tar.return_value.__enter__.return_value = mock_tar_instance

        # Perform test
        result = downloader.download_file("GSE112021")

        # Verify expectations
        assert result == ["./test_output/GSE112021/GSE112021_family.xml"]
        mock_tar_instance.extractall.assert_called_once_with(path="./test_output/GSE112021")

def test_download_file_invalid_url(downloader):
    """
    Test downloading from an invalid URL.
    Args:
        downloader: The downloader fixture.
    """
    with patch("requests.get", side_effect=requests.RequestException("Invalid URL")):
        result = downloader.download_file("INVALID")
        assert result is None

def test_download_file_extraction_error(downloader, mock_output_dir):
    """
    Test handling of errors during file extraction.
    Args:
        downloader: The downloader fixture.
        mock_output_dir: The mock output directory fixture.
    """
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"mock tar content"
        mock_get.return_value = mock_response

        with patch("tarfile.open", side_effect=tarfile.TarError("Extraction failed")):
            result = downloader.download_file("GSE112021")
            assert result is None

def test_download_files(downloader, mock_output_dir):
    """
    Test downloading multiple GEO files.
    Args:
        downloader: The downloader fixture.
        mock_output_dir: The mock output directory fixture.
    """
    with patch.object(downloader, "download_file", return_value=["mock_file.xml"]) as mock_download_file:
        downloader.download_files(SAMPLE_IDS)
        assert mock_download_file.call_count == len(SAMPLE_IDS)

def test_invalid_geo_id_handling(downloader):
    """
    Test invalid GEO ID input.
    Args:
        downloader: The downloader fixture.
    """
    with pytest.raises(ValueError):
        downloader.download_file("")