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
# Define the path for the test GEO IDs file
GEO_IDS_FILE = "./test_geo_ids.txt"
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
    # Initialize the GeoFileHandler with the GEO IDs file and output directory
    return GeoFileHandler(geo_ids_file=GEO_IDS_FILE, output_dir=OUTPUT_DIR, compress_files=False)

@pytest.fixture
def downloader(file_handler):
    """
    Fixture to initialize the GeoMetadataDownloader.
    Args:
        file_handler: The mock GeoFileHandler instance.
    Returns:
        GeoMetadataDownloader: Downloader instance with file handler injected.
    """
    # Initialize the downloader with the output directory and the file handler
    return GeoMetadataDownloader(output_dir=OUTPUT_DIR, debug=True, file_handler=file_handler)

@pytest.fixture
def setup_geo_ids_file():
    """
    Fixture to create a temporary GEO IDs file for testing.
    Yields:
        str: Path to the GEO IDs file.
    """
    # Write sample GEO IDs to the file
    with open(GEO_IDS_FILE, 'w') as f:
        f.writelines(f"{sample_id}\n" for sample_id in SAMPLE_IDS)
    yield GEO_IDS_FILE
    # Remove the GEO IDs file after the test
    os.remove(GEO_IDS_FILE)

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
    # Clean up the directory and its contents after the test
    for file in os.listdir(OUTPUT_DIR):
        os.remove(os.path.join(OUTPUT_DIR, file))
    os.rmdir(OUTPUT_DIR)

# ---------------- Test Cases ----------------
def test_initialization(downloader):
    """
    Test GeoMetadataDownloader initialization.
    Args:
        downloader: The downloader fixture.
    """
    # Assert that the base URL is set correctly
    assert downloader.base_url == "https://ftp.ncbi.nlm.nih.gov/geo/series"
    # Assert that the output directory exists
    assert os.path.exists(OUTPUT_DIR)

def test_download_file_success(downloader, mock_output_dir):
    """
    Test successful download and extraction of a GEO file.
    Args:
        downloader: The downloader fixture.
        mock_output_dir: The mock output directory fixture.
    """
    # Mock the requests.get call to simulate a successful download
    with patch("requests.get") as mock_get:
        # Create a mock response object with a successful status code and content
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"mock tar content"
        mock_get.return_value = mock_response

        # Mock the file operations and tarfile extraction
        with patch("builtins.open", mock_open()) as mock_file:
            with patch("tarfile.open") as mock_tar:
                # Simulate the tarfile extraction process
                mock_tar_instance = MagicMock()
                mock_tar.return_value.__enter__.return_value = mock_tar_instance
                mock_tar_instance.extractall = MagicMock()

                # Mock the os.path.exists call to simulate file presence
                with patch("os.path.exists") as mock_exists:
                    # Define a side effect for file existence checks
                    def exists_side_effect(path):
                        if path == f"{mock_output_dir}/GSE112021_family.xml.tgz":
                            return True
                        if path == f"{mock_output_dir}/GSE112021_family.xml":
                            return True
                        return False

                    mock_exists.side_effect = exists_side_effect

                    # Mock the os.remove call to simulate file cleanup
                    with patch("os.remove") as mock_remove:
                        # Call the download_file method and assert results
                        extracted_path = downloader.download_file("GSE112021")
                        assert extracted_path == f"{mock_output_dir}/GSE112021_family.xml"
                        mock_tar_instance.extractall.assert_called_once_with(path=mock_output_dir)
                        mock_remove.assert_called_once_with(f"{mock_output_dir}/GSE112021_family.xml.tgz")

def test_download_file_invalid_url(downloader):
    """
    Test downloading from an invalid URL.
    Args:
        downloader: The downloader fixture.
    """
    # Mock the requests.get call to simulate a failed download
    with patch("requests.get") as mock_get:
        mock_get.side_effect = requests.RequestException("Invalid URL")

        # Call the download_file method and assert that it returns None
        result = downloader.download_file("INVALID")
        assert result is None

def test_download_file_file_exists(downloader, mock_output_dir):
    """
    Test skipping download if file already exists.
    Args:
        downloader: The downloader fixture.
        mock_output_dir: The mock output directory fixture.
    """
    # Create a mock existing file in the output directory
    existing_file_path = f"{OUTPUT_DIR}/GSE112021_family.xml"
    with open(existing_file_path, "w") as f:
        f.write("mock content")

    # Call the download_file method and assert that the existing file is returned
    result = downloader.download_file("GSE112021")
    assert result == existing_file_path
    os.remove(existing_file_path)

def test_download_file_extraction_error(downloader, mock_output_dir):
    """
    Test handling of errors during file extraction.
    Args:
        downloader: The downloader fixture.
        mock_output_dir: The mock output directory fixture.
    """
    # Mock the requests.get call to simulate a successful download
    with patch("requests.get") as mock_get:
        # Create a mock response object with a successful status code and content
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"mock tar content"
        mock_get.return_value = mock_response

        # Mock the tarfile.open call to simulate an extraction error
        with patch("builtins.open", mock_open()):
            with patch("tarfile.open") as mock_tar:
                mock_tar.side_effect = tarfile.TarError("Extraction failed")

                # Call the download_file method and assert that it returns None
                result = downloader.download_file("GSE112021")
                assert result is None

def test_download_files(downloader, setup_geo_ids_file, mock_output_dir):
    """
    Test downloading multiple GEO files from a GEO IDs file.
    Args:
        downloader: The downloader fixture.
        setup_geo_ids_file: The setup_geo_ids_file fixture.
        mock_output_dir: The mock output directory fixture.
    """
    # Mock the download_file method to simulate multiple downloads
    with patch.object(downloader, "download_file") as mock_download_file:
        mock_download_file.return_value = f"{OUTPUT_DIR}/mock_file.xml"

        # Read the GEO IDs from the test file
        with open(setup_geo_ids_file, "r") as f:
            file_ids = [line.strip() for line in f.readlines()]

        # Call the download_files method and assert the call count
        downloader.download_files(file_ids)
        assert mock_download_file.call_count == len(file_ids)

def test_invalid_geo_id_handling(downloader):
    """
    Test invalid GEO ID input.
    Args:
        downloader: The downloader fixture.
    """
    # Assert that passing an empty GEO ID raises a ValueError
    with pytest.raises(ValueError):
        downloader.download_file("")
