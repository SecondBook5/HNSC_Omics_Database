# File: tests/geo_pipeline_tests/test_geo_metadata_downloader.py

import os
import pytest
import requests
import tarfile
from unittest.mock import patch, mock_open, MagicMock
from requests.exceptions import HTTPError, Timeout
from pipeline.geo_pipeline.geo_metadata_downloader import GeoMetadataDownloader
from pipeline.geo_pipeline.geo_file_handler import GeoFileHandler

# ---------------- Helper Constants ----------------
OUTPUT_DIR = "./test_output"  # Output directory for testing
SAMPLE_IDS = ["GSE112021", "GSE112026", "GSE112023"]  # Sample GEO IDs
VALID_URL = "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE112nnn/GSE112021/miniml/GSE112021_family.xml.tgz"
INVALID_URL = "https://ftp.ncbi.nlm.nih.gov/geo/series/INVALID/miniml/INVALID.tgz"

# ---------------- Test Fixtures ----------------
@pytest.fixture
def file_handler():
    """Mock GeoFileHandler for testing."""
    mock_handler = MagicMock()
    mock_handler.log_download = MagicMock()
    return mock_handler

@pytest.fixture
def downloader(file_handler):
    """Initialize GeoMetadataDownloader with mock file handler."""
    return GeoMetadataDownloader(output_dir=OUTPUT_DIR, file_handler=file_handler, debug=True)

@pytest.fixture
def mock_output_dir():
    """Create and cleanup a temporary output directory."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    yield OUTPUT_DIR
    for entry in os.listdir(OUTPUT_DIR):
        path = os.path.join(OUTPUT_DIR, entry)
        if os.path.isfile(path):
            os.remove(path)
        elif os.path.isdir(path):
            for root, dirs, files in os.walk(path, topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))
            os.rmdir(path)
    os.rmdir(OUTPUT_DIR)

# ---------------- Test Cases ----------------
def test_initialization(downloader):
    """Test initialization of GeoMetadataDownloader."""
    assert downloader.base_url == "https://ftp.ncbi.nlm.nih.gov/geo/series"
    assert os.path.exists(OUTPUT_DIR)

def test_download_file_success(downloader, mock_output_dir):
    with patch("requests.get") as mock_get, \
         patch("builtins.open", mock_open()), \
         patch("os.path.exists", side_effect=lambda path: True), \
         patch("tarfile.open") as mock_tar:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"mock tar content"
        mock_get.return_value = mock_response

        mock_tar_instance = MagicMock()
        mock_tar.return_value.__enter__.return_value = mock_tar_instance
        mock_tar_instance.extractall = MagicMock()

        result = downloader.download_file("GSE112021")
        assert result is not None
        mock_tar_instance.extractall.assert_called_once()


def test_download_file_invalid_url(downloader):
    """Test invalid URL handling."""
    with patch("requests.get", side_effect=requests.RequestException("Invalid URL")):
        result = downloader.download_file("INVALID")
        assert result is None

def test_download_file_extraction_error(downloader, mock_output_dir):
    """Test extraction errors during download."""
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"mock tar content"
        mock_get.return_value = mock_response

        with patch("tarfile.open", side_effect=tarfile.TarError("Extraction failed")):
            result = downloader.download_file("GSE112021")
            assert result is None

def test_download_files(downloader, mock_output_dir):
    """Test downloading multiple files."""
    with patch.object(downloader, "download_file", return_value=["mock_file.xml"]) as mock_download_file:
        downloader.download_files(SAMPLE_IDS)
        assert mock_download_file.call_count == len(SAMPLE_IDS)

def test_invalid_geo_id_handling(downloader):
    """Test handling of invalid GEO IDs."""
    with pytest.raises(ValueError):
        downloader.download_file("")

def test_retry_logic_on_timeout(downloader):
    with patch("requests.Session.send", side_effect=Timeout("Request timed out")) as mock_send:
        result = downloader.download_file("GSE112021")
        assert result is None
        assert mock_send.call_count == 3  # Ensure retry logic is applied

def test_specific_http_error_handling(downloader):
    """Test handling of specific HTTP errors (e.g., 404)."""
    with patch("requests.Session.send") as mock_send:
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = HTTPError("404 Not Found")
        mock_send.return_value = mock_response

        result = downloader.download_file("GSE112021")
        assert result is None

def test_unexpected_exception_handling(downloader):
    """Test handling of unexpected exceptions."""
    with patch("requests.Session.send", side_effect=Exception("Unexpected error")) as mock_send:
        result = downloader.download_file("GSE112021")
        assert result is None
        mock_send.assert_called_once()

def test_recoverable_http_error_handling(downloader):
    with patch("requests.Session.send") as mock_send:
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = HTTPError("500 Internal Server Error")
        mock_send.return_value = mock_response

        result = downloader.download_file("GSE112021")
        assert result is None
        assert mock_send.call_count == 3

def test_logging_of_failed_retries(downloader, caplog):
    with patch("requests.Session.send", side_effect=Timeout("Request timed out")):
        result = downloader.download_file("GSE112021")
        assert result is None
        assert "Timeout" in caplog.text
        assert "Retrying..." in caplog.text
