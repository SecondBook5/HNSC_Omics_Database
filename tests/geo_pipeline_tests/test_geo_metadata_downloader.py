import os
import pytest
import tarfile
from unittest.mock import MagicMock, patch, mock_open
from pipeline.geo_pipeline.geo_metadata_downloader import GeoMetadataDownloader
from pipeline.geo_pipeline.geo_file_handler import GeoFileHandler

@pytest.fixture
def downloader(tmp_path):
    """Fixture to create a GeoMetadataDownloader instance."""
    mock_file_handler = MagicMock(spec=GeoFileHandler)
    output_dir = str(tmp_path)
    return GeoMetadataDownloader(output_dir=output_dir, file_handler=mock_file_handler, debug=True)

def test_initialization(downloader, tmp_path):
    """Test that the downloader initializes with valid arguments."""
    assert downloader.output_dir == str(tmp_path)
    assert isinstance(downloader.file_handler, GeoFileHandler)


def test_download_file_success(downloader):
    """Test successful file download and extraction."""
    with patch("requests.get") as mock_get, \
         patch("builtins.open", mock_open()), \
         patch("os.makedirs", return_value=True), \
         patch("os.path.exists", return_value=True), \
         patch("os.listdir", return_value=["GSE112021_family.xml"]), \
         patch("tarfile.open") as mock_tar, \
         patch("os.remove", return_value=None), \
         patch("os.path.isfile", side_effect=lambda x: "GSE112021_family.xml" in x):  # Simulate valid files in extracted dir

        # Mock response for requests.get
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"mock tar content"
        mock_get.return_value = mock_response

        # Mock tarfile extraction
        mock_tar_instance = MagicMock()
        mock_tar.return_value.__enter__.return_value = mock_tar_instance
        mock_tar_instance.extractall = MagicMock()

        # Expected path of the extracted file
        extracted_file_path = os.path.join(downloader.output_dir, "GSE112021", "GSE112021_family.xml")

        # Perform the download
        result = downloader.download_file("GSE112021")

        # Assert the result matches the expected file path
        assert result == [extracted_file_path]

        # Ensure `os.remove` is called to clean up the tar file
        mock_tar_instance.extractall.assert_called_once()
        mock_tar.assert_called_once_with(os.path.join(downloader.output_dir, "GSE112021_family.xml.tgz"), 'r:gz')


def test_download_file_invalid_url(downloader):
    """Test behavior when an invalid URL is constructed."""
    with patch("requests.get", side_effect=ValueError("Invalid URL")):
        result = downloader.download_file("INVALID")
        assert result is None

def test_download_file_extraction_error(downloader):
    """Test behavior when extraction fails."""
    with patch("requests.get") as mock_get, \
         patch("builtins.open", mock_open()), \
         patch("tarfile.open", side_effect=tarfile.TarError("Extraction failed")):
        # Mock response for requests.get
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"mock tar content"
        mock_get.return_value = mock_response

        # Perform the download
        result = downloader.download_file("GSE112021")
        assert result is None

def test_download_files(downloader):
    """Test downloading multiple GEO files."""
    with patch.object(downloader, "download_file", return_value=["mock_file.xml"]) as mock_download:
        downloader.download_files(["GSE112021", "GSE112026"])
        assert mock_download.call_count == 2

def test_invalid_geo_id_handling(downloader):
    """Test behavior when a GEO ID is invalid."""
    with pytest.raises(ValueError, match="File ID cannot be empty."):
        downloader.download_file("")

def test_logging_of_failed_retries(downloader, caplog):
    """Test logging of failed retries."""
    with patch("requests.get", side_effect=Exception("Mocked Exception")):
        result = downloader.download_file("GSE112021")
        assert result is None
        assert "Failed to download file for GEO ID GSE112021" in caplog.text

def test_extract_file_no_tarfile(downloader):
    """Test behavior when a tar.gz file does not exist."""
    with patch("os.path.exists", return_value=False):
        result = downloader._extract_file("nonexistent.tar.gz", "/mock/directory")
        assert result is None

def test_extract_file_os_error(downloader):
    """Test behavior when an OS error occurs during extraction."""
    with patch("os.path.exists", return_value=True), \
         patch("tarfile.open", side_effect=OSError("OS error")):
        result = downloader._extract_file("mock.tar.gz", "/mock/directory")
        assert result is None
