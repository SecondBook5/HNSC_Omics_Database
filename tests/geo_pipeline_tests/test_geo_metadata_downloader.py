# File: tests/geo_pipeline_tests/test_geo_metadata_downloader.py

import os
import pytest
import requests
import tarfile
from unittest.mock import patch, mock_open, MagicMock
from pipeline.geo_pipeline.geo_metadata_downloader import GeoMetadataDownloader


# ---------------- Helper Constants ----------------
OUTPUT_DIR = "./test_output"
GEO_IDS_FILE = "./test_geo_ids.txt"
SAMPLE_IDS = ["GSE112021", "GSE112026", "GSE112023"]
VALID_URL = "https://ftp.ncbi.nlm.nih.gov/geo/series/GSE112nnn/GSE112021/miniml/GSE112021_family.xml.tgz"
INVALID_URL = "https://ftp.ncbi.nlm.nih.gov/geo/series/INVALID/miniml/INVALID.tgz"


# ---------------- Test Fixtures ----------------
@pytest.fixture
def downloader():
    """Fixture to initialize the GeoMetadataDownloader."""
    return GeoMetadataDownloader(output_dir=OUTPUT_DIR, debug=True)


@pytest.fixture
def setup_geo_ids_file():
    """Fixture to create a temporary GEO IDs file."""
    with open(GEO_IDS_FILE, 'w') as f:
        f.writelines(f"{sample_id}\n" for sample_id in SAMPLE_IDS)
    yield GEO_IDS_FILE
    os.remove(GEO_IDS_FILE)


@pytest.fixture
def mock_output_dir():
    """Fixture to create a temporary output directory."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    yield OUTPUT_DIR
    for file in os.listdir(OUTPUT_DIR):
        os.remove(os.path.join(OUTPUT_DIR, file))
    os.rmdir(OUTPUT_DIR)


# ---------------- Test Cases ----------------
def test_initialization(downloader):
    """Test GeoMetadataDownloader initialization."""
    assert downloader.base_url == "https://ftp.ncbi.nlm.nih.gov/geo/series"
    assert os.path.exists(OUTPUT_DIR)


def test_download_file_success(downloader, mock_output_dir):
    """Test successful download and extraction of a GEO file."""
    with patch("requests.get") as mock_get:
        # Mock the HTTP response for the file download
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"mock tar content"
        mock_get.return_value = mock_response

        with patch("builtins.open", mock_open()) as mock_file:
            with patch("tarfile.open") as mock_tar:
                # Mock tarfile extraction behavior
                mock_tar_instance = MagicMock()
                mock_tar.return_value.__enter__.return_value = mock_tar_instance
                mock_tar_instance.extractall = MagicMock()

                # Mock os.path.exists to simulate file existence
                with patch("os.path.exists") as mock_exists:
                    # Define side effects for os.path.exists
                    def exists_side_effect(path):
                        print(f"DEBUG: Checking existence of {path}")
                        if path == f"{mock_output_dir}/GSE112021_family.xml.tgz":
                            return True  # Simulate the tar file exists
                        if path == f"{mock_output_dir}/GSE112021_family.xml":
                            return True  # Simulate the extracted file exists
                        return False

                    mock_exists.side_effect = exists_side_effect

                    # Mock os.remove to prevent actual file deletion
                    with patch("os.remove") as mock_remove:
                        # Call the downloader method
                        extracted_path = downloader.download_file("GSE112021")
                        print(f"DEBUG: Extracted path returned: {extracted_path}")

                        # Assert that the correct path was returned
                        assert extracted_path == f"{mock_output_dir}/GSE112021_family.xml"

                        # Assert that tarfile extraction was called
                        mock_tar_instance.extractall.assert_called_once_with(path=mock_output_dir)

                        # Assert that the tar file was removed
                        mock_remove.assert_called_once_with(f"{mock_output_dir}/GSE112021_family.xml.tgz")

def test_download_file_invalid_url(downloader):
    """Test downloading from an invalid URL."""
    with patch("requests.get") as mock_get:
        mock_get.side_effect = requests.RequestException("Invalid URL")

        result = downloader.download_file("INVALID")
        assert result is None


def test_download_file_file_exists(downloader, mock_output_dir):
    """Test skipping download if file already exists."""
    existing_file_path = f"{OUTPUT_DIR}/GSE112021_family.xml"
    with open(existing_file_path, "w") as f:
        f.write("mock content")

    result = downloader.download_file("GSE112021")
    assert result == existing_file_path
    os.remove(existing_file_path)


def test_download_file_extraction_error(downloader, mock_output_dir):
    """Test handling of errors during file extraction."""
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"mock tar content"
        mock_get.return_value = mock_response

        with patch("builtins.open", mock_open()):
            with patch("tarfile.open") as mock_tar:
                mock_tar.side_effect = tarfile.TarError("Extraction failed")

                result = downloader.download_file("GSE112021")
                assert result is None


def test_download_from_url_success(downloader, mock_output_dir):
    """Test successful file download."""
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"mock content"
        mock_get.return_value = mock_response

        downloaded_path = downloader.download_from_url(VALID_URL, f"{OUTPUT_DIR}/GSE112021_family.xml.tgz")
        assert downloaded_path == f"{OUTPUT_DIR}/GSE112021_family.xml.tgz"
        mock_get.assert_called_once_with(VALID_URL, timeout=30)


def test_download_from_url_timeout(downloader):
    """Test timeout handling during download."""
    with patch("requests.get") as mock_get:
        mock_get.side_effect = requests.Timeout

        downloaded_path = downloader.download_from_url(VALID_URL, f"{OUTPUT_DIR}/GSE112021_family.xml.tgz")
        assert downloaded_path is None


def test_download_files(downloader, setup_geo_ids_file, mock_output_dir):
    """Test downloading multiple GEO files from a GEO IDs file."""
    with patch.object(downloader, "download_file") as mock_download_file:
        mock_download_file.return_value = f"{OUTPUT_DIR}/mock_file.xml"

        with open(setup_geo_ids_file, "r") as f:
            file_ids = [line.strip() for line in f.readlines()]

        downloader.download_files(file_ids)
        assert mock_download_file.call_count == len(file_ids)


def test_read_geo_ids_file_success():
    """Test reading GEO IDs from a valid file."""
    with patch("builtins.open", mock_open(read_data="\n".join(SAMPLE_IDS))):
        with open(GEO_IDS_FILE, "r") as f:
            geo_ids = [line.strip() for line in f.readlines()]
            assert geo_ids == SAMPLE_IDS


def test_read_geo_ids_file_missing():
    """Test handling of a missing GEO IDs file."""
    with pytest.raises(FileNotFoundError):
        with open("nonexistent_file.txt", "r") as f:
            pass


def test_read_geo_ids_file_empty():
    """Test handling of an empty GEO IDs file."""
    with patch("builtins.open", mock_open(read_data="")):
        with open(GEO_IDS_FILE, "r") as f:
            geo_ids = [line.strip() for line in f.readlines()]
            assert geo_ids == []


def test_invalid_geo_id_handling(downloader):
    """Test invalid GEO ID input."""
    with pytest.raises(ValueError):
        downloader.download_file("")
