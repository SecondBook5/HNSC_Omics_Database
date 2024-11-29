# File: tests/geo_pipeline_tests/test_geo_file_handler.py

import os
import zipfile
import pytest
from unittest.mock import patch, MagicMock, mock_open
from datetime import date
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from pipeline.geo_pipeline.geo_file_handler import GeoFileHandler
from db.schema.geo_metadata_schema import GeoMetadataLog

# Mock database connection URL
TEST_DB_URL = "sqlite:///:memory:"


# ---------------- Test Fixtures ----------------

@pytest.fixture(scope="session")
def engine():
    """Create an SQLite in-memory database engine for testing."""
    return create_engine(TEST_DB_URL, connect_args={"check_same_thread": False})


@pytest.fixture(scope="function")
def db_session(engine):
    """Set up a database session for tests and tear it down afterward."""
    GeoMetadataLog.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()
    GeoMetadataLog.metadata.drop_all(engine)


@pytest.fixture
def geo_file_handler(tmp_path):
    """Create a GeoFileHandler instance for testing."""
    geo_ids_file = tmp_path / "geo_ids.txt"
    geo_ids_file.write_text("GSE123456\nGSE789012\n")
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    return GeoFileHandler(geo_ids_file=str(geo_ids_file), output_dir=str(output_dir), compress_files=False)


# ---------------- Test Cases ----------------

def test_initialize_log_table(db_session, geo_file_handler):
    """Test the initialization of the log table."""
    with patch("pipeline.geo_pipeline.geo_file_handler.get_session_context", return_value=db_session):
        # Initialize the log table
        geo_file_handler.initialize_log_table()

        # Verify that GEO IDs were added to the database
        logs = db_session.query(GeoMetadataLog).all()
        assert len(logs) == 2
        assert logs[0].GeoID == "GSE123456"
        assert logs[0].Status == "not_downloaded"


def test_log_download(db_session, geo_file_handler, tmp_path):
    """Test logging a download operation."""
    # Create files to simulate a successful download
    geo_dir = tmp_path / "output" / "GSE123456"
    geo_dir.mkdir(parents=True)
    (geo_dir / "file1.xml").write_text("File content 1")
    (geo_dir / "file2.xml").write_text("File content 2")

    with patch("pipeline.geo_pipeline.geo_file_handler.get_session_context", return_value=db_session):
        # Log a download for a specific GEO ID
        geo_file_handler.log_download(geo_id="GSE123456", file_names=["file1.xml", "file2.xml"])

        # Verify the log entry
        log = db_session.query(GeoMetadataLog).filter_by(GeoID="GSE123456").first()
        assert log is not None
        assert log.Status == "downloaded"
        assert log.FileNames == ["file1.xml", "file2.xml"]


def test_log_processed(db_session, geo_file_handler):
    """Test logging a processed operation."""
    with patch("pipeline.geo_pipeline.geo_file_handler.get_session_context", return_value=db_session):
        # Log a processed operation for a specific GEO ID
        geo_file_handler.log_processed(geo_id="GSE123456")

        # Verify the log entry
        log = db_session.query(GeoMetadataLog).filter_by(GeoID="GSE123456").first()
        assert log is not None
        assert log.Status == "processed"
        assert log.Message == "Metadata uploaded successfully."


def test_clean_files_no_files(geo_file_handler):
    """Test cleaning up files when no files exist."""
    with patch("os.path.exists", return_value=False):
        # Attempt to clean files for a non-existent GEO ID
        geo_file_handler.clean_files(geo_id="GSE123456")
        # Ensure no exception is raised


def test_clean_files_delete(tmp_path, geo_file_handler):
    """Test cleaning up files by deleting them."""
    # Create a directory and files for the test
    geo_dir = tmp_path / "output" / "GSE123456"
    geo_dir.mkdir(parents=True)
    (geo_dir / "file1.xml").write_text("Test content")
    (geo_dir / "file2.xml").write_text("Test content")

    # Track removed files and directory state
    removed_files = []
    removed_dirs = []

    # Patch os.path.exists to simulate file and directory existence correctly
    def mock_exists(path):
        # Simulate removal: return False for removed files/directories
        return path not in removed_files and path not in removed_dirs

    # Patch os.remove and os.rmdir to track removals
    def mock_remove(path):
        removed_files.append(path)  # Mark file as removed

    def mock_rmdir(path):
        removed_dirs.append(path)  # Mark directory as removed

    with patch("os.path.exists", side_effect=mock_exists), \
         patch("os.remove", side_effect=mock_remove), \
         patch("os.rmdir", side_effect=mock_rmdir):
        # Clean files
        geo_file_handler.clean_files(geo_id="GSE123456")

        # Assertions
        # Ensure files were removed
        assert str(geo_dir / "file1.xml") in removed_files
        assert str(geo_dir / "file2.xml") in removed_files

        # Ensure directory was removed
        assert str(geo_dir) in removed_dirs


def test_clean_files_compress(tmp_path):
    """Test cleaning up files by compressing them."""
    geo_dir = tmp_path / "output" / "GSE123456"
    geo_dir.mkdir(parents=True)
    (geo_dir / "file1.xml").write_text("Test content")
    file_handler = GeoFileHandler(geo_ids_file=None, output_dir=str(tmp_path / "output"), compress_files=True)
    zip_path = str(geo_dir) + ".zip"

    # Patch os.path.exists to simulate file and directory existence
    with patch("os.path.exists", side_effect=lambda p: p in [str(geo_dir), zip_path]):
        # Perform the cleanup, which compresses the files
        file_handler.clean_files(geo_id="GSE123456")

        # Verify that the zip file was created
        assert os.path.exists(zip_path)

        # Verify the contents of the zip file
        with zipfile.ZipFile(zip_path, "r") as zipf:
            # The file path inside the zip includes the relative directory structure
            assert "GSE123456/file1.xml" in zipf.namelist()


def test_validate_file_log_integrity(db_session, geo_file_handler, tmp_path):
    """Test that file names are correctly logged and validated."""
    geo_dir = tmp_path / "output" / "GSE123456"
    geo_dir.mkdir(parents=True)
    (geo_dir / "file1.xml").write_text("File content 1")
    (geo_dir / "file2.xml").write_text("File content 2")

    with patch("pipeline.geo_pipeline.geo_file_handler.get_session_context", return_value=db_session):
        geo_file_handler.log_download(geo_id="GSE123456", file_names=["file1.xml", "file2.xml"])
        log = db_session.query(GeoMetadataLog).filter_by(GeoID="GSE123456").first()
        assert log is not None
        assert sorted(log.FileNames) == ["file1.xml", "file2.xml"]
