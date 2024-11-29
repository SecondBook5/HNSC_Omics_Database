# File: tests/geo_pipeline_tests/test_geo_metadata_etl.py

import pytest  # Testing framework
from sqlalchemy.orm import sessionmaker  # SQLAlchemy session manager
from sqlalchemy import create_engine  # SQLAlchemy engine creation
from unittest.mock import patch  # Mocking external dependencies
from pipeline.geo_pipeline.geo_metadata_etl import GeoMetadataETL  # Class to test
from pipeline.geo_pipeline.geo_file_handler import GeoFileHandler  # File handler dependency
from db.schema.geo_metadata_schema import DatasetSeriesMetadata, DatasetSampleMetadata  # Database schema
from lxml import etree  # XML parsing
import json  # JSON handling


# Define an in-memory SQLite database URL for testing
TEST_DB_URL = "sqlite:///:memory:"


# ---------------- Test Fixtures ----------------

@pytest.fixture(scope="session")
def engine():
    """
    Create an SQLite in-memory database engine for testing.
    Returns:
        Engine: SQLAlchemy engine instance.
    """
    return create_engine(TEST_DB_URL, connect_args={"check_same_thread": False})


@pytest.fixture(scope="function")
def db_session(engine):
    """
    Set up a database session for tests and tear it down afterward.
    Args:
        engine: The in-memory SQLite database engine.
    Yields:
        Session: A SQLAlchemy session instance for the test.
    """
    # Create tables for the session
    DatasetSeriesMetadata.metadata.create_all(engine)
    DatasetSampleMetadata.metadata.create_all(engine)
    # Create a session
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    # Tear down the session
    session.close()
    DatasetSeriesMetadata.metadata.drop_all(engine)
    DatasetSampleMetadata.metadata.drop_all(engine)


@pytest.fixture
def valid_miniml_file(tmp_path):
    """
    Create a valid MINiML XML file for testing.
    Args:
        tmp_path: Temporary directory fixture for file creation.
    Returns:
        str: Path to the temporary MINiML XML file.
    """
    # Define XML content for testing
    content = """
    <MINiML xmlns="http://www.ncbi.nlm.nih.gov/geo/info/MINiML">
        <Series>
            <Title>Test Series</Title>
            <Accession database="GEO">GSE123456</Accession>
        </Series>
        <Sample iid="GSM123456">
            <Title>Test Sample</Title>
            <Accession database="GEO">GSM123456</Accession>
            <Characteristics tag="test-tag">Test Value</Characteristics>
        </Sample>
    </MINiML>
    """
    # Write content to a temporary file
    file_path = tmp_path / "GSE123456_family.xml"
    file_path.write_text(content)
    return str(file_path)


@pytest.fixture
def valid_template_file(tmp_path):
    """
    Create a valid JSON template file for testing field mappings.
    Args:
        tmp_path: Temporary directory fixture for file creation.
    Returns:
        str: Path to the temporary JSON template file.
    """
    # Define field mappings for Series and Sample
    template_content = {
        "Series": {
            "Title": ".//geo:Title",
            "SeriesID": ".//geo:Accession[@database='GEO']/text()"
        },
        "Sample": {
            "SampleID": ".//geo:Accession[@database='GEO']/text()",
            "Title": ".//geo:Title",
            "Characteristics": ".//geo:Characteristics"
        }
    }
    # Write the template to a temporary file
    template_path = tmp_path / "template.json"
    template_path.write_text(json.dumps(template_content))
    return str(template_path)


@pytest.fixture
def file_handler(tmp_path):
    """
    Create a GeoFileHandler instance for managing file operations during tests.
    Args:
        tmp_path: Temporary directory fixture.
    Returns:
        GeoFileHandler: An instance of the file handler.
    """
    # Create a temporary GEO IDs file
    geo_ids_file = tmp_path / "geo_ids.txt"
    geo_ids_file.write_text("GSE123456\n")
    # Create an output directory
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    # Return a GeoFileHandler instance
    return GeoFileHandler(geo_ids_file=str(geo_ids_file), output_dir=str(output_dir))


# ---------------- Test Cases ----------------

def test_initialization(valid_miniml_file, valid_template_file, file_handler):
    """
    Test the initialization of GeoMetadataETL.
    Args:
        valid_miniml_file: Path to a valid MINiML file fixture.
        valid_template_file: Path to a valid template file fixture.
        file_handler: GeoFileHandler instance for managing file operations.
    """
    # Initialize the GeoMetadataETL class
    extractor = GeoMetadataETL(
        file_path=valid_miniml_file,
        template_path=valid_template_file,
        debug_mode=True,
        file_handler=file_handler
    )
    # Validate the attributes
    assert extractor.file_path == valid_miniml_file
    assert extractor.template_path == valid_template_file
    assert extractor.debug_mode is True


def test_validate_xml(valid_miniml_file, valid_template_file, file_handler):
    """
    Test XML validation to ensure the file is well-formed.
    Args:
        valid_miniml_file: Path to a valid MINiML file fixture.
        valid_template_file: Path to a valid template file fixture.
        file_handler: GeoFileHandler instance for managing file operations.
    """
    # Initialize the GeoMetadataETL class
    extractor = GeoMetadataETL(valid_miniml_file, valid_template_file, file_handler=file_handler)
    # Ensure XML validation does not raise exceptions
    try:
        extractor._validate_xml()
    except Exception as e:
        pytest.fail(f"XML validation failed with error: {e}")


def test_extract_fields(valid_miniml_file, valid_template_file, file_handler):
    """
    Test field extraction from XML elements using the template.
    Args:
        valid_miniml_file: Path to a valid MINiML file fixture.
        valid_template_file: Path to a valid template file fixture.
        file_handler: GeoFileHandler instance for managing file operations.
    """
    # Initialize the GeoMetadataETL class
    extractor = GeoMetadataETL(valid_miniml_file, valid_template_file, file_handler=file_handler)
    # Define XML namespaces
    ns = {'geo': 'http://www.ncbi.nlm.nih.gov/geo/info/MINiML'}
    # Parse the XML file
    tree = etree.parse(valid_miniml_file)
    # Find Series and Sample elements
    series_elem = tree.find(".//geo:Series", namespaces=ns)
    sample_elem = tree.find(".//geo:Sample", namespaces=ns)

    # Extract fields for Series and Sample
    series_fields = extractor._extract_fields(series_elem, extractor.template['Series'], ns)
    sample_fields = extractor._extract_fields(sample_elem, extractor.template['Sample'], ns)

    # Validate the extracted fields
    assert series_fields['Title'] == "Test Series"
    assert series_fields['SeriesID'] == "GSE123456"
    assert sample_fields['SampleID'] == "GSM123456"
    assert sample_fields['Characteristics'] == [{'tag': 'test-tag', 'value': 'Test Value'}]


def test_parse_and_stream(db_session, valid_miniml_file, valid_template_file, engine, file_handler):
    """
    Test parsing and streaming metadata to the database.
    Args:
        db_session: Database session fixture.
        valid_miniml_file: Path to a valid MINiML file fixture.
        valid_template_file: Path to a valid template file fixture.
        engine: Database engine fixture.
        file_handler: GeoFileHandler instance for managing file operations.
    """
    # Patch the database engine to use in-memory SQLite
    with patch("pipeline.geo_pipeline.geo_metadata_etl.get_postgres_engine", return_value=engine):
        # Initialize GeoMetadataETL
        extractor = GeoMetadataETL(valid_miniml_file, valid_template_file, file_handler=file_handler)
        # Run the ETL process
        extractor.parse_and_stream()

        # Validate the Series metadata
        series = db_session.query(DatasetSeriesMetadata).filter_by(SeriesID="GSE123456").first()
        assert series is not None, "Series metadata was not found in the database."
        assert series.Title == "Test Series"

        # Validate the Sample metadata
        sample = db_session.query(DatasetSampleMetadata).filter_by(SampleID="GSM123456").first()
        assert sample is not None, "Sample metadata was not found in the database."
        assert sample.Title == "Test Sample"
