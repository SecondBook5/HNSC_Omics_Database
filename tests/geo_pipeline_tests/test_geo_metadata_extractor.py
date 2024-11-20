import pytest
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from unittest.mock import patch
from pipeline.geo_pipeline.geo_metadata_extractor import GeoMetadataExtractor
from db.schema.metadata_schema import DatasetSeriesMetadata, DatasetSampleMetadata
from lxml import etree
import json


# Mock database connection URL
TEST_DB_URL = "sqlite:///:memory:"

@pytest.fixture(scope="session")
def engine():
    """Fixture to set up an SQLite in-memory database engine."""
    return create_engine(TEST_DB_URL, connect_args={"check_same_thread": False})


@pytest.fixture(scope="function")
def db_session(engine):
    """Fixture to set up an SQLite session."""
    DatasetSeriesMetadata.metadata.create_all(engine)
    DatasetSampleMetadata.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()
    DatasetSeriesMetadata.metadata.drop_all(engine)
    DatasetSampleMetadata.metadata.drop_all(engine)


@pytest.fixture
def valid_miniml_file(tmp_path):
    """Fixture to create a valid MINiML file for testing."""
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
    file_path = tmp_path / "GSE123456_family.xml"
    file_path.write_text(content)
    return str(file_path)


@pytest.fixture
def valid_template_file(tmp_path):
    """Fixture to create a valid template file for testing."""
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
    template_path = tmp_path / "template.json"
    template_path.write_text(json.dumps(template_content))
    return str(template_path)


def test_initialization(valid_miniml_file, valid_template_file):
    """Test GeoMetadataExtractor initialization."""
    extractor = GeoMetadataExtractor(valid_miniml_file, valid_template_file, debug_mode=True)
    assert extractor.file_path == valid_miniml_file
    assert extractor.template_path == valid_template_file
    assert extractor.debug_mode is True

def test_validate_xml(valid_miniml_file, valid_template_file):
    """Test XML validation."""
    extractor = GeoMetadataExtractor(valid_miniml_file, valid_template_file)
    try:
        extractor._validate_xml()
    except Exception as e:
        pytest.fail(f"XML validation failed with error: {e}")

def test_extract_fields(valid_miniml_file, valid_template_file):
    """Test field extraction."""
    extractor = GeoMetadataExtractor(valid_miniml_file, valid_template_file)
    ns = {'geo': 'http://www.ncbi.nlm.nih.gov/geo/info/MINiML'}
    tree = etree.parse(valid_miniml_file)
    series_elem = tree.find(".//geo:Series", namespaces=ns)
    sample_elem = tree.find(".//geo:Sample", namespaces=ns)

    series_fields = extractor._extract_fields(series_elem, extractor.template['Series'], ns)
    sample_fields = extractor._extract_fields(sample_elem, extractor.template['Sample'], ns)

    assert series_fields['Title'] == "Test Series"
    assert series_fields['SeriesID'] == "GSE123456"
    assert sample_fields['SampleID'] == "GSM123456"
    assert sample_fields['Characteristics'] == [{'tag': 'test-tag', 'value': 'Test Value'}]

def test_pre_insert_series_id(db_session, valid_miniml_file, valid_template_file):
    """Test pre-inserting SeriesID."""
    extractor = GeoMetadataExtractor(valid_miniml_file, valid_template_file)
    series_id = "GSE123456"

    try:
        extractor._pre_insert_series_id(db_session, series_id)
    except Exception as e:
        pytest.fail(f"Pre-inserting SeriesID failed with error: {e}")

    result = db_session.query(DatasetSeriesMetadata).filter_by(SeriesID=series_id).first()
    assert result is not None
    assert result.SeriesID == series_id

def test_update_series_sample_count(db_session, valid_miniml_file, valid_template_file):
    """Test updating Series SampleCount."""
    extractor = GeoMetadataExtractor(valid_miniml_file, valid_template_file)
    series_id = "GSE123456"

    # Pre-insert the SeriesID
    extractor._pre_insert_series_id(db_session, series_id)

    # Update the SampleCount
    sample_count = 10
    try:
        extractor._update_series_sample_count(db_session, series_id, sample_count)
    except Exception as e:
        pytest.fail(f"Updating Series SampleCount failed with error: {e}")

    result = db_session.query(DatasetSeriesMetadata).filter_by(SeriesID=series_id).first()
    assert result is not None
    assert result.SampleCount == sample_count

def test_parse_and_stream(db_session, valid_miniml_file, valid_template_file, engine):
    """Test parsing and streaming metadata."""
    # Mock the `get_postgres_engine` method to return the in-memory SQLite engine
    with patch("pipeline.geo_pipeline.geo_metadata_extractor.get_postgres_engine", return_value=engine):
        extractor = GeoMetadataExtractor(valid_miniml_file, valid_template_file)
        extractor.parse_and_stream()

        # Verify series metadata
        series = db_session.query(DatasetSeriesMetadata).filter_by(SeriesID="GSE123456").first()
        assert series is not None, "Series metadata was not found in the database."
        assert series.Title == "Test Series"

        # Verify sample metadata
        sample = db_session.query(DatasetSampleMetadata).filter_by(SampleID="GSM123456").first()
        assert sample is not None, "Sample metadata was not found in the database."
        assert sample.Title == "Test Sample"

        # Ensure Characteristics is already a list
        expected_characteristics = [{'tag': 'test-tag', 'value': 'Test Value'}]
        if isinstance(sample.Characteristics, str):
            assert json.loads(sample.Characteristics) == expected_characteristics
        else:
            assert sample.Characteristics == expected_characteristics
