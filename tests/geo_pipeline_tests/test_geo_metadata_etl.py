import pytest
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from unittest.mock import patch
from pipeline.geo_pipeline.geo_metadata_etl import GeoMetadataETL
from pipeline.geo_pipeline.geo_file_handler import GeoFileHandler
from db.schema.geo_metadata_schema import GeoSeriesMetadata, GeoSampleMetadata
from lxml import etree
import json


# Define an in-memory SQLite database URL for testing
TEST_DB_URL = "sqlite:///:memory:"


# ---------------- Test Fixtures ----------------

@pytest.fixture(scope="session")
def engine():
    """
    Create an SQLite in-memory database engine for testing.
    """
    return create_engine(TEST_DB_URL, connect_args={"check_same_thread": False})


@pytest.fixture(scope="function")
def db_session(engine):
    """
    Set up a database session for tests and tear it down afterward.
    """
    GeoSeriesMetadata.metadata.create_all(engine)
    GeoSampleMetadata.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()
    GeoSeriesMetadata.metadata.drop_all(engine)
    GeoSampleMetadata.metadata.drop_all(engine)


@pytest.fixture
def valid_miniml_file(tmp_path):
    """
    Create a valid MINiML XML file for testing.
    """
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
    """
    Create a valid JSON template file for testing field mappings.
    """
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


@pytest.fixture
def file_handler(tmp_path):
    """
    Create a GeoFileHandler instance for managing file operations during tests.
    """
    geo_ids_file = tmp_path / "geo_ids.txt"
    geo_ids_file.write_text("GSE123456\n")
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    return GeoFileHandler(geo_ids_file=str(geo_ids_file), output_dir=str(output_dir))


# ---------------- Test Cases ----------------

def test_initialization(valid_miniml_file, valid_template_file, file_handler):
    """
    Test the initialization of GeoMetadataETL.
    """
    extractor = GeoMetadataETL(
        file_path=valid_miniml_file,
        template_path=valid_template_file,
        debug_mode=True,
        file_handler=file_handler
    )
    assert extractor.file_path == valid_miniml_file
    assert extractor.template_path == valid_template_file
    assert extractor.debug_mode is True


def test_validate_xml(valid_miniml_file, valid_template_file, file_handler):
    """
    Test XML validation to ensure the file is well-formed.
    """
    extractor = GeoMetadataETL(valid_miniml_file, valid_template_file, file_handler=file_handler)
    try:
        extractor._validate_xml()
    except Exception as e:
        pytest.fail(f"XML validation failed: {e}")


def test_extract_fields(valid_miniml_file, valid_template_file, file_handler):
    """
    Test field extraction from XML elements using the template.
    """
    extractor = GeoMetadataETL(valid_miniml_file, valid_template_file, file_handler=file_handler)
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


@patch("pipeline.geo_pipeline.geo_metadata_etl.get_postgres_engine")
def test_parse_and_stream(mock_engine, db_session, valid_miniml_file, valid_template_file, file_handler, engine):
    """
    Test parsing and streaming metadata to the database.
    """
    mock_engine.return_value = engine
    with patch("sqlalchemy.orm.sessionmaker", return_value=lambda: db_session):
        extractor = GeoMetadataETL(valid_miniml_file, valid_template_file, file_handler=file_handler)
        processed_samples = extractor.parse_and_stream()

        assert processed_samples == 1

        series = db_session.query(GeoSeriesMetadata).filter_by(SeriesID="GSE123456").first()
        assert series is not None
        assert series.Title == "Test Series"

        sample = db_session.query(GeoSampleMetadata).filter_by(SampleID="GSM123456").first()
        assert sample is not None
        assert sample.Title == "Test Sample"
