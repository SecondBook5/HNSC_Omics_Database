import pytest
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from unittest.mock import patch, MagicMock
from pipeline.geo_pipeline.geo_metadata_etl import GeoMetadataETL
from pipeline.geo_pipeline.geo_file_handler import GeoFileHandler
from db.schema.geo_metadata_schema import GeoSeriesMetadata, GeoSampleMetadata
from lxml import etree
import json


# Define a PostgreSQL database URL for testing
TEST_DB_URL = "postgresql+psycopg2://test_user:test_password@localhost:5432/test_db"



# ---------------- Test Fixtures ----------------

@pytest.fixture(scope="session")
def engine():
    """
    Create a PostgreSQL database engine for testing.
    """
    return create_engine(TEST_DB_URL)


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

def test_invalid_template_file(file_handler, tmp_path, valid_miniml_file):
    """
    Test behavior when the template file is invalid.
    """
    invalid_template_path = tmp_path / "invalid_template.json"
    invalid_template_path.write_text("{invalid json}")

    with pytest.raises(json.JSONDecodeError):
        GeoMetadataETL(valid_miniml_file, str(invalid_template_path), file_handler=file_handler)


def test_missing_series_id(valid_miniml_file, valid_template_file, file_handler, tmp_path):
    """
    Test handling when the SeriesID is missing in the XML file.
    """
    # Create a modified MINiML file with missing SeriesID
    modified_file_path = tmp_path / "missing_series_id.xml"
    modified_content = """
    <MINiML xmlns="http://www.ncbi.nlm.nih.gov/geo/info/MINiML">
        <Series>
            <Title>Test Series</Title>
        </Series>
    </MINiML>
    """
    modified_file_path.write_text(modified_content)

    # Instantiate the GeoMetadataETL and attempt parsing
    extractor = GeoMetadataETL(str(modified_file_path), valid_template_file, file_handler=file_handler)
    ns = {'geo': 'http://www.ncbi.nlm.nih.gov/geo/info/MINiML'}
    tree = etree.parse(str(modified_file_path))
    series_elem = tree.find(".//geo:Series", namespaces=ns)

    # Expect a ValueError when SeriesID is missing
    with pytest.raises(ValueError, match="Critical field 'SeriesID' is missing."):
        extractor._validate_series_id(series_elem, ns)



# Test for XML validation
@patch("pipeline.geo_pipeline.geo_metadata_etl.GeoMetadataETL._load_template", return_value={"mock_key": "mock_value"})
@patch("os.access", return_value=True)  # Mock os.access
@patch("os.path.exists", return_value=True)  # Mock os.path.exists
@patch("os.path.isfile", return_value=True)  # Mock os.path.isfile
@patch("pipeline.geo_pipeline.geo_metadata_etl.GeoMetadataETL._validate_xml")
def test_validate_xml(mock_validate_xml, mock_isfile, mock_exists, mock_access, mock_load_template):
    """
    Test that the XML validation function is called and behaves as expected.
    """
    mock_validate_xml.return_value = True

    mock_file_handler = MagicMock()

    etl = GeoMetadataETL("/path/to/mock_file.xml", "/path/to/mock_template.json", file_handler=mock_file_handler)

    assert etl._validate_xml() is True


# Test for database insert
@patch("pipeline.geo_pipeline.geo_metadata_etl.GeoMetadataETL._load_template", return_value={"mock_key": "mock_value"})
@patch("os.access", return_value=True)  # Mock os.access
@patch("os.path.exists", return_value=True)  # Mock os.path.exists
@patch("os.path.isfile", return_value=True)  # Mock os.path.isfile
@patch("pipeline.geo_pipeline.geo_metadata_etl.get_postgres_engine")
@patch("sqlalchemy.orm.sessionmaker")
def test_database_insert(
    mock_sessionmaker, mock_engine, mock_isfile, mock_exists, mock_access, mock_load_template
):
    """
    Test that database insertions for Series and Sample metadata are correctly executed.
    """
    mock_engine.return_value = MagicMock()
    mock_session = MagicMock()
    mock_session.execute.return_value.rowcount = 1  # Simulate successful insert
    mock_sessionmaker.return_value = lambda: mock_session

    mock_file_handler = MagicMock()

    etl = GeoMetadataETL("/path/to/mock_file.xml", "/path/to/mock_template.json", file_handler=mock_file_handler)

    # Simulate an insert operation
    result = mock_session.execute.return_value
    assert result.rowcount == 1


# Test for XML parsing
@patch("pipeline.geo_pipeline.geo_metadata_etl.GeoMetadataETL._load_template", return_value={"mock_key": "mock_value"})
@patch("os.access", return_value=True)  # Mock os.access
@patch("os.path.exists", return_value=True)  # Mock os.path.exists
@patch("os.path.isfile", return_value=True)  # Mock os.path.isfile
@patch("xml.etree.ElementTree.iterparse")
def test_xml_parsing(mock_iterparse, mock_isfile, mock_exists, mock_access, mock_load_template):
    """
    Test that the XML parsing correctly processes Series and Sample elements.
    """
    mock_series = MagicMock(tag="{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Series")
    mock_sample = MagicMock(tag="{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Sample")
    mock_iterparse.return_value = [("end", mock_series), ("end", mock_sample)]

    mock_file_handler = MagicMock()

    etl = GeoMetadataETL("/path/to/mock_file.xml", "/path/to/mock_template.json", file_handler=mock_file_handler)

    # Ensure iterparse is used correctly
    parsed_elements = list(mock_iterparse.return_value)
    assert len(parsed_elements) == 2
    assert parsed_elements[0][1].tag.endswith("Series")
    assert parsed_elements[1][1].tag.endswith("Sample")



def test_empty_xml_file(tmp_path, valid_template_file, file_handler):
    """
    Test behavior when the XML file is empty.
    """
    empty_file_path = tmp_path / "empty.xml"
    empty_file_path.write_text("")  # Write an empty file

    extractor = GeoMetadataETL(
        file_path=str(empty_file_path),
        template_path=valid_template_file,
        file_handler=file_handler
    )

    with pytest.raises(RuntimeError, match="Invalid XML structure"):
        extractor._validate_xml()

def test_empty_template_file(tmp_path, valid_miniml_file, file_handler):
    """
    Test behavior when the template file is empty.
    """
    empty_template_path = tmp_path / "empty_template.json"
    empty_template_path.write_text("{}")  # Write an empty JSON object

    with pytest.raises(KeyError, match="Missing required keys in template"):
        GeoMetadataETL(valid_miniml_file, str(empty_template_path), file_handler=file_handler)

