import pytest
from pipeline.geo_pipeline.geo_metadata_extractor import GeoMetadataExtractor
from lxml import etree
import json
import os

# Fixture for minimal valid XML
@pytest.fixture
def minimal_xml_file(tmp_path):
    """Creates a minimal valid XML file for testing."""
    content = """
    <MINiML xmlns="http://www.ncbi.nlm.nih.gov/geo/info/MINiML">
        <Series>
            <Accession>GSE112026</Accession>
            <Title>Test Series</Title>
            <Status>
                <Submission-Date>2021-01-01</Submission-Date>
                <Last-Update-Date>2021-02-01</Last-Update-Date>
            </Status>
            <Pubmed-ID>123456</Pubmed-ID>
            <Summary>Test Summary</Summary>
            <Overall-Design>Test Design</Overall-Design>
        </Series>
        <Sample>
            <Accession>GSM123456</Accession>
            <Title>Test Sample</Title>
            <Status>
                <Submission-Date>2021-01-01</Submission-Date>
                <Last-Update-Date>2021-02-01</Last-Update-Date>
            </Status>
            <Channel>
                <Organism>Homo sapiens</Organism>
                <Characteristics tag="Gender">Male</Characteristics>
                <Characteristics tag="Age">30</Characteristics>
            </Channel>
            <Relation type="BioSample" target="SAMN12345678"/>
        </Sample>
    </MINiML>
    """
    file_path = tmp_path / "minimal.xml"
    file_path.write_text(content)
    return str(file_path)

# Fixture for complex XML
@pytest.fixture
def complex_xml_file(tmp_path):
    """Creates a complex XML file with missing and unexpected fields."""
    content = """
    <MINiML xmlns="http://www.ncbi.nlm.nih.gov/geo/info/MINiML">
        <Series>
            <Accession>GSE999999</Accession>
            <Title>Complex Test Series</Title>
            <Status>
                <Submission-Date>2022-01-01</Submission-Date>
            </Status>
        </Series>
        <Sample>
            <Accession>GSM999999</Accession>
            <Title>Complex Test Sample</Title>
            <Channel>
                <Organism>Mus musculus</Organism>
                <Characteristics tag="Weight">20g</Characteristics>
            </Channel>
        </Sample>
    </MINiML>
    """
    file_path = tmp_path / "complex.xml"
    file_path.write_text(content)
    return str(file_path)

# Fixture for a valid template
@pytest.fixture
def template_json(tmp_path):
    """Creates a valid JSON template file."""
    content = {
        "Series": {
            "SeriesID": "geo:Accession",
            "Title": "geo:Title",
            "SubmissionDate": "geo:Status/geo:Submission-Date",
            "LastUpdateDate": "geo:Status/geo:Last-Update-Date",
            "PubMedID": "geo:Pubmed-ID",
            "Summary": "geo:Summary",
            "OverallDesign": "geo:Overall-Design",
            "Relations": "geo:Relation",
            "SupplementaryData": "geo:Supplementary-Data"
        },
        "Sample": {
            "SampleID": "geo:Accession",
            "Title": "geo:Title",
            "SubmissionDate": "geo:Status/geo:Submission-Date",
            "LastUpdateDate": "geo:Status/geo:Last-Update-Date",
            "Organism": "geo:Channel/geo:Organism",
            "Characteristics": "geo:Channel/geo:Characteristics",
            "Relations": "geo:Relation",
            "SupplementaryData": "geo:Supplementary-Data"
        }
    }
    file_path = tmp_path / "template.json"
    file_path.write_text(json.dumps(content))
    return str(file_path)

# Test for valid series extraction
def test_valid_series_extraction(minimal_xml_file, template_json):
    """Tests valid series metadata extraction."""
    extractor = GeoMetadataExtractor(file_path=minimal_xml_file, template_path=template_json)
    series_data = []
    ns = {'geo': 'http://www.ncbi.nlm.nih.gov/geo/info/MINiML'}

    context = etree.iterparse(minimal_xml_file, events=("start", "end"), tag="{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Series")
    for event, elem in context:
        if event == "end":
            data = extractor._process_series_data(elem, ns)
            if data:
                series_data.append(data)
            elem.clear()

    assert len(series_data) == 1
    assert series_data[0]['SeriesID'] == "GSE112026"
    assert series_data[0]['Title'] == "Test Series"

# Test for valid sample extraction
def test_valid_sample_extraction(minimal_xml_file, template_json):
    """Tests valid sample metadata extraction."""
    extractor = GeoMetadataExtractor(file_path=minimal_xml_file, template_path=template_json)
    sample_data = []
    ns = {'geo': 'http://www.ncbi.nlm.nih.gov/geo/info/MINiML'}

    context = etree.iterparse(minimal_xml_file, events=("start", "end"), tag="{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Sample")
    for event, elem in context:
        if event == "end":
            data = extractor._process_sample_data(elem, ns)
            if data:
                sample_data.append(data)
            elem.clear()

    assert len(sample_data) == 1
    assert sample_data[0]['SampleID'] == "GSM123456"
    assert sample_data[0]['Characteristics']['Gender'] == "Male"
    assert sample_data[0]['Characteristics']['Age'] == "30"

# Test for missing fields in XML
def test_missing_fields_in_xml(complex_xml_file, template_json):
    """Tests handling of missing fields in XML."""
    extractor = GeoMetadataExtractor(file_path=complex_xml_file, template_path=template_json)
    sample_data = []
    ns = {'geo': 'http://www.ncbi.nlm.nih.gov/geo/info/MINiML'}

    context = etree.iterparse(complex_xml_file, events=("start", "end"), tag="{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Sample")
    for event, elem in context:
        if event == "end":
            data = extractor._process_sample_data(elem, ns)
            if data:
                sample_data.append(data)
            elem.clear()

    assert len(sample_data) == 1
    assert sample_data[0]['SampleID'] == "GSM999999"
    assert sample_data[0]['Characteristics'].get('Weight') == "20g"

# Test for invalid XML structure
def test_invalid_xml_structure(template_json):
    """Tests handling of invalid XML structure."""
    invalid_xml = "<Invalid><UnclosedTag></Invalid"
    with pytest.raises(Exception):
        extractor = GeoMetadataExtractor(file_path="invalid.xml", template_path=template_json)

# Test for unexpected fields
def test_unexpected_fields_in_xml(complex_xml_file, template_json):
    """Tests handling of unexpected fields in XML."""
    extractor = GeoMetadataExtractor(file_path=complex_xml_file, template_path=template_json)
    series_data = []
    ns = {'geo': 'http://www.ncbi.nlm.nih.gov/geo/info/MINiML'}

    context = etree.iterparse(complex_xml_file, events=("start", "end"), tag="{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Series")
    for event, elem in context:
        if event == "end":
            data = extractor._process_series_data(elem, ns)
            if data:
                series_data.append(data)
            elem.clear()

    assert len(series_data) == 1
    assert series_data[0]['SeriesID'] == "GSE999999"
    assert series_data[0]['LastUpdateDate'] is None  # Field missing in XML

# Test for invalid template path
def test_invalid_template_path(minimal_xml_file):
    """Tests handling of invalid template path."""
    with pytest.raises(Exception):
        GeoMetadataExtractor(file_path=minimal_xml_file, template_path="nonexistent_template.json")

# Test for invalid file path
def test_invalid_file_path(template_json):
    """Tests handling of invalid file path."""
    with pytest.raises(Exception):
        GeoMetadataExtractor(file_path="nonexistent.xml", template_path=template_json)
