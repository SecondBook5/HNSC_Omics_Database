# File: tests/geo_pipeline_tests/test_geo_metadata_extractor.py

import pytest
from pipeline.geo_pipeline.geo_metadata_extractor import GeoMetadataExtractor


@pytest.fixture
def setup_fields_to_extract():
    """
    Fixture to provide a dictionary of tags and fields to extract for testing.
    """
    return {
        "Sample": ["iid", "Title", "Organism"],
        "Channel": ["Source", "Characteristics"],
        "Series": ["Title", "Summary"]
    }


@pytest.fixture
def valid_xml_file(tmp_path):
    """
    Fixture to create a valid XML file for testing.

    Args:
        tmp_path (Path): Temporary directory for test files.

    Returns:
        str: Path to the valid XML file.
    """
    xml_content = """
    <MINiML>
        <Sample iid="GSM123456">
            <Title>Sample Title</Title>
            <Channel position="1">
                <Source>Test Source</Source>
                <Characteristics tag="test">Value</Characteristics>
            </Channel>
            <Organism taxid="9606">Homo sapiens</Organism>
        </Sample>
        <Series>
            <Title>Series Title</Title>
            <Summary>Series Summary</Summary>
        </Series>
    </MINiML>
    """
    xml_path = tmp_path / "valid_test.xml"
    xml_path.write_text(xml_content)
    return str(xml_path)


@pytest.fixture
def missing_fields_xml_file(tmp_path):
    """
    Fixture to create an XML file missing some specified fields.

    Args:
        tmp_path (Path): Temporary directory for test files.

    Returns:
        str: Path to the XML file with missing fields.
    """
    xml_content = """
    <MINiML>
        <Sample iid="GSM123456">
            <Title>Sample Title</Title>
        </Sample>
        <Series>
            <Title>Series Title</Title>
        </Series>
    </MINiML>
    """
    xml_path = tmp_path / "missing_fields_test.xml"
    xml_path.write_text(xml_content)
    return str(xml_path)


def test_extract_metadata_success(setup_fields_to_extract, valid_xml_file):
    """
    Test successful extraction of metadata from a well-formed XML file.
    """
    # Instantiate extractor with valid fields to extract
    extractor = GeoMetadataExtractor(fields_to_extract=setup_fields_to_extract, debug=True)
    metadata = extractor.extract_metadata(valid_xml_file)

    # Assertions for successful extraction of tags and fields
    assert metadata is not None, "Expected metadata to be extracted successfully"
    assert "Sample" in metadata, "Expected 'Sample' tag to be present in metadata"
    assert metadata["Sample"]["Title"] == "Sample Title", "Incorrect title for 'Sample'"
    assert metadata["Sample"]["Organism"] == "Homo sapiens", "Incorrect organism for 'Sample'"
    assert "Series" in metadata, "Expected 'Series' tag to be present in metadata"
    assert metadata["Series"]["Summary"] == "Series Summary", "Incorrect summary for 'Series'"


def test_missing_fields_handling(setup_fields_to_extract, missing_fields_xml_file):
    """
    Test handling of missing fields in the XML file.
    """
    # Instantiate extractor with fields to extract and debug enabled
    extractor = GeoMetadataExtractor(fields_to_extract=setup_fields_to_extract, debug=True)
    metadata = extractor.extract_metadata(missing_fields_xml_file)

    # Assertions for missing field handling (expecting 'N/A' for missing fields)
    assert metadata is not None, "Expected metadata to be extracted with missing fields"
    assert metadata["Sample"].get("Organism", "N/A") == "N/A", "Expected 'Organism' to be 'N/A'"
    assert metadata["Series"].get("Summary", "N/A") == "N/A", "Expected 'Summary' to be 'N/A'"


def test_nonexistent_xml_file(setup_fields_to_extract):
    """
    Test that a FileNotFoundError is raised for a non-existent XML file.
    """
    # Instantiate extractor
    extractor = GeoMetadataExtractor(fields_to_extract=setup_fields_to_extract)

    # Assert FileNotFoundError is raised when file does not exist
    with pytest.raises(FileNotFoundError):
        extractor.extract_metadata("nonexistent_file.xml")


def test_invalid_xml_structure(setup_fields_to_extract, tmp_path):
    """
    Test handling of an invalid XML structure.
    """
    # Create an invalid XML structure
    invalid_xml_content = "<MINiML><Sample><Title>Sample Title</Title></Sample"
    xml_path = tmp_path / "invalid_structure.xml"
    xml_path.write_text(invalid_xml_content)

    # Instantiate extractor
    extractor = GeoMetadataExtractor(fields_to_extract=setup_fields_to_extract, debug=True)
    metadata = extractor.extract_metadata(str(xml_path))

    # Assert that None is returned for invalid XML structure
    assert metadata is None, "Expected None for invalid XML structure"


def test_debug_logging(setup_fields_to_extract, valid_xml_file, capsys):
    """
    Test debug logging output when debug mode is enabled.
    """
    # Instantiate extractor with debug enabled
    extractor = GeoMetadataExtractor(fields_to_extract=setup_fields_to_extract, debug=True)
    extractor.extract_metadata(valid_xml_file)

    # Capture the debug output
    captured = capsys.readouterr()

    # Check that debug messages are included in the output
    assert "[DEBUG] Extracting metadata from XML file" in captured.out
    assert "[DEBUG] XML tree successfully parsed." in captured.out
    assert "[DEBUG] Completed extraction for tag 'Sample'" in captured.out
