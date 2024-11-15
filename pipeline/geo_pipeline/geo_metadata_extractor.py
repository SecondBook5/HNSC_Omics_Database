# File: pipeline/geo_pipeline/geo_metadata_extractor.py

from typing import Dict, Optional
import xml.etree.ElementTree as ET
import json
from utils.xml_tree_parser import parse_and_populate_xml_tree
from utils.validate_tags import validate_tags


# Define default paths for fields and XML file
FIELDS_FILE = "/mnt/c/Users/ajboo/BookAbraham/BiologicalDatabases/HNSC_Omics_Database/resources/geo_tag_template.json"
XML_FILE = "/mnt/c/Users/ajboo/BookAbraham/BiologicalDatabases/HNSC_Omics_Database/resources/data/metadata/geo_metadata/GSE112026_family.xml"


class GeoMetadataExtractor:
    """
    Extracts metadata from GEO XML files by parsing XML and retrieving fields specified
    in fields_to_extract, ensuring each required field is validated and present.

    Attributes:
        fields_to_extract (Dict[str, str]): Specifies tags and fields to extract with their paths.
        debug (bool): If True, enables debug output for detailed tracing of the extraction process.
    """

    def __init__(self, fields_to_extract: Dict[str, str], debug: bool = False) -> None:
        """
        Initialize the GeoMetadataExtractor with specific fields to extract and optional debug mode.

        Args:
            fields_to_extract (Dict[str, str]): Flattened dictionary defining tags and fields to extract.
            debug (bool): Enable debug output if True (default is False).

        Raises:
            ValueError: If fields_to_extract is empty or not a dictionary.
        """
        # Validate that fields_to_extract is a dictionary and not empty
        if not fields_to_extract or not isinstance(fields_to_extract, dict):
            raise ValueError("fields_to_extract must be a non-empty dictionary.")

        # Store fields_to_extract directly since it’s already flattened
        self.fields_to_extract = fields_to_extract

        # Set debug mode for extra output during extraction
        self.debug = debug

    def print_xml_structure(self, node, indent=""):
        """
        Recursively prints the XML tree structure for debugging purposes.

        Args:
            node: The root or current node of the XMLTree structure.
            indent (str): Used for formatting nested elements.
        """
        # Print the node tag with current indentation
        print(f"{indent}<{node.tag}>")

        # Print attributes of the node if they exist
        if node.attributes:
            print(f"{indent}  Attributes: {node.attributes}")

        # Recursively print each child with increased indentation
        for child in node.children:
            self.print_xml_structure(child, indent + "  ")

    def extract_metadata(self, xml_path: str) -> Optional[Dict[str, Optional[str]]]:
        """
        Parses and validates XML file to extract specified metadata fields.

        Args:
            xml_path (str): Path to the XML file to be parsed.

        Returns:
            Optional[Dict[str, Optional[str]]]: A dictionary with fields and their extracted values,
            or None if parsing fails or required fields are missing.
        """
        # Print debug message for extraction start if debug is enabled
        if self.debug:
            print(f"[DEBUG] Extracting metadata from XML file: {xml_path}")

        # Attempt to parse the XML file into an XMLTree structure
        try:
            # Parse the XML tree using fields_to_extract paths as the guide
            xml_tree = parse_and_populate_xml_tree(xml_path, list(self.fields_to_extract.values()))

            # Confirm successful parsing if debug mode is enabled
            if self.debug:
                print("[DEBUG] XML tree successfully parsed.")
        except FileNotFoundError:
            print(f"Error: XML file '{xml_path}' not found.")
            return None
        except ET.ParseError as e:
            print(f"Error parsing XML file at '{xml_path}': {e}")
            return None
        except Exception as e:
            print(f"Unexpected error while parsing '{xml_path}': {e}")
            return None

        # Print the XML structure for verification if debug is enabled
        if self.debug:
            print("[DEBUG] Printing XML structure for verification:")
            self.print_xml_structure(xml_tree.root)

        # Validate the XML structure against fields_to_extract
        if not validate_tags(xml_tree, self.fields_to_extract, debug=self.debug):
            print("Error: XML does not contain all required tags and fields.")
            return None

        # Initialize a dictionary to store extracted metadata
        metadata = {}
        # Iterate over each field and extract data from the parsed XML
        for field_name, field_path in self.fields_to_extract.items():
            # Attempt to find the element in XML and extract the required field
            element = xml_tree.root.find(field_path)
            metadata[field_name] = element.text.strip() if element is not None and element.text else None

            # Print debug message for each extracted field
            if self.debug:
                print(f"[DEBUG] Extracted field '{field_name}' with value: {metadata[field_name]}")

        # Return the populated metadata dictionary, or None if empty
        return metadata if metadata else None

    @staticmethod
    def prepare_for_output(metadata: Dict[str, Optional[str]]) -> Dict[str, str]:
        """
        Converts any None values in the metadata to 'N/A' for display or export purposes.

        Args:
            metadata (Dict[str, Optional[str]]): The extracted metadata.

        Returns:
            Dict[str, str]: Metadata with None values replaced by 'N/A'.
        """
        # Replace all None values with 'N/A' for clearer output
        return {field: (value if value is not None else 'N/A') for field, value in metadata.items()}


# Automatically load files if the script is executed directly
if __name__ == "__main__":
    # Attempt to load fields to extract from the specified JSON file
    try:
        with open(FIELDS_FILE, 'r') as f:
            fields_to_extract = json.load(f)
    except FileNotFoundError:
        print(f"Error: Fields file '{FIELDS_FILE}' not found.")
        exit(1)

    # Initialize an instance of GeoMetadataExtractor with debug mode enabled
    extractor = GeoMetadataExtractor(fields_to_extract=fields_to_extract, debug=True)

    # Attempt to extract metadata from the specified XML file
    extracted_metadata = extractor.extract_metadata(XML_FILE)

    # Print extracted metadata or notify if extraction failed
    if extracted_metadata:
        print("Extracted Metadata:")
        print(json.dumps(extracted_metadata, indent=4))
    else:
        print("No metadata extracted.")
