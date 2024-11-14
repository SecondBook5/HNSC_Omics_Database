# File: pipeline/geo_pipeline/geo_metadata_extractor.py

from typing import Dict, Optional
import xml.etree.ElementTree as ET
import json
from utils.xml_tree_parser import parse_and_populate_xml_tree
from utils.validate_tags import validate_tags


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
            fields_to_extract (Dict[str, str]): Dictionary defining tags and fields to extract.
            debug (bool): Enable debug output if True (default is False).

        Raises:
            ValueError: If fields_to_extract is empty or not a dictionary.
        """
        # Flatten fields_to_extract to remove nested groups like 'Series' or 'Sample'
        self.fields_to_extract = self.flatten_fields(fields_to_extract)
        # Set debug mode if specified
        self.debug = debug

    @staticmethod
    def flatten_fields(fields_to_extract: Dict[str, Dict[str, str]]) -> Dict[str, str]:
        """
        Flattens the fields_to_extract dictionary to remove conceptual groupings like 'Series' or 'Sample'.

        Args:
            fields_to_extract (Dict[str, Dict[str, str]]): The original structured dictionary.

        Returns:
            Dict[str, str]: A flattened dictionary with paths as single keys.
        """
        flattened = {}
        for group, fields in fields_to_extract.items():
            for field, path in fields.items():
                flattened[field] = path
        return flattened

    def print_xml_structure(self, node, indent=""):
        """
        Recursively prints the XML tree structure for debugging purposes.

        Args:
            node: The root or current node of the XMLTree structure.
            indent (str): Used for formatting nested elements.
        """
        # Print the tag name with indentation to show structure
        print(f"{indent}<{node.tag}>")
        if node.attributes:
            print(f"{indent}  Attributes: {node.attributes}")
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
        if self.debug:
            print(f"[DEBUG] Extracting metadata from XML file: {xml_path}")

        try:
            # Parse the XML tree
            xml_tree = parse_and_populate_xml_tree(xml_path, list(self.fields_to_extract.values()))
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

        # Print XML structure for debug purposes
        if self.debug:
            print("[DEBUG] Printing XML structure for verification:")
            self.print_xml_structure(xml_tree.root)

        # Validate XML against fields_to_extract
        if not validate_tags(xml_tree, self.fields_to_extract, debug=self.debug):
            print("Error: XML does not contain all required tags and fields.")
            return None

        metadata = {}
        for field_name, field_path in self.fields_to_extract.items():
            element = xml_tree.root.find(field_path)
            metadata[field_name] = element.text.strip() if element is not None and element.text else None
            if self.debug:
                print(f"[DEBUG] Extracted field '{field_name}' with value: {metadata[field_name]}")

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
        return {field: (value if value is not None else 'N/A') for field, value in metadata.items()}


# Automatically load files if script is run directly
if __name__ == "__main__":
    fields_file = "/mnt/c/Users/ajboo/BookAbraham/BiologicalDatabases/HNSC_Omics_Database/resources/geo_tag_template.json"
    xml_file = "/mnt/c/Users/ajboo/BookAbraham/BiologicalDatabases/HNSC_Omics_Database/resources/data/metadata/geo_metadata/GSE112026_family.xml"

    try:
        with open(fields_file, 'r') as f:
            fields_to_extract = json.load(f)
    except FileNotFoundError:
        print(f"Error: Fields file '{fields_file}' not found.")
        exit(1)

    extractor = GeoMetadataExtractor(fields_to_extract=fields_to_extract, debug=True)
    extracted_metadata = extractor.extract_metadata(xml_file)

    if extracted_metadata:
        print("Extracted Metadata:")
        print(json.dumps(extracted_metadata, indent=4))
    else:
        print("No metadata extracted.")
