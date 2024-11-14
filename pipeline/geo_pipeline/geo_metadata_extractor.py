# File: pipeline/geo_pipeline/geo_metadata_extractor.py

from typing import Dict, List, Optional
import xml.etree.ElementTree as ET
from utils.xml_tree_parser import parse_and_populate_xml_tree


class GeoMetadataExtractor:
    """
    Extracts metadata from GEO XML files. Parses XML to retrieve fields specified
    in fields_to_extract, ensuring each required field is validated and present.

    Attributes:
        fields_to_extract (Dict[str, List[str]]): Specifies the tags and fields to extract for each XML element.
    """

    def __init__(self, fields_to_extract: Dict[str, List[str]]) -> None:
        """
        Initialize the GeoMetadataExtractor with specific fields to extract.

        Args:
            fields_to_extract (Dict[str, List[str]]): Dictionary defining tags and fields to extract.

        Raises:
            ValueError: If fields_to_extract is empty or None.
        """
        # Ensure fields_to_extract is provided and is not empty
        if not fields_to_extract or not isinstance(fields_to_extract, dict):
            raise ValueError("fields_to_extract must be a non-empty dictionary.")

        # Store the tags and fields to extract for validation
        self.fields_to_extract = fields_to_extract

    def extract_metadata(self, xml_path: str) -> Optional[Dict[str, Dict[str, str]]]:
        """
        Parses an XML file to extract specified metadata fields.

        Args:
            xml_path (str): Path to the XML file to be parsed.

        Returns:
            Optional[Dict[str, Dict[str, str]]]: A nested dictionary with tags as keys and extracted fields as values,
            or None if the file couldn't be parsed or if required fields are missing.

        Raises:
            FileNotFoundError: If the XML file at xml_path is not found.
            ValueError: If xml_path is empty or invalid.
        """
        # Validate XML file path input
        if not xml_path or not isinstance(xml_path, str):
            raise ValueError("XML file path must be a non-empty string.")

        # Initialize an empty dictionary to store extracted metadata
        metadata = {}

        # Attempt to parse the XML file and populate XMLTree structure
        try:
            xml_tree = parse_and_populate_xml_tree(xml_path, self.fields_to_extract)
        except FileNotFoundError:
            raise FileNotFoundError(f"The specified XML file '{xml_path}' does not exist.")
        except ET.ParseError as e:
            print(f"Error parsing XML file at '{xml_path}': {e}")
            return None
        except Exception as e:
            print(f"Unexpected error while parsing '{xml_path}': {e}")
            return None

        # Traverse XMLTree and extract data for each specified tag
        for tag, fields in self.fields_to_extract.items():
            # Locate the node by tag in the XML tree
            tag_node = xml_tree.find_element_by_tag(tag)
            if not tag_node:
                print(f"Warning: Required tag '{tag}' not found in XML file.")
                continue  # Skip to the next tag instead of returning None

            # Extract specified fields from the XML node
            tag_data = {}
            for field in fields:
                try:
                    # Retrieve attribute or text from the node based on the field
                    if field in tag_node.attributes:
                        tag_data[field] = tag_node.attributes[field]
                    else:
                        sub_element = tag_node.find(field)
                        tag_data[
                            field] = sub_element.text.strip() if sub_element is not None and sub_element.text else "N/A"
                except AttributeError:
                    # Handle cases where the field is unexpectedly missing
                    print(f"Warning: Field '{field}' missing in tag '{tag}'. Setting to 'N/A'.")
                    tag_data[field] = "N/A"

            # Check if we have extracted any meaningful data for the tag
            if not tag_data:
                print(f"Warning: No data extracted for tag '{tag}'.")
            else:
                # Add the extracted data to the metadata dictionary under the tag name
                metadata[tag] = tag_data

        # Return the dictionary containing extracted metadata for each tag
        return metadata if metadata else None
