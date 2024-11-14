# File: pipeline/geo_pipeline/geo_metadata_extractor.py

from typing import Dict, List, Optional
import xml.etree.ElementTree as ET
from utils.xml_tree_parser import parse_and_populate_xml_tree


class GeoMetadataExtractor:
    """
    Extracts metadata from GEO XML files by parsing XML and retrieving fields specified
    in fields_to_extract, ensuring each required field is validated and present.

    Attributes:
        fields_to_extract (Dict[str, List[str]]): Specifies tags and fields to extract for each XML element.
        debug (bool): If True, enables debug output for detailed tracing of the extraction process.
    """

    def __init__(self, fields_to_extract: Dict[str, List[str]], debug: bool = False) -> None:
        """
        Initialize the GeoMetadataExtractor with specific fields to extract and optional debug mode.

        Args:
            fields_to_extract (Dict[str, List[str]]): Dictionary defining tags and fields to extract.
            debug (bool): Enable debug output if True (default is False).

        Raises:
            ValueError: If fields_to_extract is empty or not a dictionary.
        """
        # Check if fields_to_extract is valid and non-empty, raise error if not
        if not fields_to_extract or not isinstance(fields_to_extract, dict):
            raise ValueError("fields_to_extract must be a non-empty dictionary.")

        # Set the fields_to_extract attribute for later use in metadata extraction
        self.fields_to_extract = fields_to_extract

        # Set the debug attribute to enable optional debug logging
        self.debug = debug

    def extract_metadata(self, xml_path: str) -> Optional[Dict[str, Dict[str, str]]]:
        """
        Parses an XML file to extract specified metadata fields.

        Args:
            xml_path (str): Path to the XML file to be parsed.

        Returns:
            Optional[Dict[str, Dict[str, str]]]: A nested dictionary with tags as keys and extracted fields as values,
            or None if parsing fails or required fields are missing.

        Raises:
            FileNotFoundError: If the XML file at xml_path is not found.
            ValueError: If xml_path is empty or invalid.
        """
        # Validate the XML file path input
        if not xml_path or not isinstance(xml_path, str):
            raise ValueError("XML file path must be a non-empty string.")

        # Log debug information if debug mode is enabled
        if self.debug:
            print(f"[DEBUG] Extracting metadata from XML file: {xml_path}")

        # Initialize an empty dictionary to store extracted metadata
        metadata = {}

        # Attempt to parse the XML file and populate XMLTree structure
        try:
            xml_tree = parse_and_populate_xml_tree(xml_path, self.fields_to_extract)
            # Debug log to confirm XML tree was parsed successfully
            if self.debug:
                print("[DEBUG] XML tree successfully parsed.")
        except FileNotFoundError:
            # Raise specific error if XML file is missing
            raise FileNotFoundError(f"The specified XML file '{xml_path}' does not exist.")
        except ET.ParseError as e:
            # Log parsing error and return None
            print(f"Error parsing XML file at '{xml_path}': {e}")
            return None
        except Exception as e:
            # Log any unexpected errors during parsing
            print(f"Unexpected error while parsing '{xml_path}': {e}")
            return None

        # Iterate over each tag and field specified in fields_to_extract
        for tag, fields in self.fields_to_extract.items():
            # Attempt to locate the node by tag in the XML tree
            tag_node = xml_tree.find_element_by_tag(tag)
            if not tag_node:
                # Log warning if tag is not found and skip to the next tag
                print(f"Warning: Required tag '{tag}' not found in XML file.")
                if self.debug:
                    print(f"[DEBUG] Tag '{tag}' not found, skipping...")
                continue

            # Initialize dictionary to store extracted data for this tag
            tag_data = {}
            # Iterate over each specified field for the current tag
            for field in fields:
                try:
                    # If field is an attribute, retrieve it from the node attributes
                    if field in tag_node.attributes:
                        tag_data[field] = tag_node.attributes[field]
                        # Debug log for attribute extraction
                        if self.debug:
                            print(f"[DEBUG] Extracted attribute '{field}': {tag_data[field]}")
                    else:
                        # Otherwise, look for a sub-element and retrieve its text
                        sub_element = tag_node.find(field)
                        tag_data[field] = (
                            sub_element.text.strip() if sub_element is not None and sub_element.text else "N/A"
                        )
                        # Debug log for sub-element extraction
                        if self.debug:
                            print(f"[DEBUG] Extracted sub-element '{field}': {tag_data[field]}")
                except AttributeError:
                    # Handle missing field by setting it to 'N/A' and logging a warning
                    print(f"Warning: Field '{field}' missing in tag '{tag}'. Setting to 'N/A'.")
                    tag_data[field] = "N/A"
                    # Debug log for missing field
                    if self.debug:
                        print(f"[DEBUG] Field '{field}' missing, set to 'N/A'.")

            # Add extracted data to metadata dictionary if any data was found
            if not tag_data:
                print(f"Warning: No data extracted for tag '{tag}'.")
            else:
                # Add the extracted data to the metadata dictionary under the tag name
                metadata[tag] = tag_data

            # Debug log after completing extraction for the current tag
            if self.debug:
                print(f"[DEBUG] Completed extraction for tag '{tag}' with data: {tag_data}")

        # Return the populated metadata dictionary or None if no data was extracted
        return metadata if metadata else None
