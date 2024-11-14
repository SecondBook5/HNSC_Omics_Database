import xml.etree.ElementTree as ET
from utils.data_structures.xml_tree import XMLTree, XMLTreeNode
from typing import Dict, List


def parse_and_populate_xml_tree(xml_path: str, fields_to_extract: Dict[str, List[str]]) -> XMLTree:
    """
    Parse an XML file and populate an XMLTree structure, collecting only specified tags and fields.

    Args:
        xml_path (str): Path to the XML file.
        fields_to_extract (Dict[str, List[str]]): Dictionary where keys are tags to extract,
                                                  and values are lists of specific fields (attributes or sub-tags) to extract.

    Returns:
        XMLTree: Populated XMLTree structure containing nodes with specified tags and fields.

    Raises:
        FileNotFoundError: If the XML file does not exist at the specified path.
        ValueError: If there is an error parsing the XML file or if fields_to_extract contains invalid entries.
    """
    # Ensure the fields_to_extract dictionary is not empty
    if not fields_to_extract:
        raise ValueError("The fields_to_extract dictionary cannot be empty.")

    # Verify XML file existence and parse it
    try:
        tree = ET.parse(xml_path)  # Parse the XML file into an ElementTree object
    except FileNotFoundError:
        # Raise a FileNotFoundError if the file is not found
        raise FileNotFoundError(f"The specified XML file '{xml_path}' does not exist.")
    except ET.ParseError as e:
        # Raise a ValueError if there is a parsing error in the XML file
        raise ValueError(f"Error parsing XML file '{xml_path}': {e}")

    # Get the root element of the XML file
    root_element: ET.Element = tree.getroot()

    # Initialize XMLTree with the root tag and its attributes
    root_tag: str = root_element.tag
    root_attributes: Dict[str, str] = root_element.attrib
    xml_tree = XMLTree(root_tag, root_attributes)

    # Internal recursive function to populate the XMLTree with specified tags and fields
    def populate_tree(xml_element: ET.Element, tree_node: XMLTreeNode) -> None:
        """
        Recursive function to populate XMLTree with specified tags and fields.

        Args:
            xml_element (ET.Element): Current XML element being processed.
            tree_node (XMLTreeNode): Corresponding XMLTreeNode in the XMLTree structure.

        Raises:
            ValueError: If an invalid field name is encountered during extraction.
        """
        # Process each child element within the current XML element
        for child in xml_element:
            tag: str = child.tag

            # Check if the child's tag is one of the tags specified for extraction
            if tag in fields_to_extract:
                extracted_data: Dict[str, str] = {}

                # Iterate over each field specified for the current tag
                for field in fields_to_extract[tag]:
                    try:
                        # Check if the field is an attribute of the XML element
                        if field in child.attrib:
                            extracted_data[field] = child.attrib[field]
                        else:
                            # Otherwise, assume the field is a sub-element and retrieve its text
                            sub_element = child.find(field)
                            # Extract and clean text, set None if field or text is missing
                            extracted_data[
                                field] = sub_element.text.strip() if sub_element is not None and sub_element.text else None
                    except Exception as e:
                        # Raise a ValueError for any unexpected issues
                        raise ValueError(f"Error extracting field '{field}' from tag '{tag}': {e}")

                # Add the child element with extracted data to the XMLTree as a new node
                child_node = tree_node.add_child(tag=tag, attributes=extracted_data,
                                                 text=child.text.strip() if child.text else "")

                # Recursively call populate_tree to process any child nodes of the current element
                populate_tree(child, child_node)
            else:
                # If the tag is not in fields_to_extract, continue traversal without storing
                populate_tree(child, tree_node)

    # Start populating the XML tree structure from the root element
    populate_tree(root_element, xml_tree.root)

    # Return the populated XMLTree structure
    return xml_tree
