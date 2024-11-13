import xml.etree.ElementTree as ET
from utils.data_structures.xml_tree import XMLTree, XMLTreeNode


def parse_and_populate_xml_tree(xml_path: str, tags_to_extract: list) -> XMLTree:
    """
    Parse an XML file and populate an XMLTree structure, collecting only specified tags.

    Args:
        xml_path (str): Path to the XML file.
        tags_to_extract (list): List of tags to extract from the XML file.

    Returns:
        XMLTree: Populated XMLTree structure containing nodes with specified tags.
    """
    # Verify XML file existence
    try:
        tree = ET.parse(xml_path)
    except FileNotFoundError:
        raise FileNotFoundError(f"The specified XML file '{xml_path}' does not exist.")
    except ET.ParseError as e:
        raise ValueError(f"Error parsing XML file '{xml_path}': {e}")

    # Get the root element of the XML file
    root_element = tree.getroot()

    # Initialize XMLTree with the root tag and its attributes
    root_tag = root_element.tag
    root_attributes = root_element.attrib
    xml_tree = XMLTree(root_tag, root_attributes)

    # Populate the XMLTree with specified tags in a single pass
    def populate_tree(xml_element, tree_node):
        """
        Recursive function to populate XMLTree with specified tags.

        Args:
            xml_element (ET.Element): Current XML element being processed.
            tree_node (XMLTreeNode): Corresponding XMLTreeNode in the XMLTree structure.
        """
        # Process children of the current XML element
        for child in xml_element:
            # If the child's tag is in the list of tags to extract
            if child.tag in tags_to_extract:
                # Capture attributes and text content if they exist
                child_attributes = child.attrib if child.attrib else {}
                child_text = child.text.strip() if child.text else ""

                # Add the child as a node to the XMLTree
                child_node = tree_node.add_child(tag=child.tag, attributes=child_attributes, text=child_text)

                # Recursively populate tree for each child node
                populate_tree(child, child_node)
            else:
                # Skip tags not in the list of tags to extract
                continue

    # Start populating from the root
    populate_tree(root_element, xml_tree.root)

    return xml_tree
