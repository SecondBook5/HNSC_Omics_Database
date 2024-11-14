from typing import Dict, List
from utils.data_structures.xml_tree import XMLTree

def validate_tags(xml_tree: XMLTree, fields_to_extract: Dict[str, List[str]]) -> bool:
    """
    Validates that all required tags are present in the parsed XMLTree structure.

    Args:
        xml_tree (XMLTree): The XMLTree structure parsed from the XML file.
        fields_to_extract (Dict[str, List[str]]): The expected tags and fields to extract.

    Returns:
        bool: True if all required tags are found, False otherwise.
    """
    # Define a recursive function to validate tags at each level of the XMLTree
    def validate_node(node, expected_fields):
        # Check if the current node tag is in expected fields to extract
        if node.tag not in expected_fields:
            return False

        # Validate all expected sub-tags or attributes
        for sub_field in expected_fields[node.tag]:
            # If the sub_field is not in attributes or text is missing, log error and return False
            if sub_field not in node.attributes and not node.find_element_by_tag(sub_field):
                print(f"Error: Missing required field '{sub_field}' in tag '{node.tag}'")
                return False

        # Validate all children recursively
        for child in node.children:
            if not validate_node(child, expected_fields):
                return False

        return True

    # Start validation from the root
    return validate_node(xml_tree.root, fields_to_extract)
