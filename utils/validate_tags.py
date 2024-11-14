# File: utils/validate_tags.py
from typing import Dict
from utils.data_structures.xml_tree import XMLTree

def flatten_fields(fields_to_extract: Dict[str, Dict[str, str]]) -> Dict[str, str]:
    """
    Flattens the fields_to_extract dictionary to remove conceptual groupings like 'Series' or 'Sample'.

    Args:
        fields_to_extract (Dict[str, Dict[str, str]]): The original structured dictionary.

    Returns:
        Dict[str, str]: A flattened dictionary with paths as single keys.
    """
    # Initialize flattened dictionary to store field mappings without conceptual groups
    flattened = {}
    # Loop through each group and field in fields_to_extract
    for group, fields in fields_to_extract.items():
        for field, path in fields.items():
            # Ensure path is a string, if not, skip and log for debugging
            if isinstance(path, str):
                flattened[field] = path  # Map directly to the field path
            else:
                print(f"Warning: Skipping field '{field}' in group '{group}' - path is not a string.")
    return flattened

def validate_tags(xml_tree: XMLTree, fields_to_extract: Dict[str, Dict[str, str]], debug: bool = False) -> bool:
    """
    Validates that all required tags and fields are present in the parsed XMLTree structure.

    Args:
        xml_tree (XMLTree): The XMLTree structure parsed from the XML file.
        fields_to_extract (Dict[str, Dict[str, str]]): Expected fields to extract with namespaces.
        debug (bool): If True, enables debug-level output for validation steps.

    Returns:
        bool: True if all required tags and fields are found, False otherwise.
    """
    # Flatten the fields_to_extract dictionary for direct access
    flat_fields = flatten_fields(fields_to_extract)

    # Verify that xml_tree is populated with a root node
    if not xml_tree or not xml_tree.root:
        print("Error: XML tree is empty or malformed.")
        return False

    # Check that flat_fields has content
    if not flat_fields:
        print("Error: No fields provided for validation.")
        return False

    # Determine if there's a namespace in the root tag and extract it if present
    namespace = ''
    if xml_tree.root.tag.startswith('{'):
        # Extract namespace between '{' and '}'
        namespace = xml_tree.root.tag[1:xml_tree.root.tag.find('}')]
    # Add namespace prefix if one exists, otherwise keep it empty
    ns_prefix = f'{{{namespace}}}' if namespace else ''

    # Loop through each field name and its path in the flattened fields dictionary
    for field_name, field_path in flat_fields.items():
        # Add namespace prefix if necessary
        full_path = ns_prefix + field_path if namespace else field_path
        # Attempt to find the specified field path within the XML tree
        sub_element = xml_tree.root.find(full_path)

        # Check if the required field is found in the XML
        if not sub_element or (not sub_element.text and not sub_element.attrib):
            # Log error if the field is missing or empty
            print(f"Error: Missing required field '{field_name}' at path '{full_path}'.")
            return False
        # Print debug message if the field was successfully validated
        if debug:
            print(f"[DEBUG] Validated field '{field_name}' at path '{full_path}'.")

    # Return True if all fields are validated successfully
    return True
