from typing import List, Optional, Dict, Union


class XMLTreeNode:
    """
    A class to represent a node in an XML-like tree structure, including tags, attributes, and text content.

    Attributes:
        tag (str): The name of the XML element.
        attributes (Dict[str, str]): A dictionary of attribute names and values for the XML element.
        text (Optional[str]): The text content of the XML element, if any.
        children (List[XMLTreeNode]): List of child nodes representing nested XML elements.
    """

    def __init__(self, tag: str, attributes: Optional[Dict[str, str]] = None, text: Optional[str] = None):
        # Validate that the tag is not empty, as XML elements require a tag name
        if not tag:
            raise ValueError("Tag cannot be an empty string.")

        # Set the tag for the current XML element
        self.tag = tag

        # Initialize the attributes dictionary; set to empty if not provided
        self.attributes = attributes if attributes else {}

        # Initialize text content for the XML element; set to empty if not provided
        self.text = text if text else ""

        # Initialize an empty list to store child XMLTreeNode elements
        self.children: List[XMLTreeNode] = []

    def add_child(self, tag: str, attributes: Optional[Dict[str, str]] = None,
                  text: Optional[str] = None) -> 'XMLTreeNode':
        """
        Add a child XML element to the current element, with validation for an empty tag.

        Args:
            tag (str): The name of the child XML element.
            attributes (Optional[Dict[str, str]]): The attributes for the child element.
            text (Optional[str]): The text content for the child element.

        Returns:
            XMLTreeNode: The newly created child node.

        Raises:
            ValueError: If the child tag is empty.
        """
        # Ensure the child tag is not empty for valid XML element representation
        if not tag:
            raise ValueError("Child tag cannot be an empty string.")

        # Create a new XMLTreeNode instance with the given tag, attributes, and text
        child_node = XMLTreeNode(tag, attributes, text)

        # Append the newly created child node to the children list
        self.children.append(child_node)

        # Return the new child node for potential chaining or further modifications
        return child_node

    def find_children_by_attribute(self, attr_name: str, attr_value: str) -> List['XMLTreeNode']:
        """
        Find all child elements with a specified attribute and value, returning them in a list.

        Args:
            attr_name (str): The name of the attribute to search for.
            attr_value (str): The value of the attribute to match.

        Returns:
            List[XMLTreeNode]: List of child elements with the specified attribute and value.
        """
        # Initialize a list to hold all matching children
        matching_children = []

        # Loop through each child and check if it has the specified attribute and value
        for child in self.children:
            if child.attributes.get(attr_name) == attr_value:
                matching_children.append(child)

        # Return the list of all children that matched the attribute and value criteria
        return matching_children


class XMLTree:
    """
    An XML-specific tree structure, allowing traversal and data extraction from XML-like hierarchies.

    Attributes:
        root (XMLTreeNode): The root node of the XML tree, representing the root element.
    """

    def __init__(self, root_tag: str, root_attributes: Optional[Dict[str, str]] = None):
        # Validate that the root tag is not empty for a valid XML element
        if not root_tag:
            raise ValueError("Root tag cannot be an empty string.")

        # Initialize the root of the XML tree as an XMLTreeNode with the specified tag and attributes
        self.root = XMLTreeNode(root_tag, root_attributes)

    def traverse_tags(self) -> List[str]:
        """
        Perform a depth-first traversal of the XML tree, collecting element tags in a list.

        Returns:
            List[str]: A list containing the tags of elements visited in depth-first order.
        """
        # Initialize an empty list to store tags collected during traversal
        result = []

        # Define a recursive helper function to traverse the XML tree nodes
        def _traverse(node: XMLTreeNode) -> None:
            # Append the tag of the current node to the result list
            result.append(node.tag)

            # Recursively traverse each child node
            for child in node.children:
                _traverse(child)

        # Begin traversal from the root node
        _traverse(self.root)

        # Return the list of collected tags
        return result

    def find_element_by_tag(self, tag: str) -> Optional[XMLTreeNode]:
        """
        Find the first element in the XML tree with a specified tag.

        Args:
            tag (str): The name of the XML element to search for.

        Returns:
            Optional[XMLTreeNode]: The first element with the specified tag, or None if not found.
        """
        # Validate that the search tag is not empty
        if not tag:
            raise ValueError("Search tag cannot be an empty string.")

        # Define a recursive helper function to search for the node by tag
        def _find(node: XMLTreeNode) -> Optional[XMLTreeNode]:
            # If the current node's tag matches the search tag, return this node
            if node.tag == tag:
                return node

            # Recursively search each child node for a matching tag
            for child in node.children:
                found = _find(child)
                if found:
                    return found

            # Return None if no matching tag is found in this subtree
            return None

        # Start the search from the root node and return the result
        return _find(self.root)

    def collect_text_content(self) -> List[str]:
        """
        Collect the text content of each element in the XML tree in depth-first order.

        Returns:
            List[str]: A list of text content from each element with non-empty text.
        """
        # Initialize an empty list to store text content from nodes
        result = []

        # Define a recursive helper function to collect text content
        def _collect_text(node: XMLTreeNode) -> None:
            # Add the text content of the current node to the result if it is non-empty
            if node.text:
                result.append(node.text)

            # Recursively collect text content from each child node
            for child in node.children:
                _collect_text(child)

        # Start collecting text from the root node
        _collect_text(self.root)

        # Return the list of collected text content
        return result

    def collect_elements_with_specific_tags(self, tags: List[str]) -> List[Dict[str, Union[str, Dict[str, str]]]]:
        """
        Traverse the XML tree and collect elements with specific tags and their attributes.

        Args:
            tags (List[str]): The list of tags to collect from the tree.

        Returns:
            List[Dict[str, Union[str, Dict[str, str]]]]: A list of dictionaries representing elements with the specified tags, each containing 'tag', 'attributes', and 'text'.
        """
        # Initialize an empty list to store dictionaries of elements with specific tags
        result = []

        # Define a recursive helper function to collect nodes with specified tags
        def _collect_specific(node: XMLTreeNode) -> None:
            # If the current node's tag is in the list of tags to collect, add it to the result
            if node.tag in tags:
                result.append({
                    "tag": node.tag,
                    "attributes": node.attributes,
                    "text": node.text
                })

            # Recursively collect specific tags from each child node
            for child in node.children:
                _collect_specific(child)

        # Start collecting elements with specific tags from the root node
        _collect_specific(self.root)

        # Return the list of collected elements with specified tags and attributes
        return result

    def find_elements_by_tag_and_attribute(self, tag: str, attr_name: str, attr_value: str) -> List[XMLTreeNode]:
        """
        Find all elements with a specific tag and attribute value.

        Args:
            tag (str): The tag name to search for.
            attr_name (str): The name of the attribute.
            attr_value (str): The value of the attribute.

        Returns:
            List[XMLTreeNode]: List of nodes matching the tag and attribute criteria.
        """
        # Initialize an empty list to store matches
        matches = []

        # Define a recursive helper function to find nodes by tag and attribute
        def _find(node: XMLTreeNode):
            # Check if the current node's tag and attribute match the search criteria
            if node.tag == tag and node.attributes.get(attr_name) == attr_value:
                matches.append(node)

            # Recursively search each child node for matches
            for child in node.children:
                _find(child)

        # Start the search from the root node
        _find(self.root)

        # Return the list of all matching nodes
        return matches
