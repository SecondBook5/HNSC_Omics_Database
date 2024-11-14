from typing import List, Optional, Dict, Union

class XMLTreeNode:
    """
    Represents a node in an XML-like tree structure, storing tags, attributes, text, and child nodes.

    Attributes:
        tag (str): The XML element name.
        attributes (Dict[str, str]): A dictionary of attribute names and values.
        text (Optional[str]): The text content within the XML element, if any.
        children (List[XMLTreeNode]): A list of nested child XMLTreeNode objects.
    """

    def __init__(self, tag: str, attributes: Optional[Dict[str, str]] = None, text: Optional[str] = None):
        # Raise an error if the tag is empty, as every XML element requires a name
        if not tag:
            raise ValueError("Tag cannot be an empty string.")
        # Set the tag for this XML element
        self.tag = tag
        # Initialize the attributes dictionary or set to an empty dictionary if None
        self.attributes = attributes if attributes else {}
        # Set text content, defaulting to an empty string if no text is provided
        self.text = text if text else ""
        # Create an empty list to store child XMLTreeNode objects
        self.children: List[XMLTreeNode] = []

    def add_child(self, tag: str, attributes: Optional[Dict[str, str]] = None,
                  text: Optional[str] = None) -> 'XMLTreeNode':
        """
        Adds a child XMLTreeNode to this node.

        Args:
            tag (str): Tag name for the child element.
            attributes (Optional[Dict[str, str]]): Dictionary of attributes for the child.
            text (Optional[str]): Text content for the child element.

        Returns:
            XMLTreeNode: The created child node.

        Raises:
            ValueError: If the child tag is empty.
        """
        # Ensure the tag is provided for the child node
        if not tag:
            raise ValueError("Child tag cannot be an empty string.")
        # Create a new child node with provided tag, attributes, and text
        child_node = XMLTreeNode(tag, attributes, text)
        # Add the created child node to this node's children list
        self.children.append(child_node)
        # Return the created child node for further chaining if needed
        return child_node

    def find(self, tag: str) -> Optional['XMLTreeNode']:
        """
        Finds the first child node with a specific tag within this node's children.

        Args:
            tag (str): Tag name to search for.

        Returns:
            Optional[XMLTreeNode]: First matching child node, or None if not found.
        """
        # Validate that a tag is specified for searching
        if not tag:
            raise ValueError("Search tag cannot be an empty string.")
        # Iterate over child nodes to find the first node with the matching tag
        for child in self.children:
            if child.tag == tag:
                return child
        # Return None if no matching tag is found among children
        return None

    def find_children_by_attribute(self, attr_name: str, attr_value: str) -> List['XMLTreeNode']:
        """
        Finds all child nodes with a specific attribute and value.

        Args:
            attr_name (str): Attribute name to search.
            attr_value (str): Attribute value to match.

        Returns:
            List[XMLTreeNode]: List of child nodes with the matching attribute and value.
        """
        # Return list comprehension of children with specified attribute and value
        return [child for child in self.children if child.attributes.get(attr_name) == attr_value]


class XMLTree:
    """
    Represents an XML tree structure for traversal and data extraction.

    Attributes:
        root (XMLTreeNode): Root node of the XML tree.
    """

    def __init__(self, root_tag: str, root_attributes: Optional[Dict[str, str]] = None):
        # Validate the root tag is provided for the XML tree
        if not root_tag:
            raise ValueError("Root tag cannot be an empty string.")
        # Initialize root node with the specified tag and attributes
        self.root = XMLTreeNode(root_tag, root_attributes)

    def traverse_tags(self) -> List[str]:
        """
        Performs a depth-first traversal of the XML tree to collect tags.

        Returns:
            List[str]: List of tags in depth-first traversal order.
        """
        # Initialize list to store tags encountered during traversal
        result = []

        # Define a recursive helper function to traverse the XML tree nodes
        def _traverse(node: XMLTreeNode) -> None:
            # Append the current node's tag to the result list
            result.append(node.tag)

            # Recursively traverse each child node
            for child in node.children:
                _traverse(child)

        # Begin traversal starting from the root node
        _traverse(self.root)
        # Return the list of tags in traversal order
        return result

    def find_element_by_tag(self, tag: str) -> Optional[XMLTreeNode]:
        """
        Searches the XML tree for the first node with a specific tag.

        Args:
            tag (str): Tag name to search for.

        Returns:
            Optional[XMLTreeNode]: First node with the matching tag, or None if not found.
        """
        # Validate that a tag is specified for searching
        if not tag:
            raise ValueError("Search tag cannot be an empty string.")

        # Define a recursive helper function to search for the node by tag
        def _find(node: XMLTreeNode) -> Optional[XMLTreeNode]:
            # Return current node if tag matches
            if node.tag == tag:
                return node
            # Recursively search each child node
            for child in node.children:
                found = _find(child)
                if found:
                    return found
            # Return None if no matching node is found
            return None

        # Start search from root node and return result
        return _find(self.root)

    def collect_text_content(self) -> List[str]:
        """
        Collects text content from each node in the XML tree.

        Returns:
            List[str]: List of non-empty text content from nodes.
        """
        # Initialize result list to store text content
        result = []

        # Define a recursive helper function to collect text content
        def _collect_text(node: XMLTreeNode) -> None:
            # Append non-empty text content to result
            if node.text:
                result.append(node.text)
            # Recursively collect text from each child node
            for child in node.children:
                _collect_text(child)

        # Begin text collection starting from the root node
        _collect_text(self.root)
        # Return the collected text content list
        return result

    def collect_elements_with_specific_tags(self, tags: List[str]) -> List[Dict[str, Union[str, Dict[str, str]]]]:
        """
        Collects elements with specific tags, storing their attributes and text.

        Args:
            tags (List[str]): List of tags to collect.

        Returns:
            List[Dict[str, Union[str, Dict[str, str]]]]: List of dictionaries with elements' tag, attributes, and text.
        """
        # Initialize result list for specific tag elements
        result = []

        # Define a recursive helper function to collect nodes with specified tags
        def _collect_specific(node: XMLTreeNode) -> None:
            # Add node info to result if tag is in specified tags
            if node.tag in tags:
                result.append({
                    "tag": node.tag,
                    "attributes": node.attributes,
                    "text": node.text
                })
            # Recursively collect specified tags from child nodes
            for child in node.children:
                _collect_specific(child)

        # Start collecting specific tags from root node
        _collect_specific(self.root)
        # Return list of elements with specified tags
        return result

    def find_elements_by_tag_and_attribute(self, tag: str, attr_name: str, attr_value: str) -> List[XMLTreeNode]:
        """
        Finds elements with a specific tag and attribute.

        Args:
            tag (str): Tag name to search for.
            attr_name (str): Attribute name.
            attr_value (str): Attribute value to match.

        Returns:
            List[XMLTreeNode]: List of nodes with matching tag and attribute.
        """
        # Initialize list to store matching nodes
        matches = []

        # Define a recursive helper function to find nodes by tag and attribute
        def _find(node: XMLTreeNode):
            # Add node to matches if tag and attribute match
            if node.tag == tag and node.attributes.get(attr_name) == attr_value:
                matches.append(node)
            # Recursively search each child for matches
            for child in node.children:
                _find(child)

        # Begin search from root node
        _find(self.root)
        # Return list of matching nodes
        return matches
