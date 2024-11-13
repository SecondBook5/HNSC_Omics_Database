from typing import Optional, Any, List

class TreeNode:
    """
    A class to represent a node in the binary tree.

    Attributes:
        value (Any): The value/data stored in the node.
        left (Optional[TreeNode]): The left child of the node.
        right (Optional[TreeNode]): The right child of the node.
    """

    def __init__(self, value: Any):
        """
        Initialize a TreeNode with a given value.

        Args:
            value (Any): The value/data stored in the node.
        """
        # The value of the current node
        self.value = value
        # Left child node, initially set to None
        self.left: Optional[TreeNode] = None
        # Right child node, initially set to None
        self.right: Optional[TreeNode] = None


class BinaryTree:
    """
    A class to represent a binary tree and perform various operations such as insertion, traversal, and searching.
    """

    def __init__(self):
        """
        Initialize an empty binary tree.
        """
        # The root of the binary tree, initially set to None
        self.root: Optional[TreeNode] = None

    def insert(self, value: Any) -> None:
        """
        Insert a value into the binary tree.

        Args:
            value (Any): The value to be inserted into the tree.
        """
        # Create a new node with the given value
        new_node = TreeNode(value)

        # If the tree is empty, set the new node as the root
        if self.root is None:
            self.root = new_node
        else:
            # Call the helper function to find the correct position for insertion
            self._insert_recursive(self.root, new_node)

    def _insert_recursive(self, current_node: TreeNode, new_node: TreeNode) -> None:
        """
        A helper function to recursively insert a new node into the binary tree.

        Args:
            current_node (TreeNode): The current node in the traversal.
            new_node (TreeNode): The new node to be inserted.
        """
        # If the new node's value is less than the current node's value, go left
        if new_node.value < current_node.value:
            if current_node.left is None:
                current_node.left = new_node
            else:
                # Recursively insert into the left subtree
                self._insert_recursive(current_node.left, new_node)
        else:
            # If the new node's value is greater or equal, go right
            if current_node.right is None:
                current_node.right = new_node
            else:
                # Recursively insert into the right subtree
                self._insert_recursive(current_node.right, new_node)

    def search(self, value: Any) -> Optional[TreeNode]:
        """
        Search for a value in the binary tree.

        Args:
            value (Any): The value to search for.

        Returns:
            Optional[TreeNode]: The node containing the value, or None if not found.
        """
        # Call the helper function to search the value recursively
        return self._search_recursive(self.root, value)

    def _search_recursive(self, current_node: Optional[TreeNode], value: Any) -> Optional[TreeNode]:
        """
        A helper function to recursively search for a value in the binary tree.

        Args:
            current_node (Optional[TreeNode]): The current node in the traversal.
            value (Any): The value to search for.

        Returns:
            Optional[TreeNode]: The node containing the value, or None if not found.
        """
        # If the current node is None, the value is not found
        if current_node is None:
            return None

        # If the current node's value matches the search value, return the node
        if current_node.value == value:
            return current_node

        # If the value is less, search in the left subtree
        if value < current_node.value:
            return self._search_recursive(current_node.left, value)
        else:
            # Otherwise, search in the right subtree
            return self._search_recursive(current_node.right, value)

    def in_order_traversal(self) -> List[Any]:
        """
        Perform in-order traversal of the binary tree and return the values in a list.

        Returns:
            List[Any]: A list of values from the tree in in-order.
        """
        # Initialize an empty list to store traversal result
        result = []
        # Call the helper function to perform the traversal
        self._in_order_recursive(self.root, result)
        return result

    def _in_order_recursive(self, current_node: Optional[TreeNode], result: List[Any]) -> None:
        """
        A helper function to recursively perform in-order traversal.

        Args:
            current_node (Optional[TreeNode]): The current node in the traversal.
            result (List[Any]): The list to store traversal results.
        """
        # If the current node is None, return (base case)
        if current_node is None:
            return

        # Traverse the left subtree
        self._in_order_recursive(current_node.left, result)
        # Visit the current node and add its value to the result
        result.append(current_node.value)
        # Traverse the right subtree
        self._in_order_recursive(current_node.right, result)

    def pre_order_traversal(self) -> List[Any]:
        """
        Perform pre-order traversal of the binary tree and return the values in a list.

        Returns:
            List[Any]: A list of values from the tree in pre-order.
        """
        result = []
        self._pre_order_recursive(self.root, result)
        return result

    def _pre_order_recursive(self, current_node: Optional[TreeNode], result: List[Any]) -> None:
        """
        A helper function to recursively perform pre-order traversal.

        Args:
            current_node (Optional[TreeNode]): The current node in the traversal.
            result (List[Any]): The list to store traversal results.
        """
        if current_node is None:
            return
        # Visit the current node first
        result.append(current_node.value)
        # Traverse the left subtree
        self._pre_order_recursive(current_node.left, result)
        # Traverse the right subtree
        self._pre_order_recursive(current_node.right, result)

    def post_order_traversal(self) -> List[Any]:
        """
        Perform post-order traversal of the binary tree and return the values in a list.

        Returns:
            List[Any]: A list of values from the tree in post-order.
        """
        result = []
        self._post_order_recursive(self.root, result)
        return result

    def _post_order_recursive(self, current_node: Optional[TreeNode], result: List[Any]) -> None:
        """
        A helper function to recursively perform post-order traversal.

        Args:
            current_node (Optional[TreeNode]): The current node in the traversal.
            result (List[Any]): The list to store traversal results.
        """
        if current_node is None:
            return
        # Traverse the left subtree
        self._post_order_recursive(current_node.left, result)
        # Traverse the right subtree
        self._post_order_recursive(current_node.right, result)
        # Visit the current node
        result.append(current_node.value)

    def find_min(self) -> Optional[Any]:
        """
        Find the minimum value in the binary tree.

        Returns:
            Optional[Any]: The minimum value, or None if the tree is empty.
        """
        # Start the search from the root
        if self.root is None:
            return None

        # Traverse to the leftmost node to find the minimum value
        current_node = self.root
        while current_node.left is not None:
            current_node = current_node.left

        return current_node.value

    def find_max(self) -> Optional[Any]:
        """
        Find the maximum value in the binary tree.

        Returns:
            Optional[Any]: The maximum value, or None if the tree is empty.
        """
        # Start the search from the root
        if self.root is None:
            return None

        # Traverse to the rightmost node to find the maximum value
        current_node = self.root
        while current_node.right is not None:
            current_node = current_node.right

        return current_node.value

    def size(self) -> int:
        """
        Return the number of nodes in the tree.

        Returns:
            int: The number of nodes in the tree.
        """
        return self._count_nodes(self.root)

    def _count_nodes(self, current_node: Optional[TreeNode]) -> int:
        """
        Helper function to count the nodes in the binary tree.

        Args:
            current_node (Optional[TreeNode]): The current node being counted.

        Returns:
            int: The number of nodes in the tree.
        """
        if current_node is None:
            return 0
        # Count the current node and recursively count the left and right subtrees
        return 1 + self._count_nodes(current_node.left) + self._count_nodes(current_node.right)

    def is_balanced(self) -> bool:
        """
        Check if the binary tree is balanced. A tree is balanced if the heights of the two child subtrees of any node differ by no more than one.

        Returns:
            bool: True if the tree is balanced, False otherwise.
        """
        return self._check_balance(self.root) != -1

    def _check_balance(self, current_node: Optional[TreeNode]) -> int:
        """
        Helper function to check the balance of the binary tree.

        Args:
            current_node (Optional[TreeNode]): The current node being checked.

        Returns:
            int: The height of the node if balanced, -1 if not balanced.
        """
        if current_node is None:
            return 0

        # Recursively check the balance of the left subtree
        left_height = self._check_balance(current_node.left)
        if left_height == -1:
            return -1

        # Recursively check the balance of the right subtree
        right_height = self._check_balance(current_node.right)
        if right_height == -1:
            return -1

        # If the difference in heights is more than 1, the tree is not balanced
        if abs(left_height - right_height) > 1:
            return -1

        # Return the height of the current node
        return max(left_height, right_height) + 1
