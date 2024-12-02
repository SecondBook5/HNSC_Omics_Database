"""
graph.py

This module provides a Graph class to represent directed and undirected graphs using
adjacency lists. It supports fundamental graph operations, including adding vertices
and edges, performing graph traversals, detecting cycles, and exporting/importing
the graph structure to/from JSON and CSV files.

Key Classes and Functionality:

1. GraphVertexError and GraphEdgeError:
   - Custom exceptions to handle specific errors related to invalid vertex or edge
     operations within the graph.

2. Vertex Class:
   - Represents a vertex within the graph, storing its ID and associated edges.
   - This class maintains an edge list, supporting weighted and unweighted edges.

3. Edge Class:
   - Represents a directed edge between two vertices in the graph, with an optional
     weight attribute.
   - This class is used to structure edges as objects, enabling easy management and
     traversal.

4. Graph Class:
   - Core class that orm_models the graph, supporting both directed and undirected graphs.
   - It stores vertices in an adjacency list format, where each vertex ID points to
     its associated Vertex object.
   - Graph creation is flexible, allowing options to enable/disable self-loops,
     duplicate edges, and directionality.

Key Methods in the Graph Class:

- add_vertex(vertex_id: str) -> None:
  Adds a unique vertex to the graph. Raises an error if the vertex ID is empty or invalid.

- add_edge(start_vertex: str, end_vertex: str, weight: Optional[int] = None) -> None:
  Adds an edge from start_vertex to end_vertex. Supports weights and manages reverse
  edges automatically in undirected graphs. Validates vertices and edge constraints.

- has_edge(start_vertex: str, end_vertex: str) -> bool:
  Checks if an edge exists between two specified vertices.

- bfs(start_vertex: str) -> List[str]:
  Performs a Breadth-First Search traversal from the given start vertex, visiting each
  connected node layer by layer.

- dfs(start_vertex: str) -> List[str]:
  Performs a Depth-First Search traversal, exploring each path to its end before
  backtracking.

- detect_cycle() -> bool:
  Uses DFS to detect cycles in the graph. Identifies both simple and complex cycles
  and supports directed/undirected graph configurations.

- export_to_json(file_path: str) -> None:
  Exports the graph structure to a JSON file, making it compatible for data exchange
  or storage.

- import_from_json(file_path: str) -> None:
  Imports a graph structure from a JSON file, reconstructing vertices and edges.

- export_to_csv(file_path: str) -> None:
  Exports the graph structure to a CSV file, where each row represents an edge.

- import_from_csv(file_path: str) -> None:
  Imports a graph structure from a CSV file, rebuilding the graph.

Notes on Error Handling and Edge Cases:
- Extensive error handling is implemented, ensuring that invalid inputs (e.g.,
  non-string vertex IDs, nonexistent vertices for edges) are managed gracefully.
- Optional configurations (allow_self_loops, allow_duplicate_edges) provide flexibility
  and ensure that constraints are enforced based on the graph’s purpose.
- Defensive programming techniques are applied, logging warnings or errors when
  operations are misconfigured or constraints are violated.

Usage:
This module can be used to represent, manipulate, and analyze graphs across
various applications, from simple traversals to complex network structures.

"""
import logging
from typing import Dict, List, Set, Optional
from collections import deque
import json
import csv


# Define custom exceptions for more granular error handling
class GraphVertexError(Exception):
    """Exception raised when there is an issue with vertex operations, such as invalid or missing vertex IDs."""
    pass


class GraphEdgeError(Exception):
    """Exception raised when there is an issue with edge operations, such as invalid vertices or duplicate edges."""
    pass


class Vertex:
    """
    Represents a vertex in the graph, storing the vertex ID and its associated edges.

    Each vertex holds an identifier and a list of edges connecting it to other vertices.
    This class is primarily used by the Graph class to manage connections within the graph.
    """

    def __init__(self, vertex_id: str):
        # Ensure the vertex ID is a non-empty string
        if not isinstance(vertex_id, str) or not vertex_id:
            raise ValueError("Vertex ID must be a non-empty string.")
        # Store the vertex identifier
        self.vertex_id = vertex_id
        # Initialize an empty list to store edges for this vertex
        self.edges: List['Edge'] = []  # Holds Edge objects connecting this vertex to others


class Edge:
    """
    Represents a graph edge, storing start and end vertices, and an optional weight.

    An edge links two vertices, with an optional weight for weighted graphs. This class
    is used by the Graph to create connections between vertices.
    """

    def __init__(self, start: str, end: str, weight: Optional[int] = None):
        # Store the start vertex of the edge
        self.start = start
        # Store the end vertex of the edge
        self.end = end
        # Store the weight of the edge (optional)
        self.weight = weight

    def __str__(self):
        """
        Return a string representation of the edge, including weight if present.

        Returns:
            str: Formatted string representing the edge.
        """
        # Format the edge as a string, including weight if it exists
        return f"({self.start} -> {self.end}, weight={self.weight})"


class Graph:
    """
    Represents a directed or undirected graph using adjacency lists.
    Supports adding vertices/edges, performing BFS/DFS, detecting cycles,
    and exporting to JSON/CSV, with options to control self-loops and duplicate edges.
    """

    def __init__(self, is_directed: bool = True, allow_self_loops: bool = False, allow_duplicate_edges: bool = False):
        """
        Initialize the graph with an empty adjacency list and customizable options for loops and duplicates.

        Args:
            is_directed: Boolean indicating if the graph is directed. Defaults to True.
            allow_self_loops: Boolean indicating if self-loops are allowed. Defaults to False.
            allow_duplicate_edges: Boolean indicating if duplicate edges are allowed. Defaults to False.
        """
        # Initialize the adjacency list dictionary, which stores each vertex and its edges
        self._adjacency_list: Dict[str, Vertex] = {}
        # Flag to indicate whether the graph is directed or undirected
        self.is_directed = is_directed
        # Flags to control self-loops and duplicate edges
        self.allow_self_loops = allow_self_loops
        self.allow_duplicate_edges = allow_duplicate_edges
        # Set up basic logging configuration for informational messages
        logging.basicConfig(level=logging.INFO)

    def add_vertex(self, vertex_id: str) -> None:
        """
        Add a new vertex to the graph if it doesn't already exist.
        Each vertex is uniquely identified by its ID, and duplicates are ignored with a warning.

        Args:
            vertex_id: The identifier for the vertex.
        """
        # Validate the vertex ID to ensure it's a non-empty string
        if not isinstance(vertex_id, str) or not vertex_id:
            raise GraphVertexError("Vertex ID must be a non-empty string.")

        # Check if the vertex already exists in the adjacency list
        if vertex_id in self._adjacency_list:
            # Log a warning if the vertex exists and return without adding
            logging.warning(f"Vertex '{vertex_id}' already exists.")
            return

        # Create a new Vertex object and add it to the adjacency list
        self._adjacency_list[vertex_id] = Vertex(vertex_id)
        # Log information about the added vertex
        logging.info(f"Vertex '{vertex_id}' added.")

    def add_edge(self, start_vertex: str, end_vertex: str, weight: Optional[int] = None) -> None:
        """
        Add a directed edge between two vertices with an optional weight.
        In undirected graphs, this method also adds a reverse edge for bidirectional linkage.

        Args:
            start_vertex: Starting vertex of the edge.
            end_vertex: Ending vertex of the edge.
            weight: Optional weight for the edge.
        """
        # Ensure both start and end vertices are valid non-empty strings
        if not all(isinstance(v, str) and v for v in [start_vertex, end_vertex]):
            raise GraphEdgeError("Both start and end vertices must be non-empty strings.")

        # Check if the edge is a self-loop and is disallowed
        if start_vertex == end_vertex and not self.allow_self_loops:
            logging.warning(f"Self-loops are not allowed: '{start_vertex}' -> '{end_vertex}' ignored.")
            return

        # Ensure both vertices exist in the graph; otherwise, raise an error
        if start_vertex not in self._adjacency_list or end_vertex not in self._adjacency_list:
            raise GraphEdgeError(f"Vertices '{start_vertex}' and '{end_vertex}' must exist.")

        # Check if the edge already exists and is disallowed
        if self.has_edge(start_vertex, end_vertex) and not self.allow_duplicate_edges:
            logging.warning(f"Duplicate edge '{start_vertex}' -> '{end_vertex}' ignored.")
            return

        # Add the directed edge from start_vertex to end_vertex
        self._adjacency_list[start_vertex].edges.append(Edge(start_vertex, end_vertex, weight))
        # Log information about the added edge
        logging.info(f"Edge from '{start_vertex}' to '{end_vertex}' added with weight {weight}.")

        # If the graph is undirected, add the reverse edge for bidirectional linkage
        if not self.is_directed:
            self._adjacency_list[end_vertex].edges.append(Edge(end_vertex, start_vertex, weight))

    def has_edge(self, start_vertex: str, end_vertex: str) -> bool:
        """
        Check if an edge exists between two vertices by searching the adjacency list.
        Verifies that both vertices are present and returns a boolean for edge existence.

        Args:
            start_vertex: The starting vertex.
            end_vertex: The ending vertex.

        Returns:
            True if edge exists, False otherwise.
        """
        # Ensure both vertices exist in the graph
        if start_vertex not in self._adjacency_list or end_vertex not in self._adjacency_list:
            raise GraphEdgeError(f"Vertices '{start_vertex}' and '{end_vertex}' must exist.")

        # Check each edge of the start_vertex to see if it connects to end_vertex
        return any(edge.end == end_vertex for edge in self._adjacency_list[start_vertex].edges)

    def get_adjacency_list(self) -> Dict[str, List[Edge]]:
        """
        Retrieve the adjacency list of the graph.

        Returns:
            Dict[str, List[Edge]]: A dictionary where each key is a vertex ID, and the value
            is a list of edges originating from that vertex.
        """
        try:
            # Construct a dictionary representation of the adjacency list
            adjacency_list = {
                vertex_id: vertex.edges
                for vertex_id, vertex in self._adjacency_list.items()
            }
            # Log the successful retrieval of the adjacency list
            logging.info("Adjacency list retrieved successfully.")
            return adjacency_list
        except Exception as e:
            # Log any errors encountered during adjacency list retrieval
            logging.error(f"Error retrieving adjacency list: {e}")
            raise

    def bfs(self, start_vertex: str) -> List[str]:
        """
        Perform Breadth-First Search (BFS) starting from a given vertex.
        BFS explores each vertex layer by layer, finding the shortest path in unweighted graphs.

        Args:
            start_vertex: The vertex to start BFS.

        Returns:
            List of vertices visited in BFS order.
        """
        # Ensure the start_vertex exists in the graph
        if start_vertex not in self._adjacency_list:
            raise GraphVertexError(f"Vertex '{start_vertex}' does not exist.")

        # Initialize the BFS queue with the start vertex
        queue, visited, result = deque([start_vertex]), {start_vertex}, []

        # Process each vertex in the queue until it's empty
        while queue:
            # Dequeue the front vertex
            vertex = queue.popleft()
            # Add the vertex to the BFS result
            result.append(vertex)

            # Loop through each neighbor of the current vertex
            for edge in self._adjacency_list[vertex].edges:
                # If the neighbor hasn't been visited, mark it visited and enqueue it
                if edge.end not in visited:
                    queue.append(edge.end)
                    visited.add(edge.end)

        # Log and return the BFS traversal result
        logging.info(f"BFS from '{start_vertex}' visited: {result}")
        return result

    def dfs(self, start_vertex: str) -> List[str]:
        """
        Perform Depth-First Search (DFS) starting from a given vertex.
        DFS explores each vertex along one branch to its end before backtracking, which can be useful
        for tasks like cycle detection, pathfinding, and discovering deep graph structures.

        Args:
            start_vertex: The vertex to start DFS.

        Returns:
            List of vertices visited in DFS order.
        """
        # Ensure the start_vertex exists in the graph
        if start_vertex not in self._adjacency_list:
            raise GraphVertexError(f"Vertex '{start_vertex}' does not exist.")

        # Initialize visited set and result list for storing DFS traversal order
        visited, result = set(), []

        # Perform DFS traversal using a helper recursive function
        self._dfs_recursive(start_vertex, visited, result)
        # Log and return the DFS traversal result
        logging.info(f"DFS from '{start_vertex}' visited: {result}")
        return result

    def _dfs_recursive(self, vertex: str, visited: Set[str], result: List[str]) -> None:
        """
        A recursive helper for DFS traversal that explores as far as possible before backtracking.

        Args:
            vertex: Current vertex being visited.
            visited: Set of visited vertices.
            result: List storing the visit order.
        """
        # Mark the current vertex as visited and add it to the result
        visited.add(vertex)
        result.append(vertex)

        # Visit all neighbors of the current vertex recursively
        for edge in self._adjacency_list[vertex].edges:
            # Recurse for each unvisited neighbor
            if edge.end not in visited:
                self._dfs_recursive(edge.end, visited, result)

    def detect_cycle(self, return_nodes: bool = False) -> Optional[bool | List[str]]:
        """
        Detect cycles in the graph using DFS. Optionally, return nodes involved in the first cycle detected.

        Args:
            return_nodes: If True, return the nodes forming the cycle.

        Returns:
            bool: True if a cycle is detected, False otherwise (if return_nodes is False).
            Optional[List[str]]: List of nodes forming a cycle (if return_nodes is True).
        """
        # Initialize visited set and recursion stack
        visited, rec_stack = set(), []

        # Dictionary to map each vertex to its parent in the DFS tree
        parent_map = {}

        # Helper function for DFS cycle detection
        def _dfs_cycle(vertex: str) -> Optional[List[str]]:
            """
            Recursive helper for detecting a cycle.

            Args:
                vertex: The current vertex being visited.

            Returns:
                Optional[List[str]]: Nodes in the cycle if found, None otherwise.
            """
            # Mark the current vertex as visited
            visited.add(vertex)

            # Add the vertex to the recursion stack
            rec_stack.append(vertex)

            # Explore all neighbors of the current vertex
            for edge in self._adjacency_list[vertex].edges:
                neighbor = edge.end

                # If the neighbor is unvisited, continue DFS
                if neighbor not in visited:
                    # Map the neighbor's parent for cycle reconstruction
                    parent_map[neighbor] = vertex

                    # Recur to check for cycles in the neighbor's path
                    cycle = _dfs_cycle(neighbor)

                    # If a cycle is found, propagate it upwards
                    if cycle:
                        return cycle

                # If the neighbor is in the recursion stack, a cycle exists
                elif neighbor in rec_stack:
                    if return_nodes:
                        # Reconstruct the cycle by backtracking through the parent_map
                        cycle_path = []
                        current = vertex

                        # Trace the path until the cycle is reconstructed
                        while current != neighbor:
                            cycle_path.append(current)
                            current = parent_map[current]

                        # Add the start and end of the cycle
                        cycle_path.append(neighbor)
                        cycle_path.append(vertex)

                        # Return the reconstructed cycle path
                        return cycle_path[::-1]
                    else:
                        return True

            # Backtrack by removing the vertex from the recursion stack
            rec_stack.pop()

            # Return None if no cycle is found in this path
            return None

        # Iterate over all vertices in the graph
        for node in self._adjacency_list:
            # Start DFS from unvisited vertices
            if node not in visited:
                result = _dfs_cycle(node)

                # If a cycle is found, return the result
                if result:
                    return result if return_nodes else True

        # If no cycle is detected, return False or an empty list based on return_nodes
        return [] if return_nodes else False

    def get_cycle_nodes(self) -> Optional[List[str]]:
        """
        Retrieve nodes involved in a cycle if one exists.

        Returns:
            Optional[List[str]]: A list of nodes forming a cycle, or None if no cycle is found.
        """
        # Call detect_cycle with the return_nodes flag set to True
        cycle_nodes = self.detect_cycle(return_nodes=True)

        # If cycle_nodes is an empty list, return None for consistency
        return cycle_nodes if cycle_nodes else None

    def export_to_json(self, file_path: str) -> None:
        """
        Export the graph structure to a JSON file, representing each vertex and its edges.
        Each vertex has a list of edges with optional weights, making the JSON representation
        suitable for importing into other systems.

        Args:
            file_path: The path where the JSON file should be saved.
        """
        # Create a dictionary of graph data for JSON export
        graph_dict = {
            v.vertex_id: [(e.end, e.weight) for e in v.edges]
            for v in self._adjacency_list.values()
        }
        # Write the graph structure to JSON file with indentation
        with open(file_path, 'w') as json_file:
            # noinspection PyTypeChecker
            json.dump(graph_dict, json_file, indent=4)
        # Log the successful export
        logging.info(f"Graph exported to JSON file at '{file_path}'")

    def import_from_json(self, file_path: str) -> None:
        """
        Import a graph structure from a JSON file.

        Args:
            file_path: Path to the JSON file.
        """
        # Open the JSON file in read mode
        with open(file_path, 'r') as json_file:
            # Load the JSON data and reconstruct the graph structure
            graph_dict = json.load(json_file)

            # First, add all vertices from the JSON data
            for vertex_id in graph_dict:
                self.add_vertex(vertex_id)

            # Then, add edges with weights if provided
            for vertex_id, edges in graph_dict.items():
                for end_vertex, weight in edges:
                    self.add_edge(vertex_id, end_vertex, weight)

        logging.info(f"Graph imported from JSON file at '{file_path}'")

    def export_to_csv(self, file_path: str) -> None:
        """
        Export the graph structure to a CSV file, with each row representing an edge.
        The CSV format is straightforward and allows for easy visualization and analysis.

        Args:
            file_path: Path to the CSV file where graph edges will be saved.
        """
        # Open the CSV file in write mode
        with open(file_path, mode='w', newline='') as file:
            # Create a CSV writer object
            writer = csv.writer(file)
            # Write header for the CSV file
            writer.writerow(["Start Vertex", "End Vertex", "Weight"])

            # Write each edge in the graph to the CSV file
            for vertex, vertex_obj in self._adjacency_list.items():
                for edge in vertex_obj.edges:
                    writer.writerow([vertex, edge.end, edge.weight])
        logging.info(f"Graph exported to CSV at '{file_path}'")

    def import_from_csv(self, file_path: str) -> None:
        """
        Import a graph structure from a CSV file.

        Args:
            file_path: Path to the CSV file.
        """
        # Open the CSV file in read mode
        with open(file_path, mode='r') as file:
            # Create a CSV reader object
            reader = csv.reader(file)
            # Skip the header row
            next(reader)
            # Iterate through each row and add vertices/edges
            for row in reader:
                start_vertex, end_vertex, weight = row
                weight = int(weight) if weight else None
                self.add_vertex(start_vertex)
                self.add_vertex(end_vertex)
                self.add_edge(start_vertex, end_vertex, weight)
        logging.info(f"Graph imported from CSV file at '{file_path}'")
