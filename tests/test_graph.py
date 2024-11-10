import pytest
import json
from utils.data_structures.graph import Graph, GraphVertexError, GraphEdgeError

def test_add_vertex() -> None:
    """
    Test adding a single vertex to the graph and validate the behavior
    when adding invalid vertices.
    """
    # Initialize a new Graph instance
    graph = Graph()

    # Add a vertex with a valid ID
    graph.add_vertex("A")

    # Assert that the vertex has been added to the adjacency list
    assert "A" in graph._adjacency_list

    # Test that adding an empty string as a vertex raises an error
    with pytest.raises(GraphVertexError):
        graph.add_vertex("")


def test_add_edge() -> None:
    """
    Test adding an edge between two vertices, and ensure exceptions
    are raised when attempting to add edges between non-existent vertices.
    """
    # Create a new Graph instance and add two vertices
    graph = Graph()
    graph.add_vertex("A")
    graph.add_vertex("B")

    # Add a directed edge from A to B with weight 5
    graph.add_edge("A", "B", 5)

    # Confirm that the edge was added with the correct weight
    assert any(edge.end == "B" and edge.weight == 5 for edge in graph._adjacency_list["A"].edges)

    # Test adding an edge with a non-existent vertex, expecting an error
    with pytest.raises(GraphEdgeError):
        graph.add_edge("A", "C")


def test_self_loop() -> None:
    """
    Test the behavior of self-loops in the graph when allowed and disallowed.
    """
    # Initialize a graph where self-loops are not allowed
    graph = Graph(allow_self_loops=False)
    graph.add_vertex("A")

    # Attempt to add a self-loop and verify it does not get added
    graph.add_edge("A", "A")
    assert len(graph._adjacency_list["A"].edges) == 0

    # Initialize a graph where self-loops are allowed
    graph_with_self_loop = Graph(allow_self_loops=True)
    graph_with_self_loop.add_vertex("A")
    graph_with_self_loop.add_edge("A", "A")

    # Verify the self-loop has been added
    assert len(graph_with_self_loop._adjacency_list["A"].edges) == 1


def test_duplicate_edges() -> None:
    """
    Test the handling of duplicate edges in the graph with
    duplicate edges allowed and disallowed.
    """
    # Initialize a graph where duplicate edges are disallowed
    graph = Graph(allow_duplicate_edges=False)
    graph.add_vertex("A")
    graph.add_vertex("B")

    # Add an edge from A to B and attempt to add it again
    graph.add_edge("A", "B")
    graph.add_edge("A", "B")

    # Verify only one edge exists due to disallowed duplicates
    assert len(graph._adjacency_list["A"].edges) == 1

    # Initialize a graph allowing duplicate edges
    graph_with_duplicates = Graph(allow_duplicate_edges=True)
    graph_with_duplicates.add_vertex("A")
    graph_with_duplicates.add_vertex("B")
    graph_with_duplicates.add_edge("A", "B")
    graph_with_duplicates.add_edge("A", "B")

    # Verify both edges are added
    assert len(graph_with_duplicates._adjacency_list["A"].edges) == 2


def test_has_edge() -> None:
    """
    Test checking the existence of an edge between two vertices
    in a directed graph.
    """
    # Initialize a new graph and add two vertices with an edge
    graph = Graph()
    graph.add_vertex("A")
    graph.add_vertex("B")
    graph.add_edge("A", "B")

    # Check that the edge from A to B exists, but B to A does not
    assert graph.has_edge("A", "B") is True
    assert graph.has_edge("B", "A") is False


def test_bfs() -> None:
    """
    Test the Breadth-First Search (BFS) traversal of the graph
    starting from a given vertex.
    """
    # Initialize a graph with three vertices and edges
    graph = Graph()
    graph.add_vertex("A")
    graph.add_vertex("B")
    graph.add_vertex("C")
    graph.add_edge("A", "B")
    graph.add_edge("A", "C")

    # Verify that BFS returns the expected order of traversal
    assert graph.bfs("A") == ["A", "B", "C"]


def test_dfs() -> None:
    """
    Test the Depth-First Search (DFS) traversal of the graph
    starting from a given vertex.
    """
    # Initialize a graph with three vertices and edges
    graph = Graph()
    graph.add_vertex("A")
    graph.add_vertex("B")
    graph.add_vertex("C")
    graph.add_edge("A", "B")
    graph.add_edge("A", "C")

    # Verify that DFS returns the expected order of traversal
    assert graph.dfs("A") == ["A", "B", "C"]


def test_detect_cycle() -> None:
    """
    Test cycle detection in the graph by constructing graphs
    with and without cycles.
    """
    # Create an acyclic graph and confirm no cycle is detected
    graph = Graph()
    graph.add_vertex("A")
    graph.add_vertex("B")
    graph.add_edge("A", "B")
    assert graph.detect_cycle() is False

    # Create a cyclic graph and confirm a cycle is detected
    cyclic_graph = Graph()
    cyclic_graph.add_vertex("A")
    cyclic_graph.add_vertex("B")
    cyclic_graph.add_edge("A", "B")
    cyclic_graph.add_edge("B", "A")
    assert cyclic_graph.detect_cycle() is True


def test_export_import_json(tmp_path) -> None:
    """
    Test exporting the graph structure to a JSON file and then
    importing it back to ensure consistency.
    """
    # Create a graph, add vertices, and add an edge with weight
    graph = Graph()
    graph.add_vertex("A")
    graph.add_vertex("B")
    graph.add_edge("A", "B", 3)

    # Export the graph to JSON
    json_file = tmp_path / "graph.json"
    graph.export_to_json(str(json_file))

    # Import the graph from JSON
    new_graph = Graph()
    new_graph.import_from_json(str(json_file))

    # Verify that the edge is present and weight matches
    assert new_graph.has_edge("A", "B")
    assert any(edge.weight == 3 for edge in new_graph._adjacency_list["A"].edges)


def test_export_import_csv(tmp_path) -> None:
    """
    Test exporting the graph structure to a CSV file and then
    importing it back to ensure consistency.
    """
    # Create a graph, add vertices, and add an edge with weight
    graph = Graph()
    graph.add_vertex("A")
    graph.add_vertex("B")
    graph.add_edge("A", "B", 3)

    # Export the graph to CSV
    csv_file = tmp_path / "graph.csv"
    graph.export_to_csv(str(csv_file))

    # Import the graph from CSV
    new_graph = Graph()
    new_graph.import_from_csv(str(csv_file))

    # Verify that the edge is present and weight matches
    assert new_graph.has_edge("A", "B")
    assert any(edge.weight == 3 for edge in new_graph._adjacency_list["A"].edges)


def test_invalid_vertices() -> None:
    """
    Test error handling when adding invalid vertices and edges
    involving non-existent vertices.
    """
    # Initialize a graph
    graph = Graph()

    # Verify that adding an empty vertex ID raises an error
    with pytest.raises(GraphVertexError):
        graph.add_vertex("")

    # Verify that adding an edge with non-existent vertices raises an error
    with pytest.raises(GraphEdgeError):
        graph.add_edge("A", "B")

# Test adding a self-loop with weight in a graph that allows self-loops
def test_add_self_loop_with_weight() -> None:
    """Test adding a self-loop with a weight when self-loops are allowed."""
    # Initialize a graph with self-loops allowed
    graph = Graph(allow_self_loops=True)
    # Add a vertex "A" to the graph
    graph.add_vertex("A")
    # Add a self-loop from "A" to itself with weight 10
    graph.add_edge("A", "A", 10)
    # Verify the self-loop was added
    assert len(graph._adjacency_list["A"].edges) == 1
    # Check that the self-loop's weight is correctly set to 10
    assert graph._adjacency_list["A"].edges[0].weight == 10

# Test adding duplicate edges with different weights in a graph that allows duplicates
def test_duplicate_edges_with_different_weights() -> None:
    """Test adding duplicate edges with different weights when duplicates are allowed."""
    # Initialize a graph with duplicate edges allowed
    graph = Graph(allow_duplicate_edges=True)
    # Add vertices "A" and "B"
    graph.add_vertex("A")
    graph.add_vertex("B")
    # Add two edges from "A" to "B" with weights 5 and 10
    graph.add_edge("A", "B", 5)
    graph.add_edge("A", "B", 10)
    # Verify two edges exist in the adjacency list for "A"
    assert len(graph._adjacency_list["A"].edges) == 2
    # Check that the weights of these edges are 5 and 10
    assert {edge.weight for edge in graph._adjacency_list["A"].edges} == {5, 10}

# Test that an undirected graph adds reverse edges automatically
def test_undirected_graph_reverse_edge() -> None:
    """Test that an undirected graph automatically adds reverse edges."""
    # Initialize an undirected graph
    graph = Graph(is_directed=False)
    # Add vertices "A" and "B"
    graph.add_vertex("A")
    graph.add_vertex("B")
    # Add an edge from "A" to "B"
    graph.add_edge("A", "B")
    # Verify that a reverse edge from "B" to "A" also exists
    assert graph.has_edge("B", "A") is True

# Test that a directed graph does not add reverse edges
def test_directed_graph_no_reverse_edge() -> None:
    """Test that a directed graph does not add reverse edges automatically."""
    # Initialize a directed graph
    graph = Graph(is_directed=True)
    # Add vertices "A" and "B"
    graph.add_vertex("A")
    graph.add_vertex("B")
    # Add an edge from "A" to "B"
    graph.add_edge("A", "B")
    # Verify that there is no reverse edge from "B" to "A"
    assert graph.has_edge("B", "A") is False

# Test Breadth-First Search (BFS) on a graph with disconnected components
def test_bfs_on_disconnected_graph() -> None:
    """Test BFS traversal on a disconnected graph."""
    # Initialize a graph
    graph = Graph()
    # Add vertices "A", "B" (connected) and "C" (disconnected)
    graph.add_vertex("A")
    graph.add_vertex("B")
    graph.add_edge("A", "B")
    graph.add_vertex("C")
    # Perform BFS starting from "A" and verify that only connected nodes are visited
    assert graph.bfs("A") == ["A", "B"]

# Test Depth-First Search (DFS) on a graph with disconnected components
def test_dfs_on_disconnected_graph() -> None:
    """Test DFS traversal on a disconnected graph."""
    # Initialize a graph
    graph = Graph()
    # Add vertices "A", "B" (connected) and "C" (disconnected)
    graph.add_vertex("A")
    graph.add_vertex("B")
    graph.add_edge("A", "B")
    graph.add_vertex("C")
    # Perform DFS starting from "A" and verify that only connected nodes are visited
    assert graph.dfs("A") == ["A", "B"]

# Test cycle detection with a self-loop in the graph
def test_cycle_with_self_loop() -> None:
    """Test that a self-loop is detected as a cycle if allowed."""
    # Initialize a graph with self-loops allowed
    graph = Graph(allow_self_loops=True)
    # Add a vertex "A" and a self-loop
    graph.add_vertex("A")
    graph.add_edge("A", "A")
    # Verify that a cycle is detected due to the self-loop
    assert graph.detect_cycle() is True

# Test cycle detection in a complex graph with multiple cycles
def test_complex_cycle_detection() -> None:
    """Test cycle detection on a more complex graph with multiple cycles."""
    # Initialize a graph
    graph = Graph()
    # Add vertices and edges that form a cycle
    graph.add_vertex("A")
    graph.add_vertex("B")
    graph.add_vertex("C")
    graph.add_edge("A", "B")
    graph.add_edge("B", "C")
    graph.add_edge("C", "A")
    # Verify that a cycle is detected
    assert graph.detect_cycle() is True

# Test exporting and re-importing a graph from JSON to ensure data consistency
def test_round_trip_export_import_json(tmp_path) -> None:
    """Test exporting and re-importing a graph from JSON to check data consistency."""
    # Initialize a graph and add vertices and an edge
    graph = Graph()
    graph.add_vertex("A")
    graph.add_vertex("B")
    graph.add_edge("A", "B", 3)
    # Export the graph to JSON in the temporary path
    json_file = tmp_path / "graph.json"
    graph.export_to_json(str(json_file))
    # Import the graph from JSON into a new graph instance
    new_graph = Graph()
    new_graph.import_from_json(str(json_file))
    # Verify that the imported graph has the same edge and weight
    assert new_graph.has_edge("A", "B")
    assert any(edge.weight == 3 for edge in new_graph._adjacency_list["A"].edges)

# Test exporting and importing an empty graph to ensure it remains empty
def test_empty_graph_export_import_json(tmp_path) -> None:
    """Test exporting and importing an empty graph to ensure it remains empty."""
    # Initialize an empty graph
    graph = Graph()
    # Export the empty graph to JSON in the temporary path
    json_file = tmp_path / "empty_graph.json"
    graph.export_to_json(str(json_file))
    # Import the JSON file into a new graph instance
    new_graph = Graph()
    new_graph.import_from_json(str(json_file))
    # Verify that the imported graph is still empty
    assert len(new_graph._adjacency_list) == 0

# Test adding an edge with non-string vertex IDs and check for error handling
# noinspection PyTypeChecker
def test_add_edge_with_non_string_vertex() -> None:
    """Test that adding an edge with non-string vertex IDs raises an error."""
    # Initialize a graph
    graph = Graph()
    graph.add_vertex("A")
    # Try adding an edge with a non-string ID and check for an error
    with pytest.raises(GraphEdgeError):
        graph.add_edge("A", 123)

# Test importing from an invalid JSON file to ensure proper error handling
def test_import_invalid_json(tmp_path) -> None:
    """Test importing from a malformed JSON file to ensure proper error handling."""
    # Create a malformed JSON file in the temporary path
    invalid_json_file = tmp_path / "invalid.json"
    invalid_json_file.write_text("{'A': ['B']}")  # Write incorrect JSON format
    # Initialize a graph and attempt to import the malformed file
    graph = Graph()
    with pytest.raises(json.JSONDecodeError):
        graph.import_from_json(str(invalid_json_file))

# Test importing from an invalid CSV file to ensure proper error handling
def test_import_invalid_csv(tmp_path) -> None:
    """Test importing from a malformed CSV file to ensure proper error handling."""
    # Create a malformed CSV file in the temporary path
    invalid_csv_file = tmp_path / "invalid.csv"
    invalid_csv_file.write_text("Start Vertex, End Vertex\nA, B, ExtraColumn")  # Add invalid CSV format
    # Initialize a graph and attempt to import the malformed file
    graph = Graph()
    with pytest.raises(ValueError):
        graph.import_from_csv(str(invalid_csv_file))


