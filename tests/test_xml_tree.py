import pytest
from utils.data_structures.xml_tree import XMLTree, XMLTreeNode  # Adjust import as per your module structure


@pytest.fixture
def sample_gene_tree():
    """
    Fixture to create a sample XML tree resembling an XML structure for gene metadata.

    Structure:
    <root>
        <gene>
            <name>BRCA1</name>
            <sequence>ATGCGA...</sequence>
            <exon>exon1</exon>
            <exon>exon2</exon>
            <protein>
                <name>BRCA1 protein</name>
                <function>Tumor suppression</function>
            </protein>
        </gene>
        <gene>
            <name>TP53</name>
            <sequence>CGTACG...</sequence>
            <exon>exon1</exon>
            <exon>exon2</exon>
            <exon>exon3</exon>
            <protein>
                <name>p53 protein</name>
                <function>Tumor suppression</function>
            </protein>
        </gene>
    </root>
    """
    # Initialize the XML tree with a root element
    tree = XMLTree("root")
    root = tree.root

    # Add XML structure for genes with nested children
    gene1 = root.add_child("gene")
    gene1.add_child("name", text="BRCA1")
    gene1.add_child("sequence", text="ATGCGA...")
    gene1.add_child("exon", text="exon1")
    gene1.add_child("exon", text="exon2")
    protein1 = gene1.add_child("protein")
    protein1.add_child("name", text="BRCA1 protein")
    protein1.add_child("function", text="Tumor suppression")

    gene2 = root.add_child("gene")
    gene2.add_child("name", text="TP53")
    gene2.add_child("sequence", text="CGTACG...")
    gene2.add_child("exon", text="exon1")
    gene2.add_child("exon", text="exon2")
    gene2.add_child("exon", text="exon3")
    protein2 = gene2.add_child("protein")
    protein2.add_child("name", text="p53 protein")
    protein2.add_child("function", text="Tumor suppression")

    return tree


def test_collect_gene_names_and_proteins_in_single_pass(sample_gene_tree):
    """
    Test that we can traverse the tree in a single pass to collect specific tags and text, such as gene names and protein names.
    """
    tree = sample_gene_tree
    collected_data = []

    def _collect_specific_data(node: XMLTreeNode) -> None:
        # Collect specific tags or text based on conditions
        if node.tag in ["name", "function"] and node.text:
            collected_data.append((node.tag, node.text))
        # Recursively process each child
        for child in node.children:
            _collect_specific_data(child)

    # Start traversal from the root node and perform the single-pass collection
    _collect_specific_data(tree.root)

    # Verify the collected data matches our expectations
    assert collected_data == [
        ("name", "BRCA1"),
        ("name", "BRCA1 protein"),
        ("function", "Tumor suppression"),
        ("name", "TP53"),
        ("name", "p53 protein"),
        ("function", "Tumor suppression"),
    ]


def test_collect_gene_sequences(sample_gene_tree):
    """
    Test that we can traverse the tree in a single pass to collect only the gene sequences.
    """
    tree = sample_gene_tree
    collected_sequences = []

    def _collect_sequences(node: XMLTreeNode) -> None:
        # Only collect sequences
        if node.tag == "sequence":
            collected_sequences.append(node.text)
        # Recursively process each child
        for child in node.children:
            _collect_sequences(child)

    # Start traversal from the root
    _collect_sequences(tree.root)

    # Verify the collected sequences match the expected result
    assert collected_sequences == ["ATGCGA...", "CGTACG..."]


def test_find_first_gene_name(sample_gene_tree):
    """
    Test that we can find the first gene name in the tree.
    """
    tree = sample_gene_tree

    # Find the first occurrence of a 'name' tag
    first_name_node = tree.find_element_by_tag("name")

    # Verify the tag and text of the first found name node
    assert first_name_node is not None
    assert first_name_node.tag == "name"
    assert first_name_node.text == "BRCA1"


def test_exon_count_in_each_gene(sample_gene_tree):
    """
    Test that we can traverse the tree and count the number of exons for each gene in a single pass.
    """
    tree = sample_gene_tree
    gene_exon_counts = []

    def _count_exons(node: XMLTreeNode, current_gene=None) -> None:
        # If we find a gene tag, start a new exon count for this gene
        if node.tag == "gene":
            current_gene = {"name": None, "exons": 0}
            gene_exon_counts.append(current_gene)

        # Update the gene name if found
        if node.tag == "name" and current_gene and current_gene["name"] is None:
            current_gene["name"] = node.text

        # Increment the exon count for each exon found under the current gene
        if node.tag == "exon" and current_gene:
            current_gene["exons"] += 1

        # Recursively process each child
        for child in node.children:
            _count_exons(child, current_gene)

    # Start traversal from the root node
    _count_exons(tree.root)

    # Verify the exon counts for each gene
    assert gene_exon_counts == [
        {"name": "BRCA1", "exons": 2},
        {"name": "TP53", "exons": 3},
    ]


def test_collect_all_protein_functions(sample_gene_tree):
    """
    Test that we collect all protein functions in a single pass.
    """
    tree = sample_gene_tree
    protein_functions = []

    def _collect_protein_functions(node: XMLTreeNode) -> None:
        # Collect only the function tags under protein nodes
        if node.tag == "function":
            protein_functions.append(node.text)
        # Recursively process each child
        for child in node.children:
            _collect_protein_functions(child)

    # Start traversal from the root
    _collect_protein_functions(tree.root)

    # Verify the collected protein functions match expected results
    assert protein_functions == ["Tumor suppression", "Tumor suppression"]
