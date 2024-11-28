
import matplotlib.pyplot as plt
from graphviz import Digraph

# Initialize Graphviz Digraph
db_diagram = Digraph("HNSC_Omics_Database", format="png", node_attr={'shape': 'record'})

# Define tables and their fields
tables = {
    "DatasetSeriesMetadata": {
        "SeriesID (PK)": "Unique identifier for each GEO series",
        "Title": "Title of the series",
        "Platform": "Platform used",
        "LibraryStrategy": "Library strategy type",
        "SampleCount": "Number of samples in the series"
    },
    "DatasetSampleMetadata": {
        "SampleID (PK)": "Unique identifier for each GEO sample",
        "SeriesID (FK)": "Reference to DatasetSeriesMetadata",
        "Title": "Title of the sample",
        "Platform": "Platform used",
        "LibraryStrategy": "Library strategy type",
        "Characteristics": "Sample characteristics"
    },
    "MicroarrayData": {
        "id (PK)": "Unique ID for microarray data",
        "SampleID (FK)": "Reference to DatasetSampleMetadata",
        "ProbeID": "Probe identifier",
        "ExpressionValue": "Expression value",
        "SeriesID (FK)": "Reference to DatasetSeriesMetadata"
    },
    "PlatformAnnotations": {
        "id (PK)": "Unique ID for platform annotations",
        "PlatformID": "Platform identifier",
        "AnnotationData": "Annotations for probes on the platform"
    }
}

# Add tables to the diagram
for table, fields in tables.items():
    label = f"{table}|" + "|".join([f"<{field.split()[0]}> {field}" for field in fields])
    db_diagram.node(table, f"{{ {label} }}")

# Define relationships
relationships = [
    ("DatasetSeriesMetadata", "DatasetSampleMetadata"),
    ("DatasetSampleMetadata", "MicroarrayData"),
    ("DatasetSeriesMetadata", "MicroarrayData"),
    ("PlatformAnnotations", "MicroarrayData")
]

# Add edges for relationships
for source, target in relationships:
    db_diagram.edge(source, target)

# Render and display the diagram
db_diagram_path = "../docs/diagrams/hnsc_omics_database_diagram"
db_diagram.render(db_diagram_path, view=False)

plt.imshow(plt.imread(f"{db_diagram_path}.png"))
plt.axis("off")
plt.show()
