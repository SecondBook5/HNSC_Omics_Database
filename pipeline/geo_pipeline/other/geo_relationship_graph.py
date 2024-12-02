import json  # For handling JSON data
from typing import Dict, List  # For type hints
from sqlalchemy.orm import Session  # For managing database sessions
from config.logger_config import configure_logger  # For centralized logging configuration
from utils.data_structures.graph import Graph  # For the graph implementation
from db.schema.geo_metadata_schema import GeoSeriesMetadata, RelatedDatasets  # For database models


class GeoRelationshipGraph:
    """
    Represents a relationship graph for GEO datasets.
    Handles superseries/subseries, same-study, and same-data-type relationships.
    """

    def __init__(self):
        """
        Initializes the GeoRelationshipGraph with a directed graph and logger.
        """
        try:
            # Create a directed graph to represent relationships
            self.graph = Graph(is_directed=True)

            # Set up a logger for logging information and errors
            self.logger = configure_logger(name="GeoRelationshipGraph")

            # Initialize dictionaries for grouping datasets by PubMed ID and data types
            self.pubmed_groups: Dict[str, List[str]] = {}
            self.data_type_groups: Dict[str, List[str]] = {}
        except Exception as e:
            # Log any errors during initialization
            raise RuntimeError(f"Failed to initialize GeoRelationshipGraph: {e}")

    def add_relationship(self, source: str, target: str, relationship_type: str) -> None:
        """
        Adds a relationship between two datasets in the graph.

        Args:
            source (str): Source dataset (e.g., subseries).
            target (str): Target dataset (e.g., superseries).
            relationship_type (str): Type of relationship ("Superseries", "SameStudy", "SameDataType").
        """
        try:
            # Validate that the source and target are non-empty strings
            if not all(isinstance(arg, str) and arg.strip() for arg in [source, target]):
                raise ValueError("Source and target must be non-empty strings.")

            # Validate that the relationship type is valid
            if relationship_type not in {"Superseries", "SameStudy", "SameDataType"}:
                raise ValueError(f"Invalid relationship type: {relationship_type}")

            # Add the source dataset to the graph if it doesn't already exist
            self.graph.add_vertex(source)

            # Add the target dataset to the graph if it doesn't already exist
            self.graph.add_vertex(target)

            # Add directed or bidirectional edges based on the relationship type
            if relationship_type == "Superseries":
                self.graph.add_edge(source, target, weight=relationship_type)  # Directed edge for Superseries
                self.logger.info(f"Added Superseries relationship: {source} -> {target}")
            else:
                # Bidirectional edges for SameStudy or SameDataType
                self.graph.add_edge(source, target, weight=relationship_type)
                self.graph.add_edge(target, source, weight=relationship_type)
                self.logger.info(f"Added {relationship_type} relationship: {source} <-> {target}")

        except Exception as e:
            # Log errors that occur while adding relationships
            self.logger.error(f"Error adding relationship {source} -> {target}: {e}")

    def validate_related_datasets(self, raw_data, series_id) -> List[Dict]:
        """
        Validates and parses RelatedDatasets field into a list of dictionaries.

        Args:
            raw_data: Raw RelatedDatasets field from the database.
            series_id: ID of the dataset series being validated.

        Returns:
            List[Dict]: Parsed and validated RelatedDatasets entries.
        """
        try:
            # If raw_data is a JSON string, parse it
            if isinstance(raw_data, str):
                return json.loads(raw_data)

            # If raw_data is already a list, return it directly
            elif isinstance(raw_data, list):
                return raw_data

            # If raw_data is neither a string nor a list, raise an error
            else:
                raise ValueError("RelatedDatasets is neither a list nor a valid JSON string.")
        except (json.JSONDecodeError, ValueError) as e:
            # Log errors encountered while parsing RelatedDatasets
            self.logger.error(f"Failed to parse RelatedDatasets for {series_id}: {e}")
            return []

    def build_graph(self, session: Session) -> None:
        """
        Builds the relationship graph based on dataset metadata in the database.

        Args:
            session (Session): Database session for querying metadata.
        """
        self.logger.info("Starting to build the relationship graph.")
        problematic_entries = []  # List to store problematic entries for debugging
        processed_count = 0  # Counter for processed entries
        skipped_count = 0  # Counter for skipped entries

        try:
            # Fetch all dataset series metadata from the database
            series_list = session.query(DatasetSeriesMetadata).all()
            self.logger.info(f"Fetched {len(series_list)} dataset series from the database.")

            # Iterate over each dataset series
            for series in series_list:
                try:
                    # Increment the processed counter
                    processed_count += 1

                    # Add the series ID as a vertex in the graph
                    self.graph.add_vertex(series.SeriesID)

                    # Validate and process the RelatedDatasets field
                    related_datasets = self.validate_related_datasets(series.RelatedDatasets, series.SeriesID)
                    if not related_datasets:
                        self.logger.warning(f"No valid related datasets for SeriesID={series.SeriesID}. Skipping.")
                        continue

                    for related in related_datasets:
                        # Extract the target and relationship type
                        target = related.get("target")
                        relationship_type = related.get("type")

                        # Validate that both target and relationship type exist
                        if not target or not relationship_type:
                            problematic_entries.append({
                                "SeriesID": series.SeriesID,
                                "Error": "Missing target or relationship_type",
                                "Entry": related
                            })
                            continue

                        # Add the relationship to the graph
                        self.add_relationship(series.SeriesID, target, relationship_type)

                    # Group datasets by PubMed ID for SameStudy relationships
                    if series.PubMedID:
                        self.pubmed_groups.setdefault(series.PubMedID, []).append(series.SeriesID)

                except Exception as e:
                    # Log errors for specific series and skip them
                    self.logger.error(f"Error processing series {series.SeriesID}: {e}")
                    skipped_count += 1
                    problematic_entries.append({
                        "SeriesID": series.SeriesID,
                        "Error": str(e)
                    })

            # Add SameStudy relationships between datasets with the same PubMed ID
            from itertools import combinations
            for pubmed_id, series_ids in self.pubmed_groups.items():
                for source, target in combinations(series_ids, 2):  # Pairwise combinations
                    self.add_relationship(source, target, "SameStudy")

            # Export the graph for debugging
            self.graph.export_to_json("relationship_graph_debug.json")
            self.logger.info("Exported relationship graph to relationship_graph_debug.json for debugging.")

            self.logger.info(f"Finished building the relationship graph. Processed {processed_count} entries.")

        except Exception as e:
            # Log any unexpected errors during graph building
            self.logger.error(f"Error while building the relationship graph: {e}")

        finally:
            # Save problematic entries to a file for debugging
            if problematic_entries:
                with open("problematic_entries.json", "w") as file:
                    json.dump(problematic_entries, file, indent=4)
                self.logger.warning(f"Problematic entries saved to problematic_entries.json")

            # Log a summary of the graph-building process
            self.logger.info(f"Summary: {processed_count} processed, {skipped_count} skipped.")

    def detect_cycle(self) -> bool:
        """
        Detects cycles in the relationship graph and logs details.

        Returns:
            bool: True if a cycle is detected, False otherwise.
        """
        try:
            # Use the graph's detect_cycle method to check for cycles
            cycle_nodes = self.graph.get_cycle_nodes()  # Retrieves nodes in the first detected cycle, if any

            if cycle_nodes:
                # Log a warning if a cycle is detected, including the involved nodes
                self.logger.warning(f"Cycle detected in the relationship graph. Nodes involved: {cycle_nodes}")
                return True
            else:
                # Log an informational message if no cycles are found
                self.logger.info("No cycles detected in the relationship graph.")
                return False

        except Exception as e:
            # Log errors that occur during cycle detection
            self.logger.error(f"Error detecting cycle in the relationship graph: {e}")
            return False

