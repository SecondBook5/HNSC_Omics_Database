from typing import Optional, List, Set, Dict
from db.schema.metadata_schema import DatasetSampleMetadata, DatasetSeriesMetadata
from config.db_config import get_session_context
from config.logger_config import configure_logger
import json
import logging

# Configure the logger
logger = configure_logger(
    name="GEOClassifier",
    log_dir="./logs",
    log_file="geo_classifier.log",
    level=logging.INFO,
    output="both"
)

# Predefined list of single-cell RNA-Seq series
SINGLE_CELL_SERIES = {
    "GSE103322", "GSE137524", "GSE139324", "GSE164690", "GSE182227",
    "GSE234933", "GSE195832"
}

# Predefined list of expected data types for validation
EXPECTED_DATA_TYPES = {
    "GSE112021": "ChIP-Seq",
    "GSE112023": "MBD-Seq",
    "GSE112026": "RNA-Seq",
    "GSE112027": "ChIP-Seq, RNA-Seq, MBD-Seq",
    "GSE103322": "scRNA-Seq",
    "GSE114446": "RNA-Seq",
    "GSE114375": "Microarray",
    "GSE135604": "ATAC-Seq",
    "GSE137524": "scRNA-Seq",
    "GSE139324": "scRNA-Seq",
    "GSE164690": "scRNA-Seq",
    "GSE182227": "scRNA-Seq",
    "GSE234933": "scRNA-Seq",
    "GSE195832": "scRNA-Seq",
    "GSE202878": "m6A-Seq, RIP-Seq, PAR-CLIP",
    "GSE242276": "m6A-Seq",
    "GSE242281": "RIP-Seq",
    "GSE242283": "PAR-CLIP",
    "GSE208253": "Spatial Transcriptomics",
    "GSE41613": "Microarray",
    "GSE42743": "Microarray",
    "GSE66949": "Microarray",
    "GSE201714": "ATAC-Seq",
    "GSE202036": "RNA-Seq",
}


# ----------------------- GEOClassifier Class -----------------------

class GEOClassifier:
    """
    Classifies GEO Series based on metadata, identifies superseries/subseries,
    validates classifications, and connects datasets via shared PubMed IDs.

    Attributes:
        geo_id (str): The GEO Series ID to classify.
    """

    def __init__(self, geo_id: str):
        """
        Initialize the GEOClassifier with a given GEO Series ID.

        Args:
            geo_id (str): The GEO Series ID to classify.

        Raises:
            ValueError: If the GEO ID is not a non-empty string.
        """
        # Ensure the GEO ID is valid
        if not geo_id or not isinstance(geo_id, str):
            raise ValueError("GEO ID must be a non-empty string.")

        # Store the GEO ID
        self.geo_id = geo_id

    # ----------------------- Classifying DataTypes -----------------------

    def classify(self) -> None:
        """
        Classify the GEO Series, determine relationships, validate classification,
        and update the database with the results.

        Raises:
            RuntimeError: If the series contains inconsistent data types.
        """
        try:
            # Start a session with the database
            with get_session_context() as session:
                # Fetch associated samples and series metadata
                samples = self._get_samples(session)
                series_metadata = self._get_series_metadata(session)

                # Exit if no data is available for classification
                if not samples or not series_metadata:
                    logger.warning(f"No data found for Series {self.geo_id}. Skipping classification.")
                    return

                # Infer data types from the samples
                data_types = self._determine_data_types(samples)

                # Ensure consistency in data types
                if len(data_types) > 1:
                    logger.error(f"Series {self.geo_id} contains mixed data types: {data_types}.")
                    raise RuntimeError(f"Series {self.geo_id} contains mixed data types: {data_types}")

                # Extract the single inferred data type
                inferred_data_type = data_types.pop()

                # Identify relationships (superseries/subseries, shared studies)
                relationships = self._determine_relationships(series_metadata, session)

                # Validate inferred data type and relationships
                self._validate_classification(inferred_data_type, relationships)

                # Update the series metadata in the database
                self._update_series_metadata(session, inferred_data_type, relationships)

        except Exception as e:
            logger.error(f"Error classifying Series {self.geo_id}: {e}")
            raise

    # ----------------------- Fetching Metadata -----------------------

    def _get_samples(self, session) -> List[DatasetSampleMetadata]:
        """
        Retrieve all samples associated with the GEO Series ID.

        Args:
            session: Database session.

        Returns:
            List[DatasetSampleMetadata]: List of sample metadata objects.
        """
        return session.query(DatasetSampleMetadata).filter_by(SeriesID=self.geo_id).all()

    def _get_series_metadata(self, session) -> Optional[DatasetSeriesMetadata]:
        """
        Retrieve the series metadata for the GEO Series ID.

        Args:
            session: Database session.

        Returns:
            DatasetSeriesMetadata: The metadata for the GEO series.
        """
        return session.query(DatasetSeriesMetadata).filter_by(SeriesID=self.geo_id).one_or_none()

    # ----------------------- Determining DataTypes -----------------------

    def _determine_data_types(self, samples: List[DatasetSampleMetadata]) -> Set[str]:
        """
        Infer the data types for the series based on its samples.

        Args:
            samples (List[DatasetSampleMetadata]): List of sample metadata objects.

        Returns:
            Set[str]: Set of unique inferred data types.
        """
        data_types = set()  # Initialize an empty set for unique data types
        for sample in samples:
            # Classify each sample and add its data type
            data_type = self._classify_sample(sample.DataProcessing, sample.LibraryStrategy)
            data_types.add(data_type)
        return data_types

    def _classify_sample(self, data_processing: Optional[str], library_strategy: Optional[str]) -> str:
        """
        Classify a single sample based on metadata fields.

        Args:
            data_processing (Optional[str]): Data processing details.
            library_strategy (Optional[str]): Library strategy details.

        Returns:
            str: The inferred data type for the sample.
        """
        # Check for specific terms in data-processing
        if data_processing:
            data_processing = data_processing.lower()
            if "spatial transcriptomics" in data_processing:
                return "Spatial Transcriptomics"
            if "m6a-seq" in data_processing:
                return "m6A-Seq"
            if "par-clip" in data_processing:
                return "PAR-CLIP"
            if "mbd-seq" in data_processing:
                return "MBD-Seq"

        # Fallback to library-strategy if data-processing is inconclusive
        if library_strategy:
            library_strategy = library_strategy.lower()
            if "rna-seq" in library_strategy:
                return "Single Cell RNA-Seq" if self.geo_id in SINGLE_CELL_SERIES else "RNA-Seq"
            elif "atac-seq" in library_strategy:
                return "ATAC-Seq"
            elif "chip-seq" in library_strategy:
                return "ChIP-Seq"
            elif "mbd-seq" in library_strategy:
                return "MBD-Seq"
            elif "rip-seq" in library_strategy:
                return "RIP-Seq"

        # Default to "Microarray"
        logger.warning(f"Sample in Series {self.geo_id} could not be classified. Defaulting to 'Microarray'.")
        return "Microarray"

    # ----------------------- Determining Relationships -----------------------

    def _determine_relationships(self, series_metadata: DatasetSeriesMetadata, session) -> Dict[str, List[str]]:
        """
        Determine relationships for the series (e.g., superseries, subseries, shared studies).

        Args:
            series_metadata (DatasetSeriesMetadata): Metadata for the GEO Series.
            session: Database session.

        Returns:
            Dict[str, List[str]]: Relationships categorized as superseries/subseries/study links.
        """
        relationships = {"SuperseriesOf": [], "SubseriesOf": [], "SameStudy": []}

        # Parse related datasets field for superseries/subseries
        if series_metadata.RelatedDatasets:
            try:
                if isinstance(series_metadata.RelatedDatasets, list):
                    # Already in list format
                    related_datasets = series_metadata.RelatedDatasets
                else:
                    # Attempt to parse as JSON
                    try:
                        related_datasets = json.loads(series_metadata.RelatedDatasets)
                    except json.JSONDecodeError:
                        logger.warning(
                            f"RelatedDatasets for Series {self.geo_id} is not valid JSON: {series_metadata.RelatedDatasets}"
                        )
                        related_datasets = []  # Fallback to an empty list

                for dataset in related_datasets:
                    if isinstance(dataset, dict):
                        # Ensure dataset is a dictionary before accessing its fields
                        if dataset.get("type") == "SuperSeries of":
                            relationships["SuperseriesOf"].append(dataset.get("target", "Unknown"))
                        elif dataset.get("type") == "SubSeries of":
                            relationships["SubseriesOf"].append(dataset.get("target", "Unknown"))
                    else:
                        logger.warning(
                            f"Unexpected format in RelatedDatasets for Series {self.geo_id}: {dataset}"
                        )
            except Exception as e:
                logger.error(f"Error parsing RelatedDatasets for Series {self.geo_id}: {e}")

        # Identify other series with the same PubMed ID
        if series_metadata.PubMedID:
            same_study = session.query(DatasetSeriesMetadata.SeriesID).filter(
                DatasetSeriesMetadata.PubMedID == series_metadata.PubMedID,
                DatasetSeriesMetadata.SeriesID != self.geo_id
            ).all()
            relationships["SameStudy"] = [s[0] for s in same_study]

        return relationships

    # ----------------------- Validating Classification -----------------------

    def _validate_classification(self, inferred_data_type: str, relationships: Dict[str, List[str]]) -> None:
        """
        Validate the inferred data type and relationships.

        Args:
            inferred_data_type (str): The inferred data type for the series.
            relationships (Dict[str, List[str]]): Relationships identified for the series.
        """
        # Check against the expected data type
        expected_data_type = EXPECTED_DATA_TYPES.get(self.geo_id)
        if expected_data_type and inferred_data_type != expected_data_type:
            logger.warning(f"Validation mismatch for Series {self.geo_id}: "
                           f"Inferred={inferred_data_type}, Expected={expected_data_type}.")

    # ----------------------- Updating Metadata -----------------------

    def _update_series_metadata(self, session, inferred_data_type: str, relationships: Dict[str, List[str]]) -> None:
        """
        Update the database with the classification and relationships.

        Args:
            session: Database session.
            inferred_data_type (str): The inferred data type for the series.
            relationships (Dict[str, List[str]]): Relationships identified for the series.
        """
        series = session.query(DatasetSeriesMetadata).filter_by(SeriesID=self.geo_id).one_or_none()
        if series:
            series.DataTypes = inferred_data_type
            series.RelatedDatasets = json.dumps(relationships)
            session.add(series)
            session.commit()
            logger.info(
                f"Updated Series {self.geo_id} with type {inferred_data_type} and relationships {relationships}.")


# ----------------------- Main Execution -----------------------

if __name__ == "__main__":
    try:
        with get_session_context() as session:
            # Get all series IDs from the database
            series_ids = [series.SeriesID for series in session.query(DatasetSeriesMetadata).all()]
            # Classify each series
            for geo_id in series_ids:
                classifier = GEOClassifier(geo_id)
                classifier.classify()
    except Exception as e:
        logger.critical(f"Unexpected error during GEO classification: {e}")
