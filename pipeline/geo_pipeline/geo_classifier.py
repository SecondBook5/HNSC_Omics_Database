import json
from typing import Optional, List, Set
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from config.db_config import get_session_context
from db.schema.metadata_schema import DatasetSampleMetadata, DatasetSeriesMetadata
from config.logger_config import configure_logger


class DataTypeDeterminer:
    """
    Determines data types for GEO Series, including handling super-series, and updates the database.
    """

    def __init__(self, geo_id: str):
        """
        Initialize with the GEO Series ID and configure the logger.

        Args:
            geo_id (str): The GEO Series ID to process.

        Raises:
            ValueError: If the GEO ID is invalid.
        """
        if not geo_id or not isinstance(geo_id, str):
            raise ValueError("GEO ID must be a non-empty string.")
        self.geo_id = geo_id
        self.logger = configure_logger(name=f"DataTypeDeterminer-{geo_id}")

    def process(self) -> None:
        """
        Determine the data types for the GEO Series and update the database.
        """
        try:
            with get_session_context() as session:
                # Check if the series is a super-series
                series_metadata = self._get_series_metadata(session)
                if not series_metadata:
                    self.logger.warning(f"No series metadata found for {self.geo_id}.")
                    return

                if "superseries" in series_metadata.Summary.lower():
                    self.logger.info(f"{self.geo_id} identified as a super-series.")
                    data_types = self._handle_super_series(session)
                else:
                    # Get data types from samples
                    samples = self._get_samples(session)
                    if not samples:
                        self.logger.warning(f"No samples found for Series {self.geo_id}.")
                        return
                    data_types = self._determine_data_types(samples)

                # Validate and resolve conflicts
                data_types = self._resolve_conflicts(data_types)
                self.logger.info(f"Determined data types for Series {self.geo_id}: {data_types}")

                # Update the database with inferred data types
                self._update_series_metadata(session, data_types)
        except Exception as e:
            self.logger.error(f"Error processing Series {self.geo_id}: {e}")

    def _get_series_metadata(self, session: Session) -> Optional[DatasetSeriesMetadata]:
        """
        Retrieve series metadata for the given GEO Series ID.

        Args:
            session (Session): Database session.

        Returns:
            Optional[DatasetSeriesMetadata]: Series metadata if available, otherwise None.
        """
        try:
            return session.query(DatasetSeriesMetadata).filter_by(SeriesID=self.geo_id).one_or_none()
        except SQLAlchemyError as e:
            self.logger.error(f"Database error while fetching metadata for {self.geo_id}: {e}")
            return None

    def _get_samples(self, session: Session) -> List[DatasetSampleMetadata]:
        """
        Retrieve all samples for the given GEO Series.

        Args:
            session (Session): Database session.

        Returns:
            List[DatasetSampleMetadata]: List of sample metadata.
        """
        try:
            return session.query(DatasetSampleMetadata).filter_by(SeriesID=self.geo_id).all()
        except SQLAlchemyError as e:
            self.logger.error(f"Database error while fetching samples for Series {self.geo_id}: {e}")
            return []

    def _determine_data_types(self, samples: List[DatasetSampleMetadata]) -> Set[str]:
        """
        Determine data types from the samples.

        Args:
            samples (List[DatasetSampleMetadata]): List of sample metadata.

        Returns:
            Set[str]: Set of unique data types.
        """
        data_types = set()
        for sample in samples:
            data_type = self._classify_sample(
                data_processing=sample.DataProcessing,
                library_strategy=sample.LibraryStrategy,
                library_source=sample.LibrarySource,
                title=sample.Title,
            )
            if data_type:
                data_types.add(data_type)
        return data_types

    def _classify_sample(
        self,
        data_processing: Optional[str],
        library_strategy: Optional[str],
        library_source: Optional[str],
        title: Optional[str],
    ) -> Optional[str]:
        """
        Classify a single sample based on its metadata.

        Args:
            data_processing (Optional[str]): Data processing description.
            library_strategy (Optional[str]): Library strategy description.
            library_source (Optional[str]): Library source description.
            title (Optional[str]): Title of the sample.

        Returns:
            Optional[str]: The inferred data type.
        """

        if data_processing:
            data_processing = data_processing.lower()
            if "spatial transcriptomics" in data_processing:
                return "Spatial Transcriptomics"
            if "par-clip" in data_processing:
                return "PAR-CLIP"
            if "m6a-seq" in data_processing:
                return "m6A-Seq"
            if "4cseq" in data_processing:
                return "4C-Seq"


        if library_strategy:
            library_strategy = library_strategy.lower()
            if "rna-seq" in library_strategy:
                if "single cell" in (library_source or "").lower() or "single cell" in (data_processing or "").lower() or "single cell" in (title or "").lower():
                    return "Single Cell RNA-Seq"
                return "RNA-Seq"
            if "atac-seq" in library_strategy:
                return "ATAC-Seq"
            if "chip-seq" in library_strategy:
                return "ChIP-Seq"
            if "rip-seq" in library_strategy:
                return "RIP-Seq"
            if "mbd-seq" in library_strategy:
                return "MBD-Seq"
            if "hi-c" in library_strategy:
                return "Hi-C"


        if title:
            title = title.lower()
            if "single cell" in title:
                return "Single Cell RNA-Seq"

        return "Microarray"

    def _handle_super_series(self, session: Session) -> Set[str]:
        """
        Handle super-series by aggregating data types from related sub-series.

        Args:
            session (Session): Database session.

        Returns:
            Set[str]: Set of aggregated data types from sub-series.
        """
        try:
            # Fetch the current series record
            series = session.query(DatasetSeriesMetadata).filter_by(SeriesID=self.geo_id).one_or_none()
            if not series:
                self.logger.warning(f"Super-series {self.geo_id} not found in DatasetSeriesMetadata.")
                return set()

            # Ensure RelatedDatasets column is not empty
            if not series.RelatedDatasets:
                self.logger.warning(f"Super-series {self.geo_id} has no related datasets.")
                return set()

            # Parse RelatedDatasets as a JSON object
            related_datasets = series.RelatedDatasets
            if isinstance(related_datasets, str):
                try:
                    related_datasets = json.loads(related_datasets)
                except json.JSONDecodeError as e:
                    self.logger.error(f"Failed to parse RelatedDatasets for {self.geo_id}: {e}")
                    return set()

            if not isinstance(related_datasets, list):
                self.logger.error(f"Unexpected format for RelatedDatasets in {self.geo_id}. Expected list.")
                return set()

            # Identify sub-series and aggregate data types
            data_types = set()
            for related_dataset in related_datasets:
                # Validate expected structure
                target = related_dataset.get("target")
                relationship_type = related_dataset.get("type")

                if not target or not relationship_type:
                    self.logger.warning(f"Invalid related dataset entry: {related_dataset}")
                    continue

                # Only process sub-series relationships
                if relationship_type.lower().startswith("superseries of"):
                    self.logger.info(f"Processing related sub-series {target} for super-series {self.geo_id}.")

                    # Fetch the sub-series metadata
                    sub_series = session.query(DatasetSeriesMetadata).filter_by(SeriesID=target).one_or_none()
                    if sub_series:
                        if sub_series.DataTypes:
                            try:
                                sub_series_data_types = json.loads(sub_series.DataTypes)
                                data_types.update(sub_series_data_types)
                            except json.JSONDecodeError as e:
                                self.logger.error(f"Failed to parse DataTypes for sub-series {target}: {e}")
                        else:
                            self.logger.warning(f"No data types found for sub-series {target}.")
                    else:
                        self.logger.warning(f"Sub-series {target} not found in DatasetSeriesMetadata.")
                else:
                    self.logger.info(f"Ignoring unrelated dataset entry: {related_dataset}")

            if not data_types:
                self.logger.warning(f"No data types aggregated from sub-series of {self.geo_id}.")
            return data_types
        except SQLAlchemyError as e:
            self.logger.error(f"Error handling super-series {self.geo_id}: {e}")
            return set()

    def _resolve_conflicts(self, data_types: Set[str]) -> List[str]:
        """
        Resolve conflicts between RNA-Seq and Single Cell RNA-Seq.

        Args:
            data_types (Set[str]): Set of inferred data types.

        Returns:
            List[str]: Resolved and validated data types.
        """
        if "RNA-Seq" in data_types and "Single Cell RNA-Seq" in data_types:
            self.logger.info(f"Resolved conflict: 'RNA-Seq' replaced by 'Single Cell RNA-Seq' for {self.geo_id}")
            data_types.discard("RNA-Seq")
        return list(data_types)

    def _update_series_metadata(self, session: Session, inferred_data_types: List[str]) -> None:
        """
        Update the database with the inferred data types.

        Args:
            session (Session): Database session.
            inferred_data_types (List[str]): List of inferred data types.
        """
        try:
            series = session.query(DatasetSeriesMetadata).filter_by(SeriesID=self.geo_id).one_or_none()
            if series:
                series.DataTypes = json.dumps(inferred_data_types)
                session.add(series)
                session.commit()
                self.logger.info(f"Updated Series {self.geo_id} with data types: {inferred_data_types}")
            else:
                self.logger.warning(f"Series {self.geo_id} not found in DatasetSeriesMetadata.")
        except SQLAlchemyError as e:
            session.rollback()
            self.logger.error(f"Failed to update metadata for Series {self.geo_id}: {e}")


if __name__ == "__main__":
    # Configure global logger
    main_logger = configure_logger(name="main", log_file="geo_pipeline.log", output="both")

    try:
        # Fetch all GEO IDs from the database
        with get_session_context() as session:
            geo_ids = [
                series.SeriesID
                for series in session.query(DatasetSeriesMetadata.SeriesID).all()
            ]
        main_logger.info(f"Retrieved {len(geo_ids)} GEO IDs from the database.")

        # Process each GEO ID
        for geo_id in geo_ids:
            determiner = DataTypeDeterminer(geo_id)
            determiner.process()

    except Exception as e:
        main_logger.error(f"Error in main execution: {e}")
