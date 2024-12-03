# File: pipeline/geo_pipeline/geo_classifier.py

import json  # For handling JSON data
from typing import Optional, List, Set  # For type annotations
from sqlalchemy.orm import Session  # For database session management
from sqlalchemy.exc import SQLAlchemyError  # For handling SQLAlchemy errors
from config.db_config import get_session_context  # For database session context
from db.schema.geo_metadata_schema import GeoSampleMetadata, GeoSeriesMetadata  # Database schema models
from config.logger_config import configure_logger  # For configuring logging


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
        # Validate that GEO ID is a non-empty string
        if not geo_id or not isinstance(geo_id, str):
            raise ValueError("GEO ID must be a non-empty string.")

        # Assign the GEO ID to an instance variable
        self.geo_id = geo_id

        # Configure the logger specific to this GEO ID
        self.logger = configure_logger(name=f"DataTypeDeterminer-{geo_id}")

        # Define a set of manually identified single-cell datasets
        self.manual_single_cell_datasets = {
            "GSE103322", "GSE137524", "GSE139324",
            "GSE164690", "GSE182227", "GSE234933",
            "GSE195832"
        }

    def process(self) -> None:
        """
        Determine the data types for the GEO Series and update the database.
        """
        try:
            # Open a session for database interactions
            with get_session_context() as session:
                # Step 1: Retrieve metadata for the Series
                series_metadata = self._get_series_metadata(session)
                if not series_metadata:  # If no metadata is found, log and exit
                    self.logger.warning(f"No series metadata found for {self.geo_id}.")
                    return

                # Initialize a set to store data types
                data_types = set()

                # Step 2: Check if the Series is a manually defined single-cell dataset
                if self.geo_id in self.manual_single_cell_datasets:
                    self.logger.info(f"Manually setting data type as 'Single Cell RNA-Seq' for Series {self.geo_id}")
                    data_types.add("Single Cell RNA-Seq")

                # Step 3: Handle super-series or determine data types from samples
                if not data_types:  # If no manual classification, analyze the Series
                    if "superseries" in series_metadata.Summary.lower():
                        # Identify and process a super-series
                        self.logger.info(f"{self.geo_id} identified as a super-series.")
                        data_types = self._handle_super_series(session)
                    else:
                        # Fetch samples for this Series
                        samples = self._get_samples(session)
                        if not samples:  # If no samples are found, log and exit
                            self.logger.warning(f"No samples found for Series {self.geo_id}.")
                            return
                        # Determine data types from sample metadata
                        data_types = self._determine_data_types(samples)

                # Step 4: Resolve conflicts in inferred data types
                data_types = self._resolve_conflicts(data_types)
                self.logger.info(f"Determined data types for Series {self.geo_id}: {data_types}")

                # Step 5: Update the Series metadata in the database
                self._update_series_metadata(session, data_types)
        except Exception as e:
            # Log any errors encountered during processing
            self.logger.error(f"Error processing Series {self.geo_id}: {e}")

    def _get_series_metadata(self, session: Session) -> Optional[GeoSeriesMetadata]:
        """
        Retrieve series metadata for the given GEO Series ID.

        Args:
            session (Session): Database session.

        Returns:
            Optional[GeoSeriesMetadata]: Series metadata if available, otherwise None.
        """
        try:
            # Query the database for metadata of the Series
            return session.query(GeoSeriesMetadata).filter_by(SeriesID=self.geo_id).one_or_none()
        except SQLAlchemyError as e:
            # Log any database errors encountered during the query
            self.logger.error(f"Database error while fetching metadata for {self.geo_id}: {e}")
            return None

    def _get_samples(self, session: Session) -> List[GeoSampleMetadata]:
        """
        Retrieve all samples for the given GEO Series.

        Args:
            session (Session): Database session.

        Returns:
            List[GeoSampleMetadata]: List of sample metadata.
        """
        try:
            # Query the database for all samples belonging to this Series
            return session.query(GeoSampleMetadata).filter_by(SeriesID=self.geo_id).all()
        except SQLAlchemyError as e:
            # Log any database errors encountered during the query
            self.logger.error(f"Database error while fetching samples for Series {self.geo_id}: {e}")
            return []

    def _determine_data_types(self, samples: List[GeoSampleMetadata]) -> Set[str]:
        """
        Determine data types from the samples.

        Args:
            samples (List[GeoSampleMetadata]): List of sample metadata.

        Returns:
            Set[str]: Set of unique data types.
        """
        # Initialize an empty set to store data types
        data_types = set()
        for sample in samples:
            try:
                # Classify each sample and add its data type to the set
                data_type = self._classify_sample(
                    data_processing=sample.DataProcessing,
                    library_strategy=sample.LibraryStrategy,
                    library_source=sample.LibrarySource,
                    title=sample.Title,
                )
                if data_type:
                    data_types.add(data_type)
            except Exception as e:
                # Log any errors encountered during sample classification
                self.logger.warning(f"Error classifying sample {sample.SampleID}: {e}")
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
        # Check data_processing for specific keywords
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

        # Check library_strategy for specific keywords
        if library_strategy:
            library_strategy = library_strategy.lower()
            if "rna-seq" in library_strategy:
                # Further classify as single-cell RNA-Seq if relevant
                if "single cell" in (library_source or "").lower() or "single cell" in (data_processing or "").lower():
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

        # Fallback to checking the title for indications of single-cell data
        if title and "single cell" in title.lower():
            return "Single Cell RNA-Seq"

        # Default classification
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
            # Fetch the current series record using the SeriesID
            series = session.query(GeoSeriesMetadata).filter_by(SeriesID=self.geo_id).one_or_none()
            if not series:
                # Log a warning if the series is not found in the database
                self.logger.warning(f"Super-series {self.geo_id} not found in GeoSeriesMetadata.")
                return set()

            # Check if the RelatedDatasets column is empty
            if not series.RelatedDatasets:
                # Log a warning and return an empty set if no related datasets are present
                self.logger.warning(f"Super-series {self.geo_id} has no related datasets.")
                return set()

            # If RelatedDatasets is already a list (from jsonb field), use it directly
            related_datasets = series.RelatedDatasets
            if isinstance(related_datasets, str):  # If it's a JSON string, decode it
                try:
                    related_datasets = json.loads(related_datasets)
                except json.JSONDecodeError as e:
                    # Log an error if the JSON parsing fails
                    self.logger.error(f"Failed to parse RelatedDatasets for {self.geo_id}: {e}")
                    return set()
            elif not isinstance(related_datasets, list):  # Log error if format is unexpected
                self.logger.error(
                    f"Unexpected format for RelatedDatasets in {self.geo_id}. Expected list or JSON string.")
                return set()

            # Aggregate data types from related sub-series
            data_types = set()
            for related_dataset in related_datasets:
                # Validate the structure of each related dataset entry
                target = related_dataset.get("target")
                relationship_type = related_dataset.get("type")

                if not target or not relationship_type:
                    # Log a warning and skip invalid entries
                    self.logger.warning(f"Invalid related dataset entry: {related_dataset}")
                    continue

                # Process only sub-series relationships
                if relationship_type.lower().startswith("superseries of"):
                    self.logger.info(f"Processing related sub-series {target} for super-series {self.geo_id}.")

                    # Fetch metadata for the sub-series
                    sub_series = session.query(GeoSeriesMetadata).filter_by(SeriesID=target).one_or_none()
                    if sub_series and sub_series.DataTypes:
                        try:
                            sub_series_data_types = sub_series.DataTypes if isinstance(sub_series.DataTypes,list) else json.loads(sub_series.DataTypes)
                            data_types.update(sub_series_data_types)
                        except json.JSONDecodeError as e:
                            self.logger.error(f"Failed to parse DataTypes for sub-series {target}: {e}")
                    else:
                        self.logger.warning(f"No data types found for sub-series {target}.")
                else:
                    self.logger.info(f"Ignoring unrelated dataset entry: {related_dataset}")

            # Log a warning if no data types were aggregated
            if not data_types:
                self.logger.warning(f"No data types aggregated from sub-series of {self.geo_id}.")

            return data_types
        except SQLAlchemyError as e:
            # Log and return an empty set if a database error occurs
            self.logger.error(f"Error handling super-series {self.geo_id}: {e}")
            return set()
        except Exception as e:
            # Handle unexpected errors gracefully
            self.logger.error(f"Unexpected error while handling super-series {self.geo_id}: {e}")
            return set()

    def _resolve_conflicts(self, data_types: Set[str]) -> List[str]:
        """
        Resolve conflicts between RNA-Seq and Single Cell RNA-Seq.

        Args:
            data_types (Set[str]): Set of inferred data types.

        Returns:
            List[str]: Resolved and validated data types.
        """
        # Validate input is a set
        if not isinstance(data_types, set):
            self.logger.error(f"Invalid input to _resolve_conflicts. Expected set, got {type(data_types)}.")
            raise ValueError("Input data_types must be a set.")

        # Check for conflict between RNA-Seq and Single Cell RNA-Seq
        if "RNA-Seq" in data_types and "Single Cell RNA-Seq" in data_types:
            self.logger.info(f"Conflict detected: 'RNA-Seq' and 'Single Cell RNA-Seq' both found for {self.geo_id}.")
            # Resolve by prioritizing "Single Cell RNA-Seq"
            data_types.discard("RNA-Seq")
            self.logger.info(f"Resolved conflict by retaining 'Single Cell RNA-Seq' for {self.geo_id}.")

        # Return the resolved data types as a list
        return list(data_types)

    def _update_series_metadata(self, session: Session, inferred_data_types: List[str]) -> None:
        """
        Update the database with the inferred data types.

        Args:
            session (Session): Database session.
            inferred_data_types (List[str]): List of inferred data types.

        Raises:
            ValueError: If inputs are invalid.
            RuntimeError: If the database update fails.
        """
        # Validate inputs
        if not isinstance(session, Session):
            self.logger.error(
                "Invalid session provided to _update_series_metadata. Must be a valid SQLAlchemy session.")
            raise ValueError("Session must be a valid SQLAlchemy session object.")

        if not isinstance(inferred_data_types, list) or not all(isinstance(dt, str) for dt in inferred_data_types):
            self.logger.error("Invalid inferred_data_types provided. Must be a list of strings.")
            raise ValueError("Inferred data types must be a list of strings.")

        try:
            # Fetch the series metadata for the current GEO ID
            series = session.query(GeoSeriesMetadata).filter_by(SeriesID=self.geo_id).one_or_none()

            if series:
                # Update the DataTypes field with the inferred data types directly (SQLAlchemy will handle JSONB conversion)
                series.DataTypes = inferred_data_types
                session.add(series)  # Add the updated object to the session
                session.commit()  # Commit the changes to the database
                self.logger.info(f"Successfully updated Series {self.geo_id} with data types: {inferred_data_types}")
            else:
                # Log a warning if the series is not found
                self.logger.warning(f"Series {self.geo_id} not found in GeoSeriesMetadata.")
        except SQLAlchemyError as e:
            # Roll back the transaction in case of an error
            session.rollback()
            self.logger.error(f"Database error while updating metadata for Series {self.geo_id}: {e}")
            raise RuntimeError(f"Failed to update metadata for Series {self.geo_id}.") from e
        except Exception as e:
            # Handle unexpected errors gracefully
            self.logger.error(f"Unexpected error while updating metadata for Series {self.geo_id}: {e}")
            raise RuntimeError("Unexpected error during metadata update.") from e

if __name__ == "__main__":
    # Configure the global logger
    main_logger = configure_logger(name="main", log_file="geo_pipeline.log", output="both")

    try:
        # Fetch all GEO IDs from the database
        with get_session_context() as session:
            geo_ids = [
                series.SeriesID
                for series in session.query(GeoSeriesMetadata.SeriesID).all()
            ]

        # Log the number of GEO IDs retrieved
        if geo_ids:
            main_logger.info(f"Retrieved {len(geo_ids)} GEO IDs from the database.")
        else:
            main_logger.warning("No GEO IDs found in the database.")

        # Process each GEO ID
        for geo_id in geo_ids:
            try:
                # Initialize the DataTypeDeterminer for each GEO ID
                determiner = DataTypeDeterminer(geo_id)
                determiner.process()  # Process the GEO ID
            except Exception as e:
                # Log any errors that occur during processing of individual GEO IDs
                main_logger.error(f"Error processing GEO ID {geo_id}: {e}")
    except SQLAlchemyError as e:
        # Handle database-related errors during the retrieval of GEO IDs
        main_logger.critical(f"Database error while fetching GEO IDs: {e}")
    except Exception as e:
        # Log unexpected errors
        main_logger.critical(f"Unexpected error in main execution: {e}")
