# File: pipeline/geo_pipeline/geo_metadata_etl.py
import logging
import os
import json
import re
from lxml import etree
from typing import Dict, Optional
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from config.db_config import get_postgres_engine
from db.schema.geo_metadata_schema import GeoSeriesMetadata, GeoSampleMetadata
from config.logger_config import configure_logger
from pipeline.geo_pipeline.geo_file_handler import GeoFileHandler  # Import GeoFileHandler


class GeoMetadataETL:
    """
    Handles the extraction, transformation, and loading (ETL) of GEO metadata
    from XML files into a database, including logging and error handling.

    This class provides functionality to parse GEO metadata from MINiML XML files,
    validate the extracted metadata, and insert it into a PostgreSQL database.
    """
# ------------------ Initialization and Configuration ---------------------------------------------------------------
    def __init__(self, file_path: str, template_path: str, file_handler: GeoFileHandler, debug_mode: bool = False) -> None:
        """
        Initializes the GeoMetadataETL with file paths, logging settings, and a file handler.

        Args:
            file_path (str): Path to the GEO XML file.
            template_path (str): Path to the JSON field mapping template.
            file_handler (GeoFileHandler): Handles logging and file-related operations.
            debug_mode (bool): Enables detailed debug logging.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"XML file not found: {file_path}")
        if not os.path.exists(template_path):
            raise FileNotFoundError(f"Template file not found: {template_path}")

        # Assign the file paths and debug mode
        self.file_path = file_path
        self.template_path = template_path
        self.debug_mode = debug_mode
        self.file_handler = file_handler  # Assign the file handler instance

        # Configure the logger using the centralized configuration
        self.logger = configure_logger(
            name="GeoMetadataETL",  # Unique name for this component
            log_file="geo_metadata_etl.log",  # Log file specific to this ETL process
            level=logging.DEBUG if debug_mode else logging.INFO,  # Set level based on debug flag
            output="both"  # Log to both console and file
        )

        # Load the JSON template for XML field mappings
        self.template = self._load_template()


    def _load_template(self) -> Dict[str, Dict[str, str]]:
        """Loads the JSON template for XML field mappings."""
        try:
            with open(self.template_path, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            self.logger.error(f"Failed to load template: {e}")
            raise

    # ------------------------------- Validation ------------------------------------
    def _validate_xml(self) -> None:
        """Validates that the XML file is well-formed."""
        try:
            etree.parse(self.file_path)
        except etree.XMLSyntaxError as e:
            self.logger.error(f"Invalid XML structure: {e}")
            raise RuntimeError("Invalid XML structure.") from e

#-----------------------------Parsing and Extraction-----------------------------------
    def _extract_fields(self, element: etree._Element, field_paths: Dict[str, str], ns: Dict[str, str]) -> Dict:
        """
        Extracts fields from an XML element based on field paths.

        Args:
            element (etree._Element): XML element to extract data from.
            field_paths (Dict[str, str]): Dictionary mapping field names to XML paths.
            ns (Dict[str, str]): Namespace dictionary for XML.

        Returns:
            Dict: A dictionary of extracted fields.
        """
        data = {}
        for field_name, path in field_paths.items():
            try:
                if "@" in path:  # Handle attribute-based fields
                    attribute_value = element.xpath(path, namespaces=ns)
                    data[field_name] = attribute_value[0] if attribute_value else None
                elif field_name == "Characteristics":  # Handle multiple characteristics
                    characteristics = [
                        {"tag": char.attrib.get("tag", "Unknown"), "value": (char.text or "").strip()}
                        for char in element.findall(path, namespaces=ns)
                    ]
                    data[field_name] = characteristics
                elif field_name == "RelatedDatasets":  # Handle as list of dictionaries for JSON compatibility
                    relations = [
                        {"type": rel.attrib.get("type", "Unknown"), "target": rel.attrib.get("target", "")}
                        for rel in element.findall(path, namespaces=ns)
                    ]
                    data[field_name] = relations
                else:  # Default behavior for other fields
                    sub_elem = element.find(path, namespaces=ns)
                    data[field_name] = sub_elem.text.strip() if sub_elem is not None and sub_elem.text else None
            except Exception as e:
                self.logger.warning(f"Error extracting field '{field_name}' at path '{path}': {e}")
                data[field_name] = None
        return data

    def _process_series_data(self, series_elem: etree._Element, ns: Dict[str, str]) -> Optional[Dict]:
        """
        Processes and extracts metadata for a single Series element.

        This method extracts metadata from a Series XML element using the defined field template.
        It logs any errors encountered during extraction and ensures that the returned data is validated.

        Args:
            series_elem (etree._Element): XML element for the Series.
            ns (Dict[str, str]): XML namespace dictionary.

        Returns:
            Optional[Dict]: Extracted and validated metadata for the Series, or None if extraction fails.
        """
        try:
            # Extract fields using the defined template
            data = self._extract_fields(series_elem, self.template['Series'], ns)

            # Validate the extracted data (if a validation method is available for Series)
            if not data or 'SeriesID' not in data:
                self.logger.error(f"Validation failed for Series data: {data}")
                return None

            # Log the extracted data for debugging purposes
            self.logger.debug(f"Extracted Series data: {data}")
            return data
        except Exception as e:
            # Log detailed error information
            self.logger.error(f"Error processing Series element. Element: {series_elem.tag}, Error: {e}")
            return None

    def _process_sample_data(self, sample_elem: etree._Element, ns: Dict[str, str], series_id: str) -> Optional[Dict]:
        """
        Processes and validates metadata for a single Sample element.

        This method extracts metadata from a Sample XML element and associates it with the parent SeriesID.
        It logs any errors encountered during extraction or validation.

        Args:
            sample_elem (etree._Element): XML element representing the Sample.
            ns (Dict[str, str]): Namespace dictionary for parsing.
            series_id (str): The SeriesID to inherit for this sample.

        Returns:
            Optional[Dict]: A dictionary of validated Sample metadata, or None if invalid.
        """
        try:
            # Extract fields using the defined template for Samples
            data = self._extract_fields(sample_elem, self.template['Sample'], ns)

            # Inherit SeriesID from the parent Series
            data['SeriesID'] = series_id

            # Validate the extracted data
            if not self._validate_sample_data(data):
                self.logger.error(f"Validation failed for Sample data: {data}")
                return None

            # Log the extracted and validated data for debugging purposes
            self.logger.debug(f"Extracted and validated Sample data: {data}")
            return data
        except Exception as e:
            # Log detailed error information
            self.logger.error(f"Error processing Sample element. Element: {sample_elem.tag}, Error: {e}")
            return None

#-----------------------------Database Operations-----------------------------------
    def _pre_insert_series_id(self, session, series_id: str) -> None:
        """
        Pre-inserts the SeriesID into the dataset_series_metadata table to satisfy foreign key constraints.

        Args:
            session: The database session.
            series_id (str): The inferred SeriesID from the file name.
        """
        try:
            insert_query = insert(GeoSeriesMetadata).values({"SeriesID": series_id}).on_conflict_do_nothing()
            session.execute(insert_query)
            session.commit()
        except SQLAlchemyError as e:
            self.logger.error(f"Error pre-inserting SeriesID {series_id}: {e}")
            raise

    def _update_series_sample_count(self, session, series_id: str, sample_count: int) -> None:
        """
        Updates the SampleCount field for a specific SeriesID in the dataset_series_metadata table.

        Args:
            session: SQLAlchemy session object.
            series_id (str): The SeriesID whose SampleCount needs to be updated.
            sample_count (int): The total number of samples associated with the SeriesID.

        Raises:
            SQLAlchemyError: If an error occurs while updating the database.
        """
        try:
            update_query = (
                insert(GeoSeriesMetadata)
                .values(SeriesID=series_id, SampleCount=sample_count)
                .on_conflict_do_update(
                    index_elements=['SeriesID'],
                    set_={'SampleCount': sample_count}
                )
            )
            session.execute(update_query)
        except SQLAlchemyError as e:
            self.logger.error(f"Error updating SampleCount for SeriesID {series_id}: {e}")
            raise
#------------------------------ Main ETL Process -------------------------------------
    def parse_and_stream(self) -> int:
        """
        Parses a GEO MINiML file, extracts metadata, and streams it to the database.

        Gracefully handles non-XML files by skipping them and ensures the SampleCount
        field is only updated when new samples are added to prevent overwriting valid counts.

        Returns:
            int: The number of samples processed and successfully uploaded to the database.

        Raises:
            RuntimeError: If any critical error occurs during the parsing or streaming process.
        """
        # Validate that the file is an XML file.
        if not self.file_path.endswith(".xml"):
            # Log a warning and skip processing for non-XML files.
            self.logger.warning(f"Skipping non-XML file: {self.file_path}.")
            return 0  # Gracefully skip non-XML files.

        # Extract the base name of the file to infer the SeriesID.
        base_name = os.path.basename(self.file_path)  # Get only the file name.
        inferred_series_id = base_name.split("_")[0]  # Extract SeriesID from the filename.

        # Validate inferred SeriesID using a regex.
        if not inferred_series_id or not re.match(r'^GSE\d+$', inferred_series_id):
            # Log an error and skip files with invalid SeriesID.
            self.logger.error(f"Failed to infer a valid SeriesID from filename: {base_name}")
            return 0

        # Log the inferred SeriesID for debugging purposes.
        self.logger.info(f"Processing GEO Series ID: {inferred_series_id}")

        # Initialize the database engine and session for database interactions.
        engine = get_postgres_engine()  # Get the database engine for PostgreSQL.
        Session = sessionmaker(bind=engine)  # Bind the session factory to the engine.
        session = Session()  # Create a new database session.

        # Define XML namespace and initialize counters for processing.
        ns = {'geo': 'http://www.ncbi.nlm.nih.gov/geo/info/MINiML'}  # Define XML namespace.
        new_sample_count = 0  # Counter for new samples added during this run.

        try:
            # Step 2: Validate the XML structure to ensure the file is well-formed.
            self._validate_xml()  # Check if the XML file is valid.
            self.logger.info("XML structure validated.")  # Log success.

            # Ensure the SeriesID exists in the database or create it if not.
            insert_query = insert(GeoSeriesMetadata).values(
                SeriesID=inferred_series_id
            ).on_conflict_do_nothing()  # Avoid inserting duplicates.
            session.execute(insert_query)  # Execute the insert query.
            session.commit()  # Commit the transaction.
            self.logger.info(f"Ensured SeriesID {inferred_series_id} in database.")  # Log success.

            # Fetch existing SampleIDs to avoid re-uploading duplicates.
            existing_samples = session.query(GeoSampleMetadata.SampleID).filter_by(
                SeriesID=inferred_series_id).all()
            uploaded_samples = set(sample[0] for sample in existing_samples)  # Convert to a set for fast lookups.

            # Efficiently parse the XML file with iterparse to minimize memory usage.
            context = etree.iterparse(
                self.file_path, events=("start", "end"),  # Process start and end events.
                tag=["{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Series",
                     "{http://www.ncbi.nlm.nih.gov/geo/info/MINiML}Sample"]  # Target specific XML tags.
            )

            # Process Series and Sample elements from the XML file.
            for event, elem in context:
                if event == "end" and elem.tag.endswith("Series"):
                    # Extract metadata for the Series element.
                    series_data = self._process_series_data(elem, ns)
                    if not series_data:
                        # If extraction fails, create a fallback with only the SeriesID.
                        series_data = {"SeriesID": inferred_series_id}
                    else:
                        # Ensure the SeriesID matches the inferred value.
                        series_data["SeriesID"] = inferred_series_id

                    # Insert Series metadata into the database.
                    try:
                        # Stream the Series metadata into the database.
                        self._stream_series_to_db(session, series_data)
                    except SQLAlchemyError as e:
                        # Log database errors for Series insertion.
                        self.logger.error(f"Failed to insert Series {inferred_series_id}: {e}")

                    # Clear memory for the processed element.
                    elem.clear()
                    while elem.getparent() is not None:
                        del elem.getparent()[0]

                elif event == "end" and elem.tag.endswith("Sample"):
                    # Extract metadata for the Sample element.
                    sample_data = self._process_sample_data(elem, ns, inferred_series_id)
                    sample_id = sample_data.get("SampleID") if sample_data else None

                    # Avoid re-uploading samples that already exist in the database.
                    if sample_id and sample_id not in uploaded_samples:
                        try:
                            # Insert new Sample metadata into the database.
                            result = session.execute(
                                insert(GeoSampleMetadata).values(sample_data).on_conflict_do_nothing()
                            )
                            if result.rowcount > 0:  # Check if a new row was inserted.
                                uploaded_samples.add(sample_id)  # Add to the uploaded set.
                                new_sample_count += 1  # Increment the new sample counter.
                        except SQLAlchemyError as e:
                            # Log database errors for Sample insertion.
                            self.logger.error(f"Failed to insert Sample {sample_id}: {e}")

                    # Clear memory for the processed element.
                    elem.clear()
                    while elem.getparent() is not None:
                        del elem.getparent()[0]

            # Only update SampleCount if new samples were added.
            if new_sample_count > 0:
                try:
                    # Update the SampleCount field for the SeriesID.
                    self._update_series_sample_count(session, inferred_series_id,
                                                     len(uploaded_samples))
                    self.logger.info(f"Updated SampleCount for SeriesID {inferred_series_id}: {len(uploaded_samples)}")
                except SQLAlchemyError as e:
                    # Log any errors that occur while updating the count.
                    self.logger.error(f"Error updating SampleCount for SeriesID {inferred_series_id}: {e}")

            # Commit all changes made during processing.
            session.commit()

            # Log the processing status and clean up associated files.
            self.file_handler.log_processed(inferred_series_id)
            self.file_handler.clean_files(inferred_series_id)

            # Log the successful completion of the ETL process.
            self.logger.info("Metadata streaming completed successfully.")

        except Exception as e:
            # Roll back changes in case of an error.
            session.rollback()
            self.logger.critical(f"Error during streaming: {e}")
            raise RuntimeError("Streaming failed.") from e
        finally:
            # Ensure the database session is closed to avoid resource leaks.
            session.close()

        # Return the number of new samples processed during this run.
        return new_sample_count

    # ------------------- Helper Functions ----------------------------------------------
    def _validate_sample_data(self, sample_data: Dict) -> bool:
        """
        Validates the Sample metadata dictionary before insertion into the database.

        This method ensures that all required fields for a sample are present
        and logs an error if any required field is missing. It is essential
        for maintaining data integrity and preventing database insertion errors.

        Args:
            sample_data (Dict): A dictionary containing the Sample metadata to validate.

        Returns:
            bool: True if all required fields are present, False otherwise.
        """
        # Define the required fields for a valid Sample record.
        required_fields = ['SampleID', 'SeriesID', 'Title']

        # Iterate over each required field to check if it exists in sample_data.
        for field in required_fields:
            # If the field is missing, log an error and return False.
            if not sample_data.get(field):
                self.logger.error(f"Sample validation failed: Missing field '{field}'")
                return False

        # If all required fields are present, return True.
        return True

    def _stream_series_to_db(self, session, series_data: Dict) -> None:
        """
        Inserts or updates Series metadata into the dataset_series_metadata table.

        This method streams Series metadata into the database, ensuring that
        duplicate entries are handled gracefully by using an upsert operation.

        Args:
            session: SQLAlchemy session object for database operations.
            series_data (Dict): A dictionary containing the Series metadata to insert or update.

        Raises:
            SQLAlchemyError: If a database error occurs during the operation.
        """
        try:
            # Prepare an upsert query to insert or update the Series metadata.
            insert_query = insert(GeoSeriesMetadata).values(series_data).on_conflict_do_update(
                index_elements=['SeriesID'],  # Use SeriesID as the unique key for conflict resolution.
                set_={key: series_data[key] for key in series_data if key != 'SeriesID'}
                # Update all fields except SeriesID.
            )
            # Execute the upsert query within the given database session.
            session.execute(insert_query)
        except SQLAlchemyError as e:
            # Log an error if a database exception occurs and re-raise the exception.
            self.logger.error(f"Database error during Series insertion: {e}")
            raise

    def _stream_sample_to_db(self, session, sample_data: Dict) -> None:
        """
        Inserts Sample metadata into the dataset_sample_metadata table.

        This method streams Sample metadata into the database. If a duplicate
        entry is encountered, the operation will silently skip inserting the record.

        Args:
            session: SQLAlchemy session object for database operations.
            sample_data (Dict): A dictionary containing the Sample metadata to insert.

        Raises:
            SQLAlchemyError: If a database error occurs during the operation.
        """
        try:
            # Prepare an insert query to add the Sample metadata, ignoring duplicates.
            insert_query = insert(GeoSampleMetadata).values(sample_data).on_conflict_do_nothing()
            # Execute the insert query within the given database session.
            session.execute(insert_query)
        except SQLAlchemyError as e:
            # Log an error if a database exception occurs and re-raise the exception.
            self.logger.error(f"Database error during Sample insertion: {e}")
            raise
